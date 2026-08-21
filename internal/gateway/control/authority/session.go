package authority

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

func (m *Manager) OpenSession(ctx context.Context, gatewayID, gatewayInstanceID, relayAddress string) (controlmodel.Session, error) {
	if err := routing.ValidateIdentity("gateway_id", gatewayID); err != nil {
		return controlmodel.Session{}, err
	}
	if err := routing.ValidateIdentity("gateway_instance_id", gatewayInstanceID); err != nil {
		return controlmodel.Session{}, err
	}
	if err := routing.ValidateRelayAddress(relayAddress); err != nil {
		return controlmodel.Session{}, fmt.Errorf("valid relay address is required: %w", err)
	}
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()

	m.mu.RLock()
	if m.current == nil {
		m.mu.RUnlock()
		return controlmodel.Session{}, ErrNoAuthority
	}
	current := *m.current
	m.mu.RUnlock()

	gateway := controlstate.GatewaySessionRef{GatewayID: gatewayID, GatewayInstanceID: gatewayInstanceID}
	command, err := controlstate.EncodeRegisterGateway(controlstate.RegisterGateway{ClusterEpoch: current.ClusterEpoch, Gateway: gateway})
	if err != nil {
		return controlmodel.Session{}, fmt.Errorf("encode gateway registration: %w", err)
	}
	if _, err := m.applyWithParent(ctx, command); err != nil {
		return controlmodel.Session{}, err
	}
	controlSessionID, err := newID()
	if err != nil {
		return controlmodel.Session{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || *m.current != current {
		return controlmodel.Session{}, ErrNoAuthority
	}
	if old := m.sessions[gatewayID]; old != nil {
		old.close()
	}
	entry := &sessionEntry{
		ref: controlmodel.SessionRef{
			ClusterEpoch:      current.ClusterEpoch,
			AuthorityID:       current.AuthorityID,
			ControlSessionID:  controlSessionID,
			GatewayID:         gatewayID,
			GatewayInstanceID: gatewayInstanceID,
		},
		relayAddress: relayAddress,
		state:        SessionSyncing,
		bindings:     make(map[routing.BindingKey]routing.LiveBinding),
		done:         make(chan struct{}),
	}
	m.sessions[gatewayID] = entry
	// Registration establishes C only. Until the full snapshot commits, this
	// stream has no V and must still expire like an absent gateway.
	m.cleanup[gateway] = m.now().Add(m.config.GatewayRevalidationTimeout)
	return controlmodel.Session{Ref: entry.ref, Done: entry.done}, nil
}

// Revalidate atomically replaces C for this gateway and only then marks its
// leader-local V record available for Open admission.
func (m *Manager) Revalidate(ctx context.Context, ref controlmodel.SessionRef, bindings []routing.LiveBinding) error {
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	candidate, encoded, err := m.snapshotCommand(ref, bindings)
	if err != nil {
		return err
	}
	if _, err := m.applyWithParent(ctx, encoded); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return err
	}
	entry.bindings = candidate
	entry.state = SessionRevalidated
	delete(m.cleanup, gatewayRef(ref))
	return nil
}

func (m *Manager) RequireRevalidated(ref controlmodel.SessionRef) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return err
	}
	if entry.state != SessionRevalidated {
		return ErrSnapshotFirst
	}
	return nil
}

// Declare commits the exact route before changing the local V mirror.
func (m *Manager) Declare(ctx context.Context, ref controlmodel.SessionRef, binding routing.LiveBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	m.mu.RLock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		m.mu.RUnlock()
		return false, err
	}
	if entry.state != SessionRevalidated {
		m.mu.RUnlock()
		return false, ErrSnapshotFirst
	}
	m.mu.RUnlock()
	if binding.Ref.GatewayID != ref.GatewayID || binding.Ref.GatewayInstanceID != ref.GatewayInstanceID {
		return false, fmt.Errorf("%w: binding belongs to another gateway session", routing.ErrConflict)
	}
	command, err := controlstate.EncodeDeclareRoute(controlstate.DeclareRoute{
		ClusterEpoch: ref.ClusterEpoch,
		Gateway:      gatewayRef(ref),
		Binding:      bindingToState(binding),
	})
	if err != nil {
		return false, fmt.Errorf("encode route declaration: %w", err)
	}
	result, err := m.applyWithParent(ctx, command)
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err = m.sessionLocked(ref)
	if err != nil {
		return false, err
	}
	if entry.state != SessionRevalidated {
		return false, ErrSnapshotFirst
	}
	entry.bindings[binding.Key] = binding
	return result.Code == controlstate.ResultAlreadyApplied, nil
}

// Withdraw removes only the exact C route for this exact durable gateway
// incarnation. A stale control stream cannot erase a replacement instance.
func (m *Manager) Withdraw(ctx context.Context, ref controlmodel.SessionRef, binding routing.LiveBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	m.mu.RLock()
	entry, err := m.sessionLocked(ref)
	if err != nil || entry.state != SessionRevalidated {
		m.mu.RUnlock()
		return true, nil
	}
	m.mu.RUnlock()
	if binding.Ref.GatewayID != ref.GatewayID || binding.Ref.GatewayInstanceID != ref.GatewayInstanceID {
		return true, nil
	}
	command, err := controlstate.EncodeWithdrawRoute(controlstate.WithdrawRoute{
		ClusterEpoch: ref.ClusterEpoch,
		Gateway:      gatewayRef(ref),
		Binding:      bindingToState(binding),
	})
	if err != nil {
		return false, fmt.Errorf("encode route withdrawal: %w", err)
	}
	result, err := m.applyWithParent(ctx, command)
	if err != nil {
		if errors.Is(err, ErrStaleSession) {
			return true, nil
		}
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err = m.sessionLocked(ref)
	if err != nil || entry.state != SessionRevalidated {
		return true, nil
	}
	delete(entry.bindings, binding.Key)
	return result.Code == controlstate.ResultAlreadyApplied, nil
}

// AdmitOpen owns the complete pre-O admission boundary. The successful
// VerifyLeader barrier in Confirm establishes the committed read point; the
// exact Ref gate in resolveOpen then prevents mixing that decision with V from
// another local authority. A second Raft verification would add no stronger
// guarantee after the operation's linearization point.
func (m *Manager) EndSession(ref controlmodel.SessionRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.sessions[ref.GatewayID]
	if entry == nil || entry.ref != ref || entry.closed {
		return
	}
	entry.close()
	delete(m.sessions, ref.GatewayID)
	if m.current != nil {
		m.cleanup[gatewayRef(ref)] = m.now().Add(m.config.GatewayRevalidationTimeout)
	}
}

func (m *Manager) sessionLocked(ref controlmodel.SessionRef) (*sessionEntry, error) {
	if m.current == nil || ref.ClusterEpoch != m.current.ClusterEpoch || ref.AuthorityID != m.current.AuthorityID {
		return nil, ErrStaleSession
	}
	entry := m.sessions[ref.GatewayID]
	if entry == nil || entry.ref != ref || entry.closed {
		return nil, ErrStaleSession
	}
	return entry, nil
}

func (m *Manager) isCurrentGatewayLocked(ref controlmodel.SessionRef) bool {
	current, ok := m.node.LookupGateway(ref.GatewayID)
	return ok && current == gatewayRef(ref)
}

func (m *Manager) snapshotCommand(ref controlmodel.SessionRef, bindings []routing.LiveBinding) (map[routing.BindingKey]routing.LiveBinding, []byte, error) {
	if len(bindings) > routing.MaxListenerBindingsPerGateway {
		return nil, nil, routing.ErrCapacity
	}
	candidate := make(map[routing.BindingKey]routing.LiveBinding, len(bindings))
	stateBindings := make([]controlstate.Binding, 0, len(bindings))
	for _, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return nil, nil, err
		}
		if binding.Ref.GatewayID != ref.GatewayID || binding.Ref.GatewayInstanceID != ref.GatewayInstanceID {
			return nil, nil, fmt.Errorf("%w: binding belongs to another gateway session", routing.ErrConflict)
		}
		if _, exists := candidate[binding.Key]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate binding key", routing.ErrConflict)
		}
		candidate[binding.Key] = binding
		stateBindings = append(stateBindings, bindingToState(binding))
	}
	m.mu.RLock()
	_, err := m.sessionLocked(ref)
	m.mu.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	command, err := controlstate.EncodeReplaceSnapshot(controlstate.ReplaceSnapshot{
		ClusterEpoch: ref.ClusterEpoch,
		Gateway:      gatewayRef(ref),
		Bindings:     stateBindings,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("encode full snapshot: %w", err)
	}
	return candidate, command, nil
}

func (m *Manager) applyWithParent(parent context.Context, command []byte) (controlstate.ApplyResult, error) {
	ctx, cancel := context.WithTimeout(parent, m.config.ApplyTimeout)
	defer cancel()
	result, err := m.node.Apply(ctx, command)
	if err != nil {
		// A caller cannot know whether an Apply timeout raced an election. Fence
		// V on every proposal transport failure and let the Gateway reconnect to
		// a freshly barrier-confirmed authority; C remains untouched in Raft.
		m.fence()
		return controlstate.ApplyResult{}, fmt.Errorf("%w: apply replicated current state: %w", ErrNoAuthority, err)
	}
	if !result.Applied() {
		switch result.Code {
		case controlstate.ResultConflict:
			return result, fmt.Errorf("%w: %s", routing.ErrConflict, result.Error)
		case controlstate.ResultCapacity:
			return result, fmt.Errorf("%w: %s", routing.ErrCapacity, result.Error)
		case controlstate.ResultRejected:
			return result, fmt.Errorf("%w: %s", ErrStaleSession, result.Error)
		default:
			return result, fmt.Errorf("reject replicated current-state command: %s", result.Error)
		}
	}
	return result, nil
}

func gatewayRef(ref controlmodel.SessionRef) controlstate.GatewaySessionRef {
	return controlstate.GatewaySessionRef{GatewayID: ref.GatewayID, GatewayInstanceID: ref.GatewayInstanceID}
}

func bindingToState(binding routing.LiveBinding) controlstate.Binding {
	return controlstate.Binding{
		Key:               controlstate.BindingKey{ClientID: binding.Key.ClientID, EndpointPattern: binding.Key.EndpointPattern, TargetID: binding.Key.TargetID},
		ListenerBindingID: binding.Ref.ListenerBindingID,
	}
}

func routingKey(key controlstate.BindingKey) routing.BindingKey {
	return routing.BindingKey{ClientID: key.ClientID, EndpointPattern: key.EndpointPattern, TargetID: key.TargetID}
}

func routeToBinding(route controlstate.Route) routing.LiveBinding {
	return routing.LiveBinding{
		Key: routingKey(route.Key),
		Ref: routing.ListenerBindingRef{
			GatewayID:         route.Owner.GatewayID,
			GatewayInstanceID: route.Owner.GatewayInstanceID,
			ListenerBindingID: route.ListenerBindingID,
		},
	}
}

func (s *sessionEntry) close() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
}

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate control identity: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}
