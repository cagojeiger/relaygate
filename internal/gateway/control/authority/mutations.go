package authority

import (
	"context"
	"errors"
	"fmt"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

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
