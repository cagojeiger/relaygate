package authority

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

var (
	ErrNoAuthority   = errors.New("no current authority")
	ErrStaleSession  = errors.New("stale control session")
	ErrSnapshotFirst = errors.New("full snapshot has not been accepted")
)

const DefaultGatewayRevalidationTimeout = 15 * time.Second

type Config struct {
	ClusterEpoch               string
	ProbeInterval              time.Duration
	ProbeTimeout               time.Duration
	ApplyTimeout               time.Duration
	GatewayRevalidationTimeout time.Duration
	OpenContextTTL             time.Duration
}

// RaftNode owns the durable, replicated current directory. The authority owns
// only leader-local control streams, advertised addresses, and the fact that a
// gateway has revalidated those durable records for this authority term.
type RaftNode interface {
	Status() raftnode.Status
	ClusterEpoch() string
	VerifyLeader(context.Context) error
	Apply(context.Context, []byte) (controlstate.ApplyResult, error)
	State() controlstate.State
	LookupGateway(string) (controlstate.GatewaySessionRef, bool)
	LookupRoute(controlstate.BindingKey) (controlstate.Route, bool)
}

type Ref struct {
	ClusterEpoch string
	AuthorityID  string
}

type SessionRef struct {
	ClusterEpoch      string
	AuthorityID       string
	ControlSessionID  string
	GatewayID         string
	GatewayInstanceID string
}

type Session struct {
	Ref  SessionRef
	Done <-chan struct{}
}

type SessionState string

const (
	SessionSyncing     SessionState = "Syncing"
	SessionRevalidated SessionState = "Revalidated"
)

type PresenceState string

const (
	PresenceNoAuthority PresenceState = "NoAuthority"
	PresenceCurrent     PresenceState = "Current"
)

// Presence separates replicated current records (C) from this authority's
// freshly verified control streams (V). A route is eligible only when both
// conditions hold.
type Presence struct {
	State               PresenceState `json:"state"`
	CommittedGateways   int           `json:"committed_gateways"`
	CommittedRoutes     int           `json:"committed_routes"`
	RevalidatedGateways int           `json:"revalidated_gateways"`
	EligibleRoutes      int           `json:"eligible_routes"`
}

type sessionEntry struct {
	ref          SessionRef
	relayAddress string
	state        SessionState
	bindings     map[routing.BindingKey]routing.LiveBinding
	done         chan struct{}
	closed       bool
}

type Manager struct {
	config Config
	node   RaftNode

	// Raft applies are already ordered. This mutex gives the leader-local
	// session mirror the same order without serializing read-only Open
	// admission behind control-plane writes.
	mutationMu  sync.Mutex
	mu          sync.RWMutex
	current     *Ref
	currentTerm uint64
	sessions    map[string]*sessionEntry // leader-local current stream by GatewayID
	cleanup     map[controlstate.GatewaySessionRef]time.Time
	now         func() time.Time
	cancel      context.CancelFunc
	done        chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	doneOnce    sync.Once
	closed      bool
}

func New(config Config, node RaftNode) (*Manager, error) {
	if err := routing.ValidateIdentity("cluster_epoch", config.ClusterEpoch); err != nil {
		return nil, err
	}
	if config.GatewayRevalidationTimeout == 0 {
		config.GatewayRevalidationTimeout = DefaultGatewayRevalidationTimeout
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = config.ProbeTimeout
	}
	if config.ProbeInterval <= 0 || config.ProbeTimeout <= 0 || config.ApplyTimeout <= 0 || config.GatewayRevalidationTimeout <= 0 || config.OpenContextTTL <= 0 {
		return nil, fmt.Errorf("authority probe, revalidation, and Open context timeouts must be positive")
	}
	if node == nil {
		return nil, fmt.Errorf("raft node is required")
	}
	return &Manager{
		config:   config,
		node:     node,
		sessions: make(map[string]*sessionEntry),
		cleanup:  make(map[controlstate.GatewaySessionRef]time.Time),
		now:      time.Now,
		done:     make(chan struct{}),
	}, nil
}

func (m *Manager) Start(parent context.Context) {
	m.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			cancel()
			return
		}
		m.cancel = cancel
		m.mu.Unlock()
		go m.run(ctx)
	})
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		cancel := m.cancel
		m.mu.Unlock()
		if cancel != nil {
			cancel()
			<-m.done
			return
		}
		m.fence()
		m.finish()
	})
}

func (m *Manager) Current() (Ref, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil {
		return Ref{}, false
	}
	return *m.current, true
}

func (m *Manager) Confirm(ctx context.Context) (Ref, error) { return m.confirm(ctx, false) }

func (m *Manager) confirm(ctx context.Context, fenceOnContextError bool) (Ref, error) {
	status := m.node.Status()
	if status.Role != "Leader" || m.node.ClusterEpoch() != m.config.ClusterEpoch {
		m.fence()
		return Ref{}, ErrNoAuthority
	}
	if err := m.node.VerifyLeader(ctx); err != nil {
		callerEnded := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		shouldFence := !callerEnded || fenceOnContextError
		if callerEnded && !fenceOnContextError {
			shouldFence = m.node.Status().Role != "Leader" || m.node.ClusterEpoch() != m.config.ClusterEpoch
		}
		if shouldFence {
			m.fence()
		}
		return Ref{}, fmt.Errorf("%w: %w", ErrNoAuthority, err)
	}
	confirmed := m.node.Status()
	if confirmed.Role != "Leader" || m.node.ClusterEpoch() != m.config.ClusterEpoch {
		m.fence()
		return Ref{}, ErrNoAuthority
	}

	// The leader barrier above makes this a committed C snapshot. Do this
	// before taking the authority lock so a slow copy never blocks stream
	// fencing or status reads.
	committed := m.node.State()

	m.mu.Lock()
	defer m.mu.Unlock()
	if confirmed.Term < m.currentTerm {
		return Ref{}, ErrNoAuthority
	}
	if m.current != nil {
		if confirmed.Term != m.currentTerm {
			m.fenceLocked()
		}
	}
	if m.current == nil {
		authorityID, err := newID()
		if err != nil {
			return Ref{}, err
		}
		m.current = &Ref{ClusterEpoch: m.config.ClusterEpoch, AuthorityID: authorityID}
		m.currentTerm = confirmed.Term
		deadline := m.now().Add(m.config.GatewayRevalidationTimeout)
		for _, gateway := range committed.Gateways {
			m.cleanup[gateway] = deadline
		}
	}
	return *m.current, nil
}

func (m *Manager) Observe(ctx context.Context) (Ref, Presence, error) {
	ref, err := m.Confirm(ctx)
	if err != nil {
		return Ref{}, m.Presence(), err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil || *m.current != ref {
		return Ref{}, m.presenceLocked(m.node.State()), ErrNoAuthority
	}
	return ref, m.presenceLocked(m.node.State()), nil
}

// OpenSession first commits the gateway process incarnation (C), then creates
// a new leader-local control stream (V=false). A same-instance reconnect keeps
// its committed routes until its replacement snapshot arrives; a new instance
// is an atomic FSM replacement that deletes the old instance's routes.
func (m *Manager) OpenSession(ctx context.Context, gatewayID, gatewayInstanceID, relayAddress string) (Session, error) {
	if err := routing.ValidateIdentity("gateway_id", gatewayID); err != nil {
		return Session{}, err
	}
	if err := routing.ValidateIdentity("gateway_instance_id", gatewayInstanceID); err != nil {
		return Session{}, err
	}
	if err := ValidateRelayAddress(relayAddress); err != nil {
		return Session{}, fmt.Errorf("valid relay address is required: %w", err)
	}
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()

	m.mu.RLock()
	if m.current == nil {
		m.mu.RUnlock()
		return Session{}, ErrNoAuthority
	}
	current := *m.current
	m.mu.RUnlock()

	gateway := controlstate.GatewaySessionRef{GatewayID: gatewayID, GatewayInstanceID: gatewayInstanceID}
	command, err := controlstate.EncodeRegisterGateway(controlstate.RegisterGateway{ClusterEpoch: current.ClusterEpoch, Gateway: gateway})
	if err != nil {
		return Session{}, fmt.Errorf("encode gateway registration: %w", err)
	}
	if _, err := m.applyWithParent(ctx, command); err != nil {
		return Session{}, err
	}
	controlSessionID, err := newID()
	if err != nil {
		return Session{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil || *m.current != current {
		return Session{}, ErrNoAuthority
	}
	if old := m.sessions[gatewayID]; old != nil {
		old.close()
	}
	entry := &sessionEntry{
		ref: SessionRef{
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
	return Session{Ref: entry.ref, Done: entry.done}, nil
}

// Revalidate atomically replaces C for this gateway and only then marks its
// leader-local V record available for Open admission.
func (m *Manager) Revalidate(ctx context.Context, ref SessionRef, bindings []routing.LiveBinding) error {
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

func (m *Manager) RequireRevalidated(ref SessionRef) error {
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
func (m *Manager) Declare(ctx context.Context, ref SessionRef, binding routing.LiveBinding) (bool, error) {
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
func (m *Manager) Withdraw(ctx context.Context, ref SessionRef, binding routing.LiveBinding) (bool, error) {
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

// ResolveOpen requires an exact committed route plus exact, revalidated local
// ingress and owner sessions. The route itself carries no leader-local address
// or control-session identifier.
func (m *Manager) ResolveOpen(ingress SessionRef, auth AuthContext, endpoint, targetID string) (OpenContext, error) {
	key, err := ExactBindingKey(auth, endpoint, targetID)
	if err != nil {
		return OpenContext{}, err
	}
	stateKey := controlstate.BindingKey{ClientID: key.ClientID, EndpointPattern: key.EndpointPattern, TargetID: key.TargetID}
	route, ok := m.node.LookupRoute(stateKey)
	if !ok {
		return OpenContext{}, ErrRouteNotFound
	}
	if currentGateway, ok := m.node.LookupGateway(route.Owner.GatewayID); !ok || currentGateway != route.Owner {
		return OpenContext{}, ErrRouteNotFound
	}
	binding := routeToBinding(route)

	m.mu.RLock()
	defer m.mu.RUnlock()
	ingressEntry, err := m.sessionLocked(ingress)
	if err != nil || ingressEntry.state != SessionRevalidated || !m.isCurrentGatewayLocked(ingress) {
		return OpenContext{}, fmt.Errorf("%w: ingress control session", ErrOpenUnavailable)
	}
	if m.current == nil {
		return OpenContext{}, fmt.Errorf("%w: %w", ErrOpenUnavailable, ErrNoAuthority)
	}
	owner := m.sessions[route.Owner.GatewayID]
	if owner == nil || owner.closed || owner.state != SessionRevalidated || gatewayRef(owner.ref) != route.Owner || owner.bindings[key] != binding {
		return OpenContext{}, ErrRouteNotFound
	}
	attemptID, err := newID()
	if err != nil {
		return OpenContext{}, fmt.Errorf("%w: %w", ErrOpenUnavailable, err)
	}
	return NewForwardedOpenContext(
		m.current.ClusterEpoch, m.current.AuthorityID, attemptID, auth, binding,
		ForwardingContext{
			IngressGatewayID:         ingressEntry.ref.GatewayID,
			IngressGatewayInstanceID: ingressEntry.ref.GatewayInstanceID,
			IngressControlSessionID:  ingressEntry.ref.ControlSessionID,
			OwnerControlSessionID:    owner.ref.ControlSessionID,
			OwnerRelayAddress:        owner.relayAddress,
			ExpiresAt:                m.now().Add(m.config.OpenContextTTL),
		},
	)
}

// EndSession only drops V. The committed C record is retained through the
// revalidation grace period so an ordinary control reconnect does not erase a
// healthy gateway's current directory. The sweeper later performs an exact,
// conditional RemoveGateway if it has not returned.
func (m *Manager) EndSession(ref SessionRef) {
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

func (m *Manager) Presence() Presence {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.presenceLocked(m.node.State())
}

func (m *Manager) presenceLocked(committed controlstate.State) Presence {
	presence := Presence{
		CommittedGateways: len(committed.Gateways),
		CommittedRoutes:   len(committed.Routes),
	}
	if m.current == nil {
		presence.State = PresenceNoAuthority
		return presence
	}
	presence.State = PresenceCurrent
	for _, entry := range m.sessions {
		if entry.closed || entry.state != SessionRevalidated || !m.isCurrentGatewayLocked(entry.ref) {
			continue
		}
		presence.RevalidatedGateways++
	}
	for _, route := range committed.Routes {
		entry := m.sessions[route.Owner.GatewayID]
		if entry == nil || entry.closed || entry.state != SessionRevalidated || gatewayRef(entry.ref) != route.Owner {
			continue
		}
		if entry.bindings[routingKey(route.Key)] == routeToBinding(route) {
			presence.EligibleRoutes++
		}
	}
	return presence
}

func (m *Manager) run(ctx context.Context) {
	defer m.finish()
	ticker := time.NewTicker(m.config.ProbeInterval)
	defer ticker.Stop()
	for {
		m.probe(ctx)
		select {
		case <-ctx.Done():
			m.fence()
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) finish() { m.doneOnce.Do(func() { close(m.done) }) }

func (m *Manager) probe(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, m.config.ProbeTimeout)
	_, err := m.confirm(ctx, true)
	cancel()
	if err == nil {
		m.sweep(parent)
	}
}

func (m *Manager) sweep(parent context.Context) {
	now := m.now()
	m.mu.Lock()
	due := make([]controlstate.GatewaySessionRef, 0)
	for gateway, deadline := range m.cleanup {
		if entry := m.sessions[gateway.GatewayID]; entry != nil && !entry.closed && entry.state == SessionRevalidated && gatewayRef(entry.ref) == gateway {
			delete(m.cleanup, gateway)
			continue
		}
		if !deadline.After(now) {
			due = append(due, gateway)
		}
	}
	m.mu.Unlock()

	for _, gateway := range due {
		m.mutationMu.Lock()
		if !m.cleanupStillDue(gateway, now) {
			m.mutationMu.Unlock()
			continue
		}
		current, exists := m.node.LookupGateway(gateway.GatewayID)
		if !exists || current != gateway {
			m.clearCleanup(gateway)
			m.mutationMu.Unlock()
			continue
		}
		command, err := controlstate.EncodeRemoveGateway(controlstate.RemoveGateway{ClusterEpoch: m.config.ClusterEpoch, Gateway: gateway})
		if err != nil {
			m.clearCleanup(gateway)
			m.mutationMu.Unlock()
			continue
		}
		if _, err := m.applyWithParent(parent, command); err != nil {
			m.mutationMu.Unlock()
			continue // retain deadline for a later confirmed leader probe.
		}
		m.finishCleanup(gateway)
		m.mutationMu.Unlock()
	}
}

func (m *Manager) cleanupStillDue(gateway controlstate.GatewaySessionRef, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline, exists := m.cleanup[gateway]
	if !exists || deadline.After(now) {
		return false
	}
	if entry := m.sessions[gateway.GatewayID]; entry != nil && !entry.closed && entry.state == SessionRevalidated && gatewayRef(entry.ref) == gateway {
		delete(m.cleanup, gateway)
		return false
	}
	return true
}

func (m *Manager) clearCleanup(gateway controlstate.GatewaySessionRef) {
	m.mu.Lock()
	delete(m.cleanup, gateway)
	m.mu.Unlock()
}

func (m *Manager) finishCleanup(gateway controlstate.GatewaySessionRef) {
	m.mu.Lock()
	delete(m.cleanup, gateway)
	if entry := m.sessions[gateway.GatewayID]; entry != nil && gatewayRef(entry.ref) == gateway {
		entry.close()
		delete(m.sessions, gateway.GatewayID)
	}
	m.mu.Unlock()
}

func (m *Manager) fence() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenceLocked()
}

// fenceLocked intentionally never changes the Raft FSM. Losing leadership
// invalidates V and all one-term control/session addresses, while committed C
// survives for the next leader's revalidation grace period.
func (m *Manager) fenceLocked() {
	if m.current == nil && len(m.sessions) == 0 && len(m.cleanup) == 0 {
		return
	}
	for _, session := range m.sessions {
		session.close()
	}
	clear(m.sessions)
	clear(m.cleanup)
	m.current = nil
}

func (m *Manager) sessionLocked(ref SessionRef) (*sessionEntry, error) {
	if m.current == nil || ref.ClusterEpoch != m.current.ClusterEpoch || ref.AuthorityID != m.current.AuthorityID {
		return nil, ErrStaleSession
	}
	entry := m.sessions[ref.GatewayID]
	if entry == nil || entry.ref != ref || entry.closed {
		return nil, ErrStaleSession
	}
	return entry, nil
}

func (m *Manager) isCurrentGatewayLocked(ref SessionRef) bool {
	current, ok := m.node.LookupGateway(ref.GatewayID)
	return ok && current == gatewayRef(ref)
}

func (m *Manager) snapshotCommand(ref SessionRef, bindings []routing.LiveBinding) (map[routing.BindingKey]routing.LiveBinding, []byte, error) {
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

func gatewayRef(ref SessionRef) controlstate.GatewaySessionRef {
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
