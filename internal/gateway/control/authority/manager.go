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
	"github.com/cagojeiger/relaygate/internal/raft/node"
)

var (
	ErrNoAuthority   = errors.New("no current authority")
	ErrStaleSession  = errors.New("stale control session")
	ErrSnapshotFirst = errors.New("full snapshot has not been accepted")
)

type Config struct {
	ClusterEpoch   string
	ProbeInterval  time.Duration
	ProbeTimeout   time.Duration
	OpenContextTTL time.Duration
}

// RaftNode provides only election/quorum safety. Current Gateway sessions and
// routes deliberately never enter the Raft state machine.
type RaftNode interface {
	Status() raftnode.Status
	ClusterEpoch() string
	VerifyLeader(context.Context) error
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

// Presence reports only what this authority currently observes. It makes no
// claim about deployment replica completeness or historical convergence.
type Presence struct {
	State       PresenceState `json:"state"`
	Sessions    int           `json:"sessions"`
	Revalidated int           `json:"revalidated"`
	Bindings    int           `json:"bindings"`
}

type sessionEntry struct {
	ref          SessionRef
	relayAddress string
	state        SessionState
	bindings     map[routing.BindingKey]routing.LiveBinding
	done         chan struct{}
	closed       bool
}

type routeEntry struct {
	binding routing.LiveBinding
	owner   SessionRef
}

type Manager struct {
	config Config
	node   RaftNode

	mu          sync.RWMutex
	current     *Ref
	currentTerm uint64
	sessions    map[string]*sessionEntry // current session by stable GatewayID
	routes      map[routing.BindingKey]routeEntry
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
	if config.ProbeInterval <= 0 || config.ProbeTimeout <= 0 || config.OpenContextTTL <= 0 {
		return nil, fmt.Errorf("authority probe and Open context timeouts must be positive")
	}
	if node == nil {
		return nil, fmt.Errorf("raft node is required")
	}
	return &Manager{
		config:   config,
		node:     node,
		sessions: make(map[string]*sessionEntry),
		routes:   make(map[routing.BindingKey]routeEntry),
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

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current != nil && m.currentTerm != confirmed.Term {
		m.fenceLocked()
	}
	if m.current == nil {
		authorityID, err := newID()
		if err != nil {
			return Ref{}, err
		}
		m.current = &Ref{ClusterEpoch: m.config.ClusterEpoch, AuthorityID: authorityID}
		m.currentTerm = confirmed.Term
	}
	return *m.current, nil
}

func (m *Manager) Observe(ctx context.Context) (Ref, Presence, error) {
	ref, err := m.Confirm(ctx)
	if err != nil {
		return Ref{}, Presence{State: PresenceNoAuthority}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil || *m.current != ref {
		return Ref{}, Presence{State: PresenceNoAuthority}, ErrNoAuthority
	}
	return ref, m.presenceLocked(), nil
}

// OpenSession replaces any older current session for gatewayID. Replacing or
// ending a session bulk-deletes only entries owned by that exact session.
func (m *Manager) OpenSession(gatewayID, gatewayInstanceID, relayAddress string) (Session, error) {
	if err := routing.ValidateIdentity("gateway_id", gatewayID); err != nil {
		return Session{}, err
	}
	if err := routing.ValidateIdentity("gateway_instance_id", gatewayInstanceID); err != nil {
		return Session{}, err
	}
	if err := ValidateRelayAddress(relayAddress); err != nil {
		return Session{}, fmt.Errorf("valid relay address is required: %w", err)
	}
	controlSessionID, err := newID()
	if err != nil {
		return Session{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return Session{}, ErrNoAuthority
	}
	if old := m.sessions[gatewayID]; old != nil {
		m.endSessionLocked(old.ref)
	}
	entry := &sessionEntry{
		ref: SessionRef{
			ClusterEpoch:      m.current.ClusterEpoch,
			AuthorityID:       m.current.AuthorityID,
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
	return Session{Ref: entry.ref, Done: entry.done}, nil
}

// Revalidate atomically replaces this session's declared set. Any malformed or
// conflicting snapshot leaves the previous directory unchanged.
func (m *Manager) Revalidate(ref SessionRef, bindings []routing.LiveBinding) error {
	if len(bindings) > routing.MaxListenerBindingsPerGateway {
		return routing.ErrCapacity
	}
	candidate := make(map[routing.BindingKey]routing.LiveBinding, len(bindings))
	for _, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return err
		}
		if binding.Ref.GatewayID != ref.GatewayID || binding.Ref.GatewayInstanceID != ref.GatewayInstanceID {
			return fmt.Errorf("%w: binding belongs to another gateway session", routing.ErrConflict)
		}
		if _, duplicate := candidate[binding.Key]; duplicate {
			return fmt.Errorf("%w: duplicate binding key", routing.ErrConflict)
		}
		candidate[binding.Key] = binding
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return err
	}
	for key, binding := range candidate {
		if route, exists := m.routes[key]; exists && route.owner != ref {
			return fmt.Errorf("%w: binding key is owned by another current session", routing.ErrConflict)
		} else if exists && route.binding != binding {
			return fmt.Errorf("%w: binding key has a different current ref", routing.ErrConflict)
		}
	}
	m.removeRoutesForSessionLocked(ref)
	for key, binding := range candidate {
		m.routes[key] = routeEntry{binding: binding, owner: ref}
	}
	entry.bindings = candidate
	entry.state = SessionRevalidated
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

// Declare inserts one exact current-session route. The returned bool reports
// whether this was an exact already-applied duplicate. Any other owner or ref
// for the key fails closed.
func (m *Manager) Declare(ref SessionRef, binding routing.LiveBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return false, err
	}
	if entry.state != SessionRevalidated {
		return false, ErrSnapshotFirst
	}
	if binding.Ref.GatewayID != ref.GatewayID || binding.Ref.GatewayInstanceID != ref.GatewayInstanceID {
		return false, fmt.Errorf("%w: binding belongs to another gateway session", routing.ErrConflict)
	}
	if len(entry.bindings) >= routing.MaxListenerBindingsPerGateway {
		if _, exists := entry.bindings[binding.Key]; !exists {
			return false, routing.ErrCapacity
		}
	}
	if route, exists := m.routes[binding.Key]; exists {
		if route.owner == ref && route.binding == binding {
			return true, nil
		}
		return false, routing.ErrConflict
	}
	entry.bindings[binding.Key] = binding
	m.routes[binding.Key] = routeEntry{binding: binding, owner: ref}
	return false, nil
}

// Withdraw removes only the exact route currently owned by ref. Stale and
// duplicate cleanup never affects a newer declaration. The returned bool
// reports an exact already-applied absent/stale cleanup.
func (m *Manager) Withdraw(ref SessionRef, binding routing.LiveBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil || entry.state != SessionRevalidated {
		return true, nil
	}
	route, exists := m.routes[binding.Key]
	if !exists || route.owner != ref || route.binding != binding {
		return true, nil
	}
	delete(m.routes, binding.Key)
	delete(entry.bindings, binding.Key)
	return false, nil
}

// ResolveOpen returns a current-directory exact route. The caller must have
// quorum-confirmed immediately beforehand in the service control lane.
func (m *Manager) ResolveOpen(ingress SessionRef, auth AuthContext, endpoint, targetID string) (OpenContext, error) {
	key, err := ExactBindingKey(auth, endpoint, targetID)
	if err != nil {
		return OpenContext{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	ingressEntry, err := m.sessionLocked(ingress)
	if err != nil || ingressEntry.state != SessionRevalidated {
		return OpenContext{}, fmt.Errorf("%w: ingress control session", ErrOpenUnavailable)
	}
	if m.current == nil {
		return OpenContext{}, fmt.Errorf("%w: %w", ErrOpenUnavailable, ErrNoAuthority)
	}
	route, ok := m.routes[key]
	if !ok {
		return OpenContext{}, ErrRouteNotFound
	}
	owner := m.sessions[route.owner.GatewayID]
	if owner == nil || owner.closed || owner.ref != route.owner || owner.state != SessionRevalidated || owner.bindings[key] != route.binding {
		return OpenContext{}, ErrRouteNotFound
	}
	attemptID, err := newID()
	if err != nil {
		return OpenContext{}, fmt.Errorf("%w: %w", ErrOpenUnavailable, err)
	}
	return NewForwardedOpenContext(
		m.current.ClusterEpoch, m.current.AuthorityID, attemptID, auth, route.binding,
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

func (m *Manager) EndSession(ref SessionRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.endSessionLocked(ref)
}

func (m *Manager) endSessionLocked(ref SessionRef) {
	entry := m.sessions[ref.GatewayID]
	if entry == nil || entry.ref != ref || entry.closed {
		return
	}
	m.removeRoutesForSessionLocked(ref)
	entry.close()
	delete(m.sessions, ref.GatewayID)
}

func (m *Manager) Presence() Presence {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.presenceLocked()
}

func (m *Manager) presenceLocked() Presence {
	if m.current == nil {
		return Presence{State: PresenceNoAuthority}
	}
	presence := Presence{State: PresenceCurrent, Sessions: len(m.sessions), Bindings: len(m.routes)}
	for _, entry := range m.sessions {
		if entry.state == SessionRevalidated && !entry.closed {
			presence.Revalidated++
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
	_, _ = m.confirm(ctx, true)
	cancel()
}

func (m *Manager) fence() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenceLocked()
}

func (m *Manager) fenceLocked() {
	if m.current == nil && len(m.sessions) == 0 && len(m.routes) == 0 {
		return
	}
	for _, session := range m.sessions {
		session.close()
	}
	clear(m.sessions)
	clear(m.routes)
	m.current = nil
	m.currentTerm = 0
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

func (m *Manager) removeRoutesForSessionLocked(ref SessionRef) {
	for key, route := range m.routes {
		if route.owner == ref {
			delete(m.routes, key)
		}
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
