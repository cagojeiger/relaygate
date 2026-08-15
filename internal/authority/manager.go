package authority

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/controlstate"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

var (
	ErrNoAuthority   = errors.New("no current authority")
	ErrStaleSession  = errors.New("stale control session")
	ErrSnapshotFirst = errors.New("full snapshot has not been accepted")
)

type Config struct {
	ClusterEpoch        string
	ProbeInterval       time.Duration
	ProbeTimeout        time.Duration
	RevalidationTimeout time.Duration
}

type RaftNode interface {
	Status() raftnode.Status
	State() controlstate.State
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
	GatewayGeneration uint64
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
	PresenceRebuilding  PresenceState = "Rebuilding"
	PresenceComplete    PresenceState = "Complete"
)

type Presence struct {
	State       PresenceState `json:"state"`
	Committed   int           `json:"committed"`
	Classified  int           `json:"classified"`
	Revalidated int           `json:"revalidated"`
}

type classificationDeadline struct {
	GatewayGeneration uint64
	GatewayInstanceID string
	At                time.Time
}

type sessionEntry struct {
	ref      SessionRef
	state    SessionState
	bindings map[controlstate.BindingKey]controlstate.BindingSlot
	done     chan struct{}
	closed   bool
}

type Manager struct {
	config Config
	node   RaftNode

	mu          sync.RWMutex
	current     *Ref
	currentTerm uint64
	sessions    map[string]*sessionEntry
	deadlines   map[string]classificationDeadline
	now         func() time.Time
	cancel      context.CancelFunc
	done        chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	doneOnce    sync.Once
	closed      bool
}

func New(config Config, node RaftNode) (*Manager, error) {
	if config.ClusterEpoch == "" {
		return nil, fmt.Errorf("cluster epoch is required")
	}
	if config.ProbeInterval <= 0 || config.ProbeTimeout <= 0 || config.RevalidationTimeout <= 0 {
		return nil, fmt.Errorf("authority probe and revalidation timeouts must be positive")
	}
	if node == nil {
		return nil, fmt.Errorf("raft node is required")
	}
	return &Manager{
		config:    config,
		node:      node,
		sessions:  make(map[string]*sessionEntry),
		deadlines: make(map[string]classificationDeadline),
		now:       time.Now,
		done:      make(chan struct{}),
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

func (m *Manager) Confirm(ctx context.Context) (Ref, error) {
	status := m.node.Status()
	if status.Role != "Leader" || status.ClusterEpoch != m.config.ClusterEpoch {
		m.fence()
		return Ref{}, ErrNoAuthority
	}
	if err := m.node.VerifyLeader(ctx); err != nil {
		m.fence()
		return Ref{}, fmt.Errorf("%w: %w", ErrNoAuthority, err)
	}
	confirmed := m.node.Status()
	if confirmed.Role != "Leader" || confirmed.ClusterEpoch != m.config.ClusterEpoch {
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
		m.initializeDeadlinesLocked()
	}
	return *m.current, nil
}

func (m *Manager) OpenSession(slot controlstate.GatewaySlot) (Session, error) {
	if slot.GatewayID == "" || slot.Generation == 0 || slot.Ref == nil || slot.Ref.GatewayInstanceID == "" {
		return Session{}, fmt.Errorf("exact live gateway slot is required")
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
	if !m.gatewaySlotCurrentLocked(slot.GatewayID, slot.Generation, slot.Ref.GatewayInstanceID) {
		return Session{}, ErrStaleSession
	}
	if old := m.sessions[slot.GatewayID]; old != nil {
		old.close()
	}
	entry := &sessionEntry{
		ref: SessionRef{
			ClusterEpoch:      m.current.ClusterEpoch,
			AuthorityID:       m.current.AuthorityID,
			ControlSessionID:  controlSessionID,
			GatewayID:         slot.GatewayID,
			GatewayInstanceID: slot.Ref.GatewayInstanceID,
			GatewayGeneration: slot.Generation,
		},
		state:    SessionSyncing,
		bindings: make(map[controlstate.BindingKey]controlstate.BindingSlot),
		done:     make(chan struct{}),
	}
	m.sessions[slot.GatewayID] = entry
	m.deadlines[slot.GatewayID] = classificationDeadline{
		GatewayGeneration: slot.Generation,
		GatewayInstanceID: slot.Ref.GatewayInstanceID,
		At:                m.now().Add(m.config.RevalidationTimeout),
	}
	return Session{Ref: entry.ref, Done: entry.done}, nil
}

func (m *Manager) Revalidate(ref SessionRef, bindings []controlstate.BindingSlot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return err
	}
	if entry.state == SessionSyncing && m.revalidationExpiredLocked(ref) {
		entry.close()
		delete(m.sessions, ref.GatewayID)
		return ErrStaleSession
	}
	if entry.state == SessionRevalidated {
		return nil
	}
	validated := make(map[controlstate.BindingKey]controlstate.BindingSlot, len(bindings))
	for _, binding := range bindings {
		validated[binding.Key] = binding
	}
	entry.bindings = validated
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

func (m *Manager) UpdateBinding(ref SessionRef, slot controlstate.BindingSlot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return err
	}
	if entry.state != SessionRevalidated {
		return ErrSnapshotFirst
	}
	if slot.IsTombstone() {
		delete(entry.bindings, slot.Key)
	} else {
		entry.bindings[slot.Key] = slot
	}
	return nil
}

func (m *Manager) EndSession(ref SessionRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, err := m.sessionLocked(ref)
	if err != nil {
		return
	}
	entry.close()
	delete(m.sessions, ref.GatewayID)
	m.deadlines[ref.GatewayID] = classificationDeadline{
		GatewayGeneration: ref.GatewayGeneration,
		GatewayInstanceID: ref.GatewayInstanceID,
		At:                m.now(),
	}
}

func (m *Manager) Presence() Presence {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return Presence{State: PresenceNoAuthority}
	}
	state := m.node.State()
	presence := Presence{State: PresenceRebuilding}
	now := m.now()
	for _, slot := range state.Gateways {
		if slot.IsTombstone() {
			continue
		}
		presence.Committed++
		entry := m.sessions[slot.GatewayID]
		if entry != nil && entry.state == SessionSyncing && m.revalidationExpiredLocked(entry.ref) {
			entry.close()
			delete(m.sessions, slot.GatewayID)
			entry = nil
		}
		if entry != nil && entry.state == SessionRevalidated &&
			entry.ref.GatewayGeneration == slot.Generation &&
			entry.ref.GatewayInstanceID == slot.Ref.GatewayInstanceID &&
			entry.ref.AuthorityID == m.current.AuthorityID {
			presence.Classified++
			presence.Revalidated++
			continue
		}
		deadline, ok := m.deadlines[slot.GatewayID]
		if !ok || deadline.GatewayGeneration != slot.Generation || deadline.GatewayInstanceID != slot.Ref.GatewayInstanceID {
			m.deadlines[slot.GatewayID] = classificationDeadline{
				GatewayGeneration: slot.Generation,
				GatewayInstanceID: slot.Ref.GatewayInstanceID,
				At:                now.Add(m.config.RevalidationTimeout),
			}
			continue
		}
		if !now.Before(deadline.At) {
			presence.Classified++
		}
	}
	if presence.Classified == presence.Committed {
		presence.State = PresenceComplete
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

func (m *Manager) finish() {
	m.doneOnce.Do(func() {
		close(m.done)
	})
}

func (m *Manager) probe(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, m.config.ProbeTimeout)
	if _, err := m.Confirm(ctx); err == nil {
		m.expireSyncingSessions()
	}
	cancel()
}

func (m *Manager) fence() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenceLocked()
}

func (m *Manager) fenceLocked() {
	if m.current == nil && len(m.sessions) == 0 && len(m.deadlines) == 0 {
		return
	}
	for _, session := range m.sessions {
		session.close()
	}
	clear(m.sessions)
	clear(m.deadlines)
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
	if !m.gatewaySlotCurrentLocked(ref.GatewayID, ref.GatewayGeneration, ref.GatewayInstanceID) {
		return nil, ErrStaleSession
	}
	return entry, nil
}

func (m *Manager) initializeDeadlinesLocked() {
	clear(m.deadlines)
	deadline := m.now().Add(m.config.RevalidationTimeout)
	for _, slot := range m.node.State().Gateways {
		if slot.Ref == nil {
			continue
		}
		m.deadlines[slot.GatewayID] = classificationDeadline{
			GatewayGeneration: slot.Generation,
			GatewayInstanceID: slot.Ref.GatewayInstanceID,
			At:                deadline,
		}
	}
}

func (m *Manager) gatewaySlotCurrentLocked(gatewayID string, generation uint64, gatewayInstanceID string) bool {
	for _, slot := range m.node.State().Gateways {
		if slot.GatewayID != gatewayID {
			continue
		}
		return slot.Generation == generation && slot.Ref != nil && slot.Ref.GatewayInstanceID == gatewayInstanceID
	}
	return false
}

func (m *Manager) revalidationExpiredLocked(ref SessionRef) bool {
	deadline, ok := m.deadlines[ref.GatewayID]
	return ok &&
		deadline.GatewayGeneration == ref.GatewayGeneration &&
		deadline.GatewayInstanceID == ref.GatewayInstanceID &&
		!m.now().Before(deadline.At)
}

func (m *Manager) expireSyncingSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for gatewayID, entry := range m.sessions {
		if entry.state != SessionSyncing || !m.revalidationExpiredLocked(entry.ref) {
			continue
		}
		entry.close()
		delete(m.sessions, gatewayID)
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
