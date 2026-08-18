package clientsession

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
)

var (
	ErrAuthenticationFailed = errors.New("client authentication failed")
	ErrCredentialRevoked    = errors.New("client credential is no longer active")
	ErrStaleSession         = errors.New("stale client session")
	ErrCapacity             = errors.New("client session capacity reached")
	ErrClosed               = errors.New("client session manager is closed")
)

type Authenticator interface {
	Authenticate(clientID, apiKeyID, presentedKey string) (clientauth.Context, error)
	Allowed(clientID, apiKeyID string) bool
}

type Ref struct {
	ClientSessionID string
	ClientID        string
	APIKeyID        string
	AuthRevision    string
}

type Session struct {
	Ref  Ref
	Done <-chan struct{}
}

type sessionEntry struct {
	ref    Ref
	done   chan struct{}
	closed bool
}

func (s *sessionEntry) close() {
	if !s.closed {
		close(s.done)
		s.closed = true
	}
}

type Manager struct {
	auth Authenticator
	max  uint32

	mu       sync.Mutex
	sessions map[string]*sessionEntry
	closed   bool
}

func NewManager(auth Authenticator, maxSessions uint32) (*Manager, error) {
	if auth == nil {
		return nil, fmt.Errorf("authenticator is required")
	}
	if maxSessions == 0 {
		return nil, fmt.Errorf("max client sessions must be positive")
	}
	return &Manager{auth: auth, max: maxSessions, sessions: make(map[string]*sessionEntry)}, nil
}

func (m *Manager) Authenticate(clientID, apiKeyID, presentedKey string) (Session, error) {
	context, err := m.auth.Authenticate(clientID, apiKeyID, presentedKey)
	if err != nil {
		return Session{}, fmt.Errorf("%w: %w", ErrAuthenticationFailed, err)
	}
	sessionID, err := newID()
	if err != nil {
		return Session{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Session{}, ErrClosed
	}
	if uint64(len(m.sessions)) >= uint64(m.max) {
		return Session{}, ErrCapacity
	}
	if !m.auth.Allowed(context.ClientID, context.APIKeyID) {
		return Session{}, ErrCredentialRevoked
	}
	entry := &sessionEntry{
		ref: Ref{
			ClientSessionID: sessionID,
			ClientID:        context.ClientID,
			APIKeyID:        context.APIKeyID,
			AuthRevision:    context.AuthRevision,
		},
		done: make(chan struct{}),
	}
	m.sessions[sessionID] = entry
	return Session{Ref: entry.ref, Done: entry.done}, nil
}

func (m *Manager) Require(ref Ref) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.sessions[ref.ClientSessionID]
	if entry == nil || entry.ref != ref || entry.closed {
		return ErrStaleSession
	}
	if !m.auth.Allowed(ref.ClientID, ref.APIKeyID) {
		entry.close()
		delete(m.sessions, ref.ClientSessionID)
		return ErrCredentialRevoked
	}
	return nil
}

// Allowed reports whether an immutable credential identity is present in the
// current process snapshot. It does not create or validate a local session;
// the cross-Gateway owner path uses it to recheck a forwarded caller at O.
func (m *Manager) Allowed(clientID, apiKeyID string) bool {
	return m.auth.Allowed(clientID, apiKeyID)
}

func (m *Manager) End(ref Ref) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.sessions[ref.ClientSessionID]
	if entry == nil || entry.ref != ref {
		return
	}
	entry.close()
	delete(m.sessions, ref.ClientSessionID)
}

func (m *Manager) Retire(change clientauth.ChangeSet) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	retired := 0
	for sessionID, entry := range m.sessions {
		if !change.Removes(entry.ref.ClientID, entry.ref.APIKeyID) {
			continue
		}
		entry.close()
		delete(m.sessions, sessionID)
		retired++
	}
	return retired
}

func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for sessionID, entry := range m.sessions {
		entry.close()
		delete(m.sessions, sessionID)
	}
}

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate client session ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}
