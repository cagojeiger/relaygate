package relaygate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	managedConnectTimeout = 10 * time.Second
	managedInitialBackoff = 500 * time.Millisecond
	managedMaximumBackoff = 10 * time.Second
	managedStableWindow   = 30 * time.Second
	managedJitterRatio    = 0.20
)

var (
	ErrManagedNotReady      = errors.New("relaygate: managed client is not ready")
	ErrManagedClosed        = errors.New("relaygate: managed client is closed")
	ErrManagedBindingExists = errors.New("relaygate: managed listener already exists")
)

// ManagedState is the connection supervisor's observable state.
type ManagedState uint8

const (
	ManagedConnecting ManagedState = iota + 1
	ManagedRebinding
	ManagedReady
	ManagedBackoff
	ManagedFailed
	ManagedClosed
)

func (s ManagedState) String() string {
	switch s {
	case ManagedConnecting:
		return "connecting"
	case ManagedRebinding:
		return "rebinding"
	case ManagedReady:
		return "ready"
	case ManagedBackoff:
		return "backoff"
	case ManagedFailed:
		return "failed"
	case ManagedClosed:
		return "closed"
	default:
		return "unknown"
	}
}

type managedBindingKey struct {
	endpoint string
	target   string
}

type managedBinding struct {
	key        managedBindingKey
	active     bool
	current    *Listener
	generation uint64
}

// ManagedClient supervises fresh authenticated Client sessions. It keeps only
// current Listener declarations. Open, Offer, Pipe, and payload state are
// never retried, resumed, or replayed across a session boundary.
type ManagedClient struct {
	cancel context.CancelCauseFunc
	config Config
	done   chan struct{}

	mu         sync.Mutex
	state      ManagedState
	current    *Client
	generation uint64
	bindings   map[managedBindingKey]*managedBinding
	changed    chan struct{}
	finalErr   error

	connect func(context.Context, Config) (*Client, error)
	now     func() time.Time
	backoff func(time.Duration) time.Duration
}

// ManagedListener is a logical current-state declaration. Its underlying
// Listener is replaced after every successful reconnect and rebind.
type ManagedListener struct {
	owner   *ManagedClient
	binding *managedBinding
}

// ConnectManaged is the recommended application entry point. It starts an
// in-process connection supervisor and waits for its first authenticated,
// fully rebound session. The supplied context bounds setup only; Close owns the
// returned ManagedClient lifetime.
func ConnectManaged(ctx context.Context, config Config) (*ManagedClient, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	managed := &ManagedClient{
		cancel:   cancel,
		config:   config,
		done:     make(chan struct{}),
		state:    ManagedConnecting,
		bindings: make(map[managedBindingKey]*managedBinding),
		changed:  make(chan struct{}),
		connect:  Connect,
		now:      time.Now,
		backoff:  jitterBackoff,
	}
	go managed.run(lifetime)
	if err := managed.WaitReady(ctx); err != nil {
		_ = managed.Close()
		return nil, err
	}
	return managed, nil
}

func (m *ManagedClient) State() ManagedState {
	if m == nil {
		return ManagedClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *ManagedClient) Done() <-chan struct{} {
	if m == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return m.done
}

func (m *ManagedClient) Err() error {
	if m == nil {
		return ErrManagedClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finalErr
}

// WaitReady waits for a current authenticated session whose desired Listener
// declarations have all been rebound.
func (m *ManagedClient) WaitReady(ctx context.Context) error {
	if m == nil {
		return ErrManagedClosed
	}
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	for {
		m.mu.Lock()
		state, changed, finalErr := m.state, m.changed, m.finalErr
		m.mu.Unlock()
		switch state {
		case ManagedReady:
			return nil
		case ManagedFailed:
			if finalErr != nil {
				return finalErr
			}
			return ErrManagedClosed
		case ManagedClosed:
			return ErrManagedClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			if err := m.Err(); err != nil {
				return err
			}
			return ErrManagedClosed
		}
	}
}

// Bind declares a logical Listener and waits until it is bound on the current
// session. The declaration is retried only across fresh session boundaries.
func (m *ManagedClient) Open(ctx context.Context, endpoint, target string) (*Pipe, error) {
	if m == nil {
		return nil, ErrManagedClosed
	}
	m.mu.Lock()
	client, ready := m.current, m.state == ManagedReady
	m.mu.Unlock()
	if !ready || client == nil {
		return nil, ErrManagedNotReady
	}
	return client.Open(ctx, endpoint, target)
}

func (m *ManagedClient) Close() error {
	if m == nil {
		return nil
	}
	m.cancel(errExplicitClose)
	<-m.done
	return nil
}
