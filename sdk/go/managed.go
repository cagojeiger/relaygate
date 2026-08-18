package relaygate

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	ctx    context.Context //nolint:containedctx // The context is the supervisor lifetime.
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

// ConnectManaged starts an in-process connection supervisor and waits for its
// first authenticated, fully rebound session. The supplied context bounds
// setup only; Close owns the returned ManagedClient lifetime.
func ConnectManaged(ctx context.Context, config Config) (*ManagedClient, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancelCause(context.Background())
	managed := &ManagedClient{
		ctx:      lifetime,
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
	go managed.run()
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
func (m *ManagedClient) Bind(ctx context.Context, endpoint, target string) (*ManagedListener, error) {
	if m == nil {
		return nil, ErrManagedClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if !validEndpoint(endpoint) || !validIdentity(target) {
		return nil, fmt.Errorf("relaygate: invalid endpoint or target")
	}
	key := managedBindingKey{endpoint: endpoint, target: target}
	binding := &managedBinding{key: key, active: true}
	m.mu.Lock()
	if _, exists := m.bindings[key]; exists {
		m.mu.Unlock()
		return nil, ErrManagedBindingExists
	}
	if m.state == ManagedFailed {
		err := m.finalErr
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrManagedClosed
	}
	if m.state == ManagedClosed {
		m.mu.Unlock()
		return nil, ErrManagedClosed
	}
	if len(m.bindings) >= maxListeners {
		m.mu.Unlock()
		return nil, errCapacity
	}
	m.bindings[key] = binding
	m.signalLocked()
	m.mu.Unlock()

	listener := &ManagedListener{owner: m, binding: binding}
	if err := m.bindDeclaration(ctx, binding); err != nil {
		m.removeBinding(binding)
		return nil, err
	}
	return listener, nil
}

// Open performs exactly one Open on the current Ready session. It never waits
// in a reconnect queue and never retries on a later session.
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

func (l *ManagedListener) Endpoint() string { return l.binding.key.endpoint }
func (l *ManagedListener) Target() string   { return l.binding.key.target }

// Next waits across reconnects for the next Offer on the current underlying
// Listener. An Offer already returned to the caller remains session-bound.
func (l *ManagedListener) Next(ctx context.Context) (*Offer, error) {
	if l == nil || l.owner == nil || l.binding == nil {
		return nil, ErrListenerEnded
	}
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	var observed uint64
	for {
		raw, generation, err := l.owner.currentListener(ctx, l.binding, observed)
		if err != nil {
			return nil, err
		}
		offer, err := raw.Next(ctx)
		if err == nil {
			return offer, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		select {
		case <-raw.client.Done():
			observed = generation
			continue
		default:
			return nil, err
		}
	}
}

// Unbind removes the desired declaration before attempting current-session
// cleanup, so a reconnect cannot resurrect it.
func (l *ManagedListener) Unbind(ctx context.Context) error {
	if l == nil || l.owner == nil || l.binding == nil {
		return ErrListenerEnded
	}
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	m := l.owner
	m.mu.Lock()
	if !l.binding.active || m.bindings[l.binding.key] != l.binding {
		m.mu.Unlock()
		return ErrListenerEnded
	}
	l.binding.active = false
	delete(m.bindings, l.binding.key)
	raw := l.binding.current
	l.binding.current = nil
	m.signalLocked()
	m.mu.Unlock()
	if raw == nil {
		return nil
	}
	return raw.Unbind(ctx)
}

func (m *ManagedClient) currentListener(ctx context.Context, binding *managedBinding, observed uint64) (*Listener, uint64, error) {
	for {
		m.mu.Lock()
		active := binding.active && m.bindings[binding.key] == binding
		raw, generation := binding.current, binding.generation
		state, changed, finalErr := m.state, m.changed, m.finalErr
		m.mu.Unlock()
		if !active {
			return nil, 0, ErrListenerEnded
		}
		if raw != nil && generation > observed {
			return raw, generation, nil
		}
		if state == ManagedFailed {
			if finalErr != nil {
				return nil, 0, finalErr
			}
			return nil, 0, ErrManagedClosed
		}
		if state == ManagedClosed {
			return nil, 0, ErrManagedClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-m.done:
			return nil, 0, ErrManagedClosed
		}
	}
}

func (m *ManagedClient) removeBinding(binding *managedBinding) {
	m.mu.Lock()
	if m.bindings[binding.key] == binding {
		binding.active = false
		binding.current = nil
		delete(m.bindings, binding.key)
		m.signalLocked()
	}
	m.mu.Unlock()
}

func (m *ManagedClient) bindDeclaration(ctx context.Context, binding *managedBinding) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		if !binding.active || m.bindings[binding.key] != binding {
			m.mu.Unlock()
			return ErrListenerEnded
		}
		if binding.current != nil && binding.generation == m.generation {
			m.mu.Unlock()
			return nil
		}
		client, generation := m.current, m.generation
		state, changed, finalErr := m.state, m.changed, m.finalErr
		m.mu.Unlock()

		switch state {
		case ManagedFailed:
			if finalErr != nil {
				return finalErr
			}
			return ErrManagedClosed
		case ManagedClosed:
			return ErrManagedClosed
		case ManagedReady:
			if client == nil {
				continue
			}
			raw, err := client.Bind(ctx, binding.key.endpoint, binding.key.target)
			if err != nil {
				select {
				case <-client.Done():
					continue
				default:
					return err
				}
			}
			m.mu.Lock()
			current := m.current == client && m.generation == generation && m.state == ManagedReady
			active := binding.active && m.bindings[binding.key] == binding
			if current && active {
				binding.current = raw
				binding.generation = generation
				m.signalLocked()
				m.mu.Unlock()
				return nil
			}
			m.mu.Unlock()
			if active {
				select {
				case <-client.Done():
				default:
					_ = raw.Unbind(ctx)
				}
			}
			continue
		}

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return ErrManagedClosed
		}
	}
}

func (m *ManagedClient) run() {
	defer close(m.done)
	delay := managedInitialBackoff
	for {
		if err := m.ctx.Err(); err != nil {
			m.finish(ManagedClosed, nil)
			return
		}
		m.setState(ManagedConnecting, nil)
		attemptCtx, cancelAttempt := context.WithTimeout(m.ctx, managedConnectTimeout)
		client, err := m.connect(attemptCtx, m.config)
		cancelAttempt()
		if err != nil {
			if isPermanentManagedConnectError(err) {
				m.finish(ManagedFailed, err)
				return
			}
			if !m.waitBackoff(delay, err) {
				m.finish(ManagedClosed, nil)
				return
			}
			delay = nextManagedBackoff(delay)
			continue
		}

		readyAt, err := m.installAndRebind(client)
		if err != nil {
			_ = client.Close()
			if isPermanentManagedConnectError(err) {
				m.finish(ManagedFailed, err)
				return
			}
			if !m.waitBackoff(delay, err) {
				m.finish(ManagedClosed, nil)
				return
			}
			delay = nextManagedBackoff(delay)
			continue
		}

		select {
		case <-client.Done():
			clientErr := client.Err()
			if errors.Is(clientErr, errProtocol) || isPermanentManagedConnectError(clientErr) {
				m.finish(ManagedFailed, clientErr)
				return
			}
			if m.now().Sub(readyAt) >= managedStableWindow {
				delay = managedInitialBackoff
			}
			m.detach(client)
			if !m.waitBackoff(delay, clientErr) {
				m.finish(ManagedClosed, nil)
				return
			}
			delay = nextManagedBackoff(delay)
		case <-m.ctx.Done():
			_ = client.Close()
			m.finish(ManagedClosed, nil)
			return
		}
	}
}

func (m *ManagedClient) installAndRebind(client *Client) (time.Time, error) {
	m.mu.Lock()
	m.current = client
	m.generation++
	generation := m.generation
	for _, binding := range m.bindings {
		binding.current = nil
	}
	m.state = ManagedRebinding
	m.finalErr = nil
	m.signalLocked()
	m.mu.Unlock()

	for {
		m.mu.Lock()
		pending := make([]*managedBinding, 0, len(m.bindings))
		for _, binding := range m.bindings {
			if binding.active && binding.generation != generation {
				pending = append(pending, binding)
			}
		}
		m.mu.Unlock()
		if len(pending) == 0 {
			m.mu.Lock()
			if m.current != client || m.generation != generation {
				m.mu.Unlock()
				return time.Time{}, ErrManagedNotReady
			}
			m.state = ManagedReady
			m.finalErr = nil
			m.signalLocked()
			m.mu.Unlock()
			return m.now(), nil
		}

		for _, binding := range pending {
			raw, err := client.Bind(m.ctx, binding.key.endpoint, binding.key.target)
			if err != nil {
				return time.Time{}, err
			}
			m.mu.Lock()
			current := m.current == client && m.generation == generation
			active := binding.active && m.bindings[binding.key] == binding
			if current && active {
				binding.current = raw
				binding.generation = generation
				m.signalLocked()
			}
			m.mu.Unlock()
			if !current || !active {
				_ = raw.Unbind(m.ctx)
			}
		}
	}
}

func (m *ManagedClient) detach(client *Client) {
	m.mu.Lock()
	if m.current == client {
		m.current = nil
		for _, binding := range m.bindings {
			binding.current = nil
		}
		m.state = ManagedBackoff
		m.finalErr = client.Err()
		m.signalLocked()
	}
	m.mu.Unlock()
}

func (m *ManagedClient) waitBackoff(delay time.Duration, cause error) bool {
	m.mu.Lock()
	if m.state != ManagedClosed && m.state != ManagedFailed {
		m.state = ManagedBackoff
		m.finalErr = cause
		m.signalLocked()
	}
	m.mu.Unlock()
	timer := time.NewTimer(m.backoff(delay))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func (m *ManagedClient) setState(state ManagedState, err error) {
	m.mu.Lock()
	m.state = state
	m.finalErr = err
	m.signalLocked()
	m.mu.Unlock()
}

func (m *ManagedClient) finish(state ManagedState, err error) {
	m.mu.Lock()
	m.current = nil
	for _, binding := range m.bindings {
		binding.current = nil
	}
	m.state = state
	m.finalErr = err
	m.signalLocked()
	m.mu.Unlock()
}

func (m *ManagedClient) signalLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

func isPermanentManagedConnectError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

func nextManagedBackoff(delay time.Duration) time.Duration {
	if delay >= managedMaximumBackoff/2 {
		return managedMaximumBackoff
	}
	return delay * 2
}

func jitterBackoff(delay time.Duration) time.Duration {
	factor := 1 - managedJitterRatio + rand.Float64()*(2*managedJitterRatio)
	result := time.Duration(float64(delay) * factor)
	if result < 0 {
		return delay
	}
	return result
}
