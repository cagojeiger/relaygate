package localbinding

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

var (
	ErrInvalid      = errors.New("invalid listener binding")
	ErrCapacity     = errors.New("listener binding capacity reached")
	ErrConflict     = errors.New("listener binding already exists")
	ErrNotFound     = errors.New("listener binding not found")
	ErrAttemptUsed  = errors.New("open attempt was already reserved")
	ErrUnavailable  = errors.New("listener binding control unavailable")
	ErrSessionEnded = errors.New("client session ended")
)

// Committer publishes only the current declaration. It has no durable slot or
// mutation queue: losing its control session makes a new FullSnapshot the next
// declaration cut. CurrentSession is the owner-side fence for OpenContext.
type Committer interface {
	Declare(context.Context, routing.LiveBinding) error
	Withdraw(context.Context, routing.LiveBinding) error
	CurrentSession() (controlmodel.SessionRef, bool)
}

type SessionValidator interface {
	Require(clientsession.Ref) error
	Allowed(clientID, apiKeyID string) bool
}

type State string

const (
	StateRegistering State = "RegisteringB"
	StateLive        State = "LiveB"
	StateRetiring    State = "RetiringB"
	StateRetired     State = "RetiredB"
)

// Snapshot is a process-local diagnostic view. It is not a routing or
// control-plane contract.
type Snapshot struct {
	Binding routing.LiveBinding
	Session clientsession.Ref
	State   State
}

// Reservation is an immutable exact owner reservation. It intentionally does
// not expose the binding retirement signal: explicit unbind after Reserve must
// not cancel an already admitted attempt. Session lifetime remains observable
// through ListenerDone.
type Reservation struct {
	Context      routing.OpenContext
	Caller       clientsession.Ref
	Binding      routing.LiveBinding
	Listener     clientsession.Ref
	Endpoint     ListenerEndpoint
	ListenerDone <-chan struct{}
}

type entry struct {
	binding  routing.LiveBinding
	session  clientsession.Ref
	done     <-chan struct{}
	endpoint ListenerEndpoint
	state    State
	aborted  chan struct{}

	abortedClosed    bool
	terminalRecorded bool
	capacityHeld     bool
}

type bindResult struct {
	binding routing.LiveBinding
	err     error
}

type Manager struct {
	gatewayID         string
	gatewayInstanceID string
	max               uint32
	committer         Committer
	sessions          SessionValidator

	ctx    context.Context //nolint:containedctx // Manager owns one process-lifetime root context for watcher shutdown.
	cancel context.CancelFunc

	mu           sync.Mutex
	entries      map[string]*entry
	byKey        map[routing.BindingKey]string
	bySession    map[clientsession.Ref]map[string]struct{}
	retiredOrder []string
	active       uint32
	closed       bool
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

func New(gatewayID, gatewayInstanceID string, max uint32, committer Committer, sessions SessionValidator) (*Manager, error) {
	if err := routing.ValidateIdentity("gateway_id", gatewayID); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if err := routing.ValidateIdentity("gateway_instance_id", gatewayInstanceID); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if max == 0 || max > routing.MaxListenerBindingsPerGateway {
		return nil, fmt.Errorf("%w: maximum listener bindings must be between 1 and %d", ErrInvalid, routing.MaxListenerBindingsPerGateway)
	}
	if committer == nil {
		return nil, fmt.Errorf("%w: committer is required", ErrInvalid)
	}
	if sessions == nil {
		return nil, fmt.Errorf("%w: session validator is required", ErrInvalid)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		gatewayID:         gatewayID,
		gatewayInstanceID: gatewayInstanceID,
		max:               max,
		committer:         committer,
		sessions:          sessions,
		ctx:               ctx,
		cancel:            cancel,
		entries:           make(map[string]*entry),
		byKey:             make(map[routing.BindingKey]string),
		bySession:         make(map[clientsession.Ref]map[string]struct{}),
	}, nil
}

// Bind reserves local capacity before declaring its current live identity. It
// returns only after that exact declaration is acknowledged; it never queues or
// replays a lost declaration.
func (m *Manager) Bind(ctx context.Context, session clientsession.Session, endpointPattern, targetID string, endpoint ListenerEndpoint) (routing.LiveBinding, error) {
	if ctx == nil {
		return routing.LiveBinding{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if session.Ref.ClientSessionID == "" || session.Ref.ClientID == "" || session.Done == nil {
		return routing.LiveBinding{}, fmt.Errorf("%w: authenticated session is required", ErrInvalid)
	}
	if endpoint == nil {
		return routing.LiveBinding{}, fmt.Errorf("%w: listener endpoint is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return routing.LiveBinding{}, err
	}
	key := routing.BindingKey{ClientID: session.Ref.ClientID, EndpointPattern: endpointPattern, TargetID: targetID}
	if err := key.Validate(); err != nil {
		return routing.LiveBinding{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return routing.LiveBinding{}, ErrUnavailable
	}
	if err := m.sessions.Require(session.Ref); err != nil {
		m.mu.Unlock()
		return routing.LiveBinding{}, fmt.Errorf("%w: %w", ErrSessionEnded, err)
	}
	select {
	case <-session.Done:
		m.mu.Unlock()
		return routing.LiveBinding{}, ErrSessionEnded
	default:
	}
	if _, exists := m.byKey[key]; exists {
		m.mu.Unlock()
		return routing.LiveBinding{}, ErrConflict
	}
	if m.active >= m.max {
		m.mu.Unlock()
		return routing.LiveBinding{}, ErrCapacity
	}
	bindingID, err := newID()
	if err != nil {
		m.mu.Unlock()
		return routing.LiveBinding{}, fmt.Errorf("%w: generate listener binding ID: %w", ErrUnavailable, err)
	}
	e := &entry{
		binding: routing.LiveBinding{
			Key: key,
			Ref: routing.ListenerBindingRef{GatewayID: m.gatewayID, GatewayInstanceID: m.gatewayInstanceID, ListenerBindingID: bindingID},
		},
		session:      session.Ref,
		done:         session.Done,
		endpoint:     endpoint,
		state:        StateRegistering,
		aborted:      make(chan struct{}),
		capacityHeld: true,
	}
	m.entries[bindingID] = e
	m.byKey[key] = bindingID
	owned := m.bySession[session.Ref]
	if owned == nil {
		owned = make(map[string]struct{})
		m.bySession[session.Ref] = owned
	}
	owned[bindingID] = struct{}{}
	m.active++
	result := make(chan bindResult, 1)
	m.wg.Add(2)
	go m.install(e, result)
	go m.watchSession(e, session.Done)
	m.mu.Unlock()

	select {
	case outcome := <-result:
		if err := ctx.Err(); err != nil {
			m.retireAttempt(e)
			return routing.LiveBinding{}, err
		}
		select {
		case <-session.Done:
			m.retireAttempt(e)
			return routing.LiveBinding{}, ErrSessionEnded
		default:
		}
		if outcome.err == nil && !m.live(e) {
			return routing.LiveBinding{}, ErrSessionEnded
		}
		return outcome.binding, outcome.err
	case <-ctx.Done():
		m.retireAttempt(e)
		return routing.LiveBinding{}, ctx.Err()
	case <-session.Done:
		m.retireAttempt(e)
		return routing.LiveBinding{}, ErrSessionEnded
	case <-e.aborted:
		select {
		case outcome := <-result:
			return outcome.binding, outcome.err
		default:
			return routing.LiveBinding{}, ErrSessionEnded
		}
	case <-m.ctx.Done():
		m.retireAttempt(e)
		return routing.LiveBinding{}, ErrUnavailable
	}
}

func (m *Manager) GatewayID() string { return m.gatewayID }

// LiveBindings returns a stable local snapshot of only StateLive declarations.
// It is the source for the next control-session FullSnapshot.
func (m *Manager) LiveBindings() []routing.LiveBinding {
	m.mu.Lock()
	defer m.mu.Unlock()
	bindings := make([]routing.LiveBinding, 0, len(m.byKey))
	for _, bindingID := range m.byKey {
		e := m.entries[bindingID]
		if e != nil && e.state == StateLive && !m.closed {
			bindings = append(bindings, e.binding)
		}
	}
	sort.Slice(bindings, func(i, j int) bool { return bindingLess(bindings[i].Key, bindings[j].Key) })
	return bindings
}

func (m *Manager) Reserve(open routing.OpenContext, caller clientsession.Ref) (Reservation, error) {
	return m.reserve(open, caller, false)
}

func (m *Manager) ReserveForwarded(open routing.OpenContext, caller clientsession.Ref) (Reservation, error) {
	return m.reserve(open, caller, true)
}

func (m *Manager) reserve(open routing.OpenContext, caller clientsession.Ref, forwarded bool) (Reservation, error) {
	if err := validateOpenContext(open); err != nil {
		return Reservation{}, err
	}
	if open.Auth.ClientSessionID != caller.ClientSessionID || open.Auth.ClientID != caller.ClientID || open.Auth.APIKeyID != caller.APIKeyID || open.Auth.AuthRevision != caller.AuthRevision {
		return Reservation{}, fmt.Errorf("%w: caller does not match open context", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Reservation{}, ErrUnavailable
	}
	current, ok := m.committer.CurrentSession()
	if !ok || current.ClusterEpoch != open.ClusterEpoch || current.AuthorityID != open.AuthorityID ||
		current.ControlSessionID != open.OwnerControlSessionID || current.GatewayID != m.gatewayID || current.GatewayInstanceID != m.gatewayInstanceID {
		return Reservation{}, ErrNotFound
	}
	if open.Binding.Ref.GatewayID != m.gatewayID || open.Binding.Ref.GatewayInstanceID != m.gatewayInstanceID {
		return Reservation{}, ErrNotFound
	}
	e := m.entries[open.Binding.Ref.ListenerBindingID]
	if e == nil || e.state != StateLive || e.binding != open.Binding || e.endpoint == nil {
		return Reservation{}, ErrNotFound
	}
	if forwarded {
		if !m.sessions.Allowed(caller.ClientID, caller.APIKeyID) {
			return Reservation{}, fmt.Errorf("%w: caller credential is retired", ErrSessionEnded)
		}
	} else if err := m.sessions.Require(caller); err != nil {
		return Reservation{}, fmt.Errorf("%w: caller: %w", ErrSessionEnded, err)
	}
	if err := m.sessions.Require(e.session); err != nil {
		m.retireLocked(e, true)
		return Reservation{}, fmt.Errorf("%w: listener: %w", ErrSessionEnded, err)
	}
	select {
	case <-e.done:
		m.retireLocked(e, true)
		return Reservation{}, ErrSessionEnded
	default:
	}
	if e.session == caller {
		return Reservation{}, fmt.Errorf("%w: caller and listener sessions must differ", ErrInvalid)
	}
	if !forwarded && !open.TryConsume() {
		return Reservation{}, ErrAttemptUsed
	}
	return Reservation{Context: open.Clone(), Caller: caller, Binding: open.Binding, Listener: e.session, Endpoint: e.endpoint, ListenerDone: e.done}, nil
}
