package opening

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	"github.com/cagojeiger/relaygate/internal/localbinding"
)

var (
	ErrInvalid                = errors.New("invalid open")
	ErrCapacity               = errors.New("open pipe capacity reached")
	ErrNotFound               = errors.New("open target not found")
	ErrUnavailable            = errors.New("open unavailable")
	ErrRemoteOwnerUnsupported = errors.New("remote owner is unsupported")
	ErrListenerRejected       = errors.New("listener rejected open")
	ErrDeadline               = errors.New("open deadline exceeded")
	ErrUnknown                = errors.New("open outcome unknown")
	ErrSessionEnded           = errors.New("open session ended")
	ErrPayloadInvalid         = errors.New("invalid payload")
	ErrPipeNotOwned           = errors.New("pipe not owned")
	ErrPayloadBackpressure    = errors.New("payload backpressure exhausted")
)

type Config struct {
	ClusterEpoch string
	MaxPipes     uint32
	OpenTimeout  time.Duration
}

// Admitter is implemented by gatewaycontrol.Client. It issues one exact,
// quorum-confirmed authority context and performs no owner-side reservation.
type Admitter interface {
	AdmitOpen(context.Context, clientsession.Ref, string, string) (authority.OpenContext, error)
}

type ReservationStore interface {
	GatewayID() string
	Reserve(authority.OpenContext, clientsession.Ref) (localbinding.Reservation, error)
}

type State string

const (
	StateOpening  State = "OpeningO"
	StateAdmitted State = "AdmittedO"
	StateAccepted State = "AcceptedO"
	StateTerminal State = "TerminalO"
)

type Result struct {
	AttemptID string
	PipeID    string
	Binding   controlstate.BindingSlot
}

type Snapshot struct {
	AttemptID string
	PipeID    string
	Caller    clientsession.Ref
	Listener  clientsession.Ref
	Binding   controlstate.BindingSlot
	State     State
	Err       error
}

type entry struct {
	caller         clientsession.Ref
	callerDone     <-chan struct{}
	callerEndpoint localbinding.CallerEndpoint

	attemptID    string
	pipeID       string
	binding      controlstate.BindingSlot
	listener     clientsession.Ref
	listenerDone <-chan struct{}
	endpoint     localbinding.ListenerEndpoint

	state           State
	terminalErr     error
	done            chan struct{}
	attemptDone     chan struct{}
	attemptDetached bool
	cancelPhase     context.CancelCauseFunc
	provisional     bool
	offerStarted    bool
	terminationHeld bool

	activation         chan struct{}
	activated          bool
	pipeCtx            context.Context //nolint:containedctx // Entry owns this accepted-Pipe lifetime context.
	cancelPipe         context.CancelCauseFunc
	callerToListenerMu sync.Mutex
	listenerToCallerMu sync.Mutex
}

type termination struct {
	listenerEndpoint localbinding.ListenerEndpoint
	callerEndpoint   localbinding.CallerEndpoint
	message          localbinding.Termination
	entry            *entry
}

type Manager struct {
	config   Config
	admitter Admitter
	bindings ReservationStore

	ctx       context.Context //nolint:containedctx // Manager owns one process-lifetime root context for opener and termination workers.
	cancel    context.CancelFunc
	closeOnce sync.Once
	opens     sync.WaitGroup
	watchers  sync.WaitGroup

	terminationMu      sync.Mutex
	terminations       chan termination
	terminationWorker  sync.WaitGroup
	terminationSenders sync.WaitGroup
	terminationClosed  bool

	mu            sync.Mutex
	entries       map[*entry]struct{}
	byAttempt     map[string]*entry
	byPipe        map[string]*entry
	terminalOrder []*entry
	active        uint32
	pending       uint32
	closed        bool
}

func New(config Config, admitter Admitter, bindings ReservationStore) (*Manager, error) {
	if config.ClusterEpoch == "" || len(config.ClusterEpoch) > controlstate.MaxIdentityBytes {
		return nil, fmt.Errorf("%w: cluster epoch must be 1..%d bytes", ErrInvalid, controlstate.MaxIdentityBytes)
	}
	if config.MaxPipes == 0 {
		return nil, fmt.Errorf("%w: maximum pipes must be positive", ErrInvalid)
	}
	if config.OpenTimeout <= 0 {
		return nil, fmt.Errorf("%w: Open timeout must be positive", ErrInvalid)
	}
	if admitter == nil {
		return nil, fmt.Errorf("%w: admitter is required", ErrInvalid)
	}
	if bindings == nil {
		return nil, fmt.Errorf("%w: local reservation store is required", ErrInvalid)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:       config,
		admitter:     admitter,
		bindings:     bindings,
		ctx:          ctx,
		cancel:       cancel,
		entries:      make(map[*entry]struct{}),
		byAttempt:    make(map[string]*entry),
		byPipe:       make(map[string]*entry),
		terminations: make(chan termination, config.MaxPipes),
	}
	m.terminationWorker.Add(1)
	go m.runTerminations()
	return m, nil
}

// Open implements the owner-local exact-target slice only. A successful
// return means the owner AcceptedO transition and listener confirmation ACK
// have both occurred; it does not model ingress apply or caller ACK.
func (m *Manager) Open(ctx context.Context, caller clientsession.Session, endpoint, targetID string) (Result, error) {
	return m.open(ctx, caller, nil, endpoint, targetID)
}

// OpenPipe opens an owner-local exact target with both payload directions and
// accepted-Pipe terminal propagation wired before the Open linearization
// point. Payload remains gated until ActivatePipe records that the
// caller-facing PipeOpened message was written successfully.
func (m *Manager) OpenPipe(ctx context.Context, caller clientsession.Session, callerEndpoint localbinding.CallerEndpoint, endpoint, targetID string) (Result, error) {
	if callerEndpoint == nil {
		return Result{}, fmt.Errorf("%w: caller endpoint is required", ErrInvalid)
	}
	return m.open(ctx, caller, callerEndpoint, endpoint, targetID)
}

func (m *Manager) open(ctx context.Context, caller clientsession.Session, callerEndpoint localbinding.CallerEndpoint, endpoint, targetID string) (Result, error) {
	if err := validateOpenInput(ctx, caller, endpoint, targetID); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, mapContextError(err)
	}
	select {
	case <-caller.Done:
		return Result{}, ErrSessionEnded
	default:
	}

	pipeCtx, cancelPipe := context.WithCancelCause(context.Background())
	e := &entry{
		caller:         caller.Ref,
		callerDone:     caller.Done,
		callerEndpoint: callerEndpoint,
		state:          StateOpening,
		done:           make(chan struct{}),
		attemptDone:    make(chan struct{}),
		activation:     make(chan struct{}),
		pipeCtx:        pipeCtx,
		cancelPipe:     cancelPipe,
	}
	if err := m.insert(ctx, e); err != nil {
		cancelPipe(err)
		return Result{}, err
	}
	defer m.opens.Done()

	phaseBase, cancelPhase := context.WithCancelCause(ctx)
	attemptCtx, cancelTimeout := context.WithTimeoutCause(phaseBase, m.config.OpenTimeout, ErrDeadline)
	m.setPhaseCancel(e, cancelPhase)
	defer cancelTimeout()

	openContext, err := m.admitter.AdmitOpen(attemptCtx, caller.Ref, endpoint, targetID)
	if err != nil {
		return Result{}, m.fail(e, classifyAdmissionError(attemptCtx, err))
	}
	if err := validateOpenContext(m.config.ClusterEpoch, caller.Ref, endpoint, targetID, openContext); err != nil {
		return Result{}, m.fail(e, err)
	}
	if openContext.Binding.Ref.GatewayID != m.bindings.GatewayID() {
		return Result{}, m.fail(e, ErrRemoteOwnerUnsupported)
	}
	if cause := context.Cause(attemptCtx); cause != nil {
		return Result{}, m.fail(e, mapContextError(cause))
	}

	reservation, err := m.reserve(e, openContext)
	if err != nil {
		return Result{}, m.fail(e, classifyReservationError(err))
	}
	if err := m.beginOffer(e, attemptCtx, caller.Done, reservation.ListenerDone); err != nil {
		return Result{}, err
	}
	offer := localbinding.Offer{
		AttemptID: reservation.Context.AttemptID,
		Caller:    caller.Ref,
		Binding:   cloneSlot(reservation.Binding),
	}
	offerErr := reservation.Endpoint.Offer(attemptCtx, offer)
	work, terminalErr := m.finishOffer(e, offerErr == nil)
	m.launchTermination(work)
	if terminalErr != nil {
		return Result{}, terminalErr
	}
	if offerErr != nil {
		return Result{}, m.fail(e, classifyOfferError(attemptCtx, offerErr))
	}
	if cause := context.Cause(attemptCtx); cause != nil {
		return Result{}, m.fail(e, mapContextError(cause))
	}

	pipeID, err := newPipeID()
	if err != nil {
		return Result{}, m.fail(e, fmt.Errorf("%w: generate PipeID: %w", ErrUnavailable, err))
	}

	confirmBase, cancelConfirm := context.WithCancelCause(ctx)
	confirmCtx, cancelConfirmTimeout := context.WithTimeoutCause(confirmBase, m.config.OpenTimeout, ErrDeadline)
	accepted, work, terminalErr := m.accept(e, pipeID, cancelConfirm, attemptCtx)
	cancelTimeout()
	cancelPhase(nil)
	if !accepted {
		cancelConfirmTimeout()
		cancelConfirm(nil)
		if work != nil {
			m.launchTermination(work)
		}
		return Result{}, terminalErr
	}

	confirmation := localbinding.Confirmation{AttemptID: reservation.Context.AttemptID, PipeID: pipeID}
	if err := reservation.Endpoint.Confirm(confirmCtx, confirmation); err != nil {
		classified := classifyConfirmationError(confirmCtx, err)
		cancelConfirmTimeout()
		return Result{}, m.fail(e, classified)
	}
	if cause := context.Cause(confirmCtx); cause != nil {
		cancelConfirmTimeout()
		return Result{}, m.fail(e, mapContextError(cause))
	}
	cancelConfirmTimeout()

	result, err := m.result(e)
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func validateOpenInput(ctx context.Context, caller clientsession.Session, endpoint, targetID string) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if caller.Done == nil || caller.Ref.ClientSessionID == "" || caller.Ref.ClientID == "" || caller.Ref.APIKeyID == "" || caller.Ref.AuthRevision == "" {
		return fmt.Errorf("%w: authenticated caller session is required", ErrInvalid)
	}
	auth := authority.AuthContext{
		ClientSessionID: caller.Ref.ClientSessionID,
		ClientID:        caller.Ref.ClientID,
		APIKeyID:        caller.Ref.APIKeyID,
		AuthRevision:    caller.Ref.AuthRevision,
	}
	if _, err := authority.ExactBindingKey(auth, endpoint, targetID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func validateOpenContext(clusterEpoch string, caller clientsession.Ref, endpoint, targetID string, open authority.OpenContext) error {
	if open.ClusterEpoch != clusterEpoch || open.AuthorityID == "" || open.AttemptID == "" ||
		len(open.AuthorityID) > controlstate.MaxIdentityBytes || len(open.AttemptID) > controlstate.MaxIdentityBytes {
		return fmt.Errorf("%w: mismatched Open context identity", ErrUnavailable)
	}
	if open.Auth.ClientSessionID != caller.ClientSessionID || open.Auth.ClientID != caller.ClientID ||
		open.Auth.APIKeyID != caller.APIKeyID || open.Auth.AuthRevision != caller.AuthRevision {
		return fmt.Errorf("%w: mismatched Open auth context", ErrUnavailable)
	}
	if open.Binding.Generation == 0 || open.Binding.Ref == nil {
		return fmt.Errorf("%w: Open context has no exact live binding", ErrUnavailable)
	}
	expected, err := authority.ExactBindingKey(open.Auth, endpoint, targetID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if open.Binding.Key != expected {
		return fmt.Errorf("%w: Open context has a mismatched binding", ErrUnavailable)
	}
	if err := open.Binding.Ref.Validate(); err != nil {
		return fmt.Errorf("%w: invalid owner ref: %w", ErrUnavailable, err)
	}
	return nil
}

func (m *Manager) insert(ctx context.Context, e *entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrUnavailable
	}
	// Cleanup-pending listener state retains its admission slot. This makes the
	// endpoint termination backlog part of the same hard MaxPipes bound instead
	// of allowing slow listeners to accumulate blocked cleanup producers.
	if m.pending >= m.config.MaxPipes || m.active >= m.config.MaxPipes-m.pending {
		return ErrCapacity
	}
	m.entries[e] = struct{}{}
	m.active++
	m.opens.Add(1)
	m.watchers.Add(2)
	go func() {
		defer m.watchers.Done()
		m.watchAttempt(ctx, e)
	}()
	go func() {
		defer m.watchers.Done()
		m.watchCaller(e)
	}()
	return nil
}

func (m *Manager) setPhaseCancel(e *entry, cancel context.CancelCauseFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		cancel(e.terminalErr)
		return
	}
	e.cancelPhase = cancel
}

func (m *Manager) reserve(e *entry, open authority.OpenContext) (localbinding.Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		return localbinding.Reservation{}, e.terminalErr
	}
	if existing := m.byAttempt[open.AttemptID]; existing != nil && existing != e {
		return localbinding.Reservation{}, fmt.Errorf("%w: duplicate attempt", ErrUnavailable)
	}
	reservation, err := m.bindings.Reserve(open, e.caller)
	if err != nil {
		return localbinding.Reservation{}, err
	}
	if reservation.Listener == e.caller {
		return localbinding.Reservation{}, fmt.Errorf("%w: caller and listener sessions must differ", ErrInvalid)
	}
	e.attemptID = reservation.Context.AttemptID
	e.binding = cloneSlot(reservation.Binding)
	e.listener = reservation.Listener
	e.listenerDone = reservation.ListenerDone
	e.endpoint = reservation.Endpoint
	e.state = StateAdmitted
	m.byAttempt[e.attemptID] = e
	m.watchers.Add(1)
	go func() {
		defer m.watchers.Done()
		m.watchListener(e)
	}()
	return cloneReservation(reservation), nil
}

// beginOffer linearizes listener notification against terminalization. Once it
// succeeds, Close and retirement treat the endpoint call as already in flight;
// Open remains joined until a late provisional accept has been terminated.
func (m *Manager) beginOffer(e *entry, ctx context.Context, callerDone, listenerDone <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		return e.terminalErr
	}
	if m.closed || e.state != StateAdmitted {
		_, terminalErr := m.terminalizeLocked(e, ErrUnavailable)
		return terminalErr
	}
	if cause := context.Cause(ctx); cause != nil {
		_, terminalErr := m.terminalizeLocked(e, mapContextError(cause))
		return terminalErr
	}
	select {
	case <-callerDone:
		_, terminalErr := m.terminalizeLocked(e, ErrSessionEnded)
		return terminalErr
	default:
	}
	select {
	case <-listenerDone:
		_, terminalErr := m.terminalizeLocked(e, ErrSessionEnded)
		return terminalErr
	default:
	}
	e.offerStarted = true
	return nil
}

// finishOffer records the listener decision under the same lock as
// terminalization. If terminal won while Offer was in flight, its active slot
// was retained as pending cleanup and is either released on error/reject or
// transferred to exactly one compensating Terminate on provisional accept.
func (m *Manager) finishOffer(e *entry, provisional bool) (*termination, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !e.offerStarted {
		if e.state == StateTerminal {
			return nil, e.terminalErr
		}
		return nil, ErrUnavailable
	}
	e.offerStarted = false
	if e.state == StateTerminal {
		if !provisional {
			m.releaseTerminationLocked(e)
			return nil, e.terminalErr
		}
		e.provisional = true
		return m.terminationLocked(e), e.terminalErr
	}
	if e.state != StateAdmitted {
		return nil, ErrUnavailable
	}
	e.provisional = provisional
	return nil, nil
}

func (m *Manager) accept(e *entry, pipeID string, cancel context.CancelCauseFunc, attemptCtx context.Context) (bool, *termination, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		return false, nil, e.terminalErr
	}
	if e.state != StateAdmitted {
		return false, nil, fmt.Errorf("%w: attempt is not admitted", ErrUnavailable)
	}
	if cause := context.Cause(attemptCtx); cause != nil {
		e.provisional = true
		work, terminalErr := m.terminalizeLocked(e, mapContextError(cause))
		return false, work, terminalErr
	}
	if existing := m.byPipe[pipeID]; existing != nil && existing != e {
		work, terminalErr := m.terminalizeLocked(e, fmt.Errorf("%w: duplicate PipeID", ErrUnavailable))
		return false, work, terminalErr
	}
	e.pipeID = pipeID
	e.state = StateAccepted
	e.provisional = true
	e.cancelPhase = cancel
	m.byPipe[pipeID] = e
	return true, nil, nil
}

func (m *Manager) result(e *entry) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		return Result{}, e.terminalErr
	}
	if e.state != StateAccepted {
		return Result{}, ErrUnavailable
	}
	if !e.attemptDetached {
		e.attemptDetached = true
		close(e.attemptDone)
	}
	return Result{AttemptID: e.attemptID, PipeID: e.pipeID, Binding: cloneSlot(e.binding)}, nil
}

func (m *Manager) fail(e *entry, cause error) error {
	work, terminalErr := m.terminalize(e, cause)
	m.launchTermination(work)
	return terminalErr
}

func (m *Manager) terminalize(e *entry, cause error) (*termination, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		return nil, e.terminalErr
	}
	return m.terminalizeLocked(e, cause)
}

func (m *Manager) terminalizeLocked(e *entry, cause error) (*termination, error) {
	wasAccepted := e.state == StateAccepted
	if wasAccepted {
		cause = unknown(cause)
	}
	e.state = StateTerminal
	e.terminalErr = cause
	if e.cancelPipe != nil {
		e.cancelPipe(cause)
		e.cancelPipe = nil
	}
	if e.cancelPhase != nil {
		e.cancelPhase(cause)
		e.cancelPhase = nil
	}
	close(e.done)
	if m.active > 0 {
		m.active--
	}
	if e.endpoint != nil && (e.provisional || e.offerStarted) {
		e.terminationHeld = true
		m.pending++
	}

	if e.attemptID == "" {
		delete(m.entries, e)
	} else {
		m.terminalOrder = append(m.terminalOrder, e)
		m.trimTerminalLocked()
	}
	if !e.provisional {
		return nil, cause
	}
	return m.terminationLocked(e), cause
}

func (m *Manager) terminationLocked(e *entry) *termination {
	if !e.terminationHeld || e.endpoint == nil {
		return nil
	}
	var callerEndpoint localbinding.CallerEndpoint
	if e.pipeID != "" && e.activated {
		callerEndpoint = e.callerEndpoint
	}
	m.terminationSenders.Add(1)
	return &termination{
		listenerEndpoint: e.endpoint,
		callerEndpoint:   callerEndpoint,
		message:          localbinding.Termination{AttemptID: e.attemptID, PipeID: e.pipeID},
		entry:            e,
	}
}

func (m *Manager) releaseTerminationLocked(e *entry) {
	if !e.terminationHeld {
		return
	}
	e.terminationHeld = false
	if m.pending > 0 {
		m.pending--
	}
}

func (m *Manager) trimTerminalLocked() {
	for uint64(len(m.terminalOrder)) > uint64(m.config.MaxPipes) {
		oldest := m.terminalOrder[0]
		m.terminalOrder = m.terminalOrder[1:]
		if oldest.state != StateTerminal {
			continue
		}
		delete(m.entries, oldest)
		if m.byAttempt[oldest.attemptID] == oldest {
			delete(m.byAttempt, oldest.attemptID)
		}
		if oldest.pipeID != "" && m.byPipe[oldest.pipeID] == oldest {
			delete(m.byPipe, oldest.pipeID)
		}
	}
}

func (m *Manager) launchTermination(work *termination) {
	if work == nil || (work.listenerEndpoint == nil && work.callerEndpoint == nil) {
		return
	}
	defer m.terminationSenders.Done()
	m.terminationMu.Lock()
	if m.terminationClosed {
		m.terminationMu.Unlock()
		m.completeTermination(*work)
		return
	}
	// Every queued item owns one pending slot, so at most MaxPipes sends can
	// exist across the worker and this MaxPipes-sized channel. This send cannot
	// build an unbounded producer backlog.
	m.terminations <- *work
	m.terminationMu.Unlock()
}

func (m *Manager) runTerminations() {
	defer m.terminationWorker.Done()
	for work := range m.terminations {
		ctx, cancel := context.WithTimeout(m.ctx, m.config.OpenTimeout)
		var sends sync.WaitGroup
		if work.listenerEndpoint != nil {
			sends.Add(1)
			go func() {
				defer sends.Done()
				_ = work.listenerEndpoint.Terminate(ctx, work.message)
			}()
		}
		if work.callerEndpoint != nil {
			sends.Add(1)
			go func() {
				defer sends.Done()
				_ = work.callerEndpoint.TerminatePipe(ctx, work.message.PipeID)
			}()
		}
		sends.Wait()
		cancel()
		m.completeTermination(work)
	}
}

func (m *Manager) completeTermination(work termination) {
	if work.entry == nil {
		return
	}
	m.mu.Lock()
	m.releaseTerminationLocked(work.entry)
	m.mu.Unlock()
}

func (m *Manager) watchAttempt(ctx context.Context, e *entry) {
	select {
	case <-ctx.Done():
		m.failAttempt(e, mapContextError(context.Cause(ctx)))
	case <-m.ctx.Done():
		m.failAttempt(e, ErrUnavailable)
	case <-e.attemptDone:
	case <-e.done:
	}
}

func (m *Manager) failAttempt(e *entry, cause error) {
	m.mu.Lock()
	if e.state == StateTerminal || e.attemptDetached {
		m.mu.Unlock()
		return
	}
	work, _ := m.terminalizeLocked(e, cause)
	m.mu.Unlock()
	m.launchTermination(work)
}

func (m *Manager) watchCaller(e *entry) {
	select {
	case <-e.callerDone:
		_ = m.fail(e, ErrSessionEnded)
	case <-m.ctx.Done():
		_ = m.fail(e, ErrUnavailable)
	case <-e.done:
	}
}

func (m *Manager) watchListener(e *entry) {
	select {
	case <-e.listenerDone:
		_ = m.fail(e, ErrSessionEnded)
	case <-m.ctx.Done():
		_ = m.fail(e, ErrUnavailable)
	case <-e.done:
	}
}

func (m *Manager) RetireSession(session clientsession.Ref) int {
	return m.retireMatching(func(e *entry) bool { return e.caller == session || e.listener == session }, ErrSessionEnded)
}

func (m *Manager) Retire(change clientauth.ChangeSet) int {
	return m.retireMatching(func(e *entry) bool {
		return change.Removes(e.caller.ClientID, e.caller.APIKeyID) || change.Removes(e.listener.ClientID, e.listener.APIKeyID)
	}, ErrSessionEnded)
}

func (m *Manager) RetireAll() int {
	return m.retireMatching(func(*entry) bool { return true }, ErrUnavailable)
}

// ActivatePipe opens the payload gate after the caller-facing PipeOpened
// message was written successfully. Only the exact caller of a currently
// accepted Pipe may activate it. Repeated activation is an idempotent success
// while that Pipe remains live. A false result after the write tells the
// caller-facing control lane to emit PipeTerminated directly.
func (m *Manager) ActivatePipe(caller clientsession.Ref, pipeID string) bool {
	if caller.ClientSessionID == "" || pipeID == "" || len(pipeID) > controlstate.MaxIdentityBytes {
		return false
	}

	m.mu.Lock()
	e := m.byPipe[pipeID]
	if e == nil || e.caller != caller {
		m.mu.Unlock()
		return false
	}
	if e.state != StateAccepted {
		m.mu.Unlock()
		return false
	}
	if !e.activated {
		e.activated = true
		close(e.activation)
	}
	m.mu.Unlock()
	return true
}

// RelayPayload delivers one opaque payload to the opposite exact participant.
// Calls in each direction are serialized independently and remain gated until
// the caller-facing PipeOpened message has been written successfully.
func (m *Manager) RelayPayload(ctx context.Context, sender clientsession.Ref, pipeID string, data []byte) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrPayloadInvalid)
	}
	if len(data) == 0 || len(data) > localbinding.MaxPayloadBytes {
		return fmt.Errorf("%w: payload must be 1..%d bytes", ErrPayloadInvalid, localbinding.MaxPayloadBytes)
	}
	if pipeID == "" || len(pipeID) > controlstate.MaxIdentityBytes {
		return fmt.Errorf("%w: PipeID must be 1..%d bytes", ErrPayloadInvalid, controlstate.MaxIdentityBytes)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	e := m.byPipe[pipeID]
	if e == nil || e.state != StateAccepted {
		m.mu.Unlock()
		return ErrPipeNotOwned
	}
	var direction *sync.Mutex
	var destination localbinding.PayloadEndpoint
	switch sender {
	case e.caller:
		direction = &e.callerToListenerMu
		destination = e.endpoint
	case e.listener:
		direction = &e.listenerToCallerMu
		destination = e.callerEndpoint
	default:
		m.mu.Unlock()
		return ErrPipeNotOwned
	}
	activated := e.activated
	pipeCtx := e.pipeCtx
	m.mu.Unlock()

	direction.Lock()
	defer direction.Unlock()

	if !activated {
		activationTimer := time.NewTimer(m.config.OpenTimeout)
		defer activationTimer.Stop()
		select {
		case <-e.activation:
		case <-ctx.Done():
			return ctx.Err()
		case <-pipeCtx.Done():
			return ErrPipeNotOwned
		case <-activationTimer.C:
			select {
			case <-e.activation:
				// Activation won before the bounded wait was observed.
			default:
				m.failPayload(e, ErrUnavailable)
				return ErrUnavailable
			}
		}
	}

	m.mu.Lock()
	if m.byPipe[pipeID] != e || e.state != StateAccepted {
		m.mu.Unlock()
		return ErrPipeNotOwned
	}
	payload := append([]byte(nil), data...)
	m.mu.Unlock()

	if destination == nil {
		if context.Cause(pipeCtx) != nil {
			return ErrPipeNotOwned
		}
		m.failPayload(e, ErrUnavailable)
		return ErrUnavailable
	}

	deliveryCtx, cancelDelivery := context.WithCancelCause(ctx)
	stopPipeCancellation := context.AfterFunc(pipeCtx, func() {
		cancelDelivery(context.Cause(pipeCtx))
	})
	err := destination.DeliverPayload(deliveryCtx, localbinding.PipePayload{PipeID: pipeID, Data: payload})
	stopPipeCancellation()
	cancelDelivery(nil)
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if context.Cause(pipeCtx) != nil {
		return ErrPipeNotOwned
	}
	stable := classifyPayloadError(err)
	m.failPayload(e, stable)
	return stable
}

func (m *Manager) failPayload(e *entry, cause error) {
	work, _ := m.terminalize(e, cause)
	m.launchTermination(work)
}

// ClosePipe applies the caller session's exact local terminal transition. A
// true result means the Pipe belongs to caller and is now terminal; repeating
// the request while its bounded terminal record is retained is a no-op that
// also returns true. Unknown and foreign-session Pipe IDs are indistinguishable
// and never change state.
func (m *Manager) ClosePipe(caller clientsession.Ref, pipeID string) bool {
	if caller.ClientSessionID == "" || pipeID == "" || len(pipeID) > controlstate.MaxIdentityBytes {
		return false
	}

	m.mu.Lock()
	e := m.byPipe[pipeID]
	if e == nil || e.caller != caller {
		m.mu.Unlock()
		return false
	}
	if e.state == StateTerminal {
		m.mu.Unlock()
		return true
	}
	if e.state != StateAccepted {
		m.mu.Unlock()
		return false
	}
	work, _ := m.terminalizeLocked(e, context.Canceled)
	m.mu.Unlock()
	m.launchTermination(work)
	return true
}

func (m *Manager) retireMatching(match func(*entry) bool, cause error) int {
	m.mu.Lock()
	works := make([]*termination, 0)
	retired := 0
	for e := range m.entries {
		if e.state == StateTerminal || !match(e) {
			continue
		}
		work, _ := m.terminalizeLocked(e, cause)
		if work != nil {
			works = append(works, work)
		}
		retired++
	}
	m.mu.Unlock()
	for _, work := range works {
		m.launchTermination(work)
	}
	return retired
}

func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int(m.active)
}

func (m *Manager) Inspect(attemptID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.byAttempt[attemptID]
	if e == nil {
		return Snapshot{}, false
	}
	return snapshotOf(e), true
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		works := make([]*termination, 0)
		for e := range m.entries {
			if e.state == StateTerminal {
				continue
			}
			work, _ := m.terminalizeLocked(e, ErrUnavailable)
			if work != nil {
				works = append(works, work)
			}
		}
		m.mu.Unlock()
		// Cancel endpoint work before any potentially blocking enqueue. The
		// endpoint contract requires cancellation responsiveness, so this also
		// bounds Close when the queue or its worker is busy.
		m.cancel()
		for _, work := range works {
			m.launchTermination(work)
		}
		// Keep termination dispatch alive until all Opens that crossed insert have
		// returned. An in-flight Offer may accept after Close terminalizes its
		// entry, in which case Open must still enqueue the compensating Terminate.
		m.opens.Wait()
		m.watchers.Wait()
		m.terminationSenders.Wait()
		m.terminationMu.Lock()
		m.terminationClosed = true
		close(m.terminations)
		m.terminationMu.Unlock()
		m.terminationWorker.Wait()
	})
}

func snapshotOf(e *entry) Snapshot {
	return Snapshot{
		AttemptID: e.attemptID,
		PipeID:    e.pipeID,
		Caller:    e.caller,
		Listener:  e.listener,
		Binding:   cloneSlot(e.binding),
		State:     e.state,
		Err:       e.terminalErr,
	}
}

func classifyAdmissionError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, authority.ErrInvalidOpen):
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	case errors.Is(err, authority.ErrRouteNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	default:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

func classifyReservationError(err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return err
	case errors.Is(err, localbinding.ErrInvalid):
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	case errors.Is(err, localbinding.ErrAttemptUsed):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case errors.Is(err, localbinding.ErrNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	case errors.Is(err, localbinding.ErrSessionEnded):
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	default:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

func classifyOfferError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, ErrSessionEnded), errors.Is(err, localbinding.ErrSessionEnded):
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	case errors.Is(err, ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, localbinding.ErrEndpointUnavailable):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case errors.Is(err, localbinding.ErrOfferRejected):
		return fmt.Errorf("%w: %w", ErrListenerRejected, err)
	default:
		return fmt.Errorf("%w: %w", ErrListenerRejected, err)
	}
}

func classifyConfirmationError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrDeadline):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, localbinding.ErrSessionEnded), errors.Is(err, ErrSessionEnded):
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	case errors.Is(err, localbinding.ErrEndpointUnavailable):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

func classifyPayloadError(err error) error {
	if errors.Is(err, localbinding.ErrPayloadBackpressure) {
		return ErrPayloadBackpressure
	}
	return ErrUnavailable
}

func mapContextError(err error) error {
	switch {
	case errors.Is(err, ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, ErrSessionEnded):
		return ErrSessionEnded
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case err == nil:
		return ErrUnavailable
	default:
		return err
	}
}

func unknown(cause error) error {
	if cause == nil {
		return ErrUnknown
	}
	if errors.Is(cause, ErrUnknown) {
		return cause
	}
	return fmt.Errorf("%w: %w", ErrUnknown, cause)
}

func cloneReservation(reservation localbinding.Reservation) localbinding.Reservation {
	copy := reservation
	copy.Context = cloneOpenContext(reservation.Context)
	copy.Binding = cloneSlot(reservation.Binding)
	return copy
}

func cloneOpenContext(open authority.OpenContext) authority.OpenContext {
	return open.Clone()
}

func cloneSlot(slot controlstate.BindingSlot) controlstate.BindingSlot {
	copy := slot
	if slot.Ref != nil {
		ref := *slot.Ref
		copy.Ref = &ref
	}
	return copy
}

func newPipeID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
