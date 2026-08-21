package opening

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

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

func (m *Manager) reserve(e *entry, open routing.OpenContext, forwarded bool) (localbinding.Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.state == StateTerminal {
		return localbinding.Reservation{}, e.terminalErr
	}
	if existing := m.byAttempt[open.AttemptID]; existing != nil && existing != e {
		return localbinding.Reservation{}, fmt.Errorf("%w: duplicate attempt", ErrAttemptReplay)
	}
	now := m.now()
	if !now.Before(open.ExpiresAt) {
		return localbinding.Reservation{}, ErrContextExpired
	}
	if forwarded {
		m.pruneForwardedAttemptsLocked(now)
		if _, exists := m.forwardedAttempts[open.AttemptID]; exists {
			return localbinding.Reservation{}, ErrAttemptReplay
		}
		if uint64(len(m.forwardedAttempts)) >= uint64(m.config.MaxPipes) {
			return localbinding.Reservation{}, ErrCapacity
		}
	}
	var (
		reservation localbinding.Reservation
		err         error
	)
	if forwarded {
		reservation, err = m.bindings.ReserveForwarded(open, e.caller)
	} else {
		reservation, err = m.bindings.Reserve(open, e.caller)
	}
	if err != nil {
		return localbinding.Reservation{}, err
	}
	if reservation.Context.ClusterEpoch != open.ClusterEpoch || reservation.Context.AuthorityID != open.AuthorityID ||
		reservation.Context.AttemptID != open.AttemptID || reservation.Context.Auth != open.Auth ||
		!sameExactSlot(reservation.Context.Binding, open.Binding) ||
		reservation.Caller != e.caller || !sameExactSlot(reservation.Binding, open.Binding) ||
		reservation.Listener.ClientSessionID == "" || reservation.Listener.ClientID == "" ||
		reservation.Listener.APIKeyID == "" || reservation.ListenerDone == nil || reservation.Endpoint == nil {
		return localbinding.Reservation{}, fmt.Errorf("%w: reservation store returned a non-exact result", ErrUnavailable)
	}
	if reservation.Listener == e.caller {
		return localbinding.Reservation{}, fmt.Errorf("%w: caller and listener sessions must differ", ErrInvalid)
	}
	if !m.now().Before(open.ExpiresAt) {
		return localbinding.Reservation{}, ErrContextExpired
	}
	if forwarded {
		m.forwardedAttempts[open.AttemptID] = open.ExpiresAt
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

func (m *Manager) pruneForwardedAttemptsLocked(now time.Time) {
	for attemptID, expiresAt := range m.forwardedAttempts {
		if !now.Before(expiresAt) {
			delete(m.forwardedAttempts, attemptID)
		}
	}
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

func (m *Manager) acceptRemote(e *entry, open routing.OpenContext, result RemoteResult) (bool, error) {
	if result.AttemptID != open.AttemptID || result.PipeID == "" || len(result.PipeID) > routing.MaxIdentityBytes ||
		result.Endpoint == nil || !sameExactSlot(result.Binding, open.Binding) {
		return false, fmt.Errorf("%w: remote owner returned a non-exact result", ErrUnknown)
	}
	remoteDone := result.Endpoint.Done()
	if remoteDone == nil {
		return false, fmt.Errorf("%w: remote owner returned no lifetime signal", ErrUnknown)
	}

	m.mu.Lock()
	if e.state == StateTerminal {
		terminalErr := e.terminalErr
		m.mu.Unlock()
		return false, terminalErr
	}
	if e.state != StateOpening {
		m.mu.Unlock()
		return false, fmt.Errorf("%w: remote attempt is not opening", ErrUnknown)
	}
	if existing := m.byAttempt[result.AttemptID]; existing != nil && existing != e {
		m.mu.Unlock()
		return false, fmt.Errorf("%w: duplicate remote attempt", ErrUnknown)
	}
	if existing := m.byPipe[result.PipeID]; existing != nil && existing != e {
		m.mu.Unlock()
		return false, fmt.Errorf("%w: duplicate remote PipeID", ErrUnknown)
	}

	e.attemptID = result.AttemptID
	e.pipeID = result.PipeID
	e.binding = cloneSlot(result.Binding)
	e.remote = result.Endpoint
	e.listenerDone = remoteDone
	e.state = StateAccepted
	e.provisional = true
	e.activationFinished = make(chan struct{})
	m.byAttempt[e.attemptID] = e
	m.byPipe[e.pipeID] = e
	m.watchers.Add(1)
	go func() {
		defer m.watchers.Done()
		m.watchRemote(e, remoteDone)
	}()
	m.mu.Unlock()
	return true, nil
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

// A structurally valid remote result proves the owner Open LP may have passed.
// Even when a caller/session terminal won the ingress-local race, that result
// can no longer be reported as a stable pre-LP failure.
func (m *Manager) failRemoteResult(e *entry, cause error) error {
	m.mu.Lock()
	if e.state == StateTerminal {
		terminalErr := unknown(e.terminalErr)
		m.mu.Unlock()
		return terminalErr
	}
	work, terminalErr := m.terminalizeLocked(e, unknown(cause))
	m.mu.Unlock()
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
	if (e.endpoint != nil && (e.provisional || e.offerStarted)) || e.remote != nil {
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
	if !e.terminationHeld || (e.endpoint == nil && e.remote == nil) {
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
		remoteEndpoint:   e.remote,
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
	if work == nil || (work.listenerEndpoint == nil && work.callerEndpoint == nil && work.remoteEndpoint == nil) {
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
		if work.remoteEndpoint != nil {
			sends.Add(1)
			go func() {
				defer sends.Done()
				_ = work.remoteEndpoint.Close(ctx)
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

func (m *Manager) watchRemote(e *entry, done <-chan struct{}) {
	select {
	case <-done:
		_ = m.fail(e, ErrUnavailable)
	case <-m.ctx.Done():
		_ = m.fail(e, ErrUnavailable)
	case <-e.done:
	}
}
