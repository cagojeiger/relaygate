package opening

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientauth "github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	clientsession "github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

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
	if caller.ClientSessionID == "" || pipeID == "" || len(pipeID) > routing.MaxIdentityBytes {
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
	if e.remote != nil {
		if e.activationStarted {
			finished := e.activationFinished
			m.mu.Unlock()
			return m.waitRemoteActivation(e, finished)
		}
		e.activationStarted = true
		if e.activationFinished == nil {
			e.activationFinished = make(chan struct{})
		}
		remote := e.remote
		pipeCtx := e.pipeCtx
		m.mu.Unlock()

		activationCtx, cancel := context.WithTimeout(pipeCtx, m.config.OpenTimeout)
		err := remote.Activate(activationCtx)
		cancel()

		m.mu.Lock()
		var work *termination
		success := err == nil && e.state == StateAccepted
		if success {
			e.activated = true
			e.activationOK = true
			close(e.activation)
		} else if e.state == StateAccepted {
			work, _ = m.terminalizeLocked(e, ErrUnavailable)
		}
		close(e.activationFinished)
		m.mu.Unlock()
		m.launchTermination(work)
		return success
	}
	if !e.activated {
		e.activated = true
		close(e.activation)
	}
	m.mu.Unlock()
	return true
}

func (m *Manager) waitRemoteActivation(e *entry, finished <-chan struct{}) bool {
	timer := time.NewTimer(m.config.OpenTimeout)
	defer timer.Stop()
	select {
	case <-finished:
	case <-m.ctx.Done():
		return false
	case <-timer.C:
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return e.state == StateAccepted && e.activationOK
}

// RelayPayload delivers one opaque payload to the opposite exact participant.
// Calls in each direction are serialized independently and remain gated until
// the caller-facing PipeOpened message has been written successfully.
func (m *Manager) RelayPayload(ctx context.Context, sender clientsession.Ref, pipeID, payloadID string, data []byte) error { //nolint:contextcheck // Accepted Pipe lifetime is entry-owned and intentionally outlives a payload call.
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrPayloadInvalid)
	}
	if len(data) == 0 || len(data) > localbinding.MaxPayloadBytes {
		return fmt.Errorf("%w: payload must be 1..%d bytes", ErrPayloadInvalid, localbinding.MaxPayloadBytes)
	}
	if pipeID == "" || len(pipeID) > routing.MaxIdentityBytes {
		return fmt.Errorf("%w: PipeID must be 1..%d bytes", ErrPayloadInvalid, routing.MaxIdentityBytes)
	}
	if payloadID == "" || len(payloadID) > routing.MaxIdentityBytes {
		return fmt.Errorf("%w: PayloadID must be 1..%d bytes", ErrPayloadInvalid, routing.MaxIdentityBytes)
	}
	if sender.ClientSessionID == "" || sender.ClientID == "" || sender.APIKeyID == "" || sender.AuthRevision == "" {
		return ErrPipeNotOwned
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
		if e.remote != nil {
			destination = e.remote
		} else {
			destination = e.endpoint
		}
	case e.listener:
		if e.listener.ClientSessionID == "" {
			m.mu.Unlock()
			return ErrPipeNotOwned
		}
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
		if context.Cause(pipeCtx) != nil { //nolint:contextcheck // Pipe cancellation is entry-owned, not derived from this payload call.
			return ErrPipeNotOwned
		}
		m.failPayload(e, ErrUnavailable)
		return ErrUnavailable
	}

	deliveryCtx, cancelDelivery := context.WithCancelCause(ctx)
	stopPipeCancellation := context.AfterFunc(pipeCtx, func() { //nolint:contextcheck // Bridge entry-owned Pipe cancellation into this delivery call.
		cancelDelivery(context.Cause(pipeCtx))
	})
	err := destination.DeliverPayload(deliveryCtx, localbinding.PipePayload{PipeID: pipeID, PayloadID: payloadID, Data: payload})
	stopPipeCancellation()
	cancelDelivery(nil)
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if context.Cause(pipeCtx) != nil { //nolint:contextcheck // Pipe cancellation is entry-owned, not derived from this payload call.
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

// ClosePipe applies an exact participant session's local terminal transition.
// A true result means the accepted Pipe belongs to the caller or listener and
// is now terminal; repeating the request while its bounded terminal record is
// retained is a no-op that also returns true. Unknown and foreign-session Pipe
// IDs are indistinguishable and never change state.
func (m *Manager) ClosePipe(participant clientsession.Ref, pipeID string) bool {
	if participant.ClientSessionID == "" || pipeID == "" || len(pipeID) > routing.MaxIdentityBytes {
		return false
	}

	m.mu.Lock()
	e := m.byPipe[pipeID]
	if e == nil || (e.caller != participant && e.listener != participant) {
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
