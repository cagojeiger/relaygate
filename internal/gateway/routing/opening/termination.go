package opening

import (
	"context"
	"sync"

	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

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
