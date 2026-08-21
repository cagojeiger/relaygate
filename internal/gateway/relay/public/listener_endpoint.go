package relaygrpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
)

type listenerAttemptPhase uint8

const (
	listenerOffered listenerAttemptPhase = iota + 1
	listenerProvisional
	listenerConfirming
	listenerOpen
)

type listenerAttempt struct {
	phase        listenerAttemptPhase
	pipeID       string
	decision     chan bool
	confirmation chan struct{}
	terminal     chan struct{}
}

// streamListenerEndpoint adapts the protocol-neutral local listener boundary
// to one authenticated Connect stream. It owns only volatile attempt state.
type streamListenerEndpoint struct {
	ctx                 context.Context //nolint:containedctx // Endpoint owns the stream-root context for listener attempt retirement.
	outbound            *outboundActor
	pipeEndpoint        *streamPipeEndpoint
	terminalSendTimeout time.Duration

	requestOrder sync.RWMutex
	mu           sync.Mutex
	attempts     map[string]*listenerAttempt
	closed       bool
}

func newStreamListenerEndpoint(ctx context.Context, outbound *outboundActor, pipeEndpoint *streamPipeEndpoint, terminalSendTimeout time.Duration) *streamListenerEndpoint {
	return &streamListenerEndpoint{
		ctx:                 ctx,
		outbound:            outbound,
		pipeEndpoint:        pipeEndpoint,
		terminalSendTimeout: terminalSendTimeout,
		attempts:            make(map[string]*listenerAttempt),
	}
}

func (e *streamListenerEndpoint) DeliverPayload(ctx context.Context, payload localbinding.PipePayload) error {
	return e.pipeEndpoint.DeliverPayload(ctx, payload)
}

func (e *streamListenerEndpoint) Offer(ctx context.Context, offer localbinding.Offer) error {
	if err := validateOffer(ctx, offer); err != nil {
		return err
	}
	attempt := &listenerAttempt{
		phase:        listenerOffered,
		decision:     make(chan bool, 1),
		confirmation: make(chan struct{}, 1),
		terminal:     make(chan struct{}),
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return localbinding.ErrEndpointUnavailable
	}
	if _, exists := e.attempts[offer.AttemptID]; exists {
		e.mu.Unlock()
		return fmt.Errorf("%w: duplicate listener attempt", localbinding.ErrEndpointUnavailable)
	}
	e.attempts[offer.AttemptID] = attempt
	e.mu.Unlock()

	ref := offer.Binding.Ref
	response := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{
		ListenerOffer: &relayv1.ListenerOffer{
			AttemptId:         offer.AttemptID,
			ListenerBindingId: ref.ListenerBindingID,
			Endpoint:          offer.Binding.Key.EndpointPattern,
			TargetId:          offer.Binding.Key.TargetID,
			CallerSessionId:   offer.Caller.ClientSessionID,
		},
	}}
	e.requestOrder.RLock()
	err := e.outbound.send(ctx, response)
	e.requestOrder.RUnlock()
	if err != nil {
		e.remove(offer.AttemptID, attempt)
		return endpointSendError(ctx, err)
	}

	select {
	case accepted := <-attempt.decision:
		if accepted {
			return nil
		}
		return localbinding.ErrOfferRejected
	case <-attempt.terminal:
		return localbinding.ErrEndpointUnavailable
	case <-e.ctx.Done():
		e.cancelAttempt(ctx, offer.AttemptID, attempt, "")
		return localbinding.ErrEndpointUnavailable
	case <-ctx.Done():
		e.cancelAttempt(ctx, offer.AttemptID, attempt, "")
		return ctx.Err()
	}
}

func validateOffer(ctx context.Context, offer localbinding.Offer) error {
	if ctx == nil || offer.AttemptID == "" || len(offer.AttemptID) > routing.MaxIdentityBytes ||
		offer.Caller.ClientSessionID == "" || offer.Binding.Validate() != nil {
		return fmt.Errorf("%w: invalid listener offer", localbinding.ErrEndpointUnavailable)
	}
	return nil
}

func (e *streamListenerEndpoint) Confirm(ctx context.Context, confirmation localbinding.Confirmation) error {
	if ctx == nil || confirmation.AttemptID == "" || confirmation.PipeID == "" ||
		len(confirmation.AttemptID) > routing.MaxIdentityBytes || len(confirmation.PipeID) > routing.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	attempt := e.attempts[confirmation.AttemptID]
	if attempt == nil {
		e.mu.Unlock()
		return errAttemptNotPending
	}
	if attempt.phase != listenerProvisional {
		e.mu.Unlock()
		return errWrongListenerPhase
	}
	attempt.phase = listenerConfirming
	attempt.pipeID = confirmation.PipeID
	e.mu.Unlock()

	response := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerEstablished{
		ListenerEstablished: &relayv1.ListenerEstablished{AttemptId: confirmation.AttemptID, PipeId: confirmation.PipeID},
	}}
	err := e.outbound.send(ctx, response)
	if err != nil {
		e.cancelAttempt(ctx, confirmation.AttemptID, attempt, confirmation.PipeID)
		return endpointSendError(ctx, err)
	}

	select {
	case <-attempt.confirmation:
		return nil
	case <-attempt.terminal:
		return localbinding.ErrEndpointUnavailable
	case <-e.ctx.Done():
		e.cancelAttempt(ctx, confirmation.AttemptID, attempt, confirmation.PipeID)
		return localbinding.ErrEndpointUnavailable
	case <-ctx.Done():
		e.cancelAttempt(ctx, confirmation.AttemptID, attempt, confirmation.PipeID)
		return ctx.Err()
	}
}

func endpointSendError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, err)
}

func (e *streamListenerEndpoint) Terminate(ctx context.Context, termination localbinding.Termination) error {
	if ctx == nil || termination.AttemptID == "" || len(termination.AttemptID) > routing.MaxIdentityBytes ||
		len(termination.PipeID) > routing.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	attempt := e.attempts[termination.AttemptID]
	if attempt == nil {
		e.mu.Unlock()
		return nil
	}
	delete(e.attempts, termination.AttemptID)
	close(attempt.terminal)
	e.mu.Unlock()

	terminalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.terminalSendTimeout)
	defer cancel()
	if err := e.pipeEndpoint.beginOutcome(terminalCtx); err != nil {
		e.outbound.fail(err)
		return err
	}
	err := e.outbound.send(terminalCtx, &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: termination.AttemptID, PipeId: termination.PipeID},
	}})
	e.pipeEndpoint.endOutcome()
	if err != nil {
		// The opening manager treats endpoint termination as best-effort. Turn a
		// bounded delivery failure into stream failure so session retirement, not
		// a silent terminal drop, is the fallback.
		e.outbound.fail(err)
	}
	return err
}

func (e *streamListenerEndpoint) accept(attemptID string) error {
	if attemptID == "" || len(attemptID) > routing.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	attempt := e.attempts[attemptID]
	if attempt == nil {
		return errAttemptNotPending
	}
	if attempt.phase != listenerOffered {
		return errWrongListenerPhase
	}
	attempt.phase = listenerProvisional
	attempt.decision <- true
	return nil
}

func (e *streamListenerEndpoint) reject(attemptID string) error {
	if attemptID == "" || len(attemptID) > routing.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	attempt := e.attempts[attemptID]
	if attempt == nil {
		return errAttemptNotPending
	}
	if attempt.phase != listenerOffered {
		return errWrongListenerPhase
	}
	delete(e.attempts, attemptID)
	attempt.decision <- false
	return nil
}

func (e *streamListenerEndpoint) confirmed(attemptID, pipeID string) error {
	if attemptID == "" || pipeID == "" || len(attemptID) > routing.MaxIdentityBytes || len(pipeID) > routing.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	attempt := e.attempts[attemptID]
	if attempt == nil {
		return errAttemptNotPending
	}
	if attempt.phase != listenerConfirming || attempt.pipeID != pipeID {
		return errWrongListenerPhase
	}
	attempt.phase = listenerOpen
	attempt.confirmation <- struct{}{}
	return nil
}

func (e *streamListenerEndpoint) cancelAttempt(parent context.Context, attemptID string, attempt *listenerAttempt, pipeID string) {
	if !e.remove(attemptID, attempt) {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), e.terminalSendTimeout)
	err := e.outbound.send(ctx, &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: attemptID, PipeId: pipeID},
	}})
	cancel()
	if err != nil {
		// Terminal signals may not be silently dropped behind listener traffic.
		// Failing the stream makes session retirement the bounded fallback.
		e.outbound.fail(err)
	}
}

func (e *streamListenerEndpoint) remove(attemptID string, attempt *listenerAttempt) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.attempts[attemptID] != attempt {
		return false
	}
	delete(e.attempts, attemptID)
	close(attempt.terminal)
	return true
}

func (e *streamListenerEndpoint) close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	for attemptID, attempt := range e.attempts {
		delete(e.attempts, attemptID)
		close(attempt.terminal)
	}
	e.mu.Unlock()
}
