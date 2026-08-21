package relaygate

import (
	"context"
	"errors"
	"fmt"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

func (c *Client) Bind(ctx context.Context, endpoint, target string) (*Listener, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if !validEndpoint(endpoint) || !validIdentity(target) {
		return nil, &BindError{Failure: BindingFailureInvalidRequest, Endpoint: endpoint, Target: target}
	}
	c.bindingMu.Lock()
	defer c.bindingMu.Unlock()
	call := &bindingCall{kind: bindingBind, endpoint: endpoint, target: target, result: make(chan bindingResult, 1)}
	c.mu.Lock()
	if c.pendingBinding != nil || len(c.listeners) >= maxListeners {
		c.mu.Unlock()
		return nil, &BindError{Failure: BindingFailureCapacityReached, Endpoint: endpoint, Target: target}
	}
	c.pendingBinding = call
	c.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{BindListener: &relayv1.BindListener{EndpointPattern: endpoint, TargetId: target}}}
	if err := c.send(ctx, request); err != nil {
		c.clearBinding(call)
		return nil, err
	}
	select {
	case result := <-call.result:
		return result.listener, result.err
	case <-ctx.Done():
		c.stop(ctx.Err())
		<-c.done
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.terminalError()
	}
}

func (c *Client) unbind(ctx context.Context, listener *Listener) error {
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	c.bindingMu.Lock()
	defer c.bindingMu.Unlock()
	call := &bindingCall{kind: bindingUnbind, id: listener.id, result: make(chan bindingResult, 1)}
	c.mu.Lock()
	if c.listeners[listener.id] != listener || c.pendingBinding != nil {
		c.mu.Unlock()
		return ErrListenerEnded
	}
	c.pendingBinding = call
	c.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_UnbindListener{UnbindListener: &relayv1.UnbindListener{ListenerBindingId: listener.id}}}
	if err := c.send(ctx, request); err != nil {
		c.clearBinding(call)
		return err
	}
	select {
	case result := <-call.result:
		return result.err
	case <-ctx.Done():
		c.stop(ctx.Err())
		<-c.done
		return ctx.Err()
	case <-c.done:
		return c.terminalError()
	}
}

// Open requests one exact endpoint and target. If ctx ends after the Open was
// written, the SDK sends CancelOpen and drains its acknowledgement and terminal
// outcome for a fixed bounded interval. An incomplete drain closes the Client
// session and returns ErrOpenUnknown so no late Pipe can be orphaned.
func (c *Client) Open(ctx context.Context, endpoint, target string) (*Pipe, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if !validEndpoint(endpoint) || !validIdentity(target) {
		return nil, &OpenError{Outcome: OpenOutcomeFailed, Failure: OpenFailureInvalidRequest, Endpoint: endpoint, Target: target}
	}
	requestID, err := randomRequestID()
	if err != nil {
		return nil, err
	}
	if !c.reservePipeSlot() {
		return nil, &OpenError{Outcome: OpenOutcomeFailed, Failure: OpenFailureCapacityReached, Endpoint: endpoint, Target: target}
	}
	call := &openCall{
		requestID: requestID, endpoint: endpoint, target: target,
		result: make(chan openResult, 1), retired: make(chan struct{}), reserved: true,
	}
	c.mu.Lock()
	if len(c.opens) >= maxPendingOpens {
		c.mu.Unlock()
		c.releaseOpenReservation(call)
		return nil, &OpenError{Outcome: OpenOutcomeFailed, Failure: OpenFailureCapacityReached, Endpoint: endpoint, Target: target}
	}
	c.opens[requestID] = call
	c.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Open{Open: &relayv1.Open{RequestId: requestID, Endpoint: endpoint, TargetId: target}}}
	if err := c.send(ctx, request); err != nil {
		c.removeOpen(call)
		var uncertain *sendUncertainError
		if errors.As(err, &uncertain) {
			return nil, &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: endpoint, Target: target, Cause: err}
		}
		return nil, err
	}
	select {
	case result := <-call.result:
		return result.pipe, result.err
	case <-ctx.Done():
		c.mu.Lock()
		if c.opens[requestID] != call || call.outcomeReceived {
			c.mu.Unlock()
			result := <-call.result
			return result.pipe, result.err
		}
		call.cancelRequested = true
		c.mu.Unlock()
		call.mu.Lock()
		call.abandoned = true
		call.mu.Unlock()
		return nil, c.drainCancelledOpen(call, ctx.Err()) //nolint:contextcheck // Cleanup must outlive the canceled caller and is bounded by Client lifetime.
	case <-c.done:
		c.removeOpen(call)
		return nil, &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: endpoint, Target: target, Cause: c.terminalError()}
	}
}

func (c *Client) drainCancelledOpen(call *openCall, callerErr error) error {
	drainCtx, cancelDrain := context.WithTimeout(c.ctx, openCancelDrainTimeout)
	defer cancelDrain()
	cancelRequest := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_CancelOpen{
		CancelOpen: &relayv1.CancelOpen{RequestId: call.requestID},
	}}
	if err := c.send(drainCtx, cancelRequest); err != nil {
		c.stop(err)
		<-c.done
		return &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: call.endpoint, Target: call.target, Cause: callerErr}
	}

	var result openResult
	var outcomeReceived bool
	var retired bool
	retiredSignal := call.retired
	for !outcomeReceived || !retired {
		select {
		case result = <-call.result:
			outcomeReceived = true
		case <-retiredSignal:
			retired = true
			retiredSignal = nil
		case <-drainCtx.Done():
			c.stop(drainCtx.Err())
			<-c.done
			if outcomeReceived && result.pipe == nil && result.err != nil {
				return result.err
			}
			return &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: call.endpoint, Target: call.target, Cause: callerErr}
		case <-c.done:
			return &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: call.endpoint, Target: call.target, Cause: c.terminalError()}
		}
	}
	if result.pipe == nil {
		return result.err
	}
	if err := result.pipe.Close(drainCtx); err != nil && !errors.Is(err, ErrPipeClosed) {
		c.stop(err)
		<-c.done
	}
	return &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: call.endpoint, Target: call.target, Cause: callerErr}
}

func (call *openCall) complete(result openResult) {
	select {
	case call.result <- result:
	default:
	}
}

func (c *Client) clearBinding(call *bindingCall) {
	c.mu.Lock()
	if c.pendingBinding == call {
		c.pendingBinding = nil
	}
	c.mu.Unlock()
}

func (c *Client) removeOffer(offer *Offer) {
	c.mu.Lock()
	if c.offers[offer.attemptID] == offer {
		delete(c.offers, offer.attemptID)
	}
	c.mu.Unlock()
}

func (c *Client) retireOffer(offer *Offer, pipeID string) {
	c.mu.Lock()
	if c.offers[offer.attemptID] == offer {
		delete(c.offers, offer.attemptID)
		c.addOfferTombstoneLocked(offer.attemptID, offerTombstone{pipeID: pipeID})
	}
	c.mu.Unlock()
}

func (c *Client) removeOpen(call *openCall) {
	c.mu.Lock()
	if c.opens[call.requestID] == call {
		delete(c.opens, call.requestID)
	}
	reserved := call.reserved
	call.reserved = false
	c.mu.Unlock()
	if reserved {
		c.releasePipeSlot()
	}
}

func (c *Client) reservePipeSlot() bool {
	select {
	case c.pipeSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *Client) releasePipeSlot() {
	select {
	case <-c.pipeSlots:
	default:
		c.stop(protocolError("Pipe slot released twice"))
	}
}

func (c *Client) releaseOpenReservation(call *openCall) {
	c.mu.Lock()
	reserved := call.reserved
	call.reserved = false
	c.mu.Unlock()
	if reserved {
		c.releasePipeSlot()
	}
}
