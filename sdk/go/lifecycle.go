package relaygate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

type Listener struct {
	client   *Client
	id       string
	endpoint string
	target   string
	offers   chan *Offer
	done     chan struct{}

	endOnce sync.Once
	mu      sync.Mutex
	err     error
}

func newListener(client *Client, id, endpoint, target string) *Listener {
	return &Listener{client: client, id: id, endpoint: endpoint, target: target, offers: make(chan *Offer, offerQueueCapacity), done: make(chan struct{})}
}

func (l *Listener) ID() string       { return l.id }
func (l *Listener) Endpoint() string { return l.endpoint }
func (l *Listener) Target() string   { return l.target }

func (l *Listener) Next(ctx context.Context) (*Offer, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	select {
	case offer := <-l.offers:
		return offer, nil
	default:
	}
	select {
	case offer := <-l.offers:
		return offer, nil
	case <-l.done:
		return nil, l.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *Listener) Unbind(ctx context.Context) error {
	if l == nil || l.client == nil {
		return ErrListenerEnded
	}
	return l.client.unbind(ctx, l)
}

func (l *Listener) enqueue(offer *Offer) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	select {
	case l.offers <- offer:
		return true
	default:
		return false
	}
}

func (l *Listener) end(err error) {
	l.endOnce.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}

func (l *Listener) terminalError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	return ErrListenerEnded
}

type offerState uint8

const (
	offerPending offerState = iota + 1
	offerAccepting
	offerTerminal
)

type Offer struct {
	listener        *Listener
	attemptID       string
	bindingID       string
	endpoint        string
	target          string
	callerSessionID string

	mu          sync.Mutex
	state       offerState
	abandoned   bool
	delivered   *Pipe
	provisional *Pipe
	reserved    bool
	established chan *Pipe
	ack         chan *Pipe
	failure     chan error
	result      chan openResult
}

func newOffer(listener *Listener, attemptID, callerSessionID string) *Offer {
	return &Offer{
		listener: listener, attemptID: attemptID, bindingID: listener.id,
		endpoint: listener.endpoint, target: listener.target, callerSessionID: callerSessionID,
		state: offerPending, established: make(chan *Pipe, 1), ack: make(chan *Pipe, 1),
		failure: make(chan error, 1), result: make(chan openResult, 1),
	}
}

func (o *Offer) AttemptID() string       { return o.attemptID }
func (o *Offer) ListenerID() string      { return o.bindingID }
func (o *Offer) Endpoint() string        { return o.endpoint }
func (o *Offer) Target() string          { return o.target }
func (o *Offer) CallerSessionID() string { return o.callerSessionID }

func (o *Offer) Accept(ctx context.Context) (*Pipe, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !o.listener.client.reservePipeSlot() {
		return nil, errCapacity
	}
	o.mu.Lock()
	if o.state != offerPending {
		o.mu.Unlock()
		o.listener.client.releasePipeSlot()
		return nil, errAlreadyDecided
	}
	o.state = offerAccepting
	o.reserved = true
	o.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{ListenerAccept: &relayv1.ListenerAccept{AttemptId: o.attemptID}}}
	if err := o.listener.client.send(ctx, request); err != nil {
		o.releaseReservation()
		o.terminate(err)
		o.listener.client.removeOffer(o)
		return nil, err
	}
	go o.confirmAccepted()

	select {
	case result := <-o.result:
		return result.pipe, result.err
	case <-ctx.Done():
		o.mu.Lock()
		if o.delivered != nil {
			pipe := o.delivered
			o.mu.Unlock()
			return pipe, nil
		}
		o.abandoned = true
		o.mu.Unlock()
		return nil, ctx.Err()
	case <-o.listener.client.done:
		return nil, o.listener.client.terminalError()
	}
}

func (o *Offer) confirmAccepted() {
	var pipe *Pipe
	select {
	case pipe = <-o.established:
	case err := <-o.failure:
		o.complete(openResult{err: err})
		return
	case <-o.listener.client.done:
		o.complete(openResult{err: o.listener.client.terminalError()})
		return
	}
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
		ListenerConfirmed: &relayv1.ListenerConfirmed{AttemptId: o.attemptID, PipeId: pipe.id},
	}}
	if err := o.listener.client.send(o.listener.client.ctx, request); err != nil {
		o.listener.client.removePipe(pipe)
		pipe.terminate(err)
		o.complete(openResult{err: err})
		return
	}
	select {
	case acknowledged := <-o.ack:
		if acknowledged != pipe {
			o.listener.client.stop(protocolError("mismatched confirmation acknowledgement"))
			return
		}
		o.mu.Lock()
		abandoned := o.abandoned
		if !abandoned {
			o.delivered = pipe
		}
		o.state = offerTerminal
		o.mu.Unlock()
		if abandoned {
			_ = pipe.Close(o.listener.client.ctx)
			o.complete(openResult{err: context.Canceled})
			return
		}
		o.complete(openResult{pipe: pipe})
	case err := <-o.failure:
		o.listener.client.removePipe(pipe)
		pipe.terminate(err)
		o.complete(openResult{err: err})
	case <-o.listener.client.done:
		o.complete(openResult{err: o.listener.client.terminalError()})
	}
}

func (o *Offer) Reject(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	o.mu.Lock()
	if o.state != offerPending {
		o.mu.Unlock()
		return errAlreadyDecided
	}
	o.state = offerTerminal
	o.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerReject{ListenerReject: &relayv1.ListenerReject{AttemptId: o.attemptID}}}
	if err := o.listener.client.send(ctx, request); err != nil {
		return err
	}
	o.listener.client.retireOffer(o, "")
	return nil
}

func (o *Offer) isAccepting() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.state == offerAccepting
}

func (o *Offer) establish(pipe *Pipe) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != offerAccepting || o.provisional != nil {
		return false
	}
	select {
	case o.established <- pipe:
		o.provisional = pipe
		return true
	default:
		return false
	}
}

func (o *Offer) transferReservation() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.reserved {
		return false
	}
	o.reserved = false
	return true
}

func (o *Offer) releaseReservation() {
	o.mu.Lock()
	reserved := o.reserved
	o.reserved = false
	o.mu.Unlock()
	if reserved {
		o.listener.client.releasePipeSlot()
	}
}

func (o *Offer) acknowledge(pipe *Pipe) bool {
	if !o.isAccepting() {
		return false
	}
	select {
	case o.ack <- pipe:
		return true
	default:
		return false
	}
}

func (o *Offer) markDecisionRejected() (*Pipe, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != offerAccepting {
		return nil, false
	}
	o.state = offerTerminal
	return o.provisional, true
}

func (o *Offer) finishDecisionRejection(err error) {
	o.releaseReservation()
	select {
	case o.failure <- err:
	default:
	}
}

func (o *Offer) terminate(err error) {
	o.mu.Lock()
	if o.state == offerTerminal {
		o.mu.Unlock()
		return
	}
	o.state = offerTerminal
	o.mu.Unlock()
	o.releaseReservation()
	select {
	case o.failure <- err:
	default:
	}
}

func (o *Offer) complete(result openResult) {
	select {
	case o.result <- result:
	default:
	}
}

type Pipe struct {
	client    *Client
	id        string
	attemptID string
	endpoint  string
	target    string
	payloads  chan []byte
	done      chan struct{}

	termOnce  sync.Once
	slotOnce  sync.Once
	admission chan struct{}
	mu        sync.Mutex
	err       error
	closing   bool
	terminal  bool
}

func newPipe(client *Client, id, attemptID, endpoint, target string) *Pipe {
	pipe := &Pipe{
		client: client, id: id, attemptID: attemptID, endpoint: endpoint, target: target,
		payloads: make(chan []byte, pipePayloadQueueCapacity), done: make(chan struct{}),
		admission: make(chan struct{}, 1),
	}
	pipe.admission <- struct{}{}
	return pipe
}

func (p *Pipe) ID() string            { return p.id }
func (p *Pipe) AttemptID() string     { return p.attemptID }
func (p *Pipe) Endpoint() string      { return p.endpoint }
func (p *Pipe) Target() string        { return p.target }
func (p *Pipe) Done() <-chan struct{} { return p.done }

func (p *Pipe) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Pipe) Send(ctx context.Context, payload []byte) error {
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	if len(payload) == 0 || len(payload) > maxPayloadBytes {
		return fmt.Errorf("relaygate: payload must contain 1..%d bytes", maxPayloadBytes)
	}
	data := append([]byte(nil), payload...)
	if err := p.acquireAdmission(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	unavailable := p.closing || p.terminal
	p.mu.Unlock()
	if unavailable {
		p.releaseAdmission()
		return ErrPipeClosed
	}
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayload{PipePayload: &relayv1.PipePayload{PipeId: p.id, Payload: data}}}
	command, err := p.client.enqueue(ctx, request)
	p.releaseAdmission()
	if err != nil {
		return err
	}
	return p.client.awaitSend(ctx, command)
}

func (p *Pipe) Recv(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	select {
	case <-p.done:
		return nil, p.terminalError()
	default:
	}
	select {
	case payload := <-p.payloads:
		return payload, nil
	case <-p.done:
		return nil, p.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Pipe) Close(ctx context.Context) error {
	if p == nil || p.client == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	if err := p.acquireAdmission(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		p.releaseAdmission()
		select {
		case <-p.done:
			return p.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.terminal {
		p.mu.Unlock()
		p.releaseAdmission()
		return nil
	}
	p.closing = true
	p.mu.Unlock()
	call := &closeCall{pipe: p, result: make(chan error, 1)}
	p.client.mu.Lock()
	if p.client.pipes[p.id] != p {
		p.client.mu.Unlock()
		p.releaseAdmission()
		p.terminate(ErrPipeClosed)
		return ErrPipeClosed
	}
	p.client.closeCalls[p.id] = call
	p.client.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{ClosePipe: &relayv1.ClosePipe{PipeId: p.id}}}
	command, err := p.client.enqueue(ctx, request)
	p.releaseAdmission()
	if err != nil {
		p.client.mu.Lock()
		delete(p.client.closeCalls, p.id)
		p.client.mu.Unlock()
		p.terminate(err)
		return err
	}
	if err := p.client.awaitSend(ctx, command); err != nil {
		p.client.mu.Lock()
		delete(p.client.closeCalls, p.id)
		p.client.mu.Unlock()
		p.terminate(err)
		return err
	}
	select {
	case err := <-call.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.client.done:
		return p.client.terminalError()
	}
}

// acquireAdmission serializes Pipe state checks with outbound queue insertion.
// The gate is released before waiting for the single sender's network result.
func (p *Pipe) acquireAdmission(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-p.admission:
		return nil
	default:
	}
	select {
	case <-p.admission:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.client.done:
		return p.client.terminalError()
	}
}

func (p *Pipe) releaseAdmission() {
	p.admission <- struct{}{}
}

func (p *Pipe) deliver(payload []byte) bool {
	select {
	case <-p.done:
		return false
	default:
	}
	select {
	case p.payloads <- payload:
		return true
	default:
		return false
	}
}

func (p *Pipe) terminate(err error) {
	p.termOnce.Do(func() {
		p.mu.Lock()
		p.err = err
		p.terminal = true
		p.mu.Unlock()
		close(p.done)
		p.slotOnce.Do(p.client.releasePipeSlot)
	})
}

func (p *Pipe) terminalError() error {
	if err := p.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (p *Pipe) closeAfterTerminal() {
	call := &closeCall{pipe: p, result: make(chan error, 1)}
	p.client.mu.Lock()
	if _, exists := p.client.closeCalls[p.id]; exists {
		p.client.mu.Unlock()
		return
	}
	p.client.closeCalls[p.id] = call
	p.client.mu.Unlock()
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{ClosePipe: &relayv1.ClosePipe{PipeId: p.id}}}
	if err := p.client.send(p.client.ctx, request); err != nil {
		p.client.mu.Lock()
		delete(p.client.closeCalls, p.id)
		p.client.mu.Unlock()
	}
}

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
