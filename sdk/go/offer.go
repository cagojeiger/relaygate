package relaygate

import (
	"context"
	"fmt"
	"sync"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
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
