package relaygate

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

type Pipe struct {
	client    *Client
	id        string
	attemptID string
	endpoint  string
	target    string
	payloads  chan receivedPayload
	done      chan struct{}

	termOnce  sync.Once
	slotOnce  sync.Once
	admission chan struct{}
	mu        sync.Mutex
	err       error
	closing   bool
	terminal  bool

	delivery            *deliveryCall
	lastDeliveryID      string
	lastDeliveryOutcome DeliveryOutcome
	lastDeliveryFailure PipePayloadFailure
	receiveMu           sync.Mutex
	received            map[string][sha256.Size]byte
	receivedOrder       []string
}

type deliveryCall struct {
	payloadID string
	result    chan error
}

type receivedPayload struct {
	data         []byte
	acknowledged chan struct{}
}

func newPipe(client *Client, id, attemptID, endpoint, target string) *Pipe {
	pipe := &Pipe{
		client: client, id: id, attemptID: attemptID, endpoint: endpoint, target: target,
		payloads: make(chan receivedPayload, pipePayloadQueueCapacity), done: make(chan struct{}),
		admission: make(chan struct{}, 1), received: make(map[string][sha256.Size]byte),
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

// Send succeeds only after the remote SDK admits the exact payload to its
// bounded receive queue. A DeliveryError distinguishes retry-safe NotSent from
// Rejected and post-handoff Unknown outcomes.
func (p *Pipe) Send(ctx context.Context, payload []byte) error {
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	if len(payload) == 0 || len(payload) > maxPayloadBytes {
		return &DeliveryError{Outcome: DeliveryNotSent, Cause: fmt.Errorf("relaygate: payload must contain 1..%d bytes", maxPayloadBytes)}
	}
	payloadID, err := randomRequestID()
	if err != nil {
		return &DeliveryError{Outcome: DeliveryNotSent, Cause: err}
	}
	data := append([]byte(nil), payload...)
	if err := p.acquireAdmission(ctx); err != nil {
		return &DeliveryError{PayloadID: payloadID, Outcome: DeliveryNotSent, Cause: err}
	}
	defer p.releaseAdmission()
	p.mu.Lock()
	unavailable := p.closing || p.terminal
	call := &deliveryCall{payloadID: payloadID, result: make(chan error, 1)}
	if !unavailable {
		p.delivery = call
	}
	p.mu.Unlock()
	if unavailable {
		return &DeliveryError{PayloadID: payloadID, Outcome: DeliveryNotSent, Cause: ErrPipeClosed}
	}
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayload{PipePayload: &relayv1.PipePayload{PipeId: p.id, PayloadId: payloadID, Payload: data}}}
	command, err := p.client.enqueue(ctx, request)
	if err != nil {
		p.finishDelivery(payloadID, DeliveryNotSent, PipePayloadFailure(0))
		return &DeliveryError{PayloadID: payloadID, Outcome: DeliveryNotSent, Cause: err}
	}
	if err := p.client.awaitSend(ctx, command); err != nil {
		var uncertain *sendUncertainError
		if !errors.As(err, &uncertain) {
			p.finishDelivery(payloadID, DeliveryNotSent, PipePayloadFailure(0))
			return &DeliveryError{PayloadID: payloadID, Outcome: DeliveryNotSent, Cause: err}
		}
		p.finishDelivery(payloadID, DeliveryUnknown, PipePayloadFailure(0))
		return &DeliveryError{PayloadID: payloadID, Outcome: DeliveryUnknown, Cause: err}
	}
	select {
	case err := <-call.result:
		return err
	case <-ctx.Done():
		select {
		case err := <-call.result:
			return err
		default:
		}
		p.finishDelivery(payloadID, DeliveryUnknown, PipePayloadFailure(0))
		p.client.removePipe(p)
		unknown := &DeliveryError{PayloadID: payloadID, Outcome: DeliveryUnknown, Cause: ctx.Err()}
		p.terminate(unknown)
		go p.closeAfterTerminal()
		return unknown
	case <-p.client.done:
		select {
		case err := <-call.result:
			return err
		default:
		}
		p.finishDelivery(payloadID, DeliveryUnknown, PipePayloadFailure(0))
		return &DeliveryError{PayloadID: payloadID, Outcome: DeliveryUnknown, Cause: p.client.terminalError()}
	}
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
		select {
		case <-payload.acknowledged:
			return payload.data, nil
		case <-p.done:
			return nil, p.terminalError()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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

func (p *Pipe) deliver(payloadID string, payload []byte) (accepted *receivedPayload, duplicate, conflict bool) {
	fingerprint := sha256.Sum256(payload)
	p.receiveMu.Lock()
	defer p.receiveMu.Unlock()
	if known, exists := p.received[payloadID]; exists {
		if known == fingerprint {
			return nil, true, false
		}
		return nil, false, true
	}
	select {
	case <-p.done:
		return nil, false, false
	default:
	}
	received := receivedPayload{data: payload, acknowledged: make(chan struct{})}
	select {
	case p.payloads <- received:
		p.received[payloadID] = fingerprint
		p.receivedOrder = append(p.receivedOrder, payloadID)
		if len(p.receivedOrder) > maxReceivedPayloads {
			oldest := p.receivedOrder[0]
			p.receivedOrder = p.receivedOrder[1:]
			delete(p.received, oldest)
		}
		return &received, false, false
	default:
		return nil, false, false
	}
}

func (p *Pipe) finishDelivery(payloadID string, outcome DeliveryOutcome, failure PipePayloadFailure) (matched, duplicate bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.delivery != nil {
		if p.delivery.payloadID != payloadID {
			return false, false
		}
		call := p.delivery
		p.delivery = nil
		p.lastDeliveryID = payloadID
		p.lastDeliveryOutcome = outcome
		p.lastDeliveryFailure = failure
		var result error
		switch outcome {
		case DeliveryReceived:
		case DeliveryNotSent:
			result = &DeliveryError{PayloadID: payloadID, Outcome: outcome, Cause: ErrDeliveryNotSent}
		case DeliveryRejected:
			result = &DeliveryError{PayloadID: payloadID, Outcome: outcome, Cause: &PipeError{Failure: failure}}
		case DeliveryUnknown:
			result = &DeliveryError{PayloadID: payloadID, Outcome: outcome, Cause: ErrDeliveryUnknown}
		}
		call.result <- result
		return true, false
	}
	if p.lastDeliveryID == payloadID && p.lastDeliveryOutcome == outcome && p.lastDeliveryFailure == failure {
		return true, true
	}
	if p.lastDeliveryID == payloadID && p.lastDeliveryOutcome == DeliveryUnknown {
		return true, true
	}
	return false, false
}

func (p *Pipe) terminate(err error) {
	p.termOnce.Do(func() {
		p.mu.Lock()
		if p.delivery != nil {
			call := p.delivery
			p.delivery = nil
			p.lastDeliveryID = call.payloadID
			p.lastDeliveryOutcome = DeliveryUnknown
			call.result <- &DeliveryError{PayloadID: call.payloadID, Outcome: DeliveryUnknown, Cause: err}
		}
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
