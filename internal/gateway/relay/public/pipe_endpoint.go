package relaygrpc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
)

type streamPipeEndpoint struct {
	outbound            *outboundActor
	terminalSendTimeout time.Duration
	outcomeGate         chan struct{}

	mu      sync.Mutex
	pending map[payloadReceiptKey]pendingPayloadReceipt
	history map[payloadReceiptKey]payloadReceiptOutcome
	order   []payloadReceiptKey
	closed  bool
}

type payloadReceiptKey struct {
	pipeID    string
	payloadID string
}

type payloadReceiptOutcome struct {
	received bool
	failure  relayv1.PipePayloadFailure
	unknown  bool
	hash     [sha256.Size]byte
}

type pendingPayloadReceipt struct {
	result chan error
	hash   [sha256.Size]byte
}

const maxPayloadReceiptHistory = 1024

func newStreamPipeEndpoint(outbound *outboundActor, terminalSendTimeout time.Duration) *streamPipeEndpoint {
	return &streamPipeEndpoint{
		outbound: outbound, terminalSendTimeout: terminalSendTimeout,
		pending: make(map[payloadReceiptKey]pendingPayloadReceipt), history: make(map[payloadReceiptKey]payloadReceiptOutcome),
		outcomeGate: make(chan struct{}, 1),
	}
}

func (e *streamPipeEndpoint) beginOutcome(ctx context.Context) error {
	select {
	case e.outcomeGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.outbound.done:
		return e.outbound.err()
	}
}

func (e *streamPipeEndpoint) endOutcome() {
	<-e.outcomeGate
}

func (e *streamPipeEndpoint) DeliverPayload(ctx context.Context, payload localbinding.PipePayload) error {
	if ctx == nil || payload.PipeID == "" || len(payload.PipeID) > routing.MaxIdentityBytes ||
		payload.PayloadID == "" || len(payload.PayloadID) > routing.MaxIdentityBytes ||
		len(payload.Data) == 0 || len(payload.Data) > localbinding.MaxPayloadBytes {
		return fmt.Errorf("%w: invalid Pipe payload", localbinding.ErrEndpointUnavailable)
	}
	key := payloadReceiptKey{pipeID: payload.PipeID, payloadID: payload.PayloadID}
	fingerprint := sha256.Sum256(payload.Data)
	result := make(chan error, 1)
	e.mu.Lock()
	if e.closed || len(e.pending) >= maxPayloadReceiptHistory {
		e.mu.Unlock()
		return localbinding.ErrEndpointUnavailable
	}
	if pending, exists := e.pending[key]; exists {
		e.mu.Unlock()
		if pending.hash != fingerprint {
			return fmt.Errorf("%w: conflicting duplicate payload", localbinding.ErrEndpointUnavailable)
		}
		return fmt.Errorf("%w: duplicate pending payload", localbinding.ErrEndpointUnavailable)
	}
	if outcome, exists := e.history[key]; exists {
		e.mu.Unlock()
		if outcome.hash != fingerprint {
			return fmt.Errorf("%w: conflicting duplicate payload", localbinding.ErrEndpointUnavailable)
		}
		if outcome.received {
			return nil
		}
		return fmt.Errorf("%w: payload outcome already retired", localbinding.ErrEndpointUnavailable)
	}
	e.pending[key] = pendingPayloadReceipt{result: result, hash: fingerprint}
	e.mu.Unlock()
	removePending := func(outcome *payloadReceiptOutcome) {
		e.mu.Lock()
		if pending, exists := e.pending[key]; exists && pending.result == result {
			delete(e.pending, key)
			if outcome != nil {
				outcome.hash = fingerprint
				e.rememberLocked(key, *outcome)
			}
		}
		e.mu.Unlock()
	}
	data := append([]byte(nil), payload.Data...)
	if err := e.outbound.sendPayload(ctx, &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayload{
		PipePayload: &relayv1.PipePayload{PipeId: payload.PipeID, PayloadId: payload.PayloadID, Payload: data},
	}}); err != nil {
		select {
		case outcome := <-result:
			return outcome
		default:
		}
		removePending(nil)
		return err
	}
	wait, cancel := context.WithTimeout(ctx, e.terminalSendTimeout)
	defer cancel()
	select {
	case err := <-result:
		return err
	case <-wait.Done():
		select {
		case outcome := <-result:
			return outcome
		default:
		}
		removePending(&payloadReceiptOutcome{unknown: true})
		return localbinding.ErrEndpointUnavailable
	case <-e.outbound.done:
		select {
		case outcome := <-result:
			return outcome
		default:
		}
		removePending(&payloadReceiptOutcome{unknown: true})
		return localbinding.ErrEndpointUnavailable
	}
}

func (e *streamPipeEndpoint) acknowledge(pipeID, payloadID string) error {
	if pipeID == "" || payloadID == "" || len(pipeID) > routing.MaxIdentityBytes || len(payloadID) > routing.MaxIdentityBytes {
		return localbinding.ErrEndpointUnavailable
	}
	key := payloadReceiptKey{pipeID: pipeID, payloadID: payloadID}
	e.mu.Lock()
	if pending, exists := e.pending[key]; exists {
		delete(e.pending, key)
		e.rememberLocked(key, payloadReceiptOutcome{received: true, hash: pending.hash})
		e.mu.Unlock()
		pending.result <- nil
		return nil
	}
	outcome, remembered := e.history[key]
	e.mu.Unlock()
	if remembered && (outcome.received || outcome.unknown) {
		return nil
	}
	return localbinding.ErrEndpointUnavailable
}

func (e *streamPipeEndpoint) reject(pipeID, payloadID string, failure relayv1.PipePayloadFailure) error {
	if pipeID == "" || payloadID == "" || len(pipeID) > routing.MaxIdentityBytes || len(payloadID) > routing.MaxIdentityBytes {
		return localbinding.ErrEndpointUnavailable
	}
	var rejection error
	switch failure {
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE:
		rejection = localbinding.ErrPayloadBackpressure
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST,
		relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED,
		relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE:
		rejection = localbinding.ErrEndpointUnavailable
	default:
		return localbinding.ErrEndpointUnavailable
	}
	key := payloadReceiptKey{pipeID: pipeID, payloadID: payloadID}
	e.mu.Lock()
	if pending, exists := e.pending[key]; exists {
		delete(e.pending, key)
		e.rememberLocked(key, payloadReceiptOutcome{failure: failure, hash: pending.hash})
		e.mu.Unlock()
		pending.result <- rejection
		return nil
	}
	outcome, remembered := e.history[key]
	e.mu.Unlock()
	if remembered && (outcome.unknown || (!outcome.received && outcome.failure == failure)) {
		return nil
	}
	return localbinding.ErrEndpointUnavailable
}

func (e *streamPipeEndpoint) rememberLocked(key payloadReceiptKey, outcome payloadReceiptOutcome) {
	if _, exists := e.history[key]; exists {
		return
	}
	e.history[key] = outcome
	e.order = append(e.order, key)
	if len(e.order) > maxPayloadReceiptHistory {
		oldest := e.order[0]
		e.order = e.order[1:]
		delete(e.history, oldest)
	}
}

func (e *streamPipeEndpoint) close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	pending := e.pending
	e.pending = make(map[payloadReceiptKey]pendingPayloadReceipt)
	e.mu.Unlock()
	for _, pending := range pending {
		pending.result <- localbinding.ErrEndpointUnavailable
	}
}

func (e *streamPipeEndpoint) TerminatePipe(parent context.Context, pipeID string) error {
	if parent == nil || pipeID == "" || len(pipeID) > routing.MaxIdentityBytes {
		return fmt.Errorf("%w: invalid Pipe terminal", localbinding.ErrEndpointUnavailable)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), e.terminalSendTimeout)
	if err := e.beginOutcome(ctx); err != nil {
		cancel()
		return err
	}
	err := e.outbound.send(ctx, &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeTerminated{
		PipeTerminated: &relayv1.PipeTerminated{PipeId: pipeID},
	}})
	e.endOutcome()
	cancel()
	if err != nil {
		e.outbound.fail(err)
	}
	return err
}
