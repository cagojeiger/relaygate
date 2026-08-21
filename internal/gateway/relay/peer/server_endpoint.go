package gatewayrelay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
)

type serverCallerEndpoint struct {
	outbound     *sendActor[*gatewayv1.ForwardResponse]
	cancel       context.CancelCauseFunc
	timeout      time.Duration
	pipeMu       sync.Mutex
	pipeID       string
	accepted     chan struct{}
	acceptedOnce sync.Once
	terminalOnce sync.Once
	receipts     peerReceiptState
}

func newServerCallerEndpoint(outbound *sendActor[*gatewayv1.ForwardResponse], cancel context.CancelCauseFunc, timeout time.Duration) *serverCallerEndpoint {
	return &serverCallerEndpoint{outbound: outbound, cancel: cancel, timeout: timeout, accepted: make(chan struct{})}
}

func (e *serverCallerEndpoint) setPipeID(pipeID string) {
	e.pipeMu.Lock()
	if e.pipeID == "" {
		e.pipeID = pipeID
	} else if e.pipeID != pipeID {
		e.cancel(opening.ErrUnknown)
	}
	e.pipeMu.Unlock()
}

func (e *serverCallerEndpoint) markAccepted() {
	e.acceptedOnce.Do(func() { close(e.accepted) })
}

func (e *serverCallerEndpoint) DeliverPayload(parent context.Context, payload localbinding.PipePayload) error {
	e.pipeMu.Lock()
	pipeID := e.pipeID
	e.pipeMu.Unlock()
	if parent == nil || pipeID == "" || payload.PipeID != pipeID || payload.PayloadID == "" || len(payload.PayloadID) > routing.MaxIdentityBytes || len(payload.Data) == 0 || len(payload.Data) > localbinding.MaxPayloadBytes {
		return fmt.Errorf("%w: invalid remote caller payload", localbinding.ErrEndpointUnavailable)
	}
	ctx, cancel := context.WithTimeout(parent, e.timeout)
	defer cancel()
	select {
	case <-e.accepted:
	case <-ctx.Done():
		e.cancel(ctx.Err())
		return localbinding.ErrEndpointUnavailable
	}
	result, duplicate, err := e.receipts.begin(payload)
	if duplicate || err != nil {
		return err
	}
	err = e.outbound.send(ctx, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_PipePayload{
		PipePayload: &gatewayv1.PipePayload{PipeId: pipeID, PayloadId: payload.PayloadID, Payload: append([]byte(nil), payload.Data...)},
	}})
	if err != nil {
		select {
		case outcome := <-result:
			return outcome
		default:
		}
		e.receipts.retireUnknown(payload.PayloadID, result)
		e.cancel(err)
		if isSendContextError(err) {
			return localbinding.ErrPayloadBackpressure
		}
		return localbinding.ErrEndpointUnavailable
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		select {
		case outcome := <-result:
			return outcome
		default:
		}
		e.receipts.retireUnknown(payload.PayloadID, result)
		e.cancel(ctx.Err())
		return localbinding.ErrEndpointUnavailable
	}
}

func (e *serverCallerEndpoint) acknowledgePayload(payloadID string) error {
	if payloadID == "" || len(payloadID) > routing.MaxIdentityBytes {
		return localbinding.ErrEndpointUnavailable
	}
	return e.receipts.acknowledge(payloadID)
}

func (e *serverCallerEndpoint) rejectPayload(payloadID string, failure gatewayv1.PipePayloadFailure) error {
	if payloadID == "" || len(payloadID) > routing.MaxIdentityBytes {
		return localbinding.ErrEndpointUnavailable
	}
	return e.receipts.reject(payloadID, failure)
}

func (e *serverCallerEndpoint) TerminatePipe(parent context.Context, pipeID string) error {
	e.pipeMu.Lock()
	expected := e.pipeID
	if expected == "" && pipeID != "" && len(pipeID) <= routing.MaxIdentityBytes {
		e.pipeID = pipeID
		expected = pipeID
	}
	e.pipeMu.Unlock()
	if parent == nil || pipeID == "" || expected == "" || pipeID != expected {
		return fmt.Errorf("%w: invalid remote caller terminal", localbinding.ErrEndpointUnavailable)
	}
	var terminalErr error
	e.terminalOnce.Do(func() {
		wait, cancelWait := context.WithTimeout(parent, e.timeout)
		select {
		case <-e.accepted:
		case <-wait.Done():
			terminalErr = wait.Err()
			e.cancel(wait.Err())
			cancelWait()
			return
		}
		cancelWait()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), e.timeout)
		terminalErr = e.outbound.send(ctx, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_PipeTerminal{
			PipeTerminal: &gatewayv1.PipeTerminal{PipeId: pipeID},
		}})
		cancel()
		e.cancel(context.Canceled)
	})
	return terminalErr
}
