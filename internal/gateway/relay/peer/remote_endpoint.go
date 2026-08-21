package gatewayrelay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
	"google.golang.org/grpc"
)

type remoteEndpoint struct {
	client         *Client
	cancel         context.CancelCauseFunc
	connection     *grpc.ClientConn
	stream         grpc.BidiStreamingClient[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse]
	sender         *sendActor[*gatewayv1.ForwardRequest]
	callerEndpoint localbinding.CallerEndpoint
	timeout        time.Duration
	attemptID      string
	pipeID         string
	binding        routing.LiveBinding
	inbound        chan localbinding.PipePayload
	done           chan struct{}

	activateOnce sync.Once
	activateDone chan struct{}
	activateErr  error
	workers      sync.WaitGroup
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
	outcomeGate  chan struct{}

	receipts peerReceiptState
}

func newRemoteEndpoint(
	client *Client,
	cancel context.CancelCauseFunc,
	connection *grpc.ClientConn,
	stream grpc.BidiStreamingClient[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse],
	sender *sendActor[*gatewayv1.ForwardRequest],
	callerEndpoint localbinding.CallerEndpoint,
	attemptID, pipeID string,
	binding routing.LiveBinding,
) *remoteEndpoint {
	return &remoteEndpoint{
		client:         client,
		cancel:         cancel,
		connection:     connection,
		stream:         stream,
		sender:         sender,
		callerEndpoint: callerEndpoint,
		timeout:        client.openTimeout,
		attemptID:      attemptID,
		pipeID:         pipeID,
		binding:        cloneBinding(binding),
		inbound:        make(chan localbinding.PipePayload, 1),
		done:           make(chan struct{}),
		activateDone:   make(chan struct{}),
		closeDone:      make(chan struct{}),
		outcomeGate:    make(chan struct{}, 1),
	}
}

func (e *remoteEndpoint) start(ctx context.Context) {
	e.workers.Add(2)
	go e.receive()
	go e.deliver(ctx)
	go e.cleanup(ctx)
}

func (e *remoteEndpoint) DeliverPayload(parent context.Context, payload localbinding.PipePayload) error {
	if parent == nil || payload.PipeID != e.pipeID || payload.PayloadID == "" || len(payload.PayloadID) > routing.MaxIdentityBytes || len(payload.Data) == 0 || len(payload.Data) > localbinding.MaxPayloadBytes {
		return fmt.Errorf("%w: invalid remote Pipe payload", localbinding.ErrEndpointUnavailable)
	}
	ctx, cancel := context.WithTimeout(parent, e.timeout)
	defer cancel()
	result, duplicate, err := e.receipts.begin(payload)
	if duplicate || err != nil {
		return err
	}
	err = e.sender.send(ctx, &gatewayv1.ForwardRequest{Message: &gatewayv1.ForwardRequest_PipePayload{
		PipePayload: &gatewayv1.PipePayload{PipeId: e.pipeID, PayloadId: payload.PayloadID, Payload: append([]byte(nil), payload.Data...)},
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
	case <-e.done:
		select {
		case outcome := <-result:
			return outcome
		default:
		}
		e.receipts.retireUnknown(payload.PayloadID, result)
		return localbinding.ErrEndpointUnavailable
	}
}

func (e *remoteEndpoint) acknowledgePayload(payloadID string) error {
	if payloadID == "" || len(payloadID) > routing.MaxIdentityBytes {
		return localbinding.ErrEndpointUnavailable
	}
	return e.receipts.acknowledge(payloadID)
}

func (e *remoteEndpoint) rejectPayload(payloadID string, failure gatewayv1.PipePayloadFailure) error {
	if payloadID == "" || len(payloadID) > routing.MaxIdentityBytes {
		return localbinding.ErrEndpointUnavailable
	}
	return e.receipts.reject(payloadID, failure)
}

func (e *remoteEndpoint) Activate(parent context.Context) error {
	if parent == nil {
		return ErrInvalid
	}
	e.activateOnce.Do(func() {
		defer close(e.activateDone)
		ctx, cancel := context.WithTimeout(parent, e.timeout)
		defer cancel()
		if err := e.sender.send(ctx, &gatewayv1.ForwardRequest{Message: &gatewayv1.ForwardRequest_ActivatePipe{
			ActivatePipe: &gatewayv1.ActivatePipe{PipeId: e.pipeID},
		}}); err != nil {
			e.activateErr = fmt.Errorf("%w: activate owner Pipe: %w", opening.ErrUnavailable, err)
			e.cancel(err)
			return
		}
	})
	select {
	case <-e.activateDone:
		return e.activateErr
	case <-parent.Done():
		return parent.Err()
	}
}

func (e *remoteEndpoint) Close(parent context.Context) error {
	if parent == nil {
		return ErrInvalid
	}
	e.closeOnce.Do(func() {
		defer close(e.closeDone)
		select {
		case <-e.done:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(parent, e.timeout)
		err := e.beginOutcome(ctx)
		if err == nil {
			err = e.sender.send(ctx, &gatewayv1.ForwardRequest{Message: &gatewayv1.ForwardRequest_ClosePipe{
				ClosePipe: &gatewayv1.ClosePipe{PipeId: e.pipeID},
			}})
			if err == nil {
				err = e.stream.CloseSend()
			}
			e.endOutcome()
		}
		cancel()
		if err != nil {
			e.closeErr = err
			e.cancel(err)
		} else {
			wait := time.NewTimer(e.timeout)
			select {
			case <-e.done:
				wait.Stop()
			case <-parent.Done():
				wait.Stop()
				e.closeErr = parent.Err()
				e.cancel(parent.Err())
			case <-wait.C:
				e.closeErr = context.DeadlineExceeded
				e.cancel(context.DeadlineExceeded)
			}
		}
		<-e.done
	})
	select {
	case <-e.closeDone:
		return e.closeErr
	case <-parent.Done():
		return parent.Err()
	}
}

func (e *remoteEndpoint) Done() <-chan struct{} {
	return e.done
}

func (e *remoteEndpoint) receive() {
	defer e.workers.Done()
	for {
		response, err := e.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = opening.ErrUnavailable
			}
			e.cancel(err)
			return
		}
		if payload := response.GetPipePayload(); payload != nil {
			if payload.GetPipeId() != e.pipeID || payload.GetPayloadId() == "" || len(payload.GetPayloadId()) > routing.MaxIdentityBytes || len(payload.GetPayload()) == 0 || len(payload.GetPayload()) > localbinding.MaxPayloadBytes {
				e.cancel(opening.ErrUnavailable)
				return
			}
			item := localbinding.PipePayload{PipeID: e.pipeID, PayloadID: payload.GetPayloadId(), Data: append([]byte(nil), payload.GetPayload()...)}
			select {
			case e.inbound <- item:
			default:
				e.cancel(ErrBackpressure)
				return
			}
			continue
		}
		if receipt := response.GetPipePayloadReceived(); receipt != nil {
			if receipt.GetPipeId() != e.pipeID || e.acknowledgePayload(receipt.GetPayloadId()) != nil {
				e.cancel(opening.ErrUnavailable)
				return
			}
			continue
		}
		if rejection := response.GetPipePayloadRejected(); rejection != nil {
			if rejection.GetPipeId() != e.pipeID || e.rejectPayload(rejection.GetPayloadId(), rejection.GetFailure()) != nil {
				e.cancel(opening.ErrUnavailable)
				return
			}
			continue
		}
		if terminal := response.GetPipeTerminal(); terminal != nil && terminal.GetPipeId() == e.pipeID {
			e.cancel(context.Canceled)
			return
		}
		e.cancel(opening.ErrUnavailable)
		return
	}
}

func (e *remoteEndpoint) deliver(ctx context.Context) {
	defer e.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-e.inbound:
			if err := e.beginOutcome(ctx); err != nil {
				return
			}
			deliveryCtx, cancel := context.WithTimeout(ctx, e.timeout)
			err := e.callerEndpoint.DeliverPayload(deliveryCtx, payload)
			cancel()
			if err != nil {
				rejectCtx, cancelReject := context.WithTimeout(context.WithoutCancel(ctx), e.timeout)
				_ = e.sender.send(rejectCtx, &gatewayv1.ForwardRequest{Message: &gatewayv1.ForwardRequest_PipePayloadRejected{
					PipePayloadRejected: &gatewayv1.PipePayloadRejected{
						PipeId: e.pipeID, PayloadId: payload.PayloadID, Failure: peerPayloadFailure(err),
					},
				}})
				cancelReject()
				e.endOutcome()
				e.cancel(err)
				return
			}
			receiptCtx, cancelReceipt := context.WithTimeout(ctx, e.timeout)
			err = e.sender.send(receiptCtx, &gatewayv1.ForwardRequest{Message: &gatewayv1.ForwardRequest_PipePayloadReceived{
				PipePayloadReceived: &gatewayv1.PipePayloadReceived{PipeId: e.pipeID, PayloadId: payload.PayloadID},
			}})
			cancelReceipt()
			e.endOutcome()
			if err != nil {
				e.cancel(err)
				return
			}
		}
	}
}

func (e *remoteEndpoint) beginOutcome(ctx context.Context) error {
	select {
	case e.outcomeGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return ErrClosed
	}
}

func (e *remoteEndpoint) endOutcome() {
	<-e.outcomeGate
}

func peerPayloadFailure(err error) gatewayv1.PipePayloadFailure {
	switch {
	case errors.Is(err, localbinding.ErrPayloadBackpressure), errors.Is(err, opening.ErrPayloadBackpressure):
		return gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE
	case errors.Is(err, opening.ErrPipeNotOwned):
		return gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED
	case errors.Is(err, opening.ErrPayloadInvalid):
		return gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST
	default:
		return gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE
	}
}

func peerPayloadError(failure gatewayv1.PipePayloadFailure) error {
	switch failure {
	case gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE:
		return localbinding.ErrPayloadBackpressure
	case gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST,
		gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED,
		gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE:
		return localbinding.ErrEndpointUnavailable
	default:
		return nil
	}
}

func (e *remoteEndpoint) cleanup(ctx context.Context) {
	<-ctx.Done()
	e.sender.stop(context.Cause(ctx))
	_ = e.connection.Close()
	e.workers.Wait()
	<-e.sender.joined
	close(e.done)
	e.client.release()
}
