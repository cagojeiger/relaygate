package gatewayrelay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	gatewayv1.UnimplementedGatewayRelayServer

	owner       Owner
	openTimeout time.Duration
	slots       chan struct{}
	now         func() time.Time

	actors    sync.WaitGroup
	receivers sync.WaitGroup
}

var _ localbinding.CallerEndpoint = (*serverCallerEndpoint)(nil)

func NewService(owner Owner, openTimeout time.Duration, maxPipes uint32) (*Service, error) {
	if owner == nil {
		return nil, fmt.Errorf("%w: owner is required", ErrInvalid)
	}
	if openTimeout <= 0 {
		return nil, fmt.Errorf("%w: Open timeout must be positive", ErrInvalid)
	}
	if maxPipes == 0 {
		return nil, fmt.Errorf("%w: maximum Pipes must be positive", ErrInvalid)
	}
	return &Service{
		owner:       owner,
		openTimeout: openTimeout,
		slots:       make(chan struct{}, maxPipes),
		now:         time.Now,
	}, nil
}

func (s *Service) Forward(stream grpc.BidiStreamingServer[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse]) error {
	received, receiverDone := s.receiveForwardRequests(stream)
	first, err := receiveFirst(stream.Context(), received, s.openTimeout)
	if err != nil {
		return err
	}
	forward := first.GetForwardOpen()
	if forward == nil {
		return status.Error(codes.InvalidArgument, "ForwardOpen must be the first Gateway relay message")
	}
	openContext, err := openContextFromProto(forward.GetContext())
	if err != nil {
		return s.sendStableFailure(stream, forwardAttemptID(forward), gatewayv1.ForwardFailure_FORWARD_FAILURE_INVALID_REQUEST)
	}
	if !s.now().Before(openContext.ExpiresAt) {
		return s.sendStableFailure(stream, openContext.AttemptID, gatewayv1.ForwardFailure_FORWARD_FAILURE_CONTEXT_EXPIRED)
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return s.sendStableFailure(stream, openContext.AttemptID, gatewayv1.ForwardFailure_FORWARD_FAILURE_CAPACITY_REACHED)
	}
	slotOwned := true
	defer func() {
		if slotOwned {
			<-s.slots
		}
	}()

	pipeCtx, cancelPipe := context.WithCancelCause(stream.Context())
	defer cancelPipe(context.Canceled)
	outbound := newSendActor(pipeCtx, stream.Send, func(cause error) { cancelPipe(cause) })
	s.trackActor(outbound, receiverDone)
	slotOwned = false
	callerEndpoint := newServerCallerEndpoint(outbound, cancelPipe, s.openTimeout)

	// OpenForwarded uses ctx.Done as the forwarded caller and accepted-Pipe
	// lifetime. Its Manager owns the bounded Open phase; canceling a phase child
	// here would immediately terminalize every successful owner Pipe.
	result, openErr := s.owner.OpenForwarded(pipeCtx, openContext.Clone(), callerEndpoint)
	if openErr != nil {
		return s.sendOpenOutcome(stream.Context(), outbound, openContext.AttemptID, openErr)
	}
	caller := callerRef(openContext.Auth)
	if !validOwnerResult(openContext, result) {
		if result.PipeID != "" {
			s.owner.ClosePipe(caller, result.PipeID)
		}
		return s.sendOpenOutcome(stream.Context(), outbound, openContext.AttemptID, opening.ErrUnknown)
	}
	callerEndpoint.setPipeID(result.PipeID)
	closedByRequest := false
	defer func() {
		if !closedByRequest {
			s.owner.ClosePipe(caller, result.PipeID)
		}
	}()

	if err := s.sendResponse(stream.Context(), outbound, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_Accepted{
		Accepted: &gatewayv1.ForwardAccepted{
			AttemptId: result.AttemptID,
			PipeId:    result.PipeID,
			Binding:   liveBindingToProto(result.Binding),
		},
	}}); err != nil {
		return err
	}
	callerEndpoint.markAccepted()

	activated := false
	for {
		select {
		case <-pipeCtx.Done():
			return nil
		case item, ok := <-received:
			if !ok || errors.Is(item.err, io.EOF) {
				return nil
			}
			if item.err != nil {
				return item.err
			}
			request := item.request
			switch {
			case request.GetActivatePipe() != nil:
				activate := request.GetActivatePipe()
				if activate.GetPipeId() != result.PipeID || activated {
					return status.Error(codes.FailedPrecondition, "invalid Gateway Pipe activation")
				}
				if !s.owner.ActivatePipe(caller, result.PipeID) {
					_ = callerEndpoint.TerminatePipe(stream.Context(), result.PipeID)
					return nil
				}
				activated = true
			case request.GetPipePayload() != nil:
				payload := request.GetPipePayload()
				if !activated || payload.GetPipeId() != result.PipeID || payload.GetPayloadId() == "" || len(payload.GetPayloadId()) > routing.MaxIdentityBytes || len(payload.GetPayload()) == 0 || len(payload.GetPayload()) > localbinding.MaxPayloadBytes {
					return status.Error(codes.InvalidArgument, "invalid Gateway Pipe payload")
				}
				deliveryCtx, cancelDelivery := context.WithTimeout(pipeCtx, s.openTimeout)
				err := s.owner.RelayPayload(deliveryCtx, caller, result.PipeID, payload.GetPayloadId(), append([]byte(nil), payload.GetPayload()...))
				cancelDelivery()
				if err != nil {
					_ = s.sendResponse(stream.Context(), outbound, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_PipePayloadRejected{
						PipePayloadRejected: &gatewayv1.PipePayloadRejected{
							PipeId: result.PipeID, PayloadId: payload.GetPayloadId(), Failure: peerPayloadFailure(err),
						},
					}})
					_ = callerEndpoint.TerminatePipe(stream.Context(), result.PipeID)
					return nil
				}
				if err := s.sendResponse(stream.Context(), outbound, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_PipePayloadReceived{
					PipePayloadReceived: &gatewayv1.PipePayloadReceived{PipeId: result.PipeID, PayloadId: payload.GetPayloadId()},
				}}); err != nil {
					return err
				}
			case request.GetPipePayloadReceived() != nil:
				receipt := request.GetPipePayloadReceived()
				if !activated || receipt.GetPipeId() != result.PipeID || callerEndpoint.acknowledgePayload(receipt.GetPayloadId()) != nil {
					return status.Error(codes.FailedPrecondition, "invalid Gateway payload receipt")
				}
			case request.GetPipePayloadRejected() != nil:
				rejection := request.GetPipePayloadRejected()
				if !activated || rejection.GetPipeId() != result.PipeID || callerEndpoint.rejectPayload(rejection.GetPayloadId(), rejection.GetFailure()) != nil {
					return status.Error(codes.FailedPrecondition, "invalid Gateway payload rejection")
				}
			case request.GetClosePipe() != nil:
				closeRequest := request.GetClosePipe()
				if closeRequest.GetPipeId() != result.PipeID {
					return status.Error(codes.InvalidArgument, "invalid Gateway Pipe close")
				}
				s.owner.ClosePipe(caller, result.PipeID)
				closedByRequest = true
				_ = callerEndpoint.TerminatePipe(stream.Context(), result.PipeID)
				return nil
			default:
				return status.Error(codes.FailedPrecondition, "ForwardOpen must be followed by ActivatePipe, PipePayload, or ClosePipe")
			}
		}
	}
}

type receivedForwardRequest struct {
	request *gatewayv1.ForwardRequest
	err     error
}

func (s *Service) receiveForwardRequests(stream grpc.BidiStreamingServer[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse]) (<-chan receivedForwardRequest, <-chan struct{}) {
	received := make(chan receivedForwardRequest, 1)
	done := make(chan struct{})
	s.receivers.Add(1)
	go func() {
		defer s.receivers.Done()
		defer close(received)
		defer close(done)
		for {
			request, err := stream.Recv()
			select {
			case received <- receivedForwardRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received, done
}

func receiveFirst(ctx context.Context, received <-chan receivedForwardRequest, timeout time.Duration) (*gatewayv1.ForwardRequest, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, status.Error(codes.DeadlineExceeded, "ForwardOpen timed out")
	case item, ok := <-received:
		if !ok || errors.Is(item.err, io.EOF) {
			return nil, status.Error(codes.InvalidArgument, "ForwardOpen is required")
		}
		if item.err != nil {
			return nil, item.err
		}
		return item.request, nil
	}
}

func (s *Service) sendStableFailure(stream grpc.BidiStreamingServer[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse], attemptID string, failure gatewayv1.ForwardFailure) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(stream.Context()), s.openTimeout)
	defer cancel()
	return sendDirect(ctx, stream, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_Failed{
		Failed: &gatewayv1.ForwardFailed{AttemptId: attemptID, Failure: failure},
	}})
}

func sendDirect(ctx context.Context, stream grpc.BidiStreamingServer[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse], response *gatewayv1.ForwardResponse) error {
	result := make(chan error, 1)
	go func() { result <- stream.Send(response) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) sendOpenOutcome(parent context.Context, outbound *sendActor[*gatewayv1.ForwardResponse], attemptID string, openErr error) error {
	if errors.Is(openErr, opening.ErrUnknown) {
		return s.sendResponse(parent, outbound, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_Unknown{
			Unknown: &gatewayv1.ForwardUnknown{AttemptId: attemptID},
		}})
	}
	return s.sendResponse(parent, outbound, &gatewayv1.ForwardResponse{Message: &gatewayv1.ForwardResponse_Failed{
		Failed: &gatewayv1.ForwardFailed{AttemptId: attemptID, Failure: failureToProto(openErr)},
	}})
}

func (s *Service) sendResponse(parent context.Context, outbound *sendActor[*gatewayv1.ForwardResponse], response *gatewayv1.ForwardResponse) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.openTimeout)
	defer cancel()
	return outbound.send(ctx, response)
}

func failureToProto(err error) gatewayv1.ForwardFailure {
	switch {
	case errors.Is(err, opening.ErrInvalid):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_INVALID_REQUEST
	case errors.Is(err, opening.ErrCapacity):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_CAPACITY_REACHED
	case errors.Is(err, opening.ErrNotFound):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_ROUTE_NOT_FOUND
	case errors.Is(err, opening.ErrListenerRejected):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_LISTENER_REJECTED
	case errors.Is(err, opening.ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_DEADLINE_EXCEEDED
	case errors.Is(err, opening.ErrSessionEnded), errors.Is(err, context.Canceled):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_SESSION_ENDED
	case errors.Is(err, opening.ErrContextExpired):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_CONTEXT_EXPIRED
	case errors.Is(err, opening.ErrAttemptReplay):
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_ATTEMPT_REPLAYED
	default:
		return gatewayv1.ForwardFailure_FORWARD_FAILURE_UNAVAILABLE
	}
}

func callerRef(auth routing.AuthContext) clientsession.Ref {
	return clientsession.Ref{
		ClientSessionID: auth.ClientSessionID,
		ClientID:        auth.ClientID,
		APIKeyID:        auth.APIKeyID,
		AuthRevision:    auth.AuthRevision,
	}
}

func validOwnerResult(open routing.OpenContext, result opening.Result) bool {
	return result.AttemptID == open.AttemptID && result.PipeID != "" && len(result.PipeID) <= routing.MaxIdentityBytes && sameBinding(result.Binding, open.Binding)
}

func forwardAttemptID(forward *gatewayv1.ForwardOpen) string {
	if forward == nil || forward.GetContext() == nil {
		return ""
	}
	return forward.GetContext().GetAttemptId()
}

func (s *Service) trackActor(actor *sendActor[*gatewayv1.ForwardResponse], receiverDone <-chan struct{}) {
	s.actors.Add(1)
	go func() {
		defer s.actors.Done()
		<-actor.joined
		<-receiverDone
		<-s.slots
	}()
}

func (s *Service) wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.actors.Wait()
		s.receivers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
