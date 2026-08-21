package relaygrpc

import (
	"context"
	"errors"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) handleRequest(ctx context.Context, session clientsession.Session, listener *streamListenerEndpoint, coordinator *streamCoordinator, request *relayv1.ConnectRequest) (*relayv1.ConnectResponse, error) {
	if bind := request.GetBindListener(); bind != nil {
		slot, err := s.bindings.Bind(ctx, session, bind.GetEndpointPattern(), bind.GetTargetId(), listener)
		if err != nil {
			if failure, ok := listenerBindingFailure(err); ok {
				return listenerBindFailed(bind, failure), nil
			}
			return nil, bindingStatus(err)
		}
		if slot.Ref.ListenerBindingID == "" {
			return nil, status.Error(codes.Internal, "binding committed without a listener reference")
		}
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerBound{
			ListenerBound: &relayv1.ListenerBound{Binding: &relayv1.ListenerBinding{
				ListenerBindingId: slot.Ref.ListenerBindingID,
				EndpointPattern:   slot.Key.EndpointPattern,
				TargetId:          slot.Key.TargetID,
			}},
		}}, nil
	}

	if unbind := request.GetUnbindListener(); unbind != nil {
		if err := s.bindings.Unbind(session.Ref, unbind.GetListenerBindingId()); err != nil {
			if failure, ok := listenerBindingFailure(err); ok {
				return listenerUnbindFailed(unbind, failure), nil
			}
			return nil, bindingStatus(err)
		}
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
			ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: unbind.GetListenerBindingId()},
		}}, nil
	}

	if open := request.GetOpen(); open != nil {
		return coordinator.startOpen(ctx, s, open), nil
	}

	if cancelOpen := request.GetCancelOpen(); cancelOpen != nil {
		return coordinator.cancelOpen(cancelOpen), nil
	}

	if accept := request.GetListenerAccept(); accept != nil {
		if err := listener.accept(accept.GetAttemptId()); err != nil {
			return listenerDecisionRejected(accept.GetAttemptId(), err), nil
		}
		return nil, nil
	}

	if reject := request.GetListenerReject(); reject != nil {
		if err := listener.reject(reject.GetAttemptId()); err != nil {
			return listenerDecisionRejected(reject.GetAttemptId(), err), nil
		}
		return nil, nil
	}

	if confirmed := request.GetListenerConfirmed(); confirmed != nil {
		if err := listener.confirmed(confirmed.GetAttemptId(), confirmed.GetPipeId()); err != nil {
			return listenerDecisionRejected(confirmed.GetAttemptId(), err), nil
		}
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerConfirmationAcknowledged{
			ListenerConfirmationAcknowledged: &relayv1.ListenerConfirmationAcknowledged{
				AttemptId: confirmed.GetAttemptId(),
				PipeId:    confirmed.GetPipeId(),
			},
		}}, nil
	}

	return nil, status.Error(codes.FailedPrecondition, "authenticate must be followed by a relay operation")
}

func listenerBindFailed(bind *relayv1.BindListener, failure relayv1.ListenerBindingFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerBindFailed{
		ListenerBindFailed: &relayv1.ListenerBindFailed{
			EndpointPattern: safeWireValue(bind.GetEndpointPattern(), routing.MaxEndpointPatternBytes),
			TargetId:        safeWireValue(bind.GetTargetId(), routing.MaxIdentityBytes),
			Failure:         failure,
		},
	}}
}

func listenerUnbindFailed(unbind *relayv1.UnbindListener, failure relayv1.ListenerBindingFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbindFailed{
		ListenerUnbindFailed: &relayv1.ListenerUnbindFailed{
			ListenerBindingId: safeWireValue(unbind.GetListenerBindingId(), routing.MaxIdentityBytes),
			Failure:           failure,
		},
	}}
}

func listenerBindingFailure(err error) (relayv1.ListenerBindingFailure, bool) {
	switch {
	case errors.Is(err, localbinding.ErrInvalid):
		return relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_INVALID_REQUEST, true
	case errors.Is(err, localbinding.ErrCapacity):
		return relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CAPACITY_REACHED, true
	case errors.Is(err, localbinding.ErrConflict):
		return relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CONFLICT, true
	case errors.Is(err, localbinding.ErrUnavailable):
		return relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_UNAVAILABLE, true
	default:
		return relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_UNSPECIFIED, false
	}
}

func bindingStatus(err error) error {
	switch {
	case errors.Is(err, localbinding.ErrSessionEnded),
		errors.Is(err, clientsession.ErrCredentialRevoked),
		errors.Is(err, clientsession.ErrStaleSession),
		errors.Is(err, clientsession.ErrClosed):
		return status.Error(codes.Unauthenticated, "client session ended")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Internal, "listener binding operation failed")
	}
}

func (s *Service) open(ctx context.Context, session clientsession.Session, callerEndpoint localbinding.CallerEndpoint, request *relayv1.Open) *relayv1.ConnectResponse {
	requestID := safeWireValue(request.GetRequestId(), routing.MaxIdentityBytes)
	endpoint := safeWireValue(request.GetEndpoint(), routing.MaxEndpointPatternBytes)
	targetID := safeWireValue(request.GetTargetId(), routing.MaxIdentityBytes)
	if requestID == "" || endpoint == "" || targetID == "" {
		return pipeOpenFailed(requestID, endpoint, targetID, relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST)
	}

	result, err := s.opener.OpenPipe(ctx, session, callerEndpoint, endpoint, targetID)
	if err != nil {
		if errors.Is(err, opening.ErrUnknown) {
			return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenUnknown{
				PipeOpenUnknown: &relayv1.PipeOpenUnknown{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
			}}
		}
		return pipeOpenFailed(requestID, endpoint, targetID, openFailure(err))
	}
	if result.AttemptID == "" || result.PipeID == "" ||
		len(result.AttemptID) > routing.MaxIdentityBytes || len(result.PipeID) > routing.MaxIdentityBytes {
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenUnknown{
			PipeOpenUnknown: &relayv1.PipeOpenUnknown{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
		}}
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpened{
		PipeOpened: &relayv1.PipeOpened{
			RequestId: requestID,
			AttemptId: result.AttemptID,
			PipeId:    result.PipeID,
			Endpoint:  endpoint,
			TargetId:  targetID,
		},
	}}
}

func (s *Service) relayPayload(ctx context.Context, sender clientsession.Ref, request *relayv1.PipePayload) *relayv1.ConnectResponse {
	pipeID := safeWireValue(request.GetPipeId(), routing.MaxIdentityBytes)
	payloadID := safeWireValue(request.GetPayloadId(), routing.MaxIdentityBytes)
	payload := request.GetPayload()
	if pipeID == "" || payloadID == "" {
		return pipePayloadRejected(pipeID, payloadID, relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST)
	}
	if len(payload) == 0 || len(payload) > localbinding.MaxPayloadBytes {
		// ClosePipe changes state only for the exact participant. Unknown and
		// foreign IDs are intentionally indistinguishable and remain untouched.
		s.opener.ClosePipe(sender, pipeID)
		return pipePayloadRejected(pipeID, payloadID, relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST)
	}
	if err := s.opener.RelayPayload(ctx, sender, pipeID, payloadID, payload); err != nil {
		return pipePayloadRejected(pipeID, payloadID, payloadFailure(err))
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadReceived{
		PipePayloadReceived: &relayv1.PipePayloadReceived{PipeId: pipeID, PayloadId: payloadID},
	}}
}

func payloadFailure(err error) relayv1.PipePayloadFailure {
	switch {
	case errors.Is(err, opening.ErrPayloadInvalid):
		return relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST
	case errors.Is(err, opening.ErrPipeNotOwned):
		return relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED
	case errors.Is(err, opening.ErrPayloadBackpressure), errors.Is(err, localbinding.ErrPayloadBackpressure):
		return relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE
	default:
		return relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE
	}
}

func pipePayloadRejected(pipeID, payloadID string, failure relayv1.PipePayloadFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadRejected{
		PipePayloadRejected: &relayv1.PipePayloadRejected{PipeId: pipeID, PayloadId: payloadID, Failure: failure},
	}}
}

func openFailure(err error) relayv1.OpenFailure {
	switch {
	case errors.Is(err, opening.ErrInvalid):
		return relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST
	case errors.Is(err, opening.ErrNotFound):
		return relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND
	case errors.Is(err, opening.ErrCapacity):
		return relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED
	case errors.Is(err, opening.ErrListenerRejected):
		return relayv1.OpenFailure_OPEN_FAILURE_LISTENER_REJECTED
	case errors.Is(err, opening.ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return relayv1.OpenFailure_OPEN_FAILURE_DEADLINE_EXCEEDED
	case errors.Is(err, context.Canceled):
		return relayv1.OpenFailure_OPEN_FAILURE_CANCELLED
	default:
		return relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE
	}
}

func pipeOpenFailed(requestID, endpoint, targetID string, failure relayv1.OpenFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenFailed{
		PipeOpenFailed: &relayv1.PipeOpenFailed{
			RequestId: requestID,
			Endpoint:  endpoint,
			TargetId:  targetID,
			Failure:   failure,
		},
	}}
}

func safeWireValue(value string, maxBytes int) string {
	if value == "" || len(value) > maxBytes {
		return ""
	}
	return value
}

var (
	errInvalidListenerDecision = errors.New("invalid listener decision")
	errAttemptNotPending       = errors.New("listener attempt is not pending")
	errWrongListenerPhase      = errors.New("listener attempt is in the wrong phase")
)

func listenerDecisionRejected(attemptID string, err error) *relayv1.ConnectResponse {
	failure := relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE
	switch {
	case errors.Is(err, errInvalidListenerDecision):
		failure = relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST
	case errors.Is(err, errAttemptNotPending):
		failure = relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerDecisionRejected{
		ListenerDecisionRejected: &relayv1.ListenerDecisionRejected{
			AttemptId: safeWireValue(attemptID, routing.MaxIdentityBytes),
			Failure:   failure,
		},
	}}
}
