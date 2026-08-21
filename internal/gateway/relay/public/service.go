package relaygrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionManager interface {
	Authenticate(clientID, apiKeyID, presentedKey string) (clientsession.Session, error)
	End(clientsession.Ref)
}

type BindingManager interface {
	Bind(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (routing.LiveBinding, error)
	Unbind(clientsession.Ref, string) error
	RetireSession(clientsession.Ref) int
}

type Opener interface {
	OpenPipe(context.Context, clientsession.Session, localbinding.CallerEndpoint, string, string) (opening.Result, error)
	ActivatePipe(clientsession.Ref, string) bool
	RelayPayload(context.Context, clientsession.Ref, string, string, []byte) error
	ClosePipe(clientsession.Ref, string) bool
	RetireSession(clientsession.Ref) int
}

type Service struct {
	relayv1.UnimplementedRelayServer
	sessions              SessionManager
	bindings              BindingManager
	opener                Opener
	authenticationTimeout time.Duration
	terminalSendTimeout   time.Duration
	openSlots             chan struct{}
	payloadSlots          chan struct{}
}

const maxGlobalPayloadSlots = 1024

func NewService(sessions SessionManager, bindings BindingManager, opener Opener, authenticationTimeout, terminalSendTimeout time.Duration, maxInFlightOpens uint32) (*Service, error) {
	if sessions == nil {
		return nil, fmt.Errorf("client session manager is required")
	}
	if bindings == nil {
		return nil, fmt.Errorf("listener binding manager is required")
	}
	if opener == nil {
		return nil, fmt.Errorf("pipe opener is required")
	}
	if authenticationTimeout <= 0 {
		return nil, fmt.Errorf("authentication timeout must be positive")
	}
	if maxInFlightOpens == 0 {
		return nil, fmt.Errorf("maximum in-flight Opens must be positive")
	}
	if terminalSendTimeout <= 0 {
		return nil, fmt.Errorf("terminal send timeout must be positive")
	}
	return &Service{
		sessions:              sessions,
		bindings:              bindings,
		opener:                opener,
		authenticationTimeout: authenticationTimeout,
		terminalSendTimeout:   terminalSendTimeout,
		openSlots:             make(chan struct{}, maxInFlightOpens),
		payloadSlots:          make(chan struct{}, min(maxInFlightOpens, uint32(maxGlobalPayloadSlots))),
	}, nil
}

func (s *Service) Connect(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
	outbound := newOutboundActor(stream, s.payloadSlots, s.terminalSendTimeout)
	defer outbound.close()
	pipeEndpoint := newStreamPipeEndpoint(outbound, s.terminalSendTimeout)
	defer pipeEndpoint.close()
	listener := newStreamListenerEndpoint(stream.Context(), outbound, pipeEndpoint, s.terminalSendTimeout)
	defer listener.close()

	received := receiveRequests(stream)
	authenticationTimer := time.NewTimer(s.authenticationTimeout)
	defer authenticationTimer.Stop()
	var first *relayv1.ConnectRequest
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-authenticationTimer.C:
		return status.Error(codes.DeadlineExceeded, "client authentication timed out")
	case item, ok := <-received:
		if !ok || errors.Is(item.err, io.EOF) {
			return status.Error(codes.Unauthenticated, "client authentication is required")
		}
		if item.err != nil {
			return item.err
		}
		first = item.request
	}
	authenticate := first.GetAuthenticate()
	if authenticate == nil {
		return status.Error(codes.Unauthenticated, "client authentication failed")
	}
	session, err := s.sessions.Authenticate(authenticate.GetClientId(), authenticate.GetApiKeyId(), authenticate.GetApiKey())
	authenticate.ApiKey = ""
	if err != nil {
		if errors.Is(err, clientsession.ErrCapacity) {
			return status.Error(codes.ResourceExhausted, "client session capacity reached")
		}
		return status.Error(codes.Unauthenticated, "client authentication failed")
	}
	defer s.sessions.End(session.Ref)
	defer s.bindings.RetireSession(session.Ref)
	defer s.opener.RetireSession(session.Ref)
	coordinator := newStreamCoordinator(stream.Context(), session, s.opener, pipeEndpoint, outbound, s.openSlots)
	defer coordinator.close()

	if err := outbound.send(stream.Context(), &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
		ClientSessionOpened: &relayv1.ClientSessionOpened{Session: sessionRefToProto(session.Ref)},
	}}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-session.Done:
			return status.Error(codes.Unauthenticated, "client session ended")
		case err := <-outbound.failures():
			return err
		case item, ok := <-received:
			if !ok || errors.Is(item.err, io.EOF) {
				return nil
			}
			if item.err != nil {
				return item.err
			}
			if receipt := item.request.GetPipePayloadReceived(); receipt != nil {
				if err := pipeEndpoint.acknowledge(receipt.GetPipeId(), receipt.GetPayloadId()); err != nil {
					return status.Error(codes.FailedPrecondition, "invalid payload receipt")
				}
				continue
			}
			if rejection := item.request.GetPipePayloadRejected(); rejection != nil {
				if err := pipeEndpoint.reject(rejection.GetPipeId(), rejection.GetPayloadId(), rejection.GetFailure()); err != nil {
					return status.Error(codes.FailedPrecondition, "invalid payload rejection")
				}
				continue
			}
			if item.request.GetPipePayload() != nil || item.request.GetClosePipe() != nil {
				if err := coordinator.enqueuePipeWork(s, item.request); err != nil {
					return err
				}
				continue
			}
			orderBeforeOffers := item.request.GetBindListener() != nil
			if orderBeforeOffers {
				listener.requestOrder.Lock()
			}
			response, err := s.handleRequest(stream.Context(), session, listener, coordinator, item.request)
			if err != nil {
				if orderBeforeOffers {
					listener.requestOrder.Unlock()
				}
				return err
			}
			if response == nil {
				if orderBeforeOffers {
					listener.requestOrder.Unlock()
				}
				continue
			}
			if err := outbound.send(stream.Context(), response); err != nil {
				if orderBeforeOffers {
					listener.requestOrder.Unlock()
				}
				return err
			}
			if orderBeforeOffers {
				listener.requestOrder.Unlock()
			}
		}
	}
}

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
