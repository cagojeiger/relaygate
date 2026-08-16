package relaygrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"github.com/cagojeiger/relaygate/internal/localbinding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionManager interface {
	Authenticate(clientID, apiKeyID, presentedKey string) (clientsession.Session, error)
	End(clientsession.Ref)
}

type BindingManager interface {
	Bind(context.Context, clientsession.Session, string, string) (controlstate.BindingSlot, error)
	Unbind(clientsession.Ref, string) error
	RetireSession(clientsession.Ref) int
}

type Service struct {
	relayv1.UnimplementedRelayServer
	sessions              SessionManager
	bindings              BindingManager
	authenticationTimeout time.Duration
}

func NewService(sessions SessionManager, bindings BindingManager, authenticationTimeout time.Duration) (*Service, error) {
	if sessions == nil {
		return nil, fmt.Errorf("client session manager is required")
	}
	if bindings == nil {
		return nil, fmt.Errorf("listener binding manager is required")
	}
	if authenticationTimeout <= 0 {
		return nil, fmt.Errorf("authentication timeout must be positive")
	}
	return &Service{sessions: sessions, bindings: bindings, authenticationTimeout: authenticationTimeout}, nil
}

func (s *Service) Connect(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
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

	if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
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
		case item, ok := <-received:
			if !ok || errors.Is(item.err, io.EOF) {
				return nil
			}
			if item.err != nil {
				return item.err
			}
			response, err := s.handleRequest(stream.Context(), session, item.request)
			if err != nil {
				return err
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

func (s *Service) handleRequest(ctx context.Context, session clientsession.Session, request *relayv1.ConnectRequest) (*relayv1.ConnectResponse, error) {
	if bind := request.GetBindListener(); bind != nil {
		slot, err := s.bindings.Bind(ctx, session, bind.GetEndpointPattern(), bind.GetTargetId())
		if err != nil {
			return nil, bindingStatus(err)
		}
		if slot.Ref == nil || slot.Ref.ListenerBindingID == "" {
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
			return nil, bindingStatus(err)
		}
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
			ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: unbind.GetListenerBindingId()},
		}}, nil
	}

	return nil, status.Error(codes.FailedPrecondition, "authenticate must be followed by bind_listener or unbind_listener")
}

func bindingStatus(err error) error {
	switch {
	case errors.Is(err, localbinding.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid listener binding request")
	case errors.Is(err, localbinding.ErrCapacity):
		return status.Error(codes.ResourceExhausted, "listener binding capacity reached")
	case errors.Is(err, localbinding.ErrConflict):
		return status.Error(codes.AlreadyExists, "listener binding conflicts with an existing binding")
	case errors.Is(err, localbinding.ErrUnavailable):
		return status.Error(codes.Unavailable, "gateway control is unavailable")
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

type receivedRequest struct {
	request *relayv1.ConnectRequest
	err     error
}

func receiveRequests(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) <-chan receivedRequest {
	received := make(chan receivedRequest, 1)
	go func() {
		defer close(received)
		for {
			request, err := stream.Recv()
			select {
			case received <- receivedRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func sessionRefToProto(ref clientsession.Ref) *relayv1.ClientSessionRef {
	return &relayv1.ClientSessionRef{
		ClientSessionId: ref.ClientSessionID,
		ClientId:        ref.ClientID,
		ApiKeyId:        ref.APIKeyID,
		AuthRevision:    ref.AuthRevision,
	}
}
