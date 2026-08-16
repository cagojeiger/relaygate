package relaygrpc

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientsession"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionManager interface {
	Authenticate(clientID, apiKeyID, presentedKey string) (clientsession.Session, error)
	End(clientsession.Ref)
}

type Service struct {
	relayv1.UnimplementedRelayServer
	sessions              SessionManager
	authenticationTimeout time.Duration
}

func NewService(sessions SessionManager, authenticationTimeout time.Duration) (*Service, error) {
	if sessions == nil {
		return nil, fmt.Errorf("client session manager is required")
	}
	if authenticationTimeout <= 0 {
		return nil, fmt.Errorf("authentication timeout must be positive")
	}
	return &Service{sessions: sessions, authenticationTimeout: authenticationTimeout}, nil
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
			return status.Error(codes.FailedPrecondition, "relay operations are not implemented")
		}
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
