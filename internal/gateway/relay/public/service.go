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
