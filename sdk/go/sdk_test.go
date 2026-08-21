package relaygate

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type scriptedRelay struct {
	relayv1.UnimplementedRelayServer
	run func(grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error
}

type blockingRelayClientStream struct {
	ctx      context.Context
	started  chan struct{}
	requests chan *relayv1.ConnectRequest
	once     sync.Once
}

func (s *blockingRelayClientStream) Send(request *relayv1.ConnectRequest) error {
	s.once.Do(func() { close(s.started) })
	if s.requests != nil {
		s.requests <- request
	}
	<-s.ctx.Done()
	return s.ctx.Err()
}

func (s *blockingRelayClientStream) Recv() (*relayv1.ConnectResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *blockingRelayClientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *blockingRelayClientStream) Trailer() metadata.MD         { return nil }
func (s *blockingRelayClientStream) CloseSend() error             { return nil }
func (s *blockingRelayClientStream) Context() context.Context     { return s.ctx }
func (s *blockingRelayClientStream) SendMsg(message any) error {
	request, ok := message.(*relayv1.ConnectRequest)
	if !ok {
		return fmt.Errorf("unexpected SendMsg %T", message)
	}
	return s.Send(request)
}
func (s *blockingRelayClientStream) RecvMsg(any) error {
	<-s.ctx.Done()
	return s.ctx.Err()
}

func (s *scriptedRelay) Connect(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
	return s.run(stream)
}

func startScriptedRelay(t *testing.T, run func(grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	relayv1.RegisterRelayServer(server, &scriptedRelay{run: run})
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})
	return listener.Addr().String()
}

func authenticateScript(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) (*relayv1.Authenticate, error) {
	request, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	authenticate := request.GetAuthenticate()
	if authenticate == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	err = stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
		ClientSessionOpened: &relayv1.ClientSessionOpened{Session: &relayv1.ClientSessionRef{
			ClientSessionId: "session-1", ClientId: authenticate.GetClientId(), ApiKeyId: authenticate.GetApiKeyId(), AuthRevision: "revision-1",
		}},
	}})
	return authenticate, err
}

func connectTestClient(t *testing.T, address string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Connect(ctx, NewConfig(address, "client-1", "key-1", "secret-value").WithInsecureLocal())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func recvRequest(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) (*relayv1.ConnectRequest, error) {
	request, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if request == nil || request.GetMessage() == nil {
		return nil, status.Error(codes.FailedPrecondition, "empty request")
	}
	return request, nil
}
