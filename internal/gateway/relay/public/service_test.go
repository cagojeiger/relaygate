package relaygrpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type bindingCall struct {
	session         clientsession.Ref
	endpointPattern string
	targetID        string
	endpoint        localbinding.ListenerEndpoint
}

type unbindingCall struct {
	session   clientsession.Ref
	bindingID string
}

type testBindingManager struct {
	bind   func(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (routing.LiveBinding, error)
	unbind func(clientsession.Ref, string) error
	retire func(clientsession.Ref) int
}

func (m *testBindingManager) Bind(ctx context.Context, session clientsession.Session, endpointPattern, targetID string, endpoint localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
	if m.bind == nil {
		return routing.LiveBinding{}, localbinding.ErrUnavailable
	}
	return m.bind(ctx, session, endpointPattern, targetID, endpoint)
}

func (m *testBindingManager) Unbind(session clientsession.Ref, bindingID string) error {
	if m.unbind == nil {
		return nil
	}
	return m.unbind(session, bindingID)
}

func (m *testBindingManager) RetireSession(session clientsession.Ref) int {
	if m.retire == nil {
		return 0
	}
	return m.retire(session)
}

type testOpener struct {
	open         func(context.Context, clientsession.Session, string, string) (opening.Result, error)
	openPipe     func(context.Context, clientsession.Session, localbinding.CallerEndpoint, string, string) (opening.Result, error)
	activatePipe func(clientsession.Ref, string) bool
	relayPayload func(context.Context, clientsession.Ref, string, []byte) error
	closePipe    func(clientsession.Ref, string) bool
	retire       func(clientsession.Ref) int
}

func (o *testOpener) OpenPipe(ctx context.Context, session clientsession.Session, callerEndpoint localbinding.CallerEndpoint, endpoint, targetID string) (opening.Result, error) {
	if o.openPipe != nil {
		return o.openPipe(ctx, session, callerEndpoint, endpoint, targetID)
	}
	if o.open == nil {
		return opening.Result{}, opening.ErrUnavailable
	}
	return o.open(ctx, session, endpoint, targetID)
}

func (o *testOpener) ActivatePipe(session clientsession.Ref, pipeID string) bool {
	if o.activatePipe == nil {
		return true
	}
	return o.activatePipe(session, pipeID)
}

func (o *testOpener) RelayPayload(ctx context.Context, session clientsession.Ref, pipeID, payloadID string, payload []byte) error {
	if o.relayPayload == nil {
		return opening.ErrPipeNotOwned
	}
	return o.relayPayload(ctx, session, pipeID, payload)
}

func (o *testOpener) ClosePipe(session clientsession.Ref, pipeID string) bool {
	if o.closePipe == nil {
		return false
	}
	return o.closePipe(session, pipeID)
}

func (o *testOpener) RetireSession(session clientsession.Ref) int {
	if o.retire == nil {
		return 0
	}
	return o.retire(session)
}

func dialTestServer(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return connection
}

func authenticateRequest(clientID, apiKeyID, apiKey string) *relayv1.ConnectRequest {
	return &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Authenticate{
		Authenticate: &relayv1.Authenticate{ClientId: clientID, ApiKeyId: apiKeyID, ApiKey: apiKey},
	}}
}

func authenticateTestStream(t *testing.T, connection *grpc.ClientConn) grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse] {
	t.Helper()
	stream, err := relayv1.NewRelayClient(connection).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	if response, err := stream.Recv(); err != nil || response.GetClientSessionOpened() == nil {
		t.Fatalf("Recv(ClientSessionOpened) = %#v, %v", response, err)
	}
	return stream
}

func bindListener(t *testing.T, stream grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse], endpoint, targetID string) {
	t.Helper()
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: endpoint, TargetId: targetID},
	}}); err != nil {
		t.Fatalf("Send(BindListener): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerBound): %v", err)
	}
	if response.GetListenerBound() == nil {
		t.Fatalf("ListenerBound response = %#v", response)
	}
}

func openRequest(requestID, endpoint, targetID string) *relayv1.ConnectRequest {
	return &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Open{
		Open: &relayv1.Open{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
	}}
}

func testListenerSlot(clientID, endpoint, targetID string) routing.LiveBinding {
	ref := routing.ListenerBindingRef{
		GatewayID:         "gateway-a",
		GatewayInstanceID: "instance-a",
		ListenerBindingID: "listener-a",
	}
	return routing.LiveBinding{
		Key: routing.BindingKey{ClientID: clientID, EndpointPattern: endpoint, TargetID: targetID},
		Ref: ref,
	}
}

func testOpenBindingManager(bound chan<- bindingCall, slot routing.LiveBinding) *testBindingManager {
	return &testBindingManager{bind: func(_ context.Context, session clientsession.Session, endpointPattern, targetID string, endpoint localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
		bound <- bindingCall{session: session.Ref, endpointPattern: endpointPattern, targetID: targetID, endpoint: endpoint}
		result := slot
		result.Key.ClientID = session.Ref.ClientID
		result.Key.EndpointPattern = endpointPattern
		result.Key.TargetID = targetID
		return result, nil
	}}
}

func waitForSessionCount(t *testing.T, sessions *clientsession.Manager, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if sessions.ActiveCount() == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("active sessions = %d, want %d", sessions.ActiveCount(), want)
		case <-ticker.C:
		}
	}
}

func receiveWithin[T any](t *testing.T, values <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		var zero T
		t.Fatalf("%s did not complete", operation)
		return zero
	}
}

func verifier(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(digest[:])
}

type trackingRelayStream struct {
	ctx        context.Context
	activeSend atomic.Int32
	concurrent atomic.Bool
	sent       atomic.Int32
}

func newTrackingRelayStream() *trackingRelayStream {
	return &trackingRelayStream{ctx: context.Background()}
}

func (s *trackingRelayStream) Send(*relayv1.ConnectResponse) error {
	if s.activeSend.Add(1) != 1 {
		s.concurrent.Store(true)
	}
	defer s.activeSend.Add(-1)
	time.Sleep(time.Millisecond)
	s.sent.Add(1)
	return nil
}

func (*trackingRelayStream) Recv() (*relayv1.ConnectRequest, error) {
	return nil, io.EOF
}

func (*trackingRelayStream) SetHeader(metadata.MD) error  { return nil }
func (*trackingRelayStream) SendHeader(metadata.MD) error { return nil }
func (*trackingRelayStream) SetTrailer(metadata.MD)       {}
func (s *trackingRelayStream) Context() context.Context   { return s.ctx }
func (*trackingRelayStream) SendMsg(any) error            { return nil }
func (*trackingRelayStream) RecvMsg(any) error            { return io.EOF }
