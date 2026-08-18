package relaygrpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestConnectAuthenticatesAndBindsClientSessionToStream(t *testing.T) {
	store, sessions, server := startTestServer(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	connection := dialTestServer(t, server.Address())
	stream, err := relayv1.NewRelayClient(connection).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(session): %v", err)
	}
	opened := response.GetClientSessionOpened()
	if opened == nil || opened.GetSession() == nil {
		t.Fatalf("response = %#v", response)
	}
	session := opened.GetSession()
	if session.GetClientSessionId() == "" || session.GetClientId() != "client-a" || session.GetApiKeyId() != "key-a" || session.GetAuthRevision() != store.Revision() {
		t.Fatalf("session = %#v", session)
	}
	if sessions.ActiveCount() != 1 {
		t.Fatalf("active sessions = %d", sessions.ActiveCount())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend(): %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv(after close) = %v, want EOF", err)
	}
	waitForSessionCount(t, sessions, 0)
}

func TestConnectBindsAndUnbindsWithinAuthenticatedClientNamespace(t *testing.T) {
	bound := make(chan bindingCall, 1)
	unbound := make(chan unbindingCall, 1)
	retired := make(chan clientsession.Ref, 1)
	bindings := &testBindingManager{
		bind: func(_ context.Context, session clientsession.Session, endpointPattern, targetID string, endpoint localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
			bound <- bindingCall{session: session.Ref, endpointPattern: endpointPattern, targetID: targetID, endpoint: endpoint}
			ref := routing.ListenerBindingRef{
				GatewayID:         "gateway-a",
				GatewayInstanceID: "instance-a",
				ListenerBindingID: "listener-a",
			}
			return routing.LiveBinding{
				Key: routing.BindingKey{ClientID: session.Ref.ClientID, EndpointPattern: endpointPattern, TargetID: targetID},
				Ref: ref,
			}, nil
		},
		unbind: func(session clientsession.Ref, bindingID string) error {
			unbound <- unbindingCall{session: session, bindingID: bindingID}
			return nil
		},
		retire: func(session clientsession.Ref) int {
			retired <- session
			return 1
		},
	}
	_, sessions, server := startTestServerWithOptionsAndBindings(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	}, 10, time.Second, bindings)
	connection := dialTestServer(t, server.Address())
	stream, err := relayv1.NewRelayClient(connection).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv(session): %v", err)
	}

	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: "/jobs/*", TargetId: "worker"},
	}}); err != nil {
		t.Fatalf("Send(bind): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(bind): %v", err)
	}
	listener := response.GetListenerBound().GetBinding()
	if listener.GetListenerBindingId() != "listener-a" || listener.GetEndpointPattern() != "/jobs/*" || listener.GetTargetId() != "worker" {
		t.Fatalf("listener = %#v", listener)
	}
	call := receiveWithin(t, bound, "Bind")
	if call.session.ClientID != "client-a" || call.endpointPattern != "/jobs/*" || call.targetID != "worker" {
		t.Fatalf("binding call = %#v", call)
	}

	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_UnbindListener{
		UnbindListener: &relayv1.UnbindListener{ListenerBindingId: "listener-a"},
	}}); err != nil {
		t.Fatalf("Send(unbind): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(unbind): %v", err)
	}
	if got := response.GetListenerUnbound().GetListenerBindingId(); got != "listener-a" {
		t.Fatalf("unbound binding ID = %q", got)
	}
	unbindCall := receiveWithin(t, unbound, "Unbind")
	if unbindCall.session.ClientSessionID != call.session.ClientSessionID || unbindCall.bindingID != "listener-a" {
		t.Fatalf("unbinding call = %#v", unbindCall)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend(): %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv(after close) = %v, want EOF", err)
	}
	retiredSession := receiveWithin(t, retired, "RetireSession")
	if retiredSession.ClientSessionID != call.session.ClientSessionID {
		t.Fatalf("retired session = %#v", retiredSession)
	}
	waitForSessionCount(t, sessions, 0)
}

func TestConnectMapsBindingFailuresWithoutLeakingDetails(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "invalid", err: localbinding.ErrInvalid, code: codes.InvalidArgument},
		{name: "capacity", err: localbinding.ErrCapacity, code: codes.ResourceExhausted},
		{name: "conflict", err: localbinding.ErrConflict, code: codes.AlreadyExists},
		{name: "control unavailable", err: localbinding.ErrUnavailable, code: codes.Unavailable},
		{name: "session ended", err: localbinding.ErrSessionEnded, code: codes.Unauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			bindings := &testBindingManager{bind: func(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
				return routing.LiveBinding{}, fmt.Errorf("sensitive detail: %w", test.err)
			}}
			_, _, server := startTestServerWithOptionsAndBindings(t, map[string]clientauth.ClientConfig{
				"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
			}, 10, time.Second, bindings)
			connection := dialTestServer(t, server.Address())
			stream, err := relayv1.NewRelayClient(connection).Connect(context.Background())
			if err != nil {
				t.Fatalf("Connect(): %v", err)
			}
			if err := stream.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
				t.Fatalf("Send(authenticate): %v", err)
			}
			if _, err := stream.Recv(); err != nil {
				t.Fatalf("Recv(session): %v", err)
			}
			if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
				BindListener: &relayv1.BindListener{EndpointPattern: "/jobs", TargetId: "worker"},
			}}); err != nil {
				t.Fatalf("Send(bind): %v", err)
			}
			_, err = stream.Recv()
			if status.Code(err) != test.code || strings.Contains(status.Convert(err).Message(), "sensitive") {
				t.Fatalf("Recv(bind) error = %v, want redacted %v", err, test.code)
			}
		})
	}
}

func TestConnectRejectsInvalidCredentialWithoutCreatingSession(t *testing.T) {
	_, sessions, server := startTestServer(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	connection := dialTestServer(t, server.Address())
	stream, err := relayv1.NewRelayClient(connection).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest("client-b", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Recv() error = %v, want Unauthenticated", err)
	}
	if sessions.ActiveCount() != 0 {
		t.Fatalf("active sessions = %d", sessions.ActiveCount())
	}
}

func TestCredentialRemovalTerminatesConnectedSession(t *testing.T) {
	store, sessions, server := startTestServer(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	connection := dialTestServer(t, server.Address())
	stream, err := relayv1.NewRelayClient(connection).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv(session): %v", err)
	}

	change, err := store.Reload(map[string]clientauth.ClientConfig{
		"client-b": {APIKeys: map[string]string{"key-b": verifier("secret-b")}},
	})
	if err != nil {
		t.Fatalf("Reload(): %v", err)
	}
	if retired := sessions.Retire(change); retired != 1 {
		t.Fatalf("Retire() = %d, want 1", retired)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Recv(after removal) = %v, want Unauthenticated", err)
	}
}

func TestConnectMapsSessionCapacityToResourceExhausted(t *testing.T) {
	_, sessions, server := startTestServerWithLimit(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	}, 1)
	firstConnection := dialTestServer(t, server.Address())
	client := relayv1.NewRelayClient(firstConnection)
	first, err := client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(first): %v", err)
	}
	if err := first.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(first): %v", err)
	}
	if _, err := first.Recv(); err != nil {
		t.Fatalf("Recv(first): %v", err)
	}

	secondConnection := dialTestServer(t, server.Address())
	second, err := relayv1.NewRelayClient(secondConnection).Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect(second): %v", err)
	}
	if err := second.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(second): %v", err)
	}
	if _, err := second.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Recv(second) = %v, want ResourceExhausted", err)
	}
	if sessions.ActiveCount() != 1 {
		t.Fatalf("active sessions = %d, want 1", sessions.ActiveCount())
	}
}

func TestConnectRequiresAuthenticationBeforeTimeout(t *testing.T) {
	_, sessions, server := startTestServerWithOptions(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	}, 10, 50*time.Millisecond)
	connection := dialTestServer(t, server.Address())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := relayv1.NewRelayClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Recv() error = %v, want DeadlineExceeded", err)
	}
	if sessions.ActiveCount() != 0 {
		t.Fatalf("active sessions = %d, want 0", sessions.ActiveCount())
	}
}

func startTestServer(t *testing.T, config map[string]clientauth.ClientConfig) (*clientauth.Store, *clientsession.Manager, *Server) {
	t.Helper()
	return startTestServerWithOptions(t, config, 10, time.Second)
}

func startTestServerWithLimit(t *testing.T, config map[string]clientauth.ClientConfig, maxSessions uint32) (*clientauth.Store, *clientsession.Manager, *Server) {
	t.Helper()
	return startTestServerWithOptions(t, config, maxSessions, time.Second)
}

func startTestServerWithOptions(t *testing.T, config map[string]clientauth.ClientConfig, maxSessions uint32, authenticationTimeout time.Duration) (*clientauth.Store, *clientsession.Manager, *Server) {
	t.Helper()
	return startTestServerWithOptionsAndBindings(t, config, maxSessions, authenticationTimeout, &testBindingManager{})
}

func startTestServerWithOptionsAndBindings(t *testing.T, config map[string]clientauth.ClientConfig, maxSessions uint32, authenticationTimeout time.Duration, bindings BindingManager) (*clientauth.Store, *clientsession.Manager, *Server) {
	return startTestServerWithOptionsAndDependencies(t, config, maxSessions, authenticationTimeout, bindings, &testOpener{})
}

func startTestServerWithDependencies(t *testing.T, bindings BindingManager, opener Opener) (*clientauth.Store, *clientsession.Manager, *Server) {
	t.Helper()
	return startTestServerWithOptionsAndDependencies(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	}, 10, time.Second, bindings, opener)
}

func startTestServerWithOptionsAndDependencies(t *testing.T, config map[string]clientauth.ClientConfig, maxSessions uint32, authenticationTimeout time.Duration, bindings BindingManager, opener Opener) (*clientauth.Store, *clientsession.Manager, *Server) {
	return startTestServerWithRuntimeLimits(t, config, maxSessions, maxSessions, authenticationTimeout, bindings, opener)
}

func startTestServerWithRuntimeLimits(t *testing.T, config map[string]clientauth.ClientConfig, maxSessions, maxInFlightOpens uint32, authenticationTimeout time.Duration, bindings BindingManager, opener Opener) (*clientauth.Store, *clientsession.Manager, *Server) {
	t.Helper()
	store, err := clientauth.NewStore(config)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	sessions, err := clientsession.NewManager(store, maxSessions)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	service, err := NewService(sessions, bindings, opener, authenticationTimeout, time.Second, maxInFlightOpens)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	server, err := Start(context.Background(), Config{
		BindAddress:          "127.0.0.1:0",
		MaxConcurrentStreams: maxSessions,
	}, service)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		sessions.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
	})
	return store, sessions, server
}

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

func (o *testOpener) RelayPayload(ctx context.Context, session clientsession.Ref, pipeID string, payload []byte) error {
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
