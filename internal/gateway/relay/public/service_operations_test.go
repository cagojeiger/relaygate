package relaygrpc

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc/codes"
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

func TestConnectReturnsBindFailuresAndKeepsStreamUsable(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		failure relayv1.ListenerBindingFailure
	}{
		{name: "invalid", err: localbinding.ErrInvalid, failure: relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_INVALID_REQUEST},
		{name: "capacity", err: localbinding.ErrCapacity, failure: relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CAPACITY_REACHED},
		{name: "conflict", err: localbinding.ErrConflict, failure: relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CONFLICT},
		{name: "control unavailable", err: localbinding.ErrUnavailable, failure: relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_UNAVAILABLE},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			bindings := &testBindingManager{bind: func(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
				if calls.Add(1) == 1 {
					return routing.LiveBinding{}, fmt.Errorf("sensitive detail: %w", test.err)
				}
				return testListenerSlot("client-a", "/jobs", "worker"), nil
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
			response, err := stream.Recv()
			if err != nil {
				t.Fatalf("Recv(bind failure): %v", err)
			}
			failed := response.GetListenerBindFailed()
			if failed == nil || failed.GetFailure() != test.failure || failed.GetEndpointPattern() != "/jobs" || failed.GetTargetId() != "worker" {
				t.Fatalf("ListenerBindFailed = %#v, want failure %v", failed, test.failure)
			}

			if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
				BindListener: &relayv1.BindListener{EndpointPattern: "/jobs", TargetId: "worker"},
			}}); err != nil {
				t.Fatalf("Send(valid bind): %v", err)
			}
			response, err = stream.Recv()
			if err != nil {
				t.Fatalf("Recv(valid bind): %v", err)
			}
			if response.GetListenerBound() == nil {
				t.Fatalf("valid bind response = %#v, want ListenerBound", response)
			}
		})
	}
}

func TestConnectReturnsUnbindFailureAndKeepsStreamUsable(t *testing.T) {
	bindings := &testBindingManager{
		bind: func(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
			return testListenerSlot("client-a", "/jobs", "worker"), nil
		},
		unbind: func(clientsession.Ref, string) error {
			return fmt.Errorf("sensitive detail: %w", localbinding.ErrConflict)
		},
	}
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
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_UnbindListener{
		UnbindListener: &relayv1.UnbindListener{ListenerBindingId: "listener-a"},
	}}); err != nil {
		t.Fatalf("Send(unbind): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(unbind failure): %v", err)
	}
	failed := response.GetListenerUnbindFailed()
	if failed == nil || failed.GetFailure() != relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CONFLICT || failed.GetListenerBindingId() != "listener-a" {
		t.Fatalf("ListenerUnbindFailed = %#v", failed)
	}
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: "/jobs", TargetId: "worker"},
	}}); err != nil {
		t.Fatalf("Send(valid bind): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(valid bind): %v", err)
	}
	if response.GetListenerBound() == nil {
		t.Fatalf("valid bind response = %#v, want ListenerBound", response)
	}
}

func TestConnectKeepsSessionEndedBindingFailureStreamFatal(t *testing.T) {
	bindings := &testBindingManager{bind: func(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
		return routing.LiveBinding{}, localbinding.ErrSessionEnded
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
	if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Recv(session-ended bind) = %v, want Unauthenticated", err)
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
