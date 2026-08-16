package relaygrpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	store, err := clientauth.NewStore(config)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	sessions, err := clientsession.NewManager(store, maxSessions)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	service, err := NewService(sessions, authenticationTimeout)
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

func verifier(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(digest[:])
}
