package relaygrpc

import (
	"context"
	"os"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRuntimeAuthenticatedSession(t *testing.T) {
	address := os.Getenv("RELAYGATE_RUNTIME_RELAY_ADDR")
	if address == "" {
		t.Skip("RELAYGATE_RUNTIME_RELAY_ADDR is not set")
	}
	clientID := os.Getenv("RELAYGATE_RUNTIME_CLIENT_ID")
	apiKeyID := os.Getenv("RELAYGATE_RUNTIME_API_KEY_ID")
	apiKey := os.Getenv("RELAYGATE_RUNTIME_API_KEY")
	if clientID == "" || apiKeyID == "" || apiKey == "" {
		t.Fatal("runtime client ID, API key ID and API key are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	defer connection.Close()
	stream, err := relayv1.NewRelayClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest(clientID, apiKeyID, apiKey)); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(session): %v", err)
	}
	session := response.GetClientSessionOpened().GetSession()
	if session.GetClientSessionId() == "" || session.GetClientId() != clientID || session.GetApiKeyId() != apiKeyID || session.GetAuthRevision() == "" {
		t.Fatalf("session = %#v", session)
	}
}
