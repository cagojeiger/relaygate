package relaygrpc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	composeClientID = "local-development"
	composeAPIKeyID = "primary"
	composeAPIKey   = "relaygate-local-development-key"
)

func TestComposePublicRelaySmoke(t *testing.T) {
	address := os.Getenv("RELAYGATE_COMPOSE_RELAY_ADDR")
	if address == "" {
		t.Skip("RELAYGATE_COMPOSE_RELAY_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connection, err := grpc.NewClient(
		"passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	defer connection.Close()

	client := relayv1.NewRelayClient(connection)
	listener, _ := authenticateComposeStream(t, ctx, client)
	caller, callerSessionID := authenticateComposeStream(t, ctx, client)
	runID := time.Now().UnixNano()
	endpoint := fmt.Sprintf("/compose/relay/%d", runID)
	targetID := "smoke"
	requestID := fmt.Sprintf("compose-open-%d", runID)

	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: endpoint, TargetId: targetID},
	}}); err != nil {
		t.Fatalf("Send(BindListener): %v", err)
	}
	boundResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerBound): %v", err)
	}
	bound := boundResponse.GetListenerBound().GetBinding()
	if bound.GetListenerBindingId() == "" || bound.GetEndpointPattern() != endpoint || bound.GetTargetId() != targetID {
		t.Fatalf("ListenerBound = %#v", bound)
	}

	if err := caller.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Open{
		Open: &relayv1.Open{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
	}}); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	offerResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerOffer): %v", err)
	}
	offer := offerResponse.GetListenerOffer()
	if offer.GetAttemptId() == "" || offer.GetListenerBindingId() != bound.GetListenerBindingId() ||
		offer.GetEndpoint() != endpoint || offer.GetTargetId() != targetID || offer.GetCallerSessionId() != callerSessionID {
		t.Fatalf("ListenerOffer = %#v", offer)
	}

	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{
		ListenerAccept: &relayv1.ListenerAccept{AttemptId: offer.GetAttemptId()},
	}}); err != nil {
		t.Fatalf("Send(ListenerAccept): %v", err)
	}
	establishedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerEstablished): %v", err)
	}
	established := establishedResponse.GetListenerEstablished()
	if established.GetAttemptId() != offer.GetAttemptId() || established.GetPipeId() == "" {
		t.Fatalf("ListenerEstablished = %#v", established)
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
		ListenerConfirmed: &relayv1.ListenerConfirmed{
			AttemptId: established.GetAttemptId(),
			PipeId:    established.GetPipeId(),
		},
	}}); err != nil {
		t.Fatalf("Send(ListenerConfirmed): %v", err)
	}

	openedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpened): %v", err)
	}
	opened := openedResponse.GetPipeOpened()
	if opened.GetRequestId() != requestID || opened.GetAttemptId() != established.GetAttemptId() ||
		opened.GetPipeId() != established.GetPipeId() || opened.GetEndpoint() != endpoint || opened.GetTargetId() != targetID {
		t.Fatalf("PipeOpened = %#v", opened)
	}

	if err := caller.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
		ClosePipe: &relayv1.ClosePipe{PipeId: opened.GetPipeId()},
	}}); err != nil {
		t.Fatalf("Send(ClosePipe): %v", err)
	}
	closedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeCloseAcknowledged): %v", err)
	}
	closed := closedResponse.GetPipeCloseAcknowledged()
	if closed.GetPipeId() != opened.GetPipeId() || !closed.GetOwned() {
		t.Fatalf("PipeCloseAcknowledged = %#v", closed)
	}
	terminatedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerTerminated): %v", err)
	}
	terminated := terminatedResponse.GetListenerTerminated()
	if terminated.GetAttemptId() != opened.GetAttemptId() || terminated.GetPipeId() != opened.GetPipeId() {
		t.Fatalf("ListenerTerminated = %#v", terminated)
	}

	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_UnbindListener{
		UnbindListener: &relayv1.UnbindListener{ListenerBindingId: bound.GetListenerBindingId()},
	}}); err != nil {
		t.Fatalf("Send(UnbindListener): %v", err)
	}
	unboundResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerUnbound): %v", err)
	}
	if got := unboundResponse.GetListenerUnbound().GetListenerBindingId(); got != bound.GetListenerBindingId() {
		t.Fatalf("ListenerUnbound binding ID = %q, want %q", got, bound.GetListenerBindingId())
	}
}

func authenticateComposeStream(t *testing.T, ctx context.Context, client relayv1.RelayClient) (relayv1.Relay_ConnectClient, string) {
	t.Helper()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Authenticate{
		Authenticate: &relayv1.Authenticate{
			ClientId: composeClientID,
			ApiKeyId: composeAPIKeyID,
			ApiKey:   composeAPIKey,
		},
	}}); err != nil {
		t.Fatalf("Send(Authenticate): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ClientSessionOpened): %v", err)
	}
	session := response.GetClientSessionOpened().GetSession()
	if session.GetClientSessionId() == "" || session.GetClientId() != composeClientID ||
		session.GetApiKeyId() != composeAPIKeyID || session.GetAuthRevision() == "" {
		t.Fatalf("ClientSessionOpened = %#v", session)
	}
	return stream, session.GetClientSessionId()
}
