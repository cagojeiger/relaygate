package controlgrpc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
)

func TestComposeControlSmoke(t *testing.T) {
	address := os.Getenv("RELAYGATE_COMPOSE_CONTROL_ADDR")
	if address == "" {
		t.Skip("RELAYGATE_COMPOSE_CONTROL_ADDR is not set")
	}
	epoch := os.Getenv("RELAYGATE_COMPOSE_CLUSTER_EPOCH")
	if epoch == "" {
		epoch = "local-compose-1"
	}

	runID := fmt.Sprintf("compose-smoke-%d", time.Now().UnixNano())
	instanceID := runID + "-instance"
	connection, stream, session := connectAndSync(t, address, epoch, runID, instanceID)
	defer connection.Close()
	request := installRequest(
		session,
		&controlv1.BindingKey{ClientId: runID, EndpointPattern: "/health", TargetId: "self"},
		0,
		nil,
		&controlv1.ListenerBindingRef{
			GatewayInstanceId: instanceID,
			ListenerBindingId: runID + "-listener",
		},
	)
	if err := stream.Send(request); err != nil {
		t.Fatalf("Send(mutation): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(mutation): %v", err)
	}
	code := response.GetMutationResult().GetCode()
	if code != controlv1.MutationCode_MUTATION_CODE_APPLIED && code != controlv1.MutationCode_MUTATION_CODE_ALREADY_APPLIED {
		t.Fatalf("mutation code = %v", code)
	}

	// This optional oracle proves only the authority-side A/L/Q/C/V decision.
	// It does not reserve local O, offer a listener, or claim public Open success.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admission, err := controlv1.NewGatewayControlClient(connection).AdmitOpen(ctx, &controlv1.AdmitOpenRequest{
		Session: session,
		Auth: &controlv1.AuthContext{
			ClientSessionId: runID + "-client-session",
			ClientId:        runID,
			ApiKeyId:        "compose-key",
			AuthRevision:    "compose-revision",
		},
		Endpoint: "/health",
		TargetId: "self",
	})
	if err != nil {
		t.Fatalf("AdmitOpen(A/L/Q/C/V): %v", err)
	}
	openContext := admission.GetContext()
	if openContext.GetClusterEpoch() != epoch || openContext.GetAuthorityId() != session.GetAuthorityId() ||
		openContext.GetAttemptId() == "" || openContext.GetBinding().GetGeneration() == 0 ||
		openContext.GetIngressGatewayId() != session.GetGatewayId() ||
		openContext.GetIngressGatewayInstanceId() != session.GetGatewayInstanceId() ||
		openContext.GetIngressControlSessionId() != session.GetControlSessionId() ||
		openContext.GetOwnerRelayAddress() != testRelayAddress(runID) ||
		openContext.GetExpiresAtUnixMillis() <= time.Now().UnixMilli() ||
		openContext.GetBinding().GetKey().GetClientId() != runID ||
		openContext.GetBinding().GetKey().GetEndpointPattern() != "/health" ||
		openContext.GetBinding().GetKey().GetTargetId() != "self" ||
		openContext.GetBinding().GetRef().GetGatewayId() != runID ||
		openContext.GetBinding().GetRef().GetGatewayInstanceId() != instanceID ||
		openContext.GetBinding().GetRef().GetListenerBindingId() != runID+"-listener" {
		t.Fatalf("authority-only Open context = %#v", openContext)
	}
}
