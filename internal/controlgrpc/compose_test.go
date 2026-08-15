package controlgrpc

import (
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
}
