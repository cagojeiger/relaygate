package gatewaycontrol_test

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/admin"
	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/gatewaycontrol"
)

func TestComposeGatewayControlReady(t *testing.T) {
	configured := os.Getenv("RELAYGATE_COMPOSE_ADMIN_ADDRS")
	if configured == "" {
		t.Skip("RELAYGATE_COMPOSE_ADMIN_ADDRS is not set")
	}
	addresses := strings.Split(configured, ",")
	expectedCommitted := environmentInt(t, "RELAYGATE_COMPOSE_EXPECTED_COMMITTED", len(addresses))
	expectedRevalidated := environmentInt(t, "RELAYGATE_COMPOSE_EXPECTED_REVALIDATED", len(addresses))

	client := &http.Client{Timeout: 2 * time.Second}
	var leader *admin.RuntimeStatus
	seenGateways := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		readyResponse, err := client.Get("http://" + address + "/healthz/ready")
		if err != nil {
			t.Fatalf("GET %s/healthz/ready: %v", address, err)
		}
		_ = readyResponse.Body.Close()
		if readyResponse.StatusCode != http.StatusOK {
			t.Fatalf("GET %s/healthz/ready status = %d", address, readyResponse.StatusCode)
		}

		response, err := client.Get("http://" + address + "/status")
		if err != nil {
			t.Fatalf("GET %s/status: %v", address, err)
		}
		var status admin.RuntimeStatus
		decodeErr := json.NewDecoder(response.Body).Decode(&status)
		_ = response.Body.Close()
		if decodeErr != nil {
			t.Fatalf("decode %s/status: %v", address, decodeErr)
		}
		if status.GatewayControl.State != gatewaycontrol.StateRevalidated {
			t.Fatalf("%s gateway control = %#v", address, status.GatewayControl)
		}
		if _, duplicate := seenGateways[status.GatewayControl.GatewayID]; duplicate {
			t.Fatalf("duplicate GatewayId %q", status.GatewayControl.GatewayID)
		}
		seenGateways[status.GatewayControl.GatewayID] = struct{}{}
		if status.Role == "Leader" {
			if leader != nil {
				t.Fatal("multiple leaders reported")
			}
			leader = &status
		}
	}
	if leader == nil {
		t.Fatal("no leader reported")
	}
	if leader.Presence.State != authority.PresenceComplete ||
		leader.Presence.Committed != expectedCommitted ||
		leader.Presence.Classified != expectedCommitted ||
		leader.Presence.Revalidated != expectedRevalidated {
		t.Fatalf("leader presence = %#v, want committed/classified=%d revalidated=%d", leader.Presence, expectedCommitted, expectedRevalidated)
	}
}

func environmentInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		t.Fatalf("%s=%q is not a non-negative integer: %v", name, value, err)
	}
	return parsed
}
