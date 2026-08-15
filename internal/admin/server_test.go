package admin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/gatewaycontrol"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

func TestHealthStatusAndMetricsAreReadOnly(t *testing.T) {
	provider := staticStatusProvider{status: raftnode.Status{
		NodeID:        "node-1",
		Role:          "Leader",
		LeaderID:      "node-1",
		LeaderAddress: "127.0.0.1:7000",
		ClusterEpoch:  "epoch-1",
		Ready:         true,
	}}
	registry := prometheus.NewRegistry()
	gatewayProvider := staticGatewayStatusProvider{status: gatewaycontrol.Status{
		GatewayID:         "gateway-1",
		GatewayInstanceID: "instance-1",
		State:             gatewaycontrol.StateRevalidated,
	}}
	presenceProvider := staticPresenceProvider{presence: authority.Presence{
		State:       authority.PresenceComplete,
		Committed:   1,
		Classified:  1,
		Revalidated: 1,
	}}
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, provider, gatewayProvider, presenceProvider, registry)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	baseURL := "http://" + server.Address()
	for _, path := range []string{"/healthz/live", "/healthz/ready", "/status", "/metrics"} {
		response, err := http.Get(baseURL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, response.StatusCode)
		}
	}

	response, err := http.Get(baseURL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer response.Body.Close()
	var status raftnode.Status
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.NodeID != "node-1" || !status.Ready {
		t.Fatalf("status = %#v", status)
	}

	response, err = http.Get(baseURL + "/status")
	if err != nil {
		t.Fatalf("GET /status runtime: %v", err)
	}
	defer response.Body.Close()
	var runtimeStatus RuntimeStatus
	if err := json.NewDecoder(response.Body).Decode(&runtimeStatus); err != nil {
		t.Fatalf("decode runtime status: %v", err)
	}
	if !runtimeStatus.GatewayControl.Ready() || runtimeStatus.Presence.State != authority.PresenceComplete {
		t.Fatalf("runtime status = %#v", runtimeStatus)
	}

	request, err := http.NewRequest(http.MethodPost, baseURL+"/status", nil)
	if err != nil {
		t.Fatalf("NewRequest(): %v", err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST /status: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /status status = %d, want %d", response.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestReadyRequiresGatewayControlRevalidation(t *testing.T) {
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, staticStatusProvider{status: raftnode.Status{Ready: true}}, staticGatewayStatusProvider{
		status: gatewaycontrol.Status{State: gatewaycontrol.StateDisconnected},
	}, staticPresenceProvider{}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	response, err := http.Get("http://" + server.Address() + "/healthz/ready")
	if err != nil {
		t.Fatalf("GET /healthz/ready: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

type staticStatusProvider struct {
	status raftnode.Status
}

func (p staticStatusProvider) Status() raftnode.Status {
	return p.status
}

type staticGatewayStatusProvider struct {
	status gatewaycontrol.Status
}

func (p staticGatewayStatusProvider) Status() gatewaycontrol.Status {
	return p.status
}

type staticPresenceProvider struct {
	presence authority.Presence
}

func (p staticPresenceProvider) Presence() authority.Presence {
	return p.presence
}
