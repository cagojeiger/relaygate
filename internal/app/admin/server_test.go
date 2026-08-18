package admin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/control/client"
	"github.com/cagojeiger/relaygate/internal/raft/node"
)

func TestTrustedLocalHealthStatusAndMetricsAreReadOnly(t *testing.T) {
	provider := staticStatusProvider{status: raftnode.Status{
		NodeID:        "node-1",
		Role:          "Leader",
		LeaderID:      "node-1",
		LeaderAddress: "127.0.0.1:27400",
		ClusterEpoch:  "epoch-1",
		Ready:         true,
	}}
	registry := prometheus.NewRegistry()
	presenceProvider := staticPresenceProvider{
		ref: authority.Ref{ClusterEpoch: "epoch-1", AuthorityID: "authority-1"},
		presence: authority.Presence{
			State:               authority.PresenceCurrent,
			CommittedGateways:   1,
			CommittedRoutes:     2,
			RevalidatedGateways: 1,
			EligibleRoutes:      2,
		},
	}
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, RuntimeSources{
		Role:         "controller",
		ClusterEpoch: "epoch-1",
		Raft:         provider,
		Presence:     presenceProvider,
	}, nil, registry)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	baseURL := "http://" + server.Address()
	for _, path := range []string{"/healthz/live", "/healthz/ready", "/metrics"} {
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
	statusBody, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read /status: %v", err)
	}
	var status RuntimeStatus
	if err := json.Unmarshal(statusBody, &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.Raft == nil || status.Raft.NodeID != "node-1" || !status.Raft.Ready {
		t.Fatalf("status = %#v", status)
	}
	if status.GatewayControl != nil || status.Presence == nil || status.Presence.State != authority.PresenceCurrent || status.AuthorityID != "authority-1" || status.AuthRevision != "" || status.RuntimeRole != "controller" || status.ClusterEpoch != "epoch-1" {
		t.Fatalf("runtime status = %#v", status)
	}
	for _, forbidden := range []string{
		"api_key",
		"payload",
		"buffer",
		"mutation",
		"complete",
		"config_converged",
		"expected_replicas",
	} {
		if strings.Contains(string(statusBody), forbidden) {
			t.Fatalf("/status exposed forbidden field %q: %s", forbidden, statusBody)
		}
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

func TestStatusFailsClosedWhenAuthorityCannotBeConfirmed(t *testing.T) {
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, RuntimeSources{
		Role:         "controller",
		ClusterEpoch: "epoch-1",
		Raft: staticStatusProvider{status: raftnode.Status{
			Role:         "Leader",
			ClusterEpoch: "epoch-1",
			Ready:        true,
		}},
		Presence: staticPresenceProvider{
			ref: authority.Ref{ClusterEpoch: "epoch-1", AuthorityID: "stale-authority"},
			presence: authority.Presence{
				State:               authority.PresenceCurrent,
				CommittedGateways:   1,
				CommittedRoutes:     1,
				RevalidatedGateways: 1,
				EligibleRoutes:      1,
			},
			err: authority.ErrNoAuthority,
		},
	}, nil, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	response, err := http.Get("http://" + server.Address() + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /status status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	var runtimeStatus RuntimeStatus
	if err := json.NewDecoder(response.Body).Decode(&runtimeStatus); err != nil {
		t.Fatalf("decode runtime status: %v", err)
	}
	if runtimeStatus.AuthorityID != "" || runtimeStatus.Presence == nil || runtimeStatus.Presence.State != authority.PresenceNoAuthority {
		t.Fatalf("runtime status reused stale authority: %#v", runtimeStatus)
	}
}

func TestReadyRequiresGatewayControlRevalidation(t *testing.T) {
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, RuntimeSources{
		Role:         "gateway",
		ClusterEpoch: "epoch-1",
		Gateway:      staticGatewayStatusProvider{status: gatewaycontrol.Status{State: gatewaycontrol.StateDisconnected}},
	}, staticAuthRevisionProvider("sha256:revision-1"), prometheus.NewRegistry())
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

func TestGatewayRoleReadinessDoesNotRequireLocalRaft(t *testing.T) {
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, RuntimeSources{
		Role:         "gateway",
		ClusterEpoch: "epoch-1",
		Gateway: staticGatewayStatusProvider{status: gatewaycontrol.Status{
			GatewayID: "gateway-4",
			State:     gatewaycontrol.StateRevalidated,
		}},
	}, staticAuthRevisionProvider("sha256:revision-1"), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	for _, path := range []string{"/healthz/ready", "/status"} {
		response, err := http.Get("http://" + server.Address() + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			t.Fatalf("GET %s status = %d", path, response.StatusCode)
		}
		if path == "/status" {
			var status RuntimeStatus
			if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
				_ = response.Body.Close()
				t.Fatalf("decode status: %v", err)
			}
			if status.RuntimeRole != "gateway" || status.ClusterEpoch != "epoch-1" || status.AuthRevision != "sha256:revision-1" || status.Raft != nil || status.Presence != nil || status.GatewayControl == nil || !status.GatewayControl.Ready() {
				_ = response.Body.Close()
				t.Fatalf("gateway-only status = %#v", status)
			}
		}
		_ = response.Body.Close()
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
	ref      authority.Ref
	presence authority.Presence
	err      error
}

type staticAuthRevisionProvider string

func (p staticAuthRevisionProvider) Revision() string {
	return string(p)
}

func (p staticPresenceProvider) Observe(context.Context) (authority.Ref, authority.Presence, error) {
	return p.ref, p.presence, p.err
}
