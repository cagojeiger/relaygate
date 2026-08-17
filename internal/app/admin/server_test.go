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
	gatewayProvider := staticGatewayStatusProvider{status: gatewaycontrol.Status{
		GatewayID:         "gateway-1",
		GatewayInstanceID: "instance-1",
		State:             gatewaycontrol.StateRevalidated,
	}}
	presenceProvider := staticPresenceProvider{
		ref: authority.Ref{ClusterEpoch: "epoch-1", AuthorityID: "authority-1"},
		presence: authority.Presence{
			State:       authority.PresenceCurrent,
			Sessions:    1,
			Revalidated: 1,
			Bindings:    2,
		},
	}
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, provider, gatewayProvider, presenceProvider, staticAuthRevisionProvider("sha256:revision-1"), registry)
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
	if !runtimeStatus.GatewayControl.Ready() || runtimeStatus.Presence.State != authority.PresenceCurrent || runtimeStatus.AuthorityID != "authority-1" || runtimeStatus.AuthRevision != "sha256:revision-1" {
		t.Fatalf("runtime status = %#v", runtimeStatus)
	}

	response, err = http.Get(baseURL + "/status")
	if err != nil {
		t.Fatalf("GET /status redaction: %v", err)
	}
	statusBody, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read /status redaction: %v", err)
	}
	for _, forbidden := range []string{"api_key", "payload", "buffer", "mutation"} {
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
	}, staticStatusProvider{status: raftnode.Status{
		Role:         "Leader",
		ClusterEpoch: "epoch-1",
		Ready:        true,
	}}, staticGatewayStatusProvider{status: gatewaycontrol.Status{
		State: gatewaycontrol.StateRevalidated,
	}}, staticPresenceProvider{
		ref: authority.Ref{ClusterEpoch: "epoch-1", AuthorityID: "stale-authority"},
		presence: authority.Presence{
			State:       authority.PresenceCurrent,
			Sessions:    1,
			Revalidated: 1,
			Bindings:    1,
		},
		err: authority.ErrNoAuthority,
	}, staticAuthRevisionProvider("sha256:revision-1"), prometheus.NewRegistry())
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
	if runtimeStatus.AuthorityID != "" || runtimeStatus.Presence.State != authority.PresenceNoAuthority {
		t.Fatalf("runtime status reused stale authority: %#v", runtimeStatus)
	}
}

func TestReadyRequiresGatewayControlRevalidation(t *testing.T) {
	server, err := Start(context.Background(), Config{
		BindAddress:  "127.0.0.1:0",
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, staticStatusProvider{status: raftnode.Status{Ready: true}}, staticGatewayStatusProvider{
		status: gatewaycontrol.Status{State: gatewaycontrol.StateDisconnected},
	}, staticPresenceProvider{}, staticAuthRevisionProvider("sha256:revision-1"), prometheus.NewRegistry())
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
