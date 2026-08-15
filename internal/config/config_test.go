package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadCanonicalConfig(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "relaygate.yaml")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if config.Raft.NodeID != "node-1" || !config.Raft.Bootstrap {
		t.Fatalf("unexpected raft config: %#v", config.Raft)
	}
	if config.Gateway.ID != "gateway-1" || len(config.Gateway.ControlEndpoints) != 1 {
		t.Fatalf("unexpected gateway config: %#v", config.Gateway)
	}
	if config.Control.ClusterEpoch != "relaygate-v1" {
		t.Fatalf("cluster epoch = %q", config.Control.ClusterEpoch)
	}
	if config.Control.BindAddress != "0.0.0.0:7100" ||
		config.Control.AuthorityProbeInterval.Value() <= 0 ||
		config.Control.GatewayRevalidationTimeout.Value() <= 0 {
		t.Fatalf("unexpected control config: %#v", config.Control)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("RELAYGATE_RAFT_NODE_ID", "node-2")
	t.Setenv("RELAYGATE_RAFT_ADVERTISE_ADDRESS", "relaygate-2:7000")
	t.Setenv("RELAYGATE_RAFT_DATA_DIR", "/var/lib/relaygate")
	t.Setenv("RELAYGATE_RAFT_BOOTSTRAP", "true")
	t.Setenv("RELAYGATE_RAFT_BOOTSTRAP_VOTERS", `[{"node_id":"node-1","address":"relaygate-1:7000"},{"node_id":"node-2","address":"relaygate-2:7000"},{"node_id":"node-3","address":"relaygate-3:7000"}]`)
	t.Setenv("RELAYGATE_CONTROL_CLUSTER_EPOCH", "cluster-production-1")
	t.Setenv("RELAYGATE_GATEWAY_ID", "gateway-2")
	t.Setenv("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS", `["relaygate-1:7100","relaygate-2:7100","relaygate-3:7100"]`)

	path := filepath.Join("..", "..", "configs", "relaygate.yaml")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if config.Raft.NodeID != "node-2" || config.Raft.AdvertiseAddress != "relaygate-2:7000" {
		t.Fatalf("unexpected node identity: %#v", config.Raft)
	}
	if !config.Raft.Bootstrap || len(config.Raft.BootstrapVoters) != 3 {
		t.Fatalf("unexpected bootstrap voters: %#v", config.Raft)
	}
	if config.Raft.DataDir != "/var/lib/relaygate" {
		t.Fatalf("data dir = %q", config.Raft.DataDir)
	}
	if config.Control.ClusterEpoch != "cluster-production-1" {
		t.Fatalf("cluster epoch = %q", config.Control.ClusterEpoch)
	}
	if config.Gateway.ID != "gateway-2" || len(config.Gateway.ControlEndpoints) != 3 || config.Gateway.ControlEndpoints[1] != "relaygate-2:7100" {
		t.Fatalf("gateway config = %#v", config.Gateway)
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	t.Run("bootstrap bool", func(t *testing.T) {
		t.Setenv("RELAYGATE_RAFT_BOOTSTRAP", "sometimes")
		path := filepath.Join("..", "..", "configs", "relaygate.yaml")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "RELAYGATE_RAFT_BOOTSTRAP") {
			t.Fatalf("Load() error = %v, want bootstrap environment error", err)
		}
	})

	t.Run("bootstrap voters", func(t *testing.T) {
		t.Setenv("RELAYGATE_RAFT_BOOTSTRAP_VOTERS", `[{"node_id":"node-1","address":"127.0.0.1:7000","extra":true}]`)
		path := filepath.Join("..", "..", "configs", "relaygate.yaml")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("Load() error = %v, want strict voter environment error", err)
		}
	})

	t.Run("gateway endpoints", func(t *testing.T) {
		t.Setenv("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS", `["relaygate-1:7100",17]`)
		path := filepath.Join("..", "..", "configs", "relaygate.yaml")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "RELAYGATE_GATEWAY_CONTROL_ENDPOINTS") {
			t.Fatalf("Load() error = %v, want gateway endpoint environment error", err)
		}
	})
}

func TestLoadRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relaygate.yaml")
	contents := `
raft:
  node_id: node-1
  unknown_setting: true
control:
  cluster_epoch: epoch-1
admin: {}
logging: {}
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(): %v", err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field unknown_setting not found") {
		t.Fatalf("Load() error = %v, want unknown field", err)
	}
}
