package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cagojeiger/relaygate/internal/clientauth"
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
	if config.Raft.BindAddress != "127.0.0.1:7000" ||
		config.Control.BindAddress != "127.0.0.1:7100" ||
		config.Relay.BindAddress != "127.0.0.1:7200" ||
		config.Admin.BindAddress != "127.0.0.1:9090" ||
		config.Relay.AuthenticationTimeout.Value() <= 0 ||
		config.Relay.MaxClientSessions <= 0 ||
		config.Control.AuthorityProbeInterval.Value() <= 0 ||
		config.Control.GatewayRevalidationTimeout.Value() <= 0 {
		t.Fatalf("unexpected canonical bind config: raft=%#v control=%#v relay=%#v admin=%#v", config.Raft, config.Control, config.Relay, config.Admin)
	}
	if len(config.Clients) != 1 || len(config.Clients["local-development"].APIKeys) != 1 {
		t.Fatalf("unexpected client config: %#v", config.Clients)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("RELAYGATE_RAFT_NODE_ID", "node-2")
	t.Setenv("RELAYGATE_RAFT_BIND_ADDRESS", "0.0.0.0:7000")
	t.Setenv("RELAYGATE_RAFT_ADVERTISE_ADDRESS", "relaygate-2:7000")
	t.Setenv("RELAYGATE_RAFT_DATA_DIR", "/var/lib/relaygate")
	t.Setenv("RELAYGATE_RAFT_BOOTSTRAP", "true")
	t.Setenv("RELAYGATE_RAFT_BOOTSTRAP_VOTERS", `[{"node_id":"node-1","address":"relaygate-1:7000"},{"node_id":"node-2","address":"relaygate-2:7000"},{"node_id":"node-3","address":"relaygate-3:7000"}]`)
	t.Setenv("RELAYGATE_CONTROL_CLUSTER_EPOCH", "cluster-production-1")
	t.Setenv("RELAYGATE_CONTROL_BIND_ADDRESS", "0.0.0.0:7100")
	t.Setenv("RELAYGATE_GATEWAY_ID", "gateway-2")
	t.Setenv("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS", `["relaygate-1:7100","relaygate-2:7100","relaygate-3:7100"]`)
	t.Setenv("RELAYGATE_ADMIN_BIND_ADDRESS", "0.0.0.0:9090")

	path := filepath.Join("..", "..", "configs", "relaygate.yaml")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if config.Raft.NodeID != "node-2" || config.Raft.AdvertiseAddress != "relaygate-2:7000" {
		t.Fatalf("unexpected node identity: %#v", config.Raft)
	}
	if config.Raft.BindAddress != "0.0.0.0:7000" || config.Control.BindAddress != "0.0.0.0:7100" || config.Admin.BindAddress != "0.0.0.0:9090" {
		t.Fatalf("unexpected bind overrides: raft=%q control=%q admin=%q", config.Raft.BindAddress, config.Control.BindAddress, config.Admin.BindAddress)
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

func TestValidateClientReloadAllowsOnlyClientChanges(t *testing.T) {
	current := Defaults()
	current.Clients = map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": "sha256:" + strings.Repeat("0", 64)}},
	}
	candidate := current
	candidate.Clients = map[string]clientauth.ClientConfig{
		"client-b": {APIKeys: map[string]string{"key-b": "sha256:" + strings.Repeat("1", 64)}},
	}
	if err := ValidateClientReload(current, candidate); err != nil {
		t.Fatalf("ValidateClientReload(client-only): %v", err)
	}
	candidate.Admin.BindAddress = "127.0.0.1:9191"
	if err := ValidateClientReload(current, candidate); err == nil {
		t.Fatal("ValidateClientReload() accepted a static config change")
	}
}

func TestValidateRejectsNonLoopbackRelayAddress(t *testing.T) {
	config := Defaults()
	config.Raft.NodeID = "node-1"
	config.Gateway.ID = "gateway-1"
	config.Control.ClusterEpoch = "epoch-1"
	config.Relay.BindAddress = "0.0.0.0:7200"
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Validate() error = %v, want loopback error", err)
	}
}

func TestValidateRejectsInvalidRelayBounds(t *testing.T) {
	valid := Defaults()
	valid.Raft.NodeID = "node-1"
	valid.Gateway.ID = "gateway-1"
	valid.Control.ClusterEpoch = "epoch-1"

	t.Run("authentication timeout", func(t *testing.T) {
		config := valid
		config.Relay.AuthenticationTimeout = 0
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "relay timeouts") {
			t.Fatalf("Validate() error = %v, want relay timeout error", err)
		}
	})

	t.Run("session capacity", func(t *testing.T) {
		config := valid
		config.Relay.MaxClientSessions = 0
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "max_client_sessions") {
			t.Fatalf("Validate() error = %v, want max_client_sessions error", err)
		}
	})
}
