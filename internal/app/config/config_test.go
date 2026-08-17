package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
)

func TestLoadCanonicalConfig(t *testing.T) {
	path := filepath.Join("..", "..", "..", "configs", "relaygate.yaml")
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
	if config.Raft.BindAddress != "127.0.0.1:27400" ||
		config.Control.BindAddress != "127.0.0.1:27410" ||
		config.Relay.BindAddress != "127.0.0.1:27420" ||
		config.InternalRelay.BindAddress != "127.0.0.1:27430" ||
		config.InternalRelay.AdvertiseAddress != "127.0.0.1:27430" ||
		config.InternalRelay.ConnectTimeout.Value() != 2*time.Second ||
		config.InternalRelay.ShutdownTimeout.Value() != 5*time.Second ||
		config.Admin.BindAddress != "127.0.0.1:27490" ||
		config.Relay.AuthenticationTimeout.Value() <= 0 ||
		config.Relay.OpenTimeout.Value() <= 0 ||
		config.Relay.MaxClientSessions <= 0 ||
		config.Relay.MaxListenerBindings <= 0 ||
		config.Relay.MaxPipes <= 0 ||
		config.Control.AuthorityProbeInterval.Value() <= 0 {
		t.Fatalf("unexpected canonical bind config: raft=%#v control=%#v relay=%#v internal_relay=%#v admin=%#v", config.Raft, config.Control, config.Relay, config.InternalRelay, config.Admin)
	}
	if len(config.Clients) != 1 || len(config.Clients["local-development"].APIKeys) != 1 {
		t.Fatalf("unexpected client config: %#v", config.Clients)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("RELAYGATE_RAFT_NODE_ID", "node-2")
	t.Setenv("RELAYGATE_RAFT_BIND_ADDRESS", "0.0.0.0:27400")
	t.Setenv("RELAYGATE_RAFT_ADVERTISE_ADDRESS", "relaygate-2:27400")
	t.Setenv("RELAYGATE_RAFT_DATA_DIR", "/var/lib/relaygate")
	t.Setenv("RELAYGATE_RAFT_BOOTSTRAP", "true")
	t.Setenv("RELAYGATE_RAFT_BOOTSTRAP_VOTERS", `[{"node_id":"node-1","address":"relaygate-1:27400"},{"node_id":"node-2","address":"relaygate-2:27400"},{"node_id":"node-3","address":"relaygate-3:27400"}]`)
	t.Setenv("RELAYGATE_CONTROL_CLUSTER_EPOCH", "cluster-production-1")
	t.Setenv("RELAYGATE_CONTROL_BIND_ADDRESS", "0.0.0.0:27410")
	t.Setenv("RELAYGATE_GATEWAY_ID", "gateway-2")
	t.Setenv("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS", `["relaygate-1:27410","relaygate-2:27410","relaygate-3:27410"]`)
	t.Setenv("RELAYGATE_INTERNAL_RELAY_BIND_ADDRESS", "0.0.0.0:27430")
	t.Setenv("RELAYGATE_INTERNAL_RELAY_ADVERTISE_ADDRESS", "relaygate-2:27430")
	t.Setenv("RELAYGATE_ADMIN_BIND_ADDRESS", "0.0.0.0:27490")

	path := filepath.Join("..", "..", "..", "configs", "relaygate.yaml")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if config.Raft.NodeID != "node-2" || config.Raft.AdvertiseAddress != "relaygate-2:27400" {
		t.Fatalf("unexpected node identity: %#v", config.Raft)
	}
	if config.Raft.BindAddress != "0.0.0.0:27400" || config.Control.BindAddress != "0.0.0.0:27410" || config.Admin.BindAddress != "0.0.0.0:27490" {
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
	if config.Gateway.ID != "gateway-2" || len(config.Gateway.ControlEndpoints) != 3 || config.Gateway.ControlEndpoints[1] != "relaygate-2:27410" {
		t.Fatalf("gateway config = %#v", config.Gateway)
	}
	if config.InternalRelay.BindAddress != "0.0.0.0:27430" || config.InternalRelay.AdvertiseAddress != "relaygate-2:27430" {
		t.Fatalf("internal relay config = %#v", config.InternalRelay)
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	t.Run("bootstrap bool", func(t *testing.T) {
		t.Setenv("RELAYGATE_RAFT_BOOTSTRAP", "sometimes")
		path := filepath.Join("..", "..", "..", "configs", "relaygate.yaml")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "RELAYGATE_RAFT_BOOTSTRAP") {
			t.Fatalf("Load() error = %v, want bootstrap environment error", err)
		}
	})

	t.Run("bootstrap voters", func(t *testing.T) {
		t.Setenv("RELAYGATE_RAFT_BOOTSTRAP_VOTERS", `[{"node_id":"node-1","address":"127.0.0.1:27400","extra":true}]`)
		path := filepath.Join("..", "..", "..", "configs", "relaygate.yaml")
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("Load() error = %v, want strict voter environment error", err)
		}
	})

	t.Run("gateway endpoints", func(t *testing.T) {
		t.Setenv("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS", `["relaygate-1:27410",17]`)
		path := filepath.Join("..", "..", "..", "configs", "relaygate.yaml")
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

func TestLoadRejectsRemovedControlFields(t *testing.T) {
	for _, field := range []string{
		"max_distinct_binding_keys_per_epoch",
		"max_distinct_gateway_ids_per_epoch",
		"gateway_revalidation_timeout",
	} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "relaygate.yaml")
			contents := "control:\n  cluster_epoch: epoch-1\n  " + field + ": 1\n"
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatalf("WriteFile(): %v", err)
			}
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), field) {
				t.Fatalf("Load() error = %v, want removed field error", err)
			}
		})
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
	config.Relay.BindAddress = "0.0.0.0:27420"
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

	t.Run("open timeout", func(t *testing.T) {
		config := valid
		config.Relay.OpenTimeout = 0
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "relay timeouts") {
			t.Fatalf("Validate() error = %v, want relay timeout error", err)
		}
	})

	t.Run("listener capacity", func(t *testing.T) {
		config := valid
		config.Relay.MaxListenerBindings = 0
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "max_listener_bindings") {
			t.Fatalf("Validate() error = %v, want max_listener_bindings error", err)
		}
	})

	t.Run("listener capacity protocol maximum", func(t *testing.T) {
		config := valid
		config.Relay.MaxListenerBindings++
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "max_listener_bindings") {
			t.Fatalf("Validate() error = %v, want max_listener_bindings maximum error", err)
		}
	})

	t.Run("pipe capacity", func(t *testing.T) {
		config := valid
		config.Relay.MaxPipes = 0
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "max_pipes") {
			t.Fatalf("Validate() error = %v, want max_pipes error", err)
		}
	})

	t.Run("pipe capacity maximum", func(t *testing.T) {
		config := valid
		config.Relay.MaxPipes = maxRelayPipes
		if err := config.Validate(); err != nil {
			t.Fatalf("Validate() error = %v, want maximum max_pipes accepted", err)
		}

		config.Relay.MaxPipes++
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "max_pipes") {
			t.Fatalf("Validate() error = %v, want max_pipes maximum error", err)
		}
	})
}

func TestValidateRejectsInvalidInternalRelayConfig(t *testing.T) {
	valid := Defaults()
	valid.Raft.NodeID = "node-1"
	valid.Gateway.ID = "gateway-1"
	valid.Control.ClusterEpoch = "epoch-1"

	for _, test := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "bind address", mutate: func(config *Config) { config.InternalRelay.BindAddress = "missing-port" }, want: "internal_relay.bind_address"},
		{name: "bind port", mutate: func(config *Config) { config.InternalRelay.BindAddress = "127.0.0.1:" }, want: "internal_relay.bind_address"},
		{name: "advertise address", mutate: func(config *Config) { config.InternalRelay.AdvertiseAddress = "0.0.0.0:27430" }, want: "internal_relay.advertise_address"},
		{name: "advertise port", mutate: func(config *Config) { config.InternalRelay.AdvertiseAddress = "relaygate-1:" }, want: "internal_relay.advertise_address"},
		{name: "advertise address bound", mutate: func(config *Config) {
			config.InternalRelay.AdvertiseAddress = strings.Repeat("r", maxInternalRelayAddressBytes) + ":27430"
		}, want: "internal_relay.advertise_address"},
		{name: "connect timeout", mutate: func(config *Config) { config.InternalRelay.ConnectTimeout = 0 }, want: "internal_relay timeouts"},
		{name: "shutdown timeout", mutate: func(config *Config) { config.InternalRelay.ShutdownTimeout = 0 }, want: "internal_relay timeouts"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			if err := config.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}
