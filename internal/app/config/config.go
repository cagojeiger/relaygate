package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Value() time.Duration {
	return time.Duration(d)
}

type Config struct {
	Runtime       RuntimeConfig                      `yaml:"runtime"`
	Raft          RaftConfig                         `yaml:"raft"`
	Gateway       GatewayConfig                      `yaml:"gateway"`
	Relay         RelayConfig                        `yaml:"relay"`
	InternalRelay InternalRelayConfig                `yaml:"internal_relay"`
	Control       ControlConfig                      `yaml:"control"`
	Admin         AdminConfig                        `yaml:"admin"`
	Logging       LoggingConfig                      `yaml:"logging"`
	Clients       map[string]clientauth.ClientConfig `yaml:"clients"`
}

type RuntimeRole string

const (
	RuntimeRoleController RuntimeRole = "controller"
	RuntimeRoleGateway    RuntimeRole = "gateway"
)

type RuntimeConfig struct {
	Role RuntimeRole `yaml:"role"`
}

func (r RuntimeRole) HasController() bool {
	return r == RuntimeRoleController
}

type RaftConfig struct {
	NodeID           string `yaml:"node_id"`
	DataDir          string `yaml:"data_dir"`
	BindAddress      string `yaml:"bind_address"`
	AdvertiseAddress string `yaml:"advertise_address"`
	// Bootstrap is an explicit initial-cluster action, never a recovery mode.
	// Production deployment must turn it off after the first successful seed.
	Bootstrap         bool              `yaml:"bootstrap"`
	BootstrapVoters   []RaftVoterConfig `yaml:"bootstrap_voters"`
	ApplyTimeout      Duration          `yaml:"apply_timeout"`
	TransportTimeout  Duration          `yaml:"transport_timeout"`
	ShutdownTimeout   Duration          `yaml:"shutdown_timeout"`
	SnapshotThreshold uint64            `yaml:"snapshot_threshold"`
	SnapshotInterval  Duration          `yaml:"snapshot_interval"`
	SnapshotRetain    int               `yaml:"snapshot_retain"`
	MaxPool           int               `yaml:"max_pool"`
	MaxCommandBytes   int               `yaml:"max_command_bytes"`
}

type RaftVoterConfig struct {
	NodeID  string `yaml:"node_id" json:"node_id"`
	Address string `yaml:"address" json:"address"`
}

type GatewayConfig struct {
	ID               string   `yaml:"id"`
	ControlEndpoints []string `yaml:"control_endpoints"`
	ConnectTimeout   Duration `yaml:"connect_timeout"`
	RetryInterval    Duration `yaml:"retry_interval"`
}

type RelayConfig struct {
	BindAddress           string   `yaml:"bind_address"`
	AuthenticationTimeout Duration `yaml:"authentication_timeout"`
	OpenTimeout           Duration `yaml:"open_timeout"`
	ShutdownTimeout       Duration `yaml:"shutdown_timeout"`
	MaxClientSessions     uint32   `yaml:"max_client_sessions"`
	MaxListenerBindings   uint32   `yaml:"max_listener_bindings"`
	MaxPipes              uint32   `yaml:"max_pipes"`
}

type InternalRelayConfig struct {
	BindAddress      string   `yaml:"bind_address"`
	AdvertiseAddress string   `yaml:"advertise_address"`
	ConnectTimeout   Duration `yaml:"connect_timeout"`
	ShutdownTimeout  Duration `yaml:"shutdown_timeout"`
}

const (
	maxRelayPipes                uint32 = 100_000
	maxInternalRelayAddressBytes int    = 1024
	maxRaftVoters                       = 7
	minimumRaftCommandBytes             = 1 << 20
)

type ControlConfig struct {
	ClusterEpoch               string   `yaml:"cluster_epoch"`
	BindAddress                string   `yaml:"bind_address"`
	AuthorityProbeInterval     Duration `yaml:"authority_probe_interval"`
	AuthorityProbeTimeout      Duration `yaml:"authority_probe_timeout"`
	GatewayRevalidationTimeout Duration `yaml:"gateway_revalidation_timeout"`
	OpenContextTTL             Duration `yaml:"open_context_ttl"`
	ShutdownTimeout            Duration `yaml:"shutdown_timeout"`
	MaxGatewaySessions         uint32   `yaml:"max_gateway_sessions"`
	MaxRoutes                  uint32   `yaml:"max_routes"`
	MaxBindingsPerGateway      uint32   `yaml:"max_bindings_per_gateway"`
}

type AdminConfig struct {
	BindAddress     string   `yaml:"bind_address"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

func Defaults() Config {
	return Config{
		Runtime: RuntimeConfig{Role: RuntimeRoleController},
		Raft: RaftConfig{
			DataDir:           "./data/relaygate",
			BindAddress:       "127.0.0.1:27400",
			AdvertiseAddress:  "127.0.0.1:27400",
			ApplyTimeout:      Duration(5 * time.Second),
			TransportTimeout:  Duration(10 * time.Second),
			ShutdownTimeout:   Duration(10 * time.Second),
			SnapshotThreshold: 256,
			SnapshotInterval:  Duration(2 * time.Minute),
			SnapshotRetain:    2,
			MaxPool:           3,
			MaxCommandBytes:   2 << 20,
		},
		Gateway: GatewayConfig{
			ControlEndpoints: []string{"127.0.0.1:27410"},
			ConnectTimeout:   Duration(2 * time.Second),
			RetryInterval:    Duration(250 * time.Millisecond),
		},
		Relay: RelayConfig{
			BindAddress:           "127.0.0.1:27420",
			AuthenticationTimeout: Duration(5 * time.Second),
			OpenTimeout:           Duration(10 * time.Second),
			ShutdownTimeout:       Duration(5 * time.Second),
			MaxClientSessions:     10_000,
			MaxListenerBindings:   routing.MaxListenerBindingsPerGateway,
			MaxPipes:              10_000,
		},
		InternalRelay: InternalRelayConfig{
			BindAddress:      "127.0.0.1:27430",
			AdvertiseAddress: "127.0.0.1:27430",
			ConnectTimeout:   Duration(2 * time.Second),
			ShutdownTimeout:  Duration(5 * time.Second),
		},
		Control: ControlConfig{
			BindAddress:                "127.0.0.1:27410",
			AuthorityProbeInterval:     Duration(250 * time.Millisecond),
			AuthorityProbeTimeout:      Duration(2 * time.Second),
			GatewayRevalidationTimeout: Duration(15 * time.Second),
			OpenContextTTL:             Duration(10 * time.Second),
			ShutdownTimeout:            Duration(5 * time.Second),
			MaxGatewaySessions:         1024,
			MaxRoutes:                  100_000,
			MaxBindingsPerGateway:      routing.MaxListenerBindingsPerGateway,
		},
		Admin: AdminConfig{
			BindAddress:     "127.0.0.1:27490",
			ReadTimeout:     Duration(5 * time.Second),
			WriteTimeout:    Duration(10 * time.Second),
			ShutdownTimeout: Duration(5 * time.Second),
		},
		Logging: LoggingConfig{Level: "info"},
	}
}

func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	config := Defaults()
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, fmt.Errorf("decode config %q: multiple YAML documents are not allowed", path)
		}
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	if err := applyEnvironment(&config, os.LookupEnv); err != nil {
		return Config{}, fmt.Errorf("apply environment to config %q: %w", path, err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}
	return config, nil
}

func applyEnvironment(config *Config, lookupEnv func(string) (string, bool)) error {
	if value, ok := lookupEnv("RELAYGATE_RUNTIME_ROLE"); ok {
		config.Runtime.Role = RuntimeRole(value)
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_NODE_ID"); ok {
		config.Raft.NodeID = value
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_DATA_DIR"); ok {
		config.Raft.DataDir = value
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_BIND_ADDRESS"); ok {
		config.Raft.BindAddress = value
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_ADVERTISE_ADDRESS"); ok {
		config.Raft.AdvertiseAddress = value
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_BOOTSTRAP"); ok {
		bootstrap, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("RELAYGATE_RAFT_BOOTSTRAP: %w", err)
		}
		config.Raft.Bootstrap = bootstrap
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_BOOTSTRAP_VOTERS"); ok {
		voters, err := decodeBootstrapVoters(value)
		if err != nil {
			return fmt.Errorf("RELAYGATE_RAFT_BOOTSTRAP_VOTERS: %w", err)
		}
		config.Raft.BootstrapVoters = voters
	}
	if value, ok := lookupEnv("RELAYGATE_CONTROL_CLUSTER_EPOCH"); ok {
		config.Control.ClusterEpoch = value
	}
	if value, ok := lookupEnv("RELAYGATE_CONTROL_BIND_ADDRESS"); ok {
		config.Control.BindAddress = value
	}
	if value, ok := lookupEnv("RELAYGATE_GATEWAY_ID"); ok {
		config.Gateway.ID = value
	}
	if value, ok := lookupEnv("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS"); ok {
		endpoints, err := decodeControlEndpoints(value)
		if err != nil {
			return fmt.Errorf("RELAYGATE_GATEWAY_CONTROL_ENDPOINTS: %w", err)
		}
		config.Gateway.ControlEndpoints = endpoints
	}
	if value, ok := lookupEnv("RELAYGATE_INTERNAL_RELAY_BIND_ADDRESS"); ok {
		config.InternalRelay.BindAddress = value
	}
	if value, ok := lookupEnv("RELAYGATE_INTERNAL_RELAY_ADVERTISE_ADDRESS"); ok {
		config.InternalRelay.AdvertiseAddress = value
	}
	if value, ok := lookupEnv("RELAYGATE_ADMIN_BIND_ADDRESS"); ok {
		config.Admin.BindAddress = value
	}
	return nil
}

func decodeBootstrapVoters(value string) ([]RaftVoterConfig, error) {
	var voters []RaftVoterConfig
	if err := decodeJSON(value, &voters); err != nil {
		return nil, err
	}
	return voters, nil
}

func decodeControlEndpoints(value string) ([]string, error) {
	var endpoints []string
	if err := decodeJSON(value, &endpoints); err != nil {
		return nil, err
	}
	return endpoints, nil
}

func decodeJSON(value string, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}
