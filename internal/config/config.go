package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
	Raft    RaftConfig    `yaml:"raft"`
	Gateway GatewayConfig `yaml:"gateway"`
	Control ControlConfig `yaml:"control"`
	Admin   AdminConfig   `yaml:"admin"`
	Logging LoggingConfig `yaml:"logging"`
}

type RaftConfig struct {
	NodeID            string            `yaml:"node_id"`
	BindAddress       string            `yaml:"bind_address"`
	AdvertiseAddress  string            `yaml:"advertise_address"`
	DataDir           string            `yaml:"data_dir"`
	Bootstrap         bool              `yaml:"bootstrap"`
	BootstrapVoters   []RaftVoterConfig `yaml:"bootstrap_voters"`
	ApplyTimeout      Duration          `yaml:"apply_timeout"`
	TransportTimeout  Duration          `yaml:"transport_timeout"`
	StartupTimeout    Duration          `yaml:"startup_timeout"`
	ShutdownTimeout   Duration          `yaml:"shutdown_timeout"`
	SnapshotRetain    int               `yaml:"snapshot_retain"`
	SnapshotThreshold uint64            `yaml:"snapshot_threshold"`
	SnapshotInterval  Duration          `yaml:"snapshot_interval"`
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

type ControlConfig struct {
	ClusterEpoch                   string   `yaml:"cluster_epoch"`
	BindAddress                    string   `yaml:"bind_address"`
	AuthorityProbeInterval         Duration `yaml:"authority_probe_interval"`
	AuthorityProbeTimeout          Duration `yaml:"authority_probe_timeout"`
	GatewayRevalidationTimeout     Duration `yaml:"gateway_revalidation_timeout"`
	ShutdownTimeout                Duration `yaml:"shutdown_timeout"`
	MaxDistinctBindingKeysPerEpoch uint64   `yaml:"max_distinct_binding_keys_per_epoch"`
	MaxDistinctGatewayIDsPerEpoch  uint64   `yaml:"max_distinct_gateway_ids_per_epoch"`
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
		Raft: RaftConfig{
			BindAddress:       "127.0.0.1:7000",
			AdvertiseAddress:  "127.0.0.1:7000",
			DataDir:           "./data/relaygate",
			ApplyTimeout:      Duration(5 * time.Second),
			TransportTimeout:  Duration(10 * time.Second),
			StartupTimeout:    Duration(15 * time.Second),
			ShutdownTimeout:   Duration(10 * time.Second),
			SnapshotRetain:    2,
			SnapshotThreshold: 8192,
			SnapshotInterval:  Duration(2 * time.Minute),
			MaxPool:           3,
			MaxCommandBytes:   64 << 10,
		},
		Gateway: GatewayConfig{
			ControlEndpoints: []string{"127.0.0.1:7100"},
			ConnectTimeout:   Duration(2 * time.Second),
			RetryInterval:    Duration(250 * time.Millisecond),
		},
		Control: ControlConfig{
			BindAddress:                    "127.0.0.1:7100",
			AuthorityProbeInterval:         Duration(250 * time.Millisecond),
			AuthorityProbeTimeout:          Duration(2 * time.Second),
			GatewayRevalidationTimeout:     Duration(15 * time.Second),
			ShutdownTimeout:                Duration(5 * time.Second),
			MaxDistinctBindingKeysPerEpoch: 100_000,
			MaxDistinctGatewayIDsPerEpoch:  1024,
		},
		Admin: AdminConfig{
			BindAddress:     "127.0.0.1:9090",
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
	config.Raft.DataDir = filepath.Clean(config.Raft.DataDir)
	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}
	return config, nil
}

func applyEnvironment(config *Config, lookupEnv func(string) (string, bool)) error {
	if value, ok := lookupEnv("RELAYGATE_RAFT_NODE_ID"); ok {
		config.Raft.NodeID = value
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_ADVERTISE_ADDRESS"); ok {
		config.Raft.AdvertiseAddress = value
	}
	if value, ok := lookupEnv("RELAYGATE_RAFT_DATA_DIR"); ok {
		config.Raft.DataDir = value
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

func (c Config) Validate() error {
	if c.Raft.NodeID == "" {
		return fmt.Errorf("raft.node_id is required")
	}
	if err := validateListenAddress("raft.bind_address", c.Raft.BindAddress); err != nil {
		return err
	}
	if err := validateAdvertiseAddress(c.Raft.AdvertiseAddress); err != nil {
		return err
	}
	if !c.Raft.Bootstrap && len(c.Raft.BootstrapVoters) != 0 {
		return fmt.Errorf("raft.bootstrap_voters requires raft.bootstrap=true")
	}
	if c.Raft.Bootstrap && len(c.Raft.BootstrapVoters) != 0 {
		seenIDs := make(map[string]struct{}, len(c.Raft.BootstrapVoters))
		seenAddresses := make(map[string]struct{}, len(c.Raft.BootstrapVoters))
		localFound := false
		for index, voter := range c.Raft.BootstrapVoters {
			if voter.NodeID == "" {
				return fmt.Errorf("raft.bootstrap_voters[%d].node_id is required", index)
			}
			if err := validateAdvertiseAddress(voter.Address); err != nil {
				return fmt.Errorf("raft.bootstrap_voters[%d].address: %w", index, err)
			}
			if _, exists := seenIDs[voter.NodeID]; exists {
				return fmt.Errorf("raft.bootstrap_voters has duplicate node_id %q", voter.NodeID)
			}
			if _, exists := seenAddresses[voter.Address]; exists {
				return fmt.Errorf("raft.bootstrap_voters has duplicate address %q", voter.Address)
			}
			seenIDs[voter.NodeID] = struct{}{}
			seenAddresses[voter.Address] = struct{}{}
			if voter.NodeID == c.Raft.NodeID {
				if voter.Address != c.Raft.AdvertiseAddress {
					return fmt.Errorf("local bootstrap voter address %q differs from raft.advertise_address %q", voter.Address, c.Raft.AdvertiseAddress)
				}
				localFound = true
			}
		}
		if !localFound {
			return fmt.Errorf("raft.bootstrap_voters must contain local node_id %q", c.Raft.NodeID)
		}
	}
	if c.Raft.DataDir == "" || c.Raft.DataDir == "." {
		return fmt.Errorf("raft.data_dir must name a dedicated directory")
	}
	if c.Raft.ApplyTimeout.Value() <= 0 || c.Raft.TransportTimeout.Value() <= 0 || c.Raft.StartupTimeout.Value() <= 0 || c.Raft.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("raft timeouts must be positive")
	}
	if c.Raft.SnapshotRetain < 1 {
		return fmt.Errorf("raft.snapshot_retain must be at least 1")
	}
	if c.Raft.SnapshotThreshold == 0 || c.Raft.SnapshotInterval.Value() <= 0 {
		return fmt.Errorf("raft snapshot threshold and interval must be positive")
	}
	if c.Raft.MaxPool < 1 {
		return fmt.Errorf("raft.max_pool must be at least 1")
	}
	if c.Raft.MaxCommandBytes < 1 {
		return fmt.Errorf("raft.max_command_bytes must be positive")
	}
	if c.Gateway.ID == "" {
		return fmt.Errorf("gateway.id is required")
	}
	if len(c.Gateway.ControlEndpoints) == 0 {
		return fmt.Errorf("gateway.control_endpoints must not be empty")
	}
	seenControlEndpoints := make(map[string]struct{}, len(c.Gateway.ControlEndpoints))
	for index, endpoint := range c.Gateway.ControlEndpoints {
		if err := validateDialAddress("gateway.control_endpoints", endpoint); err != nil {
			return fmt.Errorf("gateway.control_endpoints[%d]: %w", index, err)
		}
		if _, exists := seenControlEndpoints[endpoint]; exists {
			return fmt.Errorf("gateway.control_endpoints has duplicate endpoint %q", endpoint)
		}
		seenControlEndpoints[endpoint] = struct{}{}
	}
	if c.Gateway.ConnectTimeout.Value() <= 0 || c.Gateway.RetryInterval.Value() <= 0 {
		return fmt.Errorf("gateway timeouts must be positive")
	}
	if c.Control.ClusterEpoch == "" {
		return fmt.Errorf("control.cluster_epoch is required")
	}
	if err := validateListenAddress("control.bind_address", c.Control.BindAddress); err != nil {
		return err
	}
	if c.Control.AuthorityProbeInterval.Value() <= 0 || c.Control.AuthorityProbeTimeout.Value() <= 0 || c.Control.GatewayRevalidationTimeout.Value() <= 0 || c.Control.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("control timeouts must be positive")
	}
	if c.Control.MaxDistinctBindingKeysPerEpoch == 0 {
		return fmt.Errorf("control.max_distinct_binding_keys_per_epoch must be positive")
	}
	if c.Control.MaxDistinctGatewayIDsPerEpoch == 0 {
		return fmt.Errorf("control.max_distinct_gateway_ids_per_epoch must be positive")
	}
	if err := validateListenAddress("admin.bind_address", c.Admin.BindAddress); err != nil {
		return err
	}
	if c.Admin.ReadTimeout.Value() <= 0 || c.Admin.WriteTimeout.Value() <= 0 || c.Admin.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("admin timeouts must be positive")
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.Logging.Level)); err != nil {
		return fmt.Errorf("logging.level: %w", err)
	}
	return nil
}

func (c Config) LogLevel() slog.Level {
	var level slog.Level
	_ = level.UnmarshalText([]byte(c.Logging.Level))
	return level
}

func validateListenAddress(name, address string) error {
	if address == "" {
		return fmt.Errorf("%s is required", name)
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func validateAdvertiseAddress(address string) error {
	return validateDialAddress("raft.advertise_address", address)
}

func validateDialAddress(name, address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if host == "" {
		return fmt.Errorf("%s must include a host", name)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("%s cannot use an unspecified host", name)
	}
	return nil
}
