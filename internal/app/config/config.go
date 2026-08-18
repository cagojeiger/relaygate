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
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftstate "github.com/cagojeiger/relaygate/internal/raft/state"
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

func (c Config) Validate() error {
	if c.Runtime.Role != RuntimeRoleController && c.Runtime.Role != RuntimeRoleGateway {
		return fmt.Errorf("runtime.role must be %q or %q", RuntimeRoleController, RuntimeRoleGateway)
	}
	if err := c.validateCommon(); err != nil {
		return err
	}
	if c.Runtime.Role.HasController() {
		if err := c.validateVoter(); err != nil {
			return err
		}
		return nil
	}
	return c.validateGateway()
}

func (c Config) validateCommon() error {
	if c.Control.ClusterEpoch == "" || len(c.Control.ClusterEpoch) > raftstate.MaxClusterEpochBytes {
		return fmt.Errorf("control.cluster_epoch must be 1..%d bytes", raftstate.MaxClusterEpochBytes)
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

func (c Config) validateVoter() error {
	if c.Raft.NodeID == "" {
		return fmt.Errorf("raft.node_id is required")
	}
	if strings.TrimSpace(c.Raft.DataDir) == "" {
		return fmt.Errorf("raft.data_dir is required")
	}
	if err := validateListenAddress("raft.bind_address", c.Raft.BindAddress); err != nil {
		return err
	}
	if err := validateAdvertiseAddress(c.Raft.AdvertiseAddress); err != nil {
		return err
	}
	if len(c.Raft.BootstrapVoters) > maxRaftVoters {
		return fmt.Errorf("raft.bootstrap_voters must contain at most %d voters", maxRaftVoters)
	}
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
	if c.Raft.Bootstrap && len(c.Raft.BootstrapVoters) == 0 {
		return fmt.Errorf("raft.bootstrap_voters is required for initial bootstrap")
	}
	if c.Raft.Bootstrap && !localFound {
		return fmt.Errorf("raft.bootstrap_voters must contain local node_id %q", c.Raft.NodeID)
	}
	if c.Raft.ApplyTimeout.Value() <= 0 || c.Raft.TransportTimeout.Value() <= 0 || c.Raft.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("raft timeouts must be positive")
	}
	if c.Raft.SnapshotThreshold == 0 || c.Raft.SnapshotInterval.Value() <= 0 {
		return fmt.Errorf("raft snapshot threshold and interval must be positive")
	}
	if c.Raft.SnapshotRetain < 1 {
		return fmt.Errorf("raft.snapshot_retain must be at least 1")
	}
	if c.Raft.MaxPool < 1 {
		return fmt.Errorf("raft.max_pool must be at least 1")
	}
	if c.Raft.MaxCommandBytes < minimumRaftCommandBytes {
		return fmt.Errorf("raft.max_command_bytes must be at least %d for a maximum-size current binding snapshot", minimumRaftCommandBytes)
	}
	if err := validateListenAddress("control.bind_address", c.Control.BindAddress); err != nil {
		return err
	}
	if c.Control.AuthorityProbeInterval.Value() <= 0 || c.Control.AuthorityProbeTimeout.Value() <= 0 || c.Control.GatewayRevalidationTimeout.Value() <= 0 || c.Control.OpenContextTTL.Value() <= 0 || c.Control.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("control timeouts must be positive")
	}
	if c.Control.MaxGatewaySessions == 0 || c.Control.MaxGatewaySessions > raftstate.MaxGatewaySessions {
		return fmt.Errorf("control.max_gateway_sessions must be between 1 and %d", raftstate.MaxGatewaySessions)
	}
	if c.Control.MaxBindingsPerGateway == 0 || c.Control.MaxBindingsPerGateway > routing.MaxListenerBindingsPerGateway {
		return fmt.Errorf("control.max_bindings_per_gateway must be between 1 and %d", routing.MaxListenerBindingsPerGateway)
	}
	maximumRoutes := uint64(c.Control.MaxGatewaySessions) * uint64(c.Control.MaxBindingsPerGateway)
	if c.Control.MaxRoutes == 0 || c.Control.MaxRoutes > raftstate.MaxRoutes || uint64(c.Control.MaxRoutes) > maximumRoutes {
		return fmt.Errorf("control.max_routes must fit the configured current Gateway capacity")
	}
	return nil
}

func (c Config) validateGateway() error {
	if c.Gateway.ID == "" || len(c.Gateway.ID) > routing.MaxIdentityBytes {
		return fmt.Errorf("gateway.id must be 1..%d bytes", routing.MaxIdentityBytes)
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
	if err := validateLoopbackListenAddress("relay.bind_address", c.Relay.BindAddress); err != nil {
		return err
	}
	if c.Relay.AuthenticationTimeout.Value() <= 0 || c.Relay.OpenTimeout.Value() <= 0 || c.Relay.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("relay timeouts must be positive")
	}
	if c.Relay.MaxClientSessions == 0 {
		return fmt.Errorf("relay.max_client_sessions must be positive")
	}
	if c.Relay.MaxListenerBindings == 0 || c.Relay.MaxListenerBindings > routing.MaxListenerBindingsPerGateway {
		return fmt.Errorf("relay.max_listener_bindings must be between 1 and %d", routing.MaxListenerBindingsPerGateway)
	}
	if c.Relay.MaxPipes == 0 || c.Relay.MaxPipes > maxRelayPipes {
		return fmt.Errorf("relay.max_pipes must be between 1 and %d", maxRelayPipes)
	}
	if err := validateListenAddress("internal_relay.bind_address", c.InternalRelay.BindAddress); err != nil {
		return err
	}
	if len(c.InternalRelay.AdvertiseAddress) > maxInternalRelayAddressBytes {
		return fmt.Errorf("internal_relay.advertise_address must be at most %d bytes", maxInternalRelayAddressBytes)
	}
	if err := validateDialAddress("internal_relay.advertise_address", c.InternalRelay.AdvertiseAddress); err != nil {
		return err
	}
	if c.InternalRelay.ConnectTimeout.Value() <= 0 || c.InternalRelay.ShutdownTimeout.Value() <= 0 {
		return fmt.Errorf("internal_relay timeouts must be positive")
	}
	return nil
}

func (c Config) LogLevel() slog.Level {
	var level slog.Level
	_ = level.UnmarshalText([]byte(c.Logging.Level))
	return level
}

func ValidateClientReload(current, candidate Config) error {
	current.Clients = nil
	candidate.Clients = nil
	if !reflect.DeepEqual(current, candidate) {
		return fmt.Errorf("SIGHUP may change clients only; restart is required for other config changes")
	}
	return nil
}

func validateListenAddress(name, address string) error {
	if address == "" {
		return fmt.Errorf("%s is required", name)
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if port == "" {
		return fmt.Errorf("%s must include a port", name)
	}
	return nil
}

func validateLoopbackListenAddress(name, address string) error {
	if err := validateListenAddress(name, address); err != nil {
		return err
	}
	host, _, _ := net.SplitHostPort(address)
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%s must use a loopback host until relay TLS is implemented", name)
	}
	return nil
}

func validateAdvertiseAddress(address string) error {
	return validateDialAddress("raft.advertise_address", address)
}

func validateDialAddress(name, address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if host == "" {
		return fmt.Errorf("%s must include a host", name)
	}
	if port == "" {
		return fmt.Errorf("%s must include a port", name)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("%s cannot use an unspecified host", name)
	}
	return nil
}
