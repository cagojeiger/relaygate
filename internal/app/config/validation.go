package config

import (
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

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
