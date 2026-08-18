// Package controlstate defines the small, current-only directory replicated by
// Raft. It intentionally has no dependency on the gateway, control RPC, or
// authority packages: a Raft snapshot must not contain leader-local transport
// state such as control-session IDs or relay addresses.
package controlstate

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	commandVersion = 1

	MaxClusterEpochBytes                 = 128
	MaxIdentityBytes                     = 128
	MaxEndpointPatternBytes              = 1024
	MaxListenerBindingsPerGateway        = 512
	MaxGatewaySessions                   = 10_000
	MaxRoutes                            = MaxGatewaySessions * MaxListenerBindingsPerGateway
	DefaultMaxGatewaySessions     uint32 = 1_000
	DefaultMaxRoutes              uint32 = DefaultMaxGatewaySessions * MaxListenerBindingsPerGateway
)

var (
	ErrInvalidCommand = errors.New("invalid raft current-state command")
	ErrEpochMismatch  = errors.New("cluster epoch mismatch")
	ErrNotInitialized = errors.New("cluster is not initialized")
	ErrStaleGateway   = errors.New("stale gateway instance")
)

// BindingKey is the exact route lookup key. Pattern matching is deliberately
// outside this state machine; the FSM stores only a fully specified key.
type BindingKey struct {
	ClientID        string `json:"client_id"`
	EndpointPattern string `json:"endpoint_pattern"`
	TargetID        string `json:"target_id"`
}

// GatewaySessionRef is the durable incarnation of one gateway process. A
// control-session ID is intentionally not present: it is leader-local and is
// re-established after every authority change.
type GatewaySessionRef struct {
	GatewayID         string `json:"gateway_id"`
	GatewayInstanceID string `json:"gateway_instance_id"`
}

// Route is one currently declared listener. Its absence is deletion; no
// generation, tombstone, expiration record, or route history is retained.
type Route struct {
	Key               BindingKey        `json:"key"`
	Owner             GatewaySessionRef `json:"owner"`
	ListenerBindingID string            `json:"listener_binding_id"`
}

// State is the complete durable Raft application state. Slices returned from
// FSM accessors are sorted and copied, so callers cannot mutate the FSM.
type State struct {
	ClusterEpoch          string              `json:"cluster_epoch"`
	MaxGatewaySessions    uint32              `json:"max_gateway_sessions"`
	MaxRoutes             uint32              `json:"max_routes"`
	MaxBindingsPerGateway uint32              `json:"max_bindings_per_gateway"`
	Gateways              []GatewaySessionRef `json:"gateways"`
	Routes                []Route             `json:"routes"`
}

type ResultCode string

const (
	ResultApplied        ResultCode = "applied"
	ResultAlreadyApplied ResultCode = "already_applied"
	ResultConflict       ResultCode = "conflict"
	ResultCapacity       ResultCode = "capacity"
	ResultRejected       ResultCode = "rejected"
)

type ApplyResult struct {
	Code  ResultCode `json:"code"`
	Error string     `json:"error,omitempty"`
}

func (r ApplyResult) Applied() bool {
	return r.Code == ResultApplied || r.Code == ResultAlreadyApplied
}

type commandKind string

const (
	commandInitializeCluster commandKind = "initialize_cluster"
	commandRegisterGateway   commandKind = "register_gateway"
	commandReplaceSnapshot   commandKind = "replace_snapshot"
	commandDeclareRoute      commandKind = "declare_route"
	commandWithdrawRoute     commandKind = "withdraw_route"
	commandRemoveGateway     commandKind = "remove_gateway"
)

type commandEnvelope struct {
	Version uint8           `json:"version"`
	Kind    commandKind     `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// InitializeCluster fixes the epoch and directory capacity for a cohort.
// Replaying an exact command is harmless; changing either epoch or limits is
// rejected because it would make replicas interpret the same log differently.
type InitializeCluster struct {
	ClusterEpoch          string `json:"cluster_epoch"`
	MaxGatewaySessions    uint32 `json:"max_gateway_sessions"`
	MaxRoutes             uint32 `json:"max_routes"`
	MaxBindingsPerGateway uint32 `json:"max_bindings_per_gateway"`
}

type RegisterGateway struct {
	ClusterEpoch string            `json:"cluster_epoch"`
	Gateway      GatewaySessionRef `json:"gateway"`
}

// Binding is a route declaration whose owner is the Gateway field of the
// enclosing command. Keeping ownership out of each binding prevents a snapshot
// from accidentally registering routes for another gateway.
type Binding struct {
	Key               BindingKey `json:"key"`
	ListenerBindingID string     `json:"listener_binding_id"`
}

type ReplaceSnapshot struct {
	ClusterEpoch string            `json:"cluster_epoch"`
	Gateway      GatewaySessionRef `json:"gateway"`
	Bindings     []Binding         `json:"bindings"`
}

type DeclareRoute struct {
	ClusterEpoch string            `json:"cluster_epoch"`
	Gateway      GatewaySessionRef `json:"gateway"`
	Binding      Binding           `json:"binding"`
}

type WithdrawRoute struct {
	ClusterEpoch string            `json:"cluster_epoch"`
	Gateway      GatewaySessionRef `json:"gateway"`
	Binding      Binding           `json:"binding"`
}

type RemoveGateway struct {
	ClusterEpoch string            `json:"cluster_epoch"`
	Gateway      GatewaySessionRef `json:"gateway"`
}

func EncodeInitializeCluster(command InitializeCluster) ([]byte, error) {
	if err := validateInitializeCluster(command); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandInitializeCluster, command)
}

func EncodeRegisterGateway(command RegisterGateway) ([]byte, error) {
	if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandRegisterGateway, command)
}

func EncodeReplaceSnapshot(command ReplaceSnapshot) ([]byte, error) {
	if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	if err := validateBindings(command.Bindings, MaxListenerBindingsPerGateway); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandReplaceSnapshot, command)
}

func EncodeDeclareRoute(command DeclareRoute) ([]byte, error) {
	if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	if err := validateBinding(command.Binding); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandDeclareRoute, command)
}

func EncodeWithdrawRoute(command WithdrawRoute) ([]byte, error) {
	if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	if err := validateBinding(command.Binding); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandWithdrawRoute, command)
}

func EncodeRemoveGateway(command RemoveGateway) ([]byte, error) {
	if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandRemoveGateway, command)
}

func validateInitializeCluster(command InitializeCluster) error {
	if err := validateClusterEpoch(command.ClusterEpoch); err != nil {
		return err
	}
	if command.MaxGatewaySessions == 0 || command.MaxGatewaySessions > MaxGatewaySessions {
		return fmt.Errorf("max_gateway_sessions must be 1..%d", MaxGatewaySessions)
	}
	if command.MaxBindingsPerGateway == 0 || command.MaxBindingsPerGateway > MaxListenerBindingsPerGateway {
		return fmt.Errorf("max_bindings_per_gateway must be 1..%d", MaxListenerBindingsPerGateway)
	}
	maxPossibleRoutes := uint64(command.MaxGatewaySessions) * uint64(command.MaxBindingsPerGateway)
	if command.MaxRoutes == 0 || uint64(command.MaxRoutes) > maxPossibleRoutes || command.MaxRoutes > MaxRoutes {
		return fmt.Errorf("max_routes must be 1..%d and no greater than gateway capacity", MaxRoutes)
	}
	return nil
}

func validateEpochAndGateway(clusterEpoch string, gateway GatewaySessionRef) error {
	if err := validateClusterEpoch(clusterEpoch); err != nil {
		return err
	}
	return validateGateway(gateway)
}

func validateClusterEpoch(clusterEpoch string) error {
	return validateIdentity("cluster_epoch", clusterEpoch)
}

func validateGateway(gateway GatewaySessionRef) error {
	if err := validateIdentity("gateway_id", gateway.GatewayID); err != nil {
		return err
	}
	return validateIdentity("gateway_instance_id", gateway.GatewayInstanceID)
}

func validateBinding(binding Binding) error {
	if err := validateBindingKey(binding.Key); err != nil {
		return err
	}
	return validateIdentity("listener_binding_id", binding.ListenerBindingID)
}

func validateBindings(bindings []Binding, maximum uint32) error {
	if uint64(len(bindings)) > uint64(maximum) {
		return fmt.Errorf("bindings must contain at most %d entries", maximum)
	}
	seen := make(map[BindingKey]struct{}, len(bindings))
	for _, binding := range bindings {
		if err := validateBinding(binding); err != nil {
			return err
		}
		if _, exists := seen[binding.Key]; exists {
			return fmt.Errorf("duplicate binding key")
		}
		seen[binding.Key] = struct{}{}
	}
	return nil
}

func validateBindingKey(key BindingKey) error {
	if err := validateIdentity("client_id", key.ClientID); err != nil {
		return err
	}
	if key.EndpointPattern == "" || len(key.EndpointPattern) > MaxEndpointPatternBytes {
		return fmt.Errorf("endpoint_pattern must be 1..%d bytes", MaxEndpointPatternBytes)
	}
	return validateIdentity("target_id", key.TargetID)
}

func validateIdentity(field, value string) error {
	if value == "" || len(value) > MaxIdentityBytes {
		return fmt.Errorf("%s must be 1..%d bytes", field, MaxIdentityBytes)
	}
	return nil
}

func encodeCommand(kind commandKind, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s payload: %w", kind, err)
	}
	encoded, err := json.Marshal(commandEnvelope{Version: commandVersion, Kind: kind, Payload: raw})
	if err != nil {
		return nil, fmt.Errorf("encode %s command: %w", kind, err)
	}
	return encoded, nil
}
