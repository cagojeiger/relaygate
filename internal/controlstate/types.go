package controlstate

import (
	"encoding/json"
	"errors"
	"fmt"
)

const commandVersion = 1

var (
	ErrInvalidCommand = errors.New("invalid control command")
	ErrEpochMismatch  = errors.New("cluster epoch mismatch")
	ErrCASMismatch    = errors.New("control compare-and-set mismatch")
	ErrKeyLimit       = errors.New("binding key limit reached")
	ErrGatewayLimit   = errors.New("gateway ID limit reached")
)

const DefaultMaxDistinctGatewayIDsPerEpoch uint64 = 1024

type BindingKey struct {
	ClientID        string `json:"client_id"`
	EndpointPattern string `json:"endpoint_pattern"`
	TargetID        string `json:"target_id"`
}

func (k BindingKey) Validate() error {
	if k.ClientID == "" {
		return fmt.Errorf("%w: client_id is required", ErrInvalidCommand)
	}
	if k.EndpointPattern == "" {
		return fmt.Errorf("%w: endpoint_pattern is required", ErrInvalidCommand)
	}
	if k.TargetID == "" {
		return fmt.Errorf("%w: target_id is required", ErrInvalidCommand)
	}
	return nil
}

type ListenerBindingRef struct {
	GatewayID         string `json:"gateway_id"`
	GatewayInstanceID string `json:"gateway_instance_id"`
	ListenerBindingID string `json:"listener_binding_id"`
}

func (r ListenerBindingRef) Validate() error {
	if r.GatewayID == "" {
		return fmt.Errorf("%w: gateway_id is required", ErrInvalidCommand)
	}
	if r.GatewayInstanceID == "" {
		return fmt.Errorf("%w: gateway_instance_id is required", ErrInvalidCommand)
	}
	if r.ListenerBindingID == "" {
		return fmt.Errorf("%w: listener_binding_id is required", ErrInvalidCommand)
	}
	return nil
}

type BindingSlot struct {
	Key        BindingKey          `json:"key"`
	Generation uint64              `json:"generation"`
	Ref        *ListenerBindingRef `json:"ref,omitempty"`
}

type GatewayRegistrationRef struct {
	GatewayInstanceID string `json:"gateway_instance_id"`
}

func (r GatewayRegistrationRef) Validate() error {
	if r.GatewayInstanceID == "" {
		return fmt.Errorf("%w: gateway_instance_id is required", ErrInvalidCommand)
	}
	return nil
}

type GatewaySlot struct {
	GatewayID  string                  `json:"gateway_id"`
	Generation uint64                  `json:"generation"`
	Ref        *GatewayRegistrationRef `json:"ref,omitempty"`
}

func (s GatewaySlot) IsTombstone() bool {
	return s.Ref == nil
}

func (s BindingSlot) IsTombstone() bool {
	return s.Ref == nil
}

type State struct {
	ClusterEpoch                   string        `json:"cluster_epoch"`
	MaxDistinctBindingKeysPerEpoch uint64        `json:"max_distinct_binding_keys_per_epoch"`
	MaxDistinctGatewayIDsPerEpoch  uint64        `json:"max_distinct_gateway_ids_per_epoch"`
	Bindings                       []BindingSlot `json:"bindings"`
	Gateways                       []GatewaySlot `json:"gateways"`
}

type ResultCode string

const (
	ResultApplied        ResultCode = "applied"
	ResultAlreadyApplied ResultCode = "already_applied"
	ResultRejected       ResultCode = "rejected"
)

type ApplyResult struct {
	Code        ResultCode   `json:"code"`
	Slot        *BindingSlot `json:"slot,omitempty"`
	GatewaySlot *GatewaySlot `json:"gateway_slot,omitempty"`
	Error       string       `json:"error,omitempty"`
}

func (r ApplyResult) Applied() bool {
	return r.Code == ResultApplied || r.Code == ResultAlreadyApplied
}

type commandKind string

const (
	commandInitializeEpoch commandKind = "initialize_epoch"
	commandInstallBinding  commandKind = "install_binding"
	commandRemoveBinding   commandKind = "remove_binding"
	commandRegisterGateway commandKind = "register_gateway"
	commandRemoveGateway   commandKind = "remove_gateway"
)

type commandEnvelope struct {
	Version uint8           `json:"version"`
	Kind    commandKind     `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type InitializeEpoch struct {
	ClusterEpoch                   string `json:"cluster_epoch"`
	MaxDistinctBindingKeysPerEpoch uint64 `json:"max_distinct_binding_keys_per_epoch"`
	MaxDistinctGatewayIDsPerEpoch  uint64 `json:"max_distinct_gateway_ids_per_epoch"`
}

type RegisterGateway struct {
	ClusterEpoch       string                  `json:"cluster_epoch"`
	GatewayID          string                  `json:"gateway_id"`
	ExpectedGeneration uint64                  `json:"expected_generation"`
	ExpectedRef        *GatewayRegistrationRef `json:"expected_ref,omitempty"`
	NewRef             GatewayRegistrationRef  `json:"new_ref"`
}

type RemoveGateway struct {
	ClusterEpoch       string                 `json:"cluster_epoch"`
	GatewayID          string                 `json:"gateway_id"`
	ExpectedGeneration uint64                 `json:"expected_generation"`
	ExpectedRef        GatewayRegistrationRef `json:"expected_ref"`
}

type InstallBinding struct {
	ClusterEpoch       string              `json:"cluster_epoch"`
	Key                BindingKey          `json:"key"`
	ExpectedGeneration uint64              `json:"expected_generation"`
	ExpectedRef        *ListenerBindingRef `json:"expected_ref,omitempty"`
	NewRef             ListenerBindingRef  `json:"new_ref"`
}

type RemoveBinding struct {
	ClusterEpoch       string             `json:"cluster_epoch"`
	Key                BindingKey         `json:"key"`
	ExpectedGeneration uint64             `json:"expected_generation"`
	ExpectedRef        ListenerBindingRef `json:"expected_ref"`
}

func EncodeInitializeEpoch(command InitializeEpoch) ([]byte, error) {
	if command.ClusterEpoch == "" {
		return nil, fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if command.MaxDistinctBindingKeysPerEpoch == 0 {
		return nil, fmt.Errorf("%w: max distinct binding keys must be positive", ErrInvalidCommand)
	}
	if command.MaxDistinctGatewayIDsPerEpoch == 0 {
		return nil, fmt.Errorf("%w: max distinct gateway IDs must be positive", ErrInvalidCommand)
	}
	return encodeCommand(commandInitializeEpoch, command)
}

func EncodeRegisterGateway(command RegisterGateway) ([]byte, error) {
	if err := validateRegisterGateway(command); err != nil {
		return nil, err
	}
	return encodeCommand(commandRegisterGateway, command)
}

func EncodeRemoveGateway(command RemoveGateway) ([]byte, error) {
	if err := validateRemoveGateway(command); err != nil {
		return nil, err
	}
	return encodeCommand(commandRemoveGateway, command)
}

func EncodeInstallBinding(command InstallBinding) ([]byte, error) {
	if command.ClusterEpoch == "" {
		return nil, fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if err := command.Key.Validate(); err != nil {
		return nil, err
	}
	if command.ExpectedRef != nil {
		if err := command.ExpectedRef.Validate(); err != nil {
			return nil, err
		}
	}
	if err := command.NewRef.Validate(); err != nil {
		return nil, err
	}
	if command.ExpectedGeneration == ^uint64(0) {
		return nil, fmt.Errorf("%w: generation overflow", ErrInvalidCommand)
	}
	return encodeCommand(commandInstallBinding, command)
}

func EncodeRemoveBinding(command RemoveBinding) ([]byte, error) {
	if command.ClusterEpoch == "" {
		return nil, fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if err := command.Key.Validate(); err != nil {
		return nil, err
	}
	if err := command.ExpectedRef.Validate(); err != nil {
		return nil, err
	}
	if command.ExpectedGeneration == ^uint64(0) {
		return nil, fmt.Errorf("%w: generation overflow", ErrInvalidCommand)
	}
	return encodeCommand(commandRemoveBinding, command)
}

func encodeCommand(kind commandKind, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s payload: %w", kind, err)
	}
	encoded, err := json.Marshal(commandEnvelope{
		Version: commandVersion,
		Kind:    kind,
		Payload: raw,
	})
	if err != nil {
		return nil, fmt.Errorf("encode %s command: %w", kind, err)
	}
	return encoded, nil
}
