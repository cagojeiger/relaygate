package controlstate

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	commandVersion = 1
	// MaxClusterEpochBytes bounds the only RelayGate application value stored in Raft.
	MaxClusterEpochBytes = 128
)

var (
	ErrInvalidCommand = errors.New("invalid raft safety command")
	ErrEpochMismatch  = errors.New("cluster epoch mismatch")
)

type State struct {
	ClusterEpoch string `json:"cluster_epoch"`
}

type ResultCode string

const (
	ResultApplied        ResultCode = "applied"
	ResultAlreadyApplied ResultCode = "already_applied"
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

const commandInitializeEpoch commandKind = "initialize_epoch"

type commandEnvelope struct {
	Version uint8           `json:"version"`
	Kind    commandKind     `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// InitializeEpoch writes the fixed cluster safety/fencing marker once. It is
// intentionally the only RelayGate application command carried by Raft.
type InitializeEpoch struct {
	ClusterEpoch string `json:"cluster_epoch"`
}

func EncodeInitializeEpoch(command InitializeEpoch) ([]byte, error) {
	if err := validateClusterEpoch(command.ClusterEpoch); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	return encodeCommand(commandInitializeEpoch, command)
}

func validateClusterEpoch(clusterEpoch string) error {
	if clusterEpoch == "" || len(clusterEpoch) > MaxClusterEpochBytes {
		return fmt.Errorf("cluster_epoch must be 1..%d bytes", MaxClusterEpochBytes)
	}
	return nil
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
