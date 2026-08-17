package controlstate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const snapshotVersion = 1

// FSM persists only the fixed cluster safety/fencing marker. Gateway control
// sessions and live routes belong to the current authority's memory and must
// never enter the Raft log or snapshot.
type FSM struct {
	mu           sync.RWMutex
	clusterEpoch string
}

func NewFSM() *FSM { return &FSM{} }

func (f *FSM) Apply(log *raft.Log) any {
	var envelope commandEnvelope
	if err := decodeStrict(log.Data, &envelope); err != nil {
		return rejected(fmt.Errorf("%w: decode envelope: %w", ErrInvalidCommand, err))
	}
	if envelope.Version != commandVersion {
		return rejected(fmt.Errorf("%w: unsupported command version %d", ErrInvalidCommand, envelope.Version))
	}
	if envelope.Kind != commandInitializeEpoch {
		return rejected(fmt.Errorf("%w: unsupported command kind %q", ErrInvalidCommand, envelope.Kind))
	}

	var command InitializeEpoch
	if err := decodeStrict(envelope.Payload, &command); err != nil {
		return rejected(fmt.Errorf("%w: decode initialize_epoch: %w", ErrInvalidCommand, err))
	}
	if command.ClusterEpoch == "" {
		return rejected(fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand))
	}
	return f.applyInitializeEpoch(command)
}

func (f *FSM) applyInitializeEpoch(command InitializeEpoch) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.clusterEpoch == "" {
		f.clusterEpoch = command.ClusterEpoch
		return ApplyResult{Code: ResultApplied}
	}
	if f.clusterEpoch == command.ClusterEpoch {
		return ApplyResult{Code: ResultAlreadyApplied}
	}
	return rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.clusterEpoch, command.ClusterEpoch))
}

func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return State{ClusterEpoch: f.clusterEpoch}
}

func (f *FSM) ClusterEpoch() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterEpoch
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &snapshot{state: State{ClusterEpoch: f.clusterEpoch}}, nil
}

func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	var envelope snapshotEnvelope
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("decode raft safety snapshot: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return fmt.Errorf("decode raft safety snapshot: %w", err)
	}
	if envelope.Version != snapshotVersion {
		return fmt.Errorf("unsupported raft safety snapshot version %d", envelope.Version)
	}
	if err := validateState(envelope.State); err != nil {
		return fmt.Errorf("validate raft safety snapshot: %w", err)
	}

	f.mu.Lock()
	f.clusterEpoch = envelope.State.ClusterEpoch
	f.mu.Unlock()
	return nil
}

type snapshotEnvelope struct {
	Version uint8 `json:"version"`
	State   State `json:"state"`
}

type snapshot struct{ state State }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(snapshotEnvelope{Version: snapshotVersion, State: s.state}); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("encode raft safety snapshot: %w", err)
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("close raft safety snapshot: %w", err)
	}
	return nil
}

func (s *snapshot) Release() {}

func validateState(state State) error {
	return nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func rejected(err error) ApplyResult {
	return ApplyResult{Code: ResultRejected, Error: err.Error()}
}
