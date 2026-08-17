package controlstate

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func TestFSMInitializesFixedEpochOnly(t *testing.T) {
	fsm := NewFSM()
	command, err := EncodeInitializeEpoch(InitializeEpoch{ClusterEpoch: "epoch-1"})
	if err != nil {
		t.Fatalf("EncodeInitializeEpoch(): %v", err)
	}
	if result := applyCommand(t, fsm, command); result.Code != ResultApplied {
		t.Fatalf("first result = %#v", result)
	}
	if result := applyCommand(t, fsm, command); result.Code != ResultAlreadyApplied {
		t.Fatalf("replay result = %#v", result)
	}
	if state := fsm.State(); state != (State{ClusterEpoch: "epoch-1"}) {
		t.Fatalf("State() = %#v", state)
	}

	mismatch, err := EncodeInitializeEpoch(InitializeEpoch{ClusterEpoch: "epoch-2"})
	if err != nil {
		t.Fatalf("EncodeInitializeEpoch(mismatch): %v", err)
	}
	if result := applyCommand(t, fsm, mismatch); result.Code != ResultRejected {
		t.Fatalf("mismatch result = %#v", result)
	}
}

func TestFSMSnapshotRestoreContainsEpochAndNoRouteDomain(t *testing.T) {
	fsm := NewFSM()
	command, err := EncodeInitializeEpoch(InitializeEpoch{ClusterEpoch: "epoch-1"})
	if err != nil {
		t.Fatalf("EncodeInitializeEpoch(): %v", err)
	}
	if result := applyCommand(t, fsm, command); !result.Applied() {
		t.Fatalf("apply result = %#v", result)
	}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	sink := &memorySnapshotSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist(): %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(sink.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	state, ok := decoded["state"].(map[string]any)
	if !ok || state["cluster_epoch"] != "epoch-1" || len(state) != 1 {
		t.Fatalf("snapshot state = %#v, want only cluster_epoch", decoded["state"])
	}

	restored := NewFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if got := restored.State(); got != (State{ClusterEpoch: "epoch-1"}) {
		t.Fatalf("restored State() = %#v", got)
	}
}

func TestFSMRejectsUnknownCommandAndSnapshotFields(t *testing.T) {
	fsm := NewFSM()
	if result := applyCommand(t, fsm, []byte(`{"version":1,"kind":"install_binding","payload":{}}`)); result.Code != ResultRejected {
		t.Fatalf("unknown command result = %#v", result)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewBufferString(`{"version":1,"state":{"cluster_epoch":"epoch-1","bindings":[]}}`))); err == nil {
		t.Fatal("Restore() accepted route-domain snapshot field")
	}
}

func TestFSMRejectsInvalidEpochCommands(t *testing.T) {
	for name, epoch := range map[string]string{
		"empty":     "",
		"oversized": strings.Repeat("x", MaxClusterEpochBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := EncodeInitializeEpoch(InitializeEpoch{ClusterEpoch: epoch}); err == nil {
				t.Fatal("EncodeInitializeEpoch() accepted invalid cluster_epoch")
			}

			payload, err := json.Marshal(commandEnvelope{
				Version: commandVersion,
				Kind:    commandInitializeEpoch,
				Payload: json.RawMessage(`{"cluster_epoch":` + mustMarshalString(t, epoch) + `}`),
			})
			if err != nil {
				t.Fatalf("marshal command envelope: %v", err)
			}
			if result := applyCommand(t, NewFSM(), payload); result.Code != ResultRejected {
				t.Fatalf("Apply() result = %#v, want rejected", result)
			}
		})
	}
}

func TestFSMRestoreRejectsCorruptSafetySnapshotsWithoutChangingState(t *testing.T) {
	fsm := NewFSM()
	command, err := EncodeInitializeEpoch(InitializeEpoch{ClusterEpoch: "epoch-current"})
	if err != nil {
		t.Fatalf("EncodeInitializeEpoch(): %v", err)
	}
	if result := applyCommand(t, fsm, command); !result.Applied() {
		t.Fatalf("apply result = %#v", result)
	}

	oversizedEpoch := strings.Repeat("x", MaxClusterEpochBytes+1)
	invalid := map[string]string{
		"empty epoch":         `{"version":1,"state":{"cluster_epoch":""}}`,
		"oversized epoch":     `{"version":1,"state":{"cluster_epoch":` + mustMarshalString(t, oversizedEpoch) + `}}`,
		"unsupported version": `{"version":2,"state":{"cluster_epoch":"epoch-1"}}`,
		"trailing value":      `{"version":1,"state":{"cluster_epoch":"epoch-1"}} {}`,
	}
	for name, encoded := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := fsm.Restore(io.NopCloser(bytes.NewBufferString(encoded))); err == nil {
				t.Fatal("Restore() accepted corrupt safety snapshot")
			}
			if got := fsm.ClusterEpoch(); got != "epoch-current" {
				t.Fatalf("ClusterEpoch() = %q after failed restore, want epoch-current", got)
			}
		})
	}
}

func TestFSMRestoreAcceptsMaximumEpochSize(t *testing.T) {
	epoch := strings.Repeat("x", MaxClusterEpochBytes)
	encoded := `{"version":1,"state":{"cluster_epoch":` + mustMarshalString(t, epoch) + `}}`
	fsm := NewFSM()
	if err := fsm.Restore(io.NopCloser(bytes.NewBufferString(encoded))); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if got := fsm.ClusterEpoch(); got != epoch {
		t.Fatalf("ClusterEpoch() length = %d, want %d", len(got), len(epoch))
	}
}

func mustMarshalString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal string: %v", err)
	}
	return string(encoded)
}

func applyCommand(t *testing.T, fsm *FSM, command []byte) ApplyResult {
	t.Helper()
	response := fsm.Apply(&raft.Log{Data: command})
	result, ok := response.(ApplyResult)
	if !ok {
		t.Fatalf("Apply() result type = %T", response)
	}
	return result
}

type memorySnapshotSink struct {
	bytes.Buffer
	cancelled bool
	closed    bool
}

func (s *memorySnapshotSink) ID() string { return "memory" }

func (s *memorySnapshotSink) Cancel() error {
	s.cancelled = true
	return nil
}

func (s *memorySnapshotSink) Close() error {
	s.closed = true
	return nil
}
