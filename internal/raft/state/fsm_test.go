package controlstate

import (
	"bytes"
	"encoding/json"
	"io"
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
