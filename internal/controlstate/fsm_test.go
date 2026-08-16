package controlstate

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func TestBindingCASReplayAndABAProtection(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", 10)

	key := BindingKey{ClientID: "client-a", EndpointPattern: "/service/*", TargetID: "primary"}
	firstRef := ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-1"}
	secondRef := ListenerBindingRef{GatewayID: "gateway-2", GatewayInstanceID: "instance-2", ListenerBindingID: "listener-2"}

	install := mustEncodeInstall(t, InstallBinding{
		ClusterEpoch: "epoch-1",
		Key:          key,
		NewRef:       firstRef,
	})
	assertResultCode(t, applyCommand(t, fsm, install), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, install), ResultAlreadyApplied)

	remove := mustEncodeRemove(t, RemoveBinding{
		ClusterEpoch:       "epoch-1",
		Key:                key,
		ExpectedGeneration: 1,
		ExpectedRef:        firstRef,
	})
	assertResultCode(t, applyCommand(t, fsm, remove), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, remove), ResultAlreadyApplied)

	rebind := mustEncodeInstall(t, InstallBinding{
		ClusterEpoch:       "epoch-1",
		Key:                key,
		ExpectedGeneration: 2,
		NewRef:             secondRef,
	})
	assertResultCode(t, applyCommand(t, fsm, rebind), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, remove), ResultRejected)

	slot := fsm.Lookup(key)
	if slot.Generation != 3 {
		t.Fatalf("generation = %d, want 3", slot.Generation)
	}
	if slot.Ref == nil || *slot.Ref != secondRef {
		t.Fatalf("ref = %#v, want %#v", slot.Ref, secondRef)
	}
}

func TestControlIdentityAndBindingKeyLengthsAreBounded(t *testing.T) {
	valid := BindingKey{
		ClientID:        strings.Repeat("c", MaxIdentityBytes),
		EndpointPattern: strings.Repeat("e", MaxEndpointPatternBytes),
		TargetID:        strings.Repeat("t", MaxIdentityBytes),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("max-size BindingKey.Validate(): %v", err)
	}
	for name, key := range map[string]BindingKey{
		"client":   {ClientID: strings.Repeat("c", MaxIdentityBytes+1), EndpointPattern: "/", TargetID: "target"},
		"endpoint": {ClientID: "client", EndpointPattern: strings.Repeat("e", MaxEndpointPatternBytes+1), TargetID: "target"},
		"target":   {ClientID: "client", EndpointPattern: "/", TargetID: strings.Repeat("t", MaxIdentityBytes+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := key.Validate(); err == nil {
				t.Fatal("oversized BindingKey.Validate() succeeded")
			}
		})
	}
	ref := ListenerBindingRef{
		GatewayID: strings.Repeat("g", MaxIdentityBytes+1), GatewayInstanceID: "instance", ListenerBindingID: "listener",
	}
	if err := ref.Validate(); err == nil {
		t.Fatal("oversized ListenerBindingRef.Validate() succeeded")
	}
}

func TestPerGatewayInstanceBindingCapacity(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", MaxListenerBindingsPerGateway+10)
	var firstKey BindingKey
	var firstRef ListenerBindingRef
	for index := 0; index < MaxListenerBindingsPerGateway; index++ {
		key := BindingKey{ClientID: "client-a", EndpointPattern: fmt.Sprintf("/%d", index), TargetID: "worker"}
		ref := ListenerBindingRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a", ListenerBindingID: fmt.Sprintf("listener-%d", index)}
		if index == 0 {
			firstKey, firstRef = key, ref
		}
		result := applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{ClusterEpoch: "epoch-1", Key: key, NewRef: ref}))
		assertResultCode(t, result, ResultApplied)
	}

	overflowKey := BindingKey{ClientID: "client-a", EndpointPattern: "/overflow", TargetID: "worker"}
	overflowRef := ListenerBindingRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a", ListenerBindingID: "listener-overflow"}
	overflow := applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{ClusterEpoch: "epoch-1", Key: overflowKey, NewRef: overflowRef}))
	assertResultCode(t, overflow, ResultCapacityReached)
	if overflow.Error != ErrBindingCapacity.Error() {
		t.Fatalf("capacity error = %q", overflow.Error)
	}

	replacement := firstRef
	replacement.ListenerBindingID = "listener-replacement"
	replaced := applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
		ClusterEpoch: "epoch-1", Key: firstKey, ExpectedGeneration: 1, ExpectedRef: &firstRef, NewRef: replacement,
	}))
	assertResultCode(t, replaced, ResultApplied)

	otherInstance := overflowRef
	otherInstance.GatewayInstanceID = "instance-b"
	otherInstance.ListenerBindingID = "listener-other-instance"
	other := applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{ClusterEpoch: "epoch-1", Key: overflowKey, NewRef: otherInstance}))
	assertResultCode(t, other, ResultApplied)
}

func TestGatewayRegistrationCASReplayAndABAProtection(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", 10)

	firstRef := GatewayRegistrationRef{GatewayInstanceID: "instance-a"}
	secondRef := GatewayRegistrationRef{GatewayInstanceID: "instance-b"}
	registerFirst := mustEncodeRegisterGateway(t, RegisterGateway{
		ClusterEpoch: "epoch-1",
		GatewayID:    "gateway-1",
		NewRef:       firstRef,
	})
	assertResultCode(t, applyCommand(t, fsm, registerFirst), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, registerFirst), ResultAlreadyApplied)

	registerSecond := mustEncodeRegisterGateway(t, RegisterGateway{
		ClusterEpoch:       "epoch-1",
		GatewayID:          "gateway-1",
		ExpectedGeneration: 1,
		ExpectedRef:        &firstRef,
		NewRef:             secondRef,
	})
	assertResultCode(t, applyCommand(t, fsm, registerSecond), ResultApplied)

	removeFirst := mustEncodeRemoveGateway(t, RemoveGateway{
		ClusterEpoch:       "epoch-1",
		GatewayID:          "gateway-1",
		ExpectedGeneration: 1,
		ExpectedRef:        firstRef,
	})
	assertResultCode(t, applyCommand(t, fsm, removeFirst), ResultRejected)

	removeSecond := mustEncodeRemoveGateway(t, RemoveGateway{
		ClusterEpoch:       "epoch-1",
		GatewayID:          "gateway-1",
		ExpectedGeneration: 2,
		ExpectedRef:        secondRef,
	})
	assertResultCode(t, applyCommand(t, fsm, removeSecond), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, removeSecond), ResultAlreadyApplied)

	slot := fsm.LookupGateway("gateway-1")
	if slot.Generation != 3 || !slot.IsTombstone() {
		t.Fatalf("gateway slot = %#v", slot)
	}
}

func TestDistinctBindingKeyLimitPreservesExistingKeys(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", 1)

	firstKey := BindingKey{ClientID: "client-a", EndpointPattern: "/one", TargetID: "primary"}
	secondKey := BindingKey{ClientID: "client-a", EndpointPattern: "/two", TargetID: "primary"}
	firstRef := ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-1"}
	secondRef := ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-2"}

	assertResultCode(t, applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
		ClusterEpoch: "epoch-1",
		Key:          firstKey,
		NewRef:       firstRef,
	})), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
		ClusterEpoch: "epoch-1",
		Key:          secondKey,
		NewRef:       secondRef,
	})), ResultRejected)

	assertResultCode(t, applyCommand(t, fsm, mustEncodeRemove(t, RemoveBinding{
		ClusterEpoch:       "epoch-1",
		Key:                firstKey,
		ExpectedGeneration: 1,
		ExpectedRef:        firstRef,
	})), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
		ClusterEpoch:       "epoch-1",
		Key:                firstKey,
		ExpectedGeneration: 2,
		NewRef:             secondRef,
	})), ResultApplied)
}

func TestDistinctGatewayIDLimitPreservesExistingIDs(t *testing.T) {
	fsm := NewFSM()
	initializeFSMWithLimits(t, fsm, "epoch-1", 10, 1)

	firstRef := GatewayRegistrationRef{GatewayInstanceID: "instance-a"}
	secondRef := GatewayRegistrationRef{GatewayInstanceID: "instance-b"}
	assertResultCode(t, applyCommand(t, fsm, mustEncodeRegisterGateway(t, RegisterGateway{
		ClusterEpoch: "epoch-1",
		GatewayID:    "gateway-1",
		NewRef:       firstRef,
	})), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, mustEncodeRegisterGateway(t, RegisterGateway{
		ClusterEpoch: "epoch-1",
		GatewayID:    "gateway-2",
		NewRef:       secondRef,
	})), ResultRejected)

	assertResultCode(t, applyCommand(t, fsm, mustEncodeRegisterGateway(t, RegisterGateway{
		ClusterEpoch:       "epoch-1",
		GatewayID:          "gateway-1",
		ExpectedGeneration: 1,
		ExpectedRef:        &firstRef,
		NewRef:             secondRef,
	})), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, mustEncodeRemoveGateway(t, RemoveGateway{
		ClusterEpoch:       "epoch-1",
		GatewayID:          "gateway-1",
		ExpectedGeneration: 2,
		ExpectedRef:        secondRef,
	})), ResultApplied)
	assertResultCode(t, applyCommand(t, fsm, mustEncodeRegisterGateway(t, RegisterGateway{
		ClusterEpoch:       "epoch-1",
		GatewayID:          "gateway-1",
		ExpectedGeneration: 3,
		NewRef:             firstRef,
	})), ResultApplied)
}

func TestBindingKeyTupleDoesNotCollapseEmbeddedSeparators(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", 2)

	keys := []BindingKey{
		{ClientID: "client\x00part", EndpointPattern: "/service", TargetID: "primary"},
		{ClientID: "client", EndpointPattern: "part\x00/service", TargetID: "primary"},
	}
	for index, key := range keys {
		assertResultCode(t, applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
			ClusterEpoch: "epoch-1",
			Key:          key,
			NewRef: ListenerBindingRef{
				GatewayID:         "gateway-1",
				GatewayInstanceID: "instance-1",
				ListenerBindingID: string(rune('a' + index)),
			},
		})), ResultApplied)
	}
	if got := len(fsm.State().Bindings); got != 2 {
		t.Fatalf("binding count = %d, want 2", got)
	}
}

func TestSnapshotRestorePreservesDeterministicControlState(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", 10)

	keys := []BindingKey{
		{ClientID: "client-z", EndpointPattern: "/z", TargetID: "primary"},
		{ClientID: "client-a", EndpointPattern: "/a", TargetID: "primary"},
	}
	for index, key := range keys {
		assertResultCode(t, applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
			ClusterEpoch: "epoch-1",
			Key:          key,
			NewRef: ListenerBindingRef{
				GatewayID:         "gateway-1",
				GatewayInstanceID: "instance-1",
				ListenerBindingID: "listener-" + string(rune('1'+index)),
			},
		})), ResultApplied)
	}
	for _, gatewayID := range []string{"gateway-z", "gateway-a"} {
		assertResultCode(t, applyCommand(t, fsm, mustEncodeRegisterGateway(t, RegisterGateway{
			ClusterEpoch: "epoch-1",
			GatewayID:    gatewayID,
			NewRef:       GatewayRegistrationRef{GatewayInstanceID: gatewayID + "-instance"},
		})), ResultApplied)
	}

	before := fsm.State()
	if before.Bindings[0].Key != keys[1] {
		t.Fatalf("bindings are not canonical-key sorted: %#v", before.Bindings)
	}
	if before.Gateways[0].GatewayID != "gateway-a" {
		t.Fatalf("gateways are not sorted: %#v", before.Gateways)
	}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	sink := &memorySnapshotSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist(): %v", err)
	}
	snapshot.Release()

	restored := NewFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if after := restored.State(); !reflect.DeepEqual(after, before) {
		t.Fatalf("restored state = %#v, want %#v", after, before)
	}
}

func TestFSMRejectsWrongEpochAndUnknownFields(t *testing.T) {
	fsm := NewFSM()
	initializeFSM(t, fsm, "epoch-1", 10)
	result := applyCommand(t, fsm, mustEncodeInstall(t, InstallBinding{
		ClusterEpoch: "epoch-old",
		Key: BindingKey{
			ClientID:        "client-a",
			EndpointPattern: "/service",
			TargetID:        "primary",
		},
		NewRef: ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-1"},
	}))
	assertResultCode(t, result, ResultRejected)
	if !strings.Contains(result.Error, ErrEpochMismatch.Error()) {
		t.Fatalf("error = %q, want epoch mismatch", result.Error)
	}

	result = applyCommand(t, fsm, []byte(`{"version":1,"kind":"initialize_epoch","payload":{},"extra":true}`))
	assertResultCode(t, result, ResultRejected)
}

func initializeFSM(t *testing.T, fsm *FSM, epoch string, limit uint64) {
	t.Helper()
	initializeFSMWithLimits(t, fsm, epoch, limit, 16)
}

func initializeFSMWithLimits(t *testing.T, fsm *FSM, epoch string, bindingLimit, gatewayLimit uint64) {
	t.Helper()
	command, err := EncodeInitializeEpoch(InitializeEpoch{
		ClusterEpoch:                   epoch,
		MaxDistinctBindingKeysPerEpoch: bindingLimit,
		MaxDistinctGatewayIDsPerEpoch:  gatewayLimit,
	})
	if err != nil {
		t.Fatalf("EncodeInitializeEpoch(): %v", err)
	}
	assertResultCode(t, applyCommand(t, fsm, command), ResultApplied)
}

func applyCommand(t *testing.T, fsm *FSM, command []byte) ApplyResult {
	t.Helper()
	result, ok := fsm.Apply(&raft.Log{Data: command}).(ApplyResult)
	if !ok {
		t.Fatalf("Apply() returned unexpected type")
	}
	return result
}

func mustEncodeInstall(t *testing.T, command InstallBinding) []byte {
	t.Helper()
	encoded, err := EncodeInstallBinding(command)
	if err != nil {
		t.Fatalf("EncodeInstallBinding(): %v", err)
	}
	return encoded
}

func mustEncodeRemove(t *testing.T, command RemoveBinding) []byte {
	t.Helper()
	encoded, err := EncodeRemoveBinding(command)
	if err != nil {
		t.Fatalf("EncodeRemoveBinding(): %v", err)
	}
	return encoded
}

func mustEncodeRegisterGateway(t *testing.T, command RegisterGateway) []byte {
	t.Helper()
	encoded, err := EncodeRegisterGateway(command)
	if err != nil {
		t.Fatalf("EncodeRegisterGateway(): %v", err)
	}
	return encoded
}

func mustEncodeRemoveGateway(t *testing.T, command RemoveGateway) []byte {
	t.Helper()
	encoded, err := EncodeRemoveGateway(command)
	if err != nil {
		t.Fatalf("EncodeRemoveGateway(): %v", err)
	}
	return encoded
}

func assertResultCode(t *testing.T, result ApplyResult, want ResultCode) {
	t.Helper()
	if result.Code != want {
		t.Fatalf("result = %#v, want code %q", result, want)
	}
}

type memorySnapshotSink struct {
	bytes.Buffer
	closed   bool
	canceled bool
}

func (s *memorySnapshotSink) ID() string { return "memory" }

func (s *memorySnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *memorySnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}
