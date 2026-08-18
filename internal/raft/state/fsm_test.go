package controlstate

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
)

const testEpoch = "epoch-1"

func TestFSMRegisterReplacementCascadesRoutesAndFencesStaleABA(t *testing.T) {
	fsm := initializedFSM(t, 2, 8, 4)
	first := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-1"}
	second := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-2"}
	register(t, fsm, first)
	declare(t, fsm, first, binding("client-a", "/echo", "worker", "listener-1"))

	register(t, fsm, second)
	if _, ok := fsm.LookupRoute(BindingKey{ClientID: "client-a", EndpointPattern: "/echo", TargetID: "worker"}); ok {
		t.Fatal("replacing a gateway instance retained a stale route")
	}
	if current, ok := fsm.LookupGateway("gateway-a"); !ok || current != second {
		t.Fatalf("LookupGateway() = %#v, %v; want second instance", current, ok)
	}

	// A stale process must not undo the replacement, even if it replays an old
	// withdrawal or an old remove after another same-ID incarnation exists.
	if result := applyCommand(t, fsm, WithdrawRoute{ClusterEpoch: testEpoch, Gateway: first, Binding: binding("client-a", "/echo", "worker", "listener-1")}); result.Code != ResultRejected {
		t.Fatalf("stale withdrawal = %#v, want rejected", result)
	}
	if result := applyCommand(t, fsm, RemoveGateway{ClusterEpoch: testEpoch, Gateway: first}); result.Code != ResultAlreadyApplied {
		t.Fatalf("stale removal = %#v, want already applied", result)
	}
	declare(t, fsm, second, binding("client-a", "/echo", "worker", "listener-2"))
	if got, ok := fsm.LookupRoute(BindingKey{ClientID: "client-a", EndpointPattern: "/echo", TargetID: "worker"}); !ok || got.Owner != second {
		t.Fatalf("current route = %#v, %v; want second owner", got, ok)
	}
}

func TestFSMReplaceSnapshotIsAtomicOnConflict(t *testing.T) {
	fsm := initializedFSM(t, 3, 8, 4)
	a := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a"}
	b := GatewaySessionRef{GatewayID: "gateway-b", GatewayInstanceID: "instance-b"}
	register(t, fsm, a)
	register(t, fsm, b)
	declare(t, fsm, a, binding("client-a", "/a", "worker", "a-1"))
	declare(t, fsm, b, binding("client-a", "/b", "worker", "b-1"))

	result := applyCommand(t, fsm, ReplaceSnapshot{
		ClusterEpoch: testEpoch,
		Gateway:      a,
		Bindings: []Binding{
			binding("client-a", "/new", "worker", "a-new"),
			binding("client-a", "/b", "worker", "a-steal"),
		},
	})
	if result.Code != ResultConflict {
		t.Fatalf("conflicting snapshot = %#v, want conflict", result)
	}
	state := fsm.State()
	if len(state.Routes) != 2 {
		t.Fatalf("route count after rejected snapshot = %d, want 2", len(state.Routes))
	}
	if _, ok := fsm.LookupRoute(BindingKey{ClientID: "client-a", EndpointPattern: "/a", TargetID: "worker"}); !ok {
		t.Fatal("rejected snapshot removed old owned route")
	}
	if _, ok := fsm.LookupRoute(BindingKey{ClientID: "client-a", EndpointPattern: "/new", TargetID: "worker"}); ok {
		t.Fatal("rejected snapshot installed a partial route")
	}
}

func TestFSMReplaceSnapshotOverConfiguredGatewayCapacityIsAtomicCapacity(t *testing.T) {
	fsm := initializedFSM(t, 1, 1, 1)
	gateway := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a"}
	register(t, fsm, gateway)
	existing := binding("client-a", "/existing", "worker", "listener-existing")
	declare(t, fsm, gateway, existing)

	result := applyCommand(t, fsm, ReplaceSnapshot{
		ClusterEpoch: testEpoch,
		Gateway:      gateway,
		Bindings: []Binding{
			binding("client-a", "/one", "worker", "listener-one"),
			binding("client-a", "/two", "worker", "listener-two"),
		},
	})
	if result.Code != ResultCapacity {
		t.Fatalf("over-capacity snapshot = %#v, want capacity", result)
	}
	state := fsm.State()
	if len(state.Routes) != 1 || state.Routes[0] != (Route{Key: existing.Key, Owner: gateway, ListenerBindingID: existing.ListenerBindingID}) {
		t.Fatalf("over-capacity snapshot changed current routes: %#v", state.Routes)
	}
}

func TestFSMSnapshotRestoreIsDeterministicAndStrict(t *testing.T) {
	fsm := initializedFSM(t, 3, 8, 4)
	b := GatewaySessionRef{GatewayID: "gateway-b", GatewayInstanceID: "instance-b"}
	a := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a"}
	register(t, fsm, b)
	register(t, fsm, a)
	declare(t, fsm, b, binding("client-b", "/z", "worker", "b-1"))
	declare(t, fsm, a, binding("client-a", "/a", "worker", "a-1"))

	encoded := snapshotBytes(t, fsm)
	var envelope snapshotEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !reflect.DeepEqual(envelope.State.Gateways, []GatewaySessionRef{a, b}) {
		t.Fatalf("snapshot gateways = %#v, want sorted", envelope.State.Gateways)
	}
	if len(envelope.State.Routes) != 2 || envelope.State.Routes[0].Key.ClientID != "client-a" {
		t.Fatalf("snapshot routes = %#v, want sorted", envelope.State.Routes)
	}

	restored := NewFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(encoded))); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if got, want := restored.State(), fsm.State(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored State() = %#v, want %#v", got, want)
	}

	// Restore is atomic: an invalid non-canonical state cannot partially alter
	// a valid current directory.
	invalid := []byte(`{"version":2,"state":{"cluster_epoch":"epoch-1","max_gateway_sessions":3,"max_routes":8,"max_bindings_per_gateway":4,"gateways":[{"gateway_id":"gateway-b","gateway_instance_id":"instance-b"},{"gateway_id":"gateway-a","gateway_instance_id":"instance-a"}],"routes":[]}}`)
	before := restored.State()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(invalid))); err == nil {
		t.Fatal("Restore() accepted unsorted gateways")
	}
	if got := restored.State(); !reflect.DeepEqual(got, before) {
		t.Fatalf("failed restore changed state: %#v, want %#v", got, before)
	}
}

func TestFSMChurnDoesNotConsumeCapacityAndTrueDeleteReclaimsIt(t *testing.T) {
	fsm := initializedFSM(t, 1, 1, 1)
	gateway := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a"}
	register(t, fsm, gateway)
	for index := 0; index < 20; index++ {
		current := binding("client-a", "/churn/"+string(rune('a'+index)), "worker", "listener")
		declare(t, fsm, gateway, current)
		if result := applyCommand(t, fsm, WithdrawRoute{ClusterEpoch: testEpoch, Gateway: gateway, Binding: current}); result.Code != ResultApplied {
			t.Fatalf("withdraw %d = %#v", index, result)
		}
	}
	if got := len(fsm.State().Routes); got != 0 {
		t.Fatalf("routes after churn = %d, want 0", got)
	}

	first := binding("client-a", "/one", "worker", "one")
	second := binding("client-a", "/two", "worker", "two")
	declare(t, fsm, gateway, first)
	if result := applyCommand(t, fsm, DeclareRoute{ClusterEpoch: testEpoch, Gateway: gateway, Binding: second}); result.Code != ResultCapacity {
		t.Fatalf("second live route = %#v, want capacity", result)
	}
	if result := applyCommand(t, fsm, WithdrawRoute{ClusterEpoch: testEpoch, Gateway: gateway, Binding: first}); result.Code != ResultApplied {
		t.Fatalf("withdraw first = %#v", result)
	}
	declare(t, fsm, gateway, second)
}

func TestFSMRemoveGatewayCascadesAndDuplicateCleanupIsIdempotent(t *testing.T) {
	fsm := initializedFSM(t, 2, 4, 2)
	gateway := GatewaySessionRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a"}
	register(t, fsm, gateway)
	first := binding("client-a", "/one", "worker", "one")
	second := binding("client-a", "/two", "worker", "two")
	declare(t, fsm, gateway, first)
	declare(t, fsm, gateway, second)
	if result := applyCommand(t, fsm, RemoveGateway{ClusterEpoch: testEpoch, Gateway: gateway}); result.Code != ResultApplied {
		t.Fatalf("RemoveGateway() = %#v", result)
	}
	state := fsm.State()
	if len(state.Gateways) != 0 || len(state.Routes) != 0 {
		t.Fatalf("remove did not cascade: %#v", state)
	}
	if result := applyCommand(t, fsm, RemoveGateway{ClusterEpoch: testEpoch, Gateway: gateway}); result.Code != ResultAlreadyApplied {
		t.Fatalf("duplicate remove = %#v", result)
	}
}

func initializedFSM(t *testing.T, maxGateways, maxRoutes, maxBindings uint32) *FSM {
	t.Helper()
	fsm := NewFSM()
	result := applyCommand(t, fsm, InitializeCluster{
		ClusterEpoch: testEpoch, MaxGatewaySessions: maxGateways, MaxRoutes: maxRoutes, MaxBindingsPerGateway: maxBindings,
	})
	if result.Code != ResultApplied {
		t.Fatalf("initialize = %#v", result)
	}
	return fsm
}

func register(t *testing.T, fsm *FSM, gateway GatewaySessionRef) {
	t.Helper()
	if result := applyCommand(t, fsm, RegisterGateway{ClusterEpoch: testEpoch, Gateway: gateway}); result.Code != ResultApplied {
		t.Fatalf("register %v = %#v", gateway, result)
	}
}

func declare(t *testing.T, fsm *FSM, gateway GatewaySessionRef, value Binding) {
	t.Helper()
	if result := applyCommand(t, fsm, DeclareRoute{ClusterEpoch: testEpoch, Gateway: gateway, Binding: value}); result.Code != ResultApplied {
		t.Fatalf("declare %v = %#v", value, result)
	}
}

func binding(clientID, endpoint, targetID, listenerID string) Binding {
	return Binding{Key: BindingKey{ClientID: clientID, EndpointPattern: endpoint, TargetID: targetID}, ListenerBindingID: listenerID}
}

func snapshotBytes(t *testing.T, fsm *FSM) []byte {
	t.Helper()
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	sink := &memorySnapshotSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist(): %v", err)
	}
	return sink.Bytes()
}

func applyCommand(t *testing.T, fsm *FSM, command any) ApplyResult {
	t.Helper()
	var (
		encoded []byte
		err     error
	)
	switch value := command.(type) {
	case InitializeCluster:
		encoded, err = EncodeInitializeCluster(value)
	case RegisterGateway:
		encoded, err = EncodeRegisterGateway(value)
	case ReplaceSnapshot:
		encoded, err = EncodeReplaceSnapshot(value)
	case DeclareRoute:
		encoded, err = EncodeDeclareRoute(value)
	case WithdrawRoute:
		encoded, err = EncodeWithdrawRoute(value)
	case RemoveGateway:
		encoded, err = EncodeRemoveGateway(value)
	default:
		t.Fatalf("unsupported command type %T", command)
	}
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	response := fsm.Apply(&raft.Log{Data: encoded})
	result, ok := response.(ApplyResult)
	if !ok {
		t.Fatalf("Apply() result type = %T", response)
	}
	return result
}

type memorySnapshotSink struct {
	bytes.Buffer
}

func (s *memorySnapshotSink) ID() string    { return "memory" }
func (s *memorySnapshotSink) Cancel() error { return nil }
func (s *memorySnapshotSink) Close() error  { return nil }
