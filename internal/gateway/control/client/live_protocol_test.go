package gatewaycontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
)

type staticSnapshot []routing.LiveBinding

func (s staticSnapshot) LiveBindings() []routing.LiveBinding {
	return append([]routing.LiveBinding(nil), s...)
}

func testLiveBinding(endpoint string) routing.LiveBinding {
	return routing.LiveBinding{
		Key: routing.BindingKey{ClientID: "client-a", EndpointPattern: endpoint, TargetID: "worker"},
		Ref: routing.ListenerBindingRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a", ListenerBindingID: "listener-" + endpoint},
	}
}

func TestCurrentSessionRequiresRevalidatedState(t *testing.T) {
	client, err := newClient(Config{ClusterEpoch: "epoch-a", GatewayID: "gateway-a", RelayAddress: "127.0.0.1:27430", ControlEndpoints: []string{"127.0.0.1:27410"}, ConnectTimeout: time.Second, RetryInterval: time.Second}, nil, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	client.admissionSession = &controlv1.SessionRef{ClusterEpoch: "epoch-a", AuthorityId: "authority-a", ControlSessionId: "session-a", GatewayId: "gateway-a", GatewayInstanceId: "instance-a"}
	if _, ok := client.CurrentSession(); ok {
		t.Fatal("non-revalidated session was published")
	}
	client.status.State = StateRevalidated
	ref, ok := client.CurrentSession()
	if !ok || ref.ControlSessionID != "session-a" {
		t.Fatalf("CurrentSession() = %#v, %v", ref, ok)
	}
	client.status.State = StateDisconnected
	if _, ok := client.CurrentSession(); ok {
		t.Fatal("disconnected session was published")
	}
}

func TestCurrentSnapshotUsesCurrentLocalBindingsOnly(t *testing.T) {
	client, err := newClient(Config{ClusterEpoch: "epoch-a", GatewayID: "gateway-a", RelayAddress: "127.0.0.1:27430", ControlEndpoints: []string{"127.0.0.1:27410"}, ConnectTimeout: time.Second, RetryInterval: time.Second}, nil, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.AttachSnapshotProvider(staticSnapshot{testLiveBinding("/z"), testLiveBinding("/a")}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.currentSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 2 || snapshot[0].GetKey().GetEndpointPattern() != "/a" || snapshot[0].GetRef().GetGatewayId() != "" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestMutationResultRequiresExactCurrentBinding(t *testing.T) {
	client, err := newClient(Config{ClusterEpoch: "epoch-a", GatewayID: "gateway-a", RelayAddress: "127.0.0.1:27430", ControlEndpoints: []string{"127.0.0.1:27410"}, ConnectTimeout: time.Second, RetryInterval: time.Second}, nil, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	binding := testLiveBinding("/a")
	mutation := &pendingMutation{kind: mutationDeclare, binding: binding}
	if err := client.mutationResult(mutation, &controlv1.MutationResult{Code: controlv1.MutationCode_MUTATION_CODE_APPLIED, Binding: liveBindingToProto(binding, false)}); err != nil {
		t.Fatal(err)
	}
	if err := client.mutationResult(mutation, &controlv1.MutationResult{Code: controlv1.MutationCode_MUTATION_CODE_APPLIED, Binding: liveBindingToProto(testLiveBinding("/other"), false)}); err == nil {
		t.Fatal("mismatched response succeeded")
	}
}

func TestSessionLossFailsPendingMutationInsteadOfReplayingIt(t *testing.T) {
	client, err := newClient(Config{ClusterEpoch: "epoch-a", GatewayID: "gateway-a", RelayAddress: "127.0.0.1:27430", ControlEndpoints: []string{"127.0.0.1:27410"}, ConnectTimeout: time.Second, RetryInterval: time.Second}, nil, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	pending := &pendingMutation{kind: mutationDeclare, binding: testLiveBinding("/a"), done: make(chan error, 1)}
	client.active = pending
	client.status.State = StateRevalidated
	client.admissionSession = &controlv1.SessionRef{ClusterEpoch: "epoch-a", AuthorityId: "authority-a", ControlSessionId: "session-a", GatewayId: "gateway-a", GatewayInstanceId: "instance-a"}
	client.endCurrentSession(errors.New("response lost"))
	if err := <-pending.done; !errors.Is(err, ErrControlUnavailable) {
		t.Fatalf("pending error = %v", err)
	}
	if client.active != nil || len(client.queue) != 0 {
		t.Fatal("lost-session mutation was retained for replay")
	}
	if _, ok := client.CurrentSession(); ok {
		t.Fatal("lost session remained published")
	}
}

func TestSyncingMutationIsSerializedAfterCurrentSnapshot(t *testing.T) {
	client, err := newClient(Config{ClusterEpoch: "epoch-a", GatewayID: "gateway-a", RelayAddress: "127.0.0.1:27430", ControlEndpoints: []string{"127.0.0.1:27410"}, ConnectTimeout: time.Second, RetryInterval: time.Second}, nil, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	session := &controlv1.SessionRef{ClusterEpoch: "epoch-a", AuthorityId: "authority-a", ControlSessionId: "session-a", GatewayId: "gateway-a", GatewayInstanceId: "instance-a"}
	client.setSyncing("127.0.0.1:27410", session)
	binding := testLiveBinding("/snapshot-race")
	done := make(chan error, 1)
	go func() { done <- client.Withdraw(context.Background(), binding) }()

	deadline := time.Now().Add(time.Second)
	for {
		client.mu.Lock()
		queued := len(client.queue)
		client.mu.Unlock()
		if queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("syncing mutation was not queued on its exact control session")
		}
		time.Sleep(time.Millisecond)
	}
	mutation := client.nextMutation()
	if mutation == nil || mutation.kind != mutationWithdraw || mutation.binding != binding {
		t.Fatalf("queued mutation = %#v", mutation)
	}
	client.finishMutation(mutation, nil)
	if err := <-done; err != nil {
		t.Fatalf("Withdraw() = %v", err)
	}
}
