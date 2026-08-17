package controlgrpc

import (
	"context"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
)

type testRaftNode struct{}

func (testRaftNode) Status() raftnode.Status {
	return raftnode.Status{Role: "Leader", ClusterEpoch: "epoch-a", Term: 1}
}
func (testRaftNode) ClusterEpoch() string               { return "epoch-a" }
func (testRaftNode) VerifyLeader(context.Context) error { return nil }

func newLiveService(t *testing.T) (*Service, *authority.Manager) {
	t.Helper()
	manager, err := authority.New(authority.Config{ClusterEpoch: "epoch-a", ProbeInterval: time.Second, ProbeTimeout: time.Second, OpenContextTTL: time.Second}, testRaftNode{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatal(err)
	}
	service, err := NewService("epoch-a", manager)
	if err != nil {
		t.Fatal(err)
	}
	return service, manager
}

func testBinding() routing.LiveBinding {
	return routing.LiveBinding{Key: routing.BindingKey{ClientID: "client-a", EndpointPattern: "/jobs", TargetID: "worker"}, Ref: routing.ListenerBindingRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a", ListenerBindingID: "listener-a"}}
}

func TestSnapshotBindingsRejectsCrossSessionOwner(t *testing.T) {
	_, manager := newLiveService(t)
	session, err := manager.OpenSession("gateway-a", "instance-a", "127.0.0.1:27430")
	if err != nil {
		t.Fatal(err)
	}
	foreign := testBinding()
	foreign.Ref.GatewayInstanceID = "instance-b"
	if _, err := snapshotBindings(&controlv1.FullSnapshot{Session: sessionRefToProto(session.Ref), Bindings: []*controlv1.LiveBinding{liveBindingToProto(foreign, false)}}, session.Ref); err == nil {
		t.Fatal("cross-session snapshot binding succeeded")
	}
}

func TestDeclareReportsCurrentLiveConflict(t *testing.T) {
	service, manager := newLiveService(t)
	first, err := manager.OpenSession("gateway-a", "instance-a", "127.0.0.1:27430")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Revalidate(first.Ref, nil); err != nil {
		t.Fatal(err)
	}
	binding := testBinding()
	result, err := service.applyMutation(context.Background(), first.Ref, &controlv1.BindingMutation{Session: sessionRefToProto(first.Ref), Mutation: &controlv1.BindingMutation_Declare{Declare: liveBindingToProto(binding, false)}})
	if err != nil || result.GetCode() != controlv1.MutationCode_MUTATION_CODE_APPLIED {
		t.Fatalf("first declare = %#v, %v", result, err)
	}
	second, err := manager.OpenSession("gateway-b", "instance-b", "127.0.0.1:27431")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Revalidate(second.Ref, nil); err != nil {
		t.Fatal(err)
	}
	foreign := binding
	foreign.Ref.GatewayID, foreign.Ref.GatewayInstanceID, foreign.Ref.ListenerBindingID = "gateway-b", "instance-b", "listener-b"
	result, err = service.applyMutation(context.Background(), second.Ref, &controlv1.BindingMutation{Session: sessionRefToProto(second.Ref), Mutation: &controlv1.BindingMutation_Declare{Declare: liveBindingToProto(foreign, false)}})
	if err != nil || result.GetCode() != controlv1.MutationCode_MUTATION_CODE_CONFLICT {
		t.Fatalf("conflicting declare = %#v, %v", result, err)
	}
}

func TestSessionEndBulkDeletesRoutes(t *testing.T) {
	_, manager := newLiveService(t)
	session, err := manager.OpenSession("gateway-a", "instance-a", "127.0.0.1:27430")
	if err != nil {
		t.Fatal(err)
	}
	binding := testBinding()
	if err := manager.Revalidate(session.Ref, []routing.LiveBinding{binding}); err != nil {
		t.Fatal(err)
	}
	if got := manager.Presence().Bindings; got != 1 {
		t.Fatalf("bindings before end = %d", got)
	}
	manager.EndSession(session.Ref)
	if got := manager.Presence().Bindings; got != 0 {
		t.Fatalf("bindings after end = %d, want 0", got)
	}
}
