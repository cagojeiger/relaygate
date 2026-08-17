package controlgrpc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

func TestSnapshotEnvelopeAcceptsMaximumLegalSetAndRejectsExcess(t *testing.T) {
	identity := strings.Repeat("i", routing.MaxIdentityBytes)
	ref := authority.SessionRef{
		ClusterEpoch:      identity,
		AuthorityID:       identity,
		ControlSessionID:  identity,
		GatewayID:         identity,
		GatewayInstanceID: identity,
	}
	snapshot := &controlv1.FullSnapshot{Session: sessionRefToProto(ref)}
	for index := 0; index < routing.MaxListenerBindingsPerGateway; index++ {
		prefix := fmt.Sprintf("/%03d/", index)
		binding := routing.LiveBinding{
			Key: routing.BindingKey{
				ClientID:        identity,
				EndpointPattern: prefix + strings.Repeat("e", routing.MaxEndpointPatternBytes-len(prefix)),
				TargetID:        identity,
			},
			Ref: routing.ListenerBindingRef{
				GatewayID:         ref.GatewayID,
				GatewayInstanceID: ref.GatewayInstanceID,
				ListenerBindingID: fmt.Sprintf("%04d-%s", index, strings.Repeat("l", routing.MaxIdentityBytes-5)),
			},
		}
		snapshot.Bindings = append(snapshot.Bindings, liveBindingToProto(binding, false))
	}
	request := &controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{FullSnapshot: snapshot}}
	if size := proto.Size(request); size > maxMessageBytes {
		t.Fatalf("maximum legal snapshot wire size = %d, exceeds server limit %d", size, maxMessageBytes)
	}
	if bindings, err := snapshotBindings(snapshot, ref); err != nil || len(bindings) != routing.MaxListenerBindingsPerGateway {
		t.Fatalf("snapshotBindings(max) = %d, %v", len(bindings), err)
	}

	excess := proto.Clone(snapshot).(*controlv1.FullSnapshot)
	excess.Bindings = append(excess.Bindings, &controlv1.LiveBinding{})
	if _, err := snapshotBindings(excess, ref); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("snapshotBindings(max+1) = %v, want ResourceExhausted", err)
	}
}
