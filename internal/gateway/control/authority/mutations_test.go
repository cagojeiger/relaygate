package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

func TestSnapshotConflictIsAtomicAndExactCAndVAreRequired(t *testing.T) {
	manager, _ := newManager(t)
	confirm(t, manager)
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	other, err := manager.OpenSession(context.Background(), "other", "other-1", "127.0.0.1:9003")
	if err != nil {
		t.Fatalf("OpenSession(other): %v", err)
	}
	foreign := binding
	foreign.Ref.GatewayID, foreign.Ref.GatewayInstanceID = "other", "other-1"
	if err := manager.Revalidate(context.Background(), other.Ref, []routing.LiveBinding{foreign}); !errors.Is(err, routing.ErrConflict) {
		t.Fatalf("conflicting snapshot = %v, want routing.ErrConflict", err)
	}
	if err := manager.RequireRevalidated(other.Ref); !errors.Is(err, ErrSnapshotFirst) {
		t.Fatalf("conflicting snapshot made V true: %v", err)
	}
	open, err := manager.AdmitOpen(context.Background(), ingress, testAuth(), "/events", "worker")
	if err != nil || open.OwnerControlSessionID != owner.ControlSessionID {
		t.Fatalf("AdmitOpen(after rejected snapshot) = %#v, %v", open, err)
	}
}

func TestSnapshotOverReplicatedGatewayCapacityReturnsCapacityWithoutV(t *testing.T) {
	manager, node := newManagerWithMaxBindings(t, 1)
	confirm(t, manager)
	session, err := manager.OpenSession(context.Background(), "owner", "owner-1", "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("OpenSession(owner): %v", err)
	}
	first := testBinding("owner", "owner-1", "listener-1")
	second := first
	second.Key.EndpointPattern = "/other"
	second.Ref.ListenerBindingID = "listener-2"
	if err := manager.Revalidate(context.Background(), session.Ref, []routing.LiveBinding{first, second}); !errors.Is(err, routing.ErrCapacity) {
		t.Fatalf("Revalidate(over capacity) = %v, want routing.ErrCapacity", err)
	}
	if err := manager.RequireRevalidated(session.Ref); !errors.Is(err, ErrSnapshotFirst) {
		t.Fatalf("RequireRevalidated(after rejected snapshot) = %v, want ErrSnapshotFirst", err)
	}
	if got := node.State(); len(got.Routes) != 0 {
		t.Fatalf("over-capacity snapshot installed routes: %#v", got.Routes)
	}
}

func TestReconnectSnapshotOrdersAfterInFlightSameInstanceMutation(t *testing.T) {
	manager, node := newManager(t)
	confirm(t, manager)
	old := openAndRevalidate(t, manager, "owner", "owner-1", nil)
	binding := testBinding("owner", "owner-1", "listener-old")
	entered, release := node.blockNextApply()
	declareDone := make(chan error, 1)
	go func() {
		_, err := manager.Declare(context.Background(), old, binding)
		declareDone <- err
	}()
	<-entered

	reconnected := make(chan controlmodel.Session, 1)
	reconnectErr := make(chan error, 1)
	go func() {
		session, err := manager.OpenSession(context.Background(), "owner", "owner-1", "127.0.0.1:9000")
		if err != nil {
			reconnectErr <- err
			return
		}
		reconnected <- session
	}()
	select {
	case session := <-reconnected:
		t.Fatalf("reconnect overtook in-flight mutation: %#v", session.Ref)
	case err := <-reconnectErr:
		t.Fatalf("OpenSession() before release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-declareDone; err != nil {
		t.Fatalf("Declare(): %v", err)
	}
	var session controlmodel.Session
	select {
	case session = <-reconnected:
	case err := <-reconnectErr:
		t.Fatalf("OpenSession(): %v", err)
	case <-time.After(time.Second):
		t.Fatal("OpenSession() remained blocked")
	}
	if err := manager.Revalidate(context.Background(), session.Ref, nil); err != nil {
		t.Fatalf("Revalidate(empty current snapshot): %v", err)
	}
	if _, ok := node.LookupRoute(controlstate.BindingKey(binding.Key)); ok {
		t.Fatal("old mutation survived the reconnect's later empty snapshot")
	}
}

func TestEndSessionDropsVWhileMutationApplyIsInFlight(t *testing.T) {
	manager, node := newManager(t)
	confirm(t, manager)
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	ownerSession, err := manager.OpenSession(context.Background(), "owner", "owner-1", "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("OpenSession(owner): %v", err)
	}
	if err := manager.Revalidate(context.Background(), ownerSession.Ref, nil); err != nil {
		t.Fatalf("Revalidate(owner): %v", err)
	}

	binding := testBinding("owner", "owner-1", "listener-1")
	entered, release := node.blockNextApply()
	declareDone := make(chan error, 1)
	go func() {
		_, err := manager.Declare(context.Background(), ownerSession.Ref, binding)
		declareDone <- err
	}()
	<-entered

	manager.EndSession(ownerSession.Ref)
	select {
	case <-ownerSession.Done:
	default:
		t.Fatal("EndSession did not fence V while the Raft apply was blocked")
	}
	if _, err := manager.AdmitOpen(context.Background(), ingress, testAuth(), "/events", "worker"); !errors.Is(err, routing.ErrRouteNotFound) {
		t.Fatalf("AdmitOpen(after V fence) = %v, want routing.ErrRouteNotFound", err)
	}

	close(release)
	if err := <-declareDone; !errors.Is(err, ErrStaleSession) {
		t.Fatalf("Declare(after session end) = %v, want ErrStaleSession", err)
	}
	if got := manager.Presence(); got.RevalidatedGateways != 1 || got.EligibleRoutes != 0 {
		t.Fatalf("presence after late commit = %#v, want ingress-only V and no eligible route", got)
	}
}

func TestAuthorityChangeDuringMutationCannotRestoreStaleV(t *testing.T) {
	manager, node := newManager(t)
	confirm(t, manager)
	owner := openAndRevalidate(t, manager, "owner", "owner-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	entered, release := node.blockNextApply()
	declareDone := make(chan error, 1)
	go func() {
		_, err := manager.Declare(context.Background(), owner, binding)
		declareDone <- err
	}()
	<-entered

	node.setTerm(2)
	confirm(t, manager)
	if got := manager.Presence(); got.RevalidatedGateways != 0 || got.EligibleRoutes != 0 {
		t.Fatalf("presence after authority change = %#v, want empty V", got)
	}

	close(release)
	if err := <-declareDone; !errors.Is(err, ErrStaleSession) {
		t.Fatalf("Declare(after authority change) = %v, want ErrStaleSession", err)
	}
	got := manager.Presence()
	if got.CommittedRoutes != 1 || got.RevalidatedGateways != 0 || got.EligibleRoutes != 0 {
		t.Fatalf("presence after late commit = %#v, want durable C but empty V", got)
	}
}
