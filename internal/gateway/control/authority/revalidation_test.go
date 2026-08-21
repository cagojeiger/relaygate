package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

func TestEndSessionRetainsCAndReconnectCancelsGraceCleanup(t *testing.T) {
	manager, node := newManager(t)
	now := time.Unix(1_000, 0)
	manager.now = func() time.Time { return now }
	confirm(t, manager)
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	manager.EndSession(owner)
	if got := node.State(); len(got.Routes) != 1 {
		t.Fatalf("EndSession deleted committed route: %#v", got)
	}
	if _, err := manager.AdmitOpen(context.Background(), ingress, testAuth(), "/events", "worker"); !errors.Is(err, routing.ErrRouteNotFound) {
		t.Fatalf("AdmitOpen(with V absent) = %v, want route unavailable", err)
	}

	openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	now = now.Add(2 * DefaultGatewayRevalidationTimeout)
	manager.sweep(context.Background())
	if got := node.State(); len(got.Routes) != 1 || len(got.Gateways) != 2 {
		t.Fatalf("same-instance reconnect did not cancel cleanup: %#v", got)
	}
}

func TestGraceCleanupDeletesOnlyUnrevalidatedCurrentGateway(t *testing.T) {
	manager, node := newManager(t)
	now := time.Unix(1_000, 0)
	manager.now = func() time.Time { return now }
	confirm(t, manager)
	openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	manager.EndSession(owner)
	now = now.Add(DefaultGatewayRevalidationTimeout)
	manager.sweep(context.Background())
	if _, ok := node.LookupGateway("owner"); ok {
		t.Fatal("expired owner gateway survived cleanup")
	}
	if _, ok := node.LookupRoute(controlstate.BindingKey{ClientID: "client-1", EndpointPattern: "/events", TargetID: "worker"}); ok {
		t.Fatal("expired owner route survived cleanup")
	}
	if got := manager.Presence(); got.CommittedGateways != 1 || got.CommittedRoutes != 0 {
		t.Fatalf("presence after cleanup = %#v", got)
	}
}

func TestNewLeaderCleansPersistedGatewayThatNeverRevalidates(t *testing.T) {
	manager, node := newManager(t)
	gateway := controlstate.GatewaySessionRef{GatewayID: "orphan", GatewayInstanceID: "orphan-1"}
	command, err := controlstate.EncodeRegisterGateway(controlstate.RegisterGateway{ClusterEpoch: "epoch-1", Gateway: gateway})
	if err != nil {
		t.Fatalf("EncodeRegisterGateway(): %v", err)
	}
	if result, err := node.Apply(context.Background(), command); err != nil || !result.Applied() {
		t.Fatalf("seed persisted gateway = %#v, %v", result, err)
	}
	now := time.Unix(1_000, 0)
	manager.now = func() time.Time { return now }
	confirm(t, manager) // schedules every durable C gateway, including orphan.
	now = now.Add(DefaultGatewayRevalidationTimeout)
	manager.sweep(context.Background())
	if _, ok := node.LookupGateway("orphan"); ok {
		t.Fatal("unrevalidated persisted gateway survived new-leader grace cleanup")
	}
}

func TestSyncingSessionExpiresWithoutFullSnapshot(t *testing.T) {
	manager, node := newManager(t)
	now := time.Unix(1_000, 0)
	manager.now = func() time.Time { return now }
	confirm(t, manager)
	session, err := manager.OpenSession(context.Background(), "syncing", "syncing-1", "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("OpenSession(): %v", err)
	}
	now = now.Add(DefaultGatewayRevalidationTimeout)
	manager.sweep(context.Background())
	if _, ok := node.LookupGateway("syncing"); ok {
		t.Fatal("syncing gateway survived revalidation timeout")
	}
	select {
	case <-session.Done:
	default:
		t.Fatal("expired syncing control session was not fenced")
	}
	if err := manager.Revalidate(context.Background(), session.Ref, nil); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("Revalidate(expired syncing session) = %v, want ErrStaleSession", err)
	}
}

func TestStaleGraceCleanupCannotDeleteReplacementInstance(t *testing.T) {
	manager, node := newManager(t)
	now := time.Unix(1_000, 0)
	manager.now = func() time.Time { return now }
	confirm(t, manager)
	oldBinding := testBinding("owner", "owner-1", "listener-1")
	old := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{oldBinding})
	manager.EndSession(old)
	newBinding := testBinding("owner", "owner-2", "listener-2")
	openAndRevalidate(t, manager, "owner", "owner-2", []routing.LiveBinding{newBinding})

	now = now.Add(DefaultGatewayRevalidationTimeout)
	manager.sweep(context.Background())
	current, ok := node.LookupGateway("owner")
	if !ok || current.GatewayInstanceID != "owner-2" {
		t.Fatalf("stale cleanup changed replacement: %#v, %v", current, ok)
	}
	route, ok := node.LookupRoute(controlstate.BindingKey{ClientID: "client-1", EndpointPattern: "/events", TargetID: "worker"})
	if !ok || route.ListenerBindingID != "listener-2" {
		t.Fatalf("stale cleanup changed replacement route: %#v, %v", route, ok)
	}
}
