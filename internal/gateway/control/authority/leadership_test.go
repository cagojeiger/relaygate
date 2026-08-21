package authority

import (
	"context"
	"errors"
	"testing"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

func TestAuthorityFailoverRetainsCommittedDirectoryButDropsV(t *testing.T) {
	manager, node := newManager(t)
	confirm(t, manager)
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	if _, err := manager.AdmitOpen(context.Background(), ingress, testAuth(), "/events", "worker"); err != nil {
		t.Fatalf("AdmitOpen(before failover): %v", err)
	}

	node.setTerm(2)
	confirm(t, manager)
	if got := node.State(); len(got.Gateways) != 2 || len(got.Routes) != 1 {
		t.Fatalf("failover changed committed C: %#v", got)
	}
	presence := manager.Presence()
	if presence.CommittedGateways != 2 || presence.CommittedRoutes != 1 || presence.RevalidatedGateways != 0 || presence.EligibleRoutes != 0 {
		t.Fatalf("presence after failover = %#v, want C retained and V cleared", presence)
	}
	if _, err := manager.AdmitOpen(context.Background(), ingress, testAuth(), "/events", "worker"); !errors.Is(err, routing.ErrOpenUnavailable) {
		t.Fatalf("AdmitOpen(stale ingress) = %v, want unavailable", err)
	}

	newIngress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	newOwner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	open, err := manager.AdmitOpen(context.Background(), newIngress, testAuth(), "/events", "worker")
	if err != nil {
		t.Fatalf("AdmitOpen(after revalidate): %v", err)
	}
	if open.OwnerControlSessionID != newOwner.ControlSessionID || open.OwnerControlSessionID == owner.ControlSessionID {
		t.Fatalf("owner control identity = %q, want newly revalidated owner", open.OwnerControlSessionID)
	}
}

func TestStaleConfirmationCannotReplaceNewerAuthorityTerm(t *testing.T) {
	manager, node := newManager(t)
	node.setTerm(2)
	want := confirm(t, manager)

	// Raft terms never decrease. This models an older concurrent confirmation
	// reaching the authority lock after the term-2 confirmation completed.
	node.setTerm(1)
	if _, err := manager.Confirm(context.Background()); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("Confirm(stale term) = %v, want ErrNoAuthority", err)
	}
	if got, ok := manager.Current(); !ok || got != want {
		t.Fatalf("current authority = %#v, %v; want unchanged %#v", got, ok, want)
	}
}

func TestAdmissionRejectsChangedAuthorityRef(t *testing.T) {
	manager, node := newManager(t)
	stale := confirm(t, manager)
	openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})

	node.setTerm(2)
	current := confirm(t, manager)
	if current == stale {
		t.Fatalf("authority ref did not change across terms: %#v", current)
	}
	newIngress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	key, err := routing.ExactBindingKey(testAuth(), "/events", "worker")
	if err != nil {
		t.Fatalf("routing.ExactBindingKey(): %v", err)
	}

	if _, err := manager.resolveOpen(stale, newIngress, testAuth(), key); !errors.Is(err, routing.ErrOpenUnavailable) {
		t.Fatalf("resolveOpen(stale authority ref) = %v, want routing.ErrOpenUnavailable", err)
	}
}

func TestSteadyStateConfirmAndAdmitOpenDoNotCopyFullState(t *testing.T) {
	manager, node := newManager(t)
	confirm(t, manager)
	if got := node.stateCallCount(); got != 1 {
		t.Fatalf("initial authority State() calls = %d, want 1", got)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	baseline := node.stateCallCount()
	baselineVerifies := node.verifyCallCount()

	confirm(t, manager)
	if got := node.verifyCallCount(); got != baselineVerifies+1 {
		t.Fatalf("steady-state Confirm VerifyLeader calls = %d, want %d", got, baselineVerifies+1)
	}
	if _, err := manager.AdmitOpen(context.Background(), ingress, testAuth(), "/events", "worker"); err != nil {
		t.Fatalf("AdmitOpen(): %v", err)
	}
	if got := node.verifyCallCount(); got != baselineVerifies+2 {
		t.Fatalf("AdmitOpen VerifyLeader calls = %d, want exactly one additional call", got)
	}
	if got := node.stateCallCount(); got != baseline {
		t.Fatalf("steady-state Confirm/AdmitOpen copied full State: calls %d -> %d", baseline, got)
	}

	node.setTerm(2)
	confirm(t, manager)
	if got := node.stateCallCount(); got != baseline+1 {
		t.Fatalf("new authority State() calls = %d, want %d", got, baseline+1)
	}
}
