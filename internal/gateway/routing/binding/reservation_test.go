package localbinding

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

func TestReserveFencesOldOwnerControlSession(t *testing.T) {
	sessions := newTestSessions()
	listener := testSession("listener", "client-1", "listener-key")
	caller := testSession("caller", "client-1", "caller-key")
	sessions.allow(listener.Ref)
	sessions.allow(caller.Ref)
	committer := &testCommitter{current: testControlSession()}
	manager := mustManager(t, 1, committer, sessions)
	binding, err := manager.Bind(context.Background(), listener, "/events", "worker", testEndpoint{})
	if err != nil {
		t.Fatalf("Bind(): %v", err)
	}
	open := mustOpenContext(t, "attempt-1", caller.Ref, binding, "old-session")
	if _, err := manager.Reserve(open, caller.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Reserve(old owner session) = %v, want ErrNotFound", err)
	}
	if !open.TryConsume() {
		t.Fatal("failed owner-session fence consumed attempt")
	}
	open = mustOpenContext(t, "attempt-2", caller.Ref, binding, testControlSession().ControlSessionID)
	reservation, err := manager.Reserve(open, caller.Ref)
	if err != nil {
		t.Fatalf("Reserve(current owner session): %v", err)
	}
	if reservation.Binding != binding || reservation.Listener != listener.Ref {
		t.Fatalf("reservation = %#v", reservation)
	}
	committer.mu.Lock()
	committer.current.ControlSessionID = "new-session"
	committer.mu.Unlock()
	if _, err := manager.Reserve(mustOpenContext(t, "attempt-3", caller.Ref, binding, "owner-session"), caller.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Reserve(after replacement) = %v, want ErrNotFound", err)
	}
}

func mustOpenContext(t *testing.T, attemptID string, caller clientsession.Ref, binding routing.LiveBinding, ownerSession string) authority.OpenContext {
	t.Helper()
	open, err := authority.NewForwardedOpenContext(
		"epoch-1", "authority-1", attemptID,
		authority.AuthContext{ClientSessionID: caller.ClientSessionID, ClientID: caller.ClientID, APIKeyID: caller.APIKeyID, AuthRevision: caller.AuthRevision},
		binding,
		authority.ForwardingContext{
			IngressGatewayID: "ingress", IngressGatewayInstanceID: "ingress-1", IngressControlSessionID: "ingress-session",
			OwnerControlSessionID: ownerSession, OwnerRelayAddress: "127.0.0.1:9000", ExpiresAt: time.Now().Add(time.Minute),
		},
	)
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	return open
}
