package localbinding

import (
	"context"
	"errors"
	"testing"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
)

func TestReserveOrdersExactlyWithUnbind(t *testing.T) {
	sessions := newFakeSessions()
	listenerDone := make(chan struct{})
	listener := sessionWithDone("listener", "client-1", "listener-key", listenerDone)
	caller := newSession("caller", "client-1", "caller-key")
	sessions.allow(listener.Ref)
	sessions.allow(caller.Ref)
	manager := mustManager(t, 1, &fakeCommitter{install: exactInstall}, sessions)
	defer manager.Close()

	slot, err := manager.Bind(context.Background(), listener, "/events", "worker", acceptingEndpoint{})
	if err != nil {
		t.Fatalf("Bind(): %v", err)
	}

	t.Run("unbind first rejects without consuming", func(t *testing.T) {
		open := mustOpenContext(t, "attempt-before", caller.Ref, slot)
		if err := manager.Unbind(listener.Ref, slot.Ref.ListenerBindingID); err != nil {
			t.Fatalf("Unbind(): %v", err)
		}
		if _, err := manager.Reserve(open, caller.Ref); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Reserve() = %v, want ErrNotFound", err)
		}
		if !open.TryConsume() {
			t.Fatal("failed exact-live guard consumed the capability")
		}
	})
}

func TestReserveBeforeUnbindKeepsImmutableEndpointReservation(t *testing.T) {
	sessions := newFakeSessions()
	listenerDone := make(chan struct{})
	listener := sessionWithDone("listener", "client-1", "listener-key", listenerDone)
	caller := newSession("caller", "client-1", "caller-key")
	sessions.allow(listener.Ref)
	sessions.allow(caller.Ref)
	manager := mustManager(t, 1, &fakeCommitter{install: exactInstall}, sessions)
	defer manager.Close()

	endpoint := &recordingEndpoint{}
	slot, err := manager.Bind(context.Background(), listener, "/events", "worker", endpoint)
	if err != nil {
		t.Fatalf("Bind(): %v", err)
	}
	open := mustOpenContext(t, "attempt-after", caller.Ref, slot)
	copy := open
	reservation, err := manager.Reserve(copy, caller.Ref)
	if err != nil {
		t.Fatalf("Reserve(): %v", err)
	}
	if reservation.Endpoint != endpoint || reservation.Listener != listener.Ref || reservation.ListenerDone != listenerDone {
		t.Fatalf("reservation endpoint/session = %#v/%#v", reservation.Endpoint, reservation.Listener)
	}

	if err := manager.Unbind(listener.Ref, slot.Ref.ListenerBindingID); err != nil {
		t.Fatalf("Unbind(): %v", err)
	}
	select {
	case <-reservation.ListenerDone:
		t.Fatal("explicit Unbind closed the listener session reservation")
	default:
	}
	if reservation.Binding.Ref == nil || *reservation.Binding.Ref != *slot.Ref {
		t.Fatalf("reservation binding = %#v, want %#v", reservation.Binding, slot)
	}
	if _, err := manager.Reserve(open, caller.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Reserve() after Unbind = %v, want ErrNotFound", err)
	}
	if open.TryConsume() {
		t.Fatal("successful Reserve capability was consumable again after retirement")
	}
}

func TestReserveFailedOwnerGuardLeavesSharedTokenConsumableByCorrectOwner(t *testing.T) {
	sessions := newFakeSessions()
	listener := newSession("listener", "client-1", "listener-key")
	caller := newSession("caller", "client-1", "caller-key")
	sessions.allow(listener.Ref)
	sessions.allow(caller.Ref)
	correct := mustManager(t, 1, &fakeCommitter{install: exactInstall}, sessions)
	defer correct.Close()
	slot, err := correct.Bind(context.Background(), listener, "/events", "worker", acceptingEndpoint{})
	if err != nil {
		t.Fatalf("Bind(): %v", err)
	}
	foreign, err := New("gateway-2", "instance-2", 1, &fakeCommitter{install: exactInstall}, sessions)
	if err != nil {
		t.Fatalf("New(foreign): %v", err)
	}
	defer foreign.Close()

	open := mustOpenContext(t, "attempt-owner", caller.Ref, slot)
	valueCopy := open
	if _, err := foreign.Reserve(valueCopy, caller.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign Reserve() = %v, want ErrNotFound", err)
	}
	if _, err := correct.Reserve(open, caller.Ref); err != nil {
		t.Fatalf("correct Reserve() after failed guard: %v", err)
	}
	if valueCopy.TryConsume() {
		t.Fatal("value copy did not share the consumed attempt token")
	}
}

func TestReserveRejectsStaleGenerationWithoutConsuming(t *testing.T) {
	sessions := newFakeSessions()
	listener := newSession("listener", "client-1", "listener-key")
	caller := newSession("caller", "client-1", "caller-key")
	sessions.allow(listener.Ref)
	sessions.allow(caller.Ref)
	manager := mustManager(t, 1, &fakeCommitter{install: exactInstall}, sessions)
	defer manager.Close()
	slot, err := manager.Bind(context.Background(), listener, "/events", "worker", acceptingEndpoint{})
	if err != nil {
		t.Fatalf("Bind(): %v", err)
	}

	open := mustOpenContext(t, "attempt-generation", caller.Ref, slot)
	stale := open.Clone()
	stale.Binding.Generation++
	if _, err := manager.Reserve(stale, caller.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Reserve(stale generation) = %v, want ErrNotFound", err)
	}
	if _, err := manager.Reserve(open, caller.Ref); err != nil {
		t.Fatalf("Reserve(exact generation) after failed guard: %v", err)
	}
	if open.TryConsume() {
		t.Fatal("successful exact reservation did not consume the shared token")
	}
}

func TestReserveRejectsSameCallerAndListenerWithoutConsuming(t *testing.T) {
	sessions := newFakeSessions()
	session := newSession("self", "client-1", "key-1")
	sessions.allow(session.Ref)
	manager := mustManager(t, 1, &fakeCommitter{install: exactInstall}, sessions)
	defer manager.Close()
	slot, err := manager.Bind(context.Background(), session, "/events", "worker", acceptingEndpoint{})
	if err != nil {
		t.Fatalf("Bind(): %v", err)
	}
	open := mustOpenContext(t, "attempt-self", session.Ref, slot)
	if _, err := manager.Reserve(open, session.Ref); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Reserve(self) = %v, want ErrInvalid", err)
	}
	if !open.TryConsume() {
		t.Fatal("self-deadlock guard consumed the capability")
	}
}

type recordingEndpoint struct{}

func (*recordingEndpoint) DeliverPayload(context.Context, PipePayload) error { return nil }
func (*recordingEndpoint) Offer(context.Context, Offer) error                { return nil }
func (*recordingEndpoint) Confirm(context.Context, Confirmation) error       { return nil }
func (*recordingEndpoint) Terminate(context.Context, Termination) error      { return nil }

func mustOpenContext(t *testing.T, attemptID string, caller clientsession.Ref, slot controlstate.BindingSlot) authority.OpenContext {
	t.Helper()
	open, err := authority.NewOpenContext(
		"epoch-1",
		"authority-1",
		attemptID,
		authority.AuthContext{
			ClientSessionID: caller.ClientSessionID,
			ClientID:        caller.ClientID,
			APIKeyID:        caller.APIKeyID,
			AuthRevision:    caller.AuthRevision,
		},
		slot,
	)
	if err != nil {
		t.Fatalf("NewOpenContext(): %v", err)
	}
	return open
}
