package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
)

func TestDirectorySnapshotConflictIsAtomicAndExactDuplicateIsIdempotent(t *testing.T) {
	manager, _ := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	if already, err := manager.Declare(owner, binding); err != nil || !already {
		t.Fatalf("Declare(exact duplicate) = already=%v, err=%v", already, err)
	}
	other, err := manager.OpenSession("other", "other-1", "127.0.0.1:9003")
	if err != nil {
		t.Fatalf("OpenSession(other): %v", err)
	}
	if err := manager.Revalidate(other.Ref, []routing.LiveBinding{binding}); !errors.Is(err, routing.ErrConflict) {
		t.Fatalf("Revalidate(conflict) = %v, want routing.ErrConflict", err)
	}
	if err := manager.RequireRevalidated(other.Ref); !errors.Is(err, ErrSnapshotFirst) {
		t.Fatalf("conflicting snapshot revalidated session: %v", err)
	}
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); err != nil {
		t.Fatalf("ResolveOpen() after rejected snapshot: %v", err)
	}
}

func TestEndAndStaleWithdrawCannotDeleteNewRoute(t *testing.T) {
	manager, _ := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	old := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	manager.EndSession(old)
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("ResolveOpen() after end = %v, want ErrRouteNotFound", err)
	}
	newBinding := testBinding("owner", "owner-2", "listener-2")
	current := openAndRevalidate(t, manager, "owner", "owner-2", []routing.LiveBinding{newBinding})
	if already, err := manager.Withdraw(old, binding); err != nil || !already {
		t.Fatalf("Withdraw(stale) = already=%v err=%v", already, err)
	}
	open, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker")
	if err != nil {
		t.Fatalf("ResolveOpen() after stale withdraw: %v", err)
	}
	if open.Binding != newBinding || open.OwnerControlSessionID != current.ControlSessionID {
		t.Fatalf("OpenContext = %#v, want current route/session", open)
	}
}

func TestSessionReplacementBulkDeletesAndStaleSnapshotCannotRepopulate(t *testing.T) {
	manager, _ := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	old := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	replacement, err := manager.OpenSession("owner", "owner-2", "127.0.0.1:9001")
	if err != nil {
		t.Fatalf("OpenSession(replacement): %v", err)
	}
	if err := manager.Revalidate(old, []routing.LiveBinding{binding}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("Revalidate(old) = %v, want ErrStaleSession", err)
	}
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("route survived session replacement: %v", err)
	}
	currentBinding := testBinding("owner", "owner-2", "listener-2")
	if already, err := manager.Declare(old, binding); !errors.Is(err, ErrStaleSession) || already {
		t.Fatalf("Declare(stale) = already=%v err=%v, want stale rejection", already, err)
	}
	if presence := manager.Presence(); presence.Bindings != 0 {
		t.Fatalf("stale declaration repopulated directory: %#v", presence)
	}
	if err := manager.Revalidate(replacement.Ref, []routing.LiveBinding{currentBinding}); err != nil {
		t.Fatalf("Revalidate(replacement): %v", err)
	}
	if open, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); err != nil || open.Binding != currentBinding {
		t.Fatalf("partial redeclare ResolveOpen() = %#v, %v", open, err)
	}
}

func TestDirectoryCardinalityFollowsCurrentLiveChurn(t *testing.T) {
	manager, _ := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	owner := openAndRevalidate(t, manager, "owner", "owner-1", nil)
	for index := 0; index < 10*routing.MaxListenerBindingsPerGateway; index++ {
		binding := routing.LiveBinding{
			Key: routing.BindingKey{ClientID: "client-1", EndpointPattern: fmt.Sprintf("/churn/%05d", index), TargetID: "worker"},
			Ref: routing.ListenerBindingRef{GatewayID: "owner", GatewayInstanceID: "owner-1", ListenerBindingID: fmt.Sprintf("listener-%05d", index)},
		}
		if already, err := manager.Declare(owner, binding); err != nil || already {
			t.Fatalf("Declare(%d) = already=%v err=%v", index, already, err)
		}
		if presence := manager.Presence(); presence.Bindings != 1 {
			t.Fatalf("presence after Declare(%d) = %#v", index, presence)
		}
		if already, err := manager.Withdraw(owner, binding); err != nil || already {
			t.Fatalf("Withdraw(%d) = already=%v err=%v", index, already, err)
		}
		if presence := manager.Presence(); presence.Bindings != 0 {
			t.Fatalf("historical keys accumulated after Withdraw(%d): %#v", index, presence)
		}
	}
}

func TestSnapshotCapacityDoesNotPartiallyPublish(t *testing.T) {
	manager, _ := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	session, err := manager.OpenSession("owner", "owner-1", "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("OpenSession(): %v", err)
	}
	bindings := make([]routing.LiveBinding, routing.MaxListenerBindingsPerGateway+1)
	for i := range bindings {
		bindings[i] = routing.LiveBinding{
			Key: routing.BindingKey{ClientID: "client-1", EndpointPattern: fmt.Sprintf("/%03d", i), TargetID: "worker"},
			Ref: routing.ListenerBindingRef{GatewayID: "owner", GatewayInstanceID: "owner-1", ListenerBindingID: fmt.Sprintf("listener-%03d", i)},
		}
	}
	if err := manager.Revalidate(session.Ref, bindings); !errors.Is(err, routing.ErrCapacity) {
		t.Fatalf("Revalidate(over capacity) = %v, want routing.ErrCapacity", err)
	}
	if presence := manager.Presence(); presence.Bindings != 0 || presence.Revalidated != 0 {
		t.Fatalf("over-capacity snapshot partially published: %#v", presence)
	}
}

func TestX01AuthorityChangeRejectsStaleSessionAndRoutesPartialRedeclaration(t *testing.T) {
	manager, raft := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{testBinding("owner", "owner-1", "listener-1")})
	first, ok := manager.Current()
	if !ok {
		t.Fatal("Current() is absent")
	}
	raft.setTerm(2)
	second, err := manager.Confirm(context.Background())
	if err != nil {
		t.Fatalf("Confirm(new term): %v", err)
	}
	if second.AuthorityID == first.AuthorityID {
		t.Fatal("authority ID was reused across term")
	}
	if err := manager.RequireRevalidated(owner); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old owner survived authority fence: %v", err)
	}
	if presence := manager.Presence(); presence.State != PresenceCurrent || presence.Sessions != 0 || presence.Bindings != 0 {
		t.Fatalf("presence after term fence = %#v", presence)
	}
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrOpenUnavailable) {
		t.Fatalf("ResolveOpen(stale ingress) = %v, want ErrOpenUnavailable", err)
	}
	newIngress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	if _, err := manager.ResolveOpen(newIngress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("ResolveOpen() before owner redeclare = %v, want ErrRouteNotFound", err)
	}
	openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{testBinding("owner", "owner-1", "listener-1")})
	if _, err := manager.ResolveOpen(newIngress, testAuth(), "/events", "worker"); err != nil {
		t.Fatalf("ResolveOpen() after fresh snapshot: %v", err)
	}
}

func TestX02SessionEndAfterUnknownDeclareRedeclaresCurrentSnapshotOnly(t *testing.T) {
	manager, _ := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	oldBinding := testBinding("owner", "owner-1", "listener-unknown-ack")
	old := openAndRevalidate(t, manager, "owner", "owner-1", nil)
	if already, err := manager.Declare(old, oldBinding); err != nil || already {
		t.Fatalf("Declare(before simulated ACK loss) = already=%v err=%v", already, err)
	}

	// The Gateway cannot distinguish an applied Declare from a lost ACK. Ending
	// the exact control session is the recovery boundary: the authority deletes
	// its possible effect instead of retaining an outcome for replay.
	manager.EndSession(old)
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("ResolveOpen() after unknown Declare/session end = %v, want ErrRouteNotFound", err)
	}

	newBinding := routing.LiveBinding{
		Key: routing.BindingKey{ClientID: "client-1", EndpointPattern: "/current", TargetID: "worker"},
		Ref: routing.ListenerBindingRef{GatewayID: "owner", GatewayInstanceID: "owner-2", ListenerBindingID: "listener-current"},
	}
	current := openAndRevalidate(t, manager, "owner", "owner-2", []routing.LiveBinding{newBinding})
	if already, err := manager.Declare(old, oldBinding); !errors.Is(err, ErrStaleSession) || already {
		t.Fatalf("late old-session Declare = already=%v err=%v, want stale rejection", already, err)
	}
	if presence := manager.Presence(); presence.Bindings != 1 || presence.Revalidated != 2 {
		t.Fatalf("presence after current snapshot = %#v, want only ingress + current owner", presence)
	}
	open, err := manager.ResolveOpen(ingress, testAuth(), "/current", "worker")
	if err != nil || open.Binding != newBinding || open.OwnerControlSessionID != current.ControlSessionID {
		t.Fatalf("ResolveOpen(current snapshot) = %#v, %v", open, err)
	}
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("old binding was replayed by reconnect: %v", err)
	}
}

func TestCallerVerificationCancellationDoesNotFenceCurrentAuthority(t *testing.T) {
	manager, raft := newManager(t)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	wantAuthority, ok := manager.Current()
	if !ok {
		t.Fatal("Current() is absent before canceled verification")
	}
	wantPresence := manager.Presence()

	for _, verificationErr := range []error{context.Canceled, context.DeadlineExceeded} {
		raft.setError(verificationErr)
		if _, err := manager.Confirm(context.Background()); !errors.Is(err, ErrNoAuthority) {
			t.Fatalf("Confirm(%v) = %v, want ErrNoAuthority", verificationErr, err)
		}
		if got, current := manager.Current(); !current || got != wantAuthority {
			t.Fatalf("caller cancellation fenced authority: current=%v ref=%#v want=%#v", current, got, wantAuthority)
		}
		if got := manager.Presence(); got != wantPresence {
			t.Fatalf("caller cancellation changed presence: got=%#v want=%#v", got, wantPresence)
		}
		if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); err != nil {
			t.Fatalf("ResolveOpen() after caller cancellation: %v", err)
		}
	}

	raft.setError(nil)
	if got, err := manager.Confirm(context.Background()); err != nil || got != wantAuthority {
		t.Fatalf("Confirm() after cancellation = %#v, %v, want unchanged %#v", got, err, wantAuthority)
	}
}

func TestDefinitiveLeadershipLossFencesAuthorityAndDirectory(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*fakeRaftNode)
	}{
		{name: "follower", mutate: func(raft *fakeRaftNode) { raft.setRole("Follower") }},
		{name: "verification failure", mutate: func(raft *fakeRaftNode) { raft.setError(errors.New("quorum verification failed")) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, raft := newManager(t)
			if _, err := manager.Confirm(context.Background()); err != nil {
				t.Fatalf("Confirm(): %v", err)
			}
			openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{testBinding("owner", "owner-1", "listener-1")})
			test.mutate(raft)
			if _, err := manager.Confirm(context.Background()); !errors.Is(err, ErrNoAuthority) {
				t.Fatalf("Confirm() = %v, want ErrNoAuthority", err)
			}
			if _, current := manager.Current(); current {
				t.Fatal("definitive leadership loss left a current authority")
			}
			if got := manager.Presence(); got.State != PresenceNoAuthority || got.Sessions != 0 || got.Revalidated != 0 || got.Bindings != 0 {
				t.Fatalf("presence after fence = %#v, want empty NoAuthority", got)
			}
		})
	}
}

func newManager(t *testing.T) (*Manager, *fakeRaftNode) {
	t.Helper()
	raft := &fakeRaftNode{status: raftnode.Status{Role: "Leader", Term: 1}, epoch: "epoch-1"}
	manager, err := New(Config{ClusterEpoch: "epoch-1", ProbeInterval: time.Hour, ProbeTimeout: time.Second, OpenContextTTL: time.Minute}, raft)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)
	return manager, raft
}

func openAndRevalidate(t *testing.T, manager *Manager, gatewayID, instanceID string, bindings []routing.LiveBinding) SessionRef {
	t.Helper()
	session, err := manager.OpenSession(gatewayID, instanceID, "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("OpenSession(%s): %v", gatewayID, err)
	}
	if err := manager.Revalidate(session.Ref, bindings); err != nil {
		t.Fatalf("Revalidate(%s): %v", gatewayID, err)
	}
	return session.Ref
}

func testBinding(gatewayID, instanceID, listenerID string) routing.LiveBinding {
	return routing.LiveBinding{Key: routing.BindingKey{ClientID: "client-1", EndpointPattern: "/events", TargetID: "worker"}, Ref: routing.ListenerBindingRef{GatewayID: gatewayID, GatewayInstanceID: instanceID, ListenerBindingID: listenerID}}
}

func testAuth() AuthContext {
	return AuthContext{ClientSessionID: "caller", ClientID: "client-1", APIKeyID: "caller-key", AuthRevision: "revision-1"}
}

type fakeRaftNode struct {
	mu     sync.Mutex
	status raftnode.Status
	epoch  string
	err    error
}

func (n *fakeRaftNode) Status() raftnode.Status { n.mu.Lock(); defer n.mu.Unlock(); return n.status }
func (n *fakeRaftNode) ClusterEpoch() string    { n.mu.Lock(); defer n.mu.Unlock(); return n.epoch }
func (n *fakeRaftNode) VerifyLeader(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.err
}
func (n *fakeRaftNode) setTerm(term uint64) { n.mu.Lock(); n.status.Term = term; n.mu.Unlock() }
func (n *fakeRaftNode) setRole(role string) { n.mu.Lock(); n.status.Role = role; n.mu.Unlock() }
func (n *fakeRaftNode) setError(err error)  { n.mu.Lock(); n.err = err; n.mu.Unlock() }
