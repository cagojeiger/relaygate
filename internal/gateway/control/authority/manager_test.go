package authority

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
	"github.com/hashicorp/raft"
)

func TestAuthorityFailoverRetainsCommittedDirectoryButDropsV(t *testing.T) {
	manager, node := newManager(t)
	confirm(t, manager)
	ingress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	binding := testBinding("owner", "owner-1", "listener-1")
	owner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); err != nil {
		t.Fatalf("ResolveOpen(before failover): %v", err)
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
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrOpenUnavailable) {
		t.Fatalf("ResolveOpen(stale ingress) = %v, want unavailable", err)
	}

	newIngress := openAndRevalidate(t, manager, "ingress", "ingress-1", nil)
	newOwner := openAndRevalidate(t, manager, "owner", "owner-1", []routing.LiveBinding{binding})
	open, err := manager.ResolveOpen(newIngress, testAuth(), "/events", "worker")
	if err != nil {
		t.Fatalf("ResolveOpen(after revalidate): %v", err)
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
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("ResolveOpen(with V absent) = %v, want route unavailable", err)
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
	open, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker")
	if err != nil || open.OwnerControlSessionID != owner.ControlSessionID {
		t.Fatalf("ResolveOpen(after rejected snapshot) = %#v, %v", open, err)
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

	reconnected := make(chan Session, 1)
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
	var session Session
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
	if _, err := manager.ResolveOpen(ingress, testAuth(), "/events", "worker"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("ResolveOpen(after V fence) = %v, want ErrRouteNotFound", err)
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

func newManager(t *testing.T) (*Manager, *fakeRaftNode) {
	return newManagerWithMaxBindings(t, routing.MaxListenerBindingsPerGateway)
}

func newManagerWithMaxBindings(t *testing.T, maxBindings uint32) (*Manager, *fakeRaftNode) {
	t.Helper()
	node := newFakeRaftNode(t, "epoch-1", maxBindings)
	manager, err := New(Config{
		ClusterEpoch:               "epoch-1",
		ProbeInterval:              time.Hour,
		ProbeTimeout:               time.Second,
		GatewayRevalidationTimeout: DefaultGatewayRevalidationTimeout,
		OpenContextTTL:             time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)
	return manager, node
}

func confirm(t *testing.T, manager *Manager) Ref {
	t.Helper()
	ref, err := manager.Confirm(context.Background())
	if err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	return ref
}

func openAndRevalidate(t *testing.T, manager *Manager, gatewayID, instanceID string, bindings []routing.LiveBinding) SessionRef {
	t.Helper()
	session, err := manager.OpenSession(context.Background(), gatewayID, instanceID, "127.0.0.1:9000")
	if err != nil {
		t.Fatalf("OpenSession(%s): %v", gatewayID, err)
	}
	if err := manager.Revalidate(context.Background(), session.Ref, bindings); err != nil {
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
	mu          sync.Mutex
	status      raftnode.Status
	err         error
	fsm         *controlstate.FSM
	beforeApply func()
}

func newFakeRaftNode(t *testing.T, epoch string, maxBindings uint32) *fakeRaftNode {
	t.Helper()
	fsm := controlstate.NewFSM()
	command, err := controlstate.EncodeInitializeCluster(controlstate.InitializeCluster{
		ClusterEpoch: epoch, MaxGatewaySessions: 100, MaxRoutes: 100 * maxBindings, MaxBindingsPerGateway: maxBindings,
	})
	if err != nil {
		t.Fatalf("EncodeInitializeCluster(): %v", err)
	}
	result := fsm.Apply(&raft.Log{Data: command}).(controlstate.ApplyResult)
	if !result.Applied() {
		t.Fatalf("initialize FSM = %#v", result)
	}
	return &fakeRaftNode{status: raftnode.Status{Role: "Leader", Term: 1}, fsm: fsm}
}

func (n *fakeRaftNode) Status() raftnode.Status { n.mu.Lock(); defer n.mu.Unlock(); return n.status }
func (n *fakeRaftNode) ClusterEpoch() string    { return n.fsm.ClusterEpoch() }
func (n *fakeRaftNode) VerifyLeader(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.err
}
func (n *fakeRaftNode) Apply(_ context.Context, command []byte) (controlstate.ApplyResult, error) {
	n.mu.Lock()
	beforeApply := n.beforeApply
	n.beforeApply = nil
	n.mu.Unlock()
	if beforeApply != nil {
		beforeApply()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return controlstate.ApplyResult{}, n.err
	}
	if n.status.Role != "Leader" {
		return controlstate.ApplyResult{}, errors.New("not leader")
	}
	return n.fsm.Apply(&raft.Log{Data: command}).(controlstate.ApplyResult), nil
}
func (n *fakeRaftNode) State() controlstate.State { return n.fsm.State() }
func (n *fakeRaftNode) LookupGateway(id string) (controlstate.GatewaySessionRef, bool) {
	return n.fsm.LookupGateway(id)
}
func (n *fakeRaftNode) LookupRoute(key controlstate.BindingKey) (controlstate.Route, bool) {
	return n.fsm.LookupRoute(key)
}
func (n *fakeRaftNode) setTerm(term uint64) { n.mu.Lock(); n.status.Term = term; n.mu.Unlock() }

func (n *fakeRaftNode) blockNextApply() (<-chan struct{}, chan<- struct{}) {
	entered := make(chan struct{})
	release := make(chan struct{})
	n.mu.Lock()
	n.beforeApply = func() {
		close(entered)
		<-release
	}
	n.mu.Unlock()
	return entered, release
}
