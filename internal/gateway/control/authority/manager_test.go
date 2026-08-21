package authority

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
	"github.com/hashicorp/raft"
)

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

func confirm(t *testing.T, manager *Manager) controlmodel.AuthorityRef {
	t.Helper()
	ref, err := manager.Confirm(context.Background())
	if err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	return ref
}

func openAndRevalidate(t *testing.T, manager *Manager, gatewayID, instanceID string, bindings []routing.LiveBinding) controlmodel.SessionRef {
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

func testAuth() routing.AuthContext {
	return routing.AuthContext{ClientSessionID: "caller", ClientID: "client-1", APIKeyID: "caller-key", AuthRevision: "revision-1"}
}

type fakeRaftNode struct {
	mu          sync.Mutex
	status      raftnode.Status
	err         error
	fsm         *controlstate.FSM
	beforeApply func()
	stateCalls  int
	verifyCalls int
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
	n.verifyCalls++
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
func (n *fakeRaftNode) State() controlstate.State {
	n.mu.Lock()
	n.stateCalls++
	n.mu.Unlock()
	return n.fsm.State()
}
func (n *fakeRaftNode) LookupGateway(id string) (controlstate.GatewaySessionRef, bool) {
	return n.fsm.LookupGateway(id)
}
func (n *fakeRaftNode) LookupRoute(key controlstate.BindingKey) (controlstate.Route, bool) {
	return n.fsm.LookupRoute(key)
}
func (n *fakeRaftNode) setTerm(term uint64) { n.mu.Lock(); n.status.Term = term; n.mu.Unlock() }

func (n *fakeRaftNode) stateCallCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stateCalls
}

func (n *fakeRaftNode) verifyCallCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.verifyCalls
}

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
