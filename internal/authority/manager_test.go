package authority

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/controlstate"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

func TestAuthorityFencesSessionsAndRebuildsPresence(t *testing.T) {
	node := &fakeRaftNode{
		status: raftnode.Status{Role: "Leader", Term: 1, ClusterEpoch: "epoch-1"},
		state: controlstate.State{
			ClusterEpoch: "epoch-1",
			Gateways: []controlstate.GatewaySlot{{
				GatewayID:  "gateway-1",
				Generation: 1,
				Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-1"},
			}},
		},
	}
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Second,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	firstAuthority, err := manager.Confirm(ctx)
	cancel()
	if err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	firstSession, err := manager.OpenSession(node.State().Gateways[0])
	if err != nil {
		t.Fatalf("OpenSession(): %v", err)
	}
	if presence := manager.Presence(); presence.State != PresenceRebuilding || presence.Revalidated != 0 {
		t.Fatalf("presence before snapshot = %#v", presence)
	}
	if err := manager.Revalidate(firstSession.Ref, nil); err != nil {
		t.Fatalf("Revalidate(): %v", err)
	}
	if presence := manager.Presence(); presence.State != PresenceComplete || presence.Revalidated != 1 {
		t.Fatalf("presence after snapshot = %#v", presence)
	}

	replacement, err := manager.OpenSession(node.State().Gateways[0])
	if err != nil {
		t.Fatalf("replacement OpenSession(): %v", err)
	}
	select {
	case <-firstSession.Done:
	default:
		t.Fatal("replaced session was not fenced")
	}
	if err := manager.RequireRevalidated(replacement.Ref); !errors.Is(err, ErrSnapshotFirst) {
		t.Fatalf("RequireRevalidated() = %v", err)
	}

	node.setVerifyError(errors.New("quorum unavailable"))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = manager.Confirm(ctx)
	cancel()
	if !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("Confirm() error = %v", err)
	}
	select {
	case <-replacement.Done:
	default:
		t.Fatal("session survived authority loss")
	}
	if presence := manager.Presence(); presence.State != PresenceNoAuthority {
		t.Fatalf("presence after fence = %#v", presence)
	}
	if err := manager.Revalidate(firstSession.Ref, nil); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old Revalidate() = %v", err)
	}

	node.setVerifyError(nil)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	secondAuthority, err := manager.Confirm(ctx)
	cancel()
	if err != nil {
		t.Fatalf("second Confirm(): %v", err)
	}
	if secondAuthority.AuthorityID == firstAuthority.AuthorityID {
		t.Fatal("authority ID was reused after fencing")
	}
}

func TestTermChangeCreatesNewAuthorityAndFencesSessions(t *testing.T) {
	node := &fakeRaftNode{
		status: raftnode.Status{Role: "Leader", Term: 1, ClusterEpoch: "epoch-1"},
		state: controlstate.State{Gateways: []controlstate.GatewaySlot{{
			GatewayID:  "gateway-1",
			Generation: 1,
			Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-1"},
		}}},
	}
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Second,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)

	first, err := manager.Confirm(context.Background())
	if err != nil {
		t.Fatalf("Confirm(first): %v", err)
	}
	session, err := manager.OpenSession(node.State().Gateways[0])
	if err != nil {
		t.Fatalf("OpenSession(): %v", err)
	}
	node.setTerm(2)
	second, err := manager.Confirm(context.Background())
	if err != nil {
		t.Fatalf("Confirm(second): %v", err)
	}
	if first.AuthorityID == second.AuthorityID {
		t.Fatal("authority ID was reused across Raft terms")
	}
	select {
	case <-session.Done:
	default:
		t.Fatal("old-term session was not fenced")
	}
}

func TestStaleGatewaySlotCannotOpenOrAdvanceSession(t *testing.T) {
	oldSlot := controlstate.GatewaySlot{
		GatewayID:  "gateway-1",
		Generation: 1,
		Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-old"},
	}
	node := &fakeRaftNode{
		status: raftnode.Status{Role: "Leader", Term: 1, ClusterEpoch: "epoch-1"},
		state:  controlstate.State{ClusterEpoch: "epoch-1", Gateways: []controlstate.GatewaySlot{oldSlot}},
	}
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Second,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	oldSession, err := manager.OpenSession(oldSlot)
	if err != nil {
		t.Fatalf("OpenSession(old): %v", err)
	}
	if err := manager.Revalidate(oldSession.Ref, nil); err != nil {
		t.Fatalf("Revalidate(old): %v", err)
	}

	newSlot := controlstate.GatewaySlot{
		GatewayID:  "gateway-1",
		Generation: 2,
		Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-new"},
	}
	node.setState(controlstate.State{ClusterEpoch: "epoch-1", Gateways: []controlstate.GatewaySlot{newSlot}})
	if err := manager.RequireRevalidated(oldSession.Ref); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("RequireRevalidated(old) = %v, want ErrStaleSession", err)
	}
	if _, err := manager.OpenSession(oldSlot); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("OpenSession(stale slot) = %v, want ErrStaleSession", err)
	}
	if _, err := manager.OpenSession(newSlot); err != nil {
		t.Fatalf("OpenSession(new): %v", err)
	}
	select {
	case <-oldSession.Done:
	default:
		t.Fatal("new current slot did not fence the old session")
	}
}

func TestPresenceClassifiesUnreportedGatewayOnlyAfterTimeout(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	node := &fakeRaftNode{
		status: raftnode.Status{Role: "Leader", Term: 1, ClusterEpoch: "epoch-1"},
		state: controlstate.State{ClusterEpoch: "epoch-1", Gateways: []controlstate.GatewaySlot{{
			GatewayID:  "gateway-1",
			Generation: 1,
			Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-1"},
		}}},
	}
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Second,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	manager.now = func() time.Time { return now }
	t.Cleanup(manager.Close)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	if presence := manager.Presence(); presence.State != PresenceRebuilding || presence.Classified != 0 {
		t.Fatalf("presence before timeout = %#v", presence)
	}
	now = now.Add(time.Minute)
	if presence := manager.Presence(); presence.State != PresenceComplete || presence.Classified != 1 || presence.Revalidated != 0 {
		t.Fatalf("presence after timeout = %#v", presence)
	}
}

func TestSyncingSessionCannotRevalidateAfterTimeout(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	slot := controlstate.GatewaySlot{
		GatewayID:  "gateway-1",
		Generation: 1,
		Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-1"},
	}
	node := &fakeRaftNode{
		status: raftnode.Status{Role: "Leader", Term: 1, ClusterEpoch: "epoch-1"},
		state:  controlstate.State{ClusterEpoch: "epoch-1", Gateways: []controlstate.GatewaySlot{slot}},
	}
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Second,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	manager.now = func() time.Time { return now }
	t.Cleanup(manager.Close)
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	session, err := manager.OpenSession(slot)
	if err != nil {
		t.Fatalf("OpenSession(): %v", err)
	}
	now = now.Add(time.Minute)
	if presence := manager.Presence(); presence.State != PresenceComplete || presence.Classified != 1 || presence.Revalidated != 0 {
		t.Fatalf("presence after syncing timeout = %#v", presence)
	}
	select {
	case <-session.Done:
	default:
		t.Fatal("timed-out syncing session was not ended")
	}
	if err := manager.Revalidate(session.Ref, nil); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("Revalidate(timed out) = %v, want ErrStaleSession", err)
	}
}

func TestManagerDoesNotStartAfterClose(t *testing.T) {
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Second,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Second,
	}, &fakeRaftNode{})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	manager.Close()
	manager.Start(context.Background())
	manager.Close()
	select {
	case <-manager.done:
	case <-time.After(time.Second):
		t.Fatal("manager did not remain closed")
	}
}

type fakeRaftNode struct {
	mu        sync.RWMutex
	status    raftnode.Status
	state     controlstate.State
	verifyErr error
}

func (n *fakeRaftNode) Status() raftnode.Status {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.status
}

func (n *fakeRaftNode) State() controlstate.State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *fakeRaftNode) VerifyLeader(context.Context) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.verifyErr
}

func (n *fakeRaftNode) setVerifyError(err error) {
	n.mu.Lock()
	n.verifyErr = err
	n.mu.Unlock()
}

func (n *fakeRaftNode) setTerm(term uint64) {
	n.mu.Lock()
	n.status.Term = term
	n.mu.Unlock()
}

func (n *fakeRaftNode) setState(state controlstate.State) {
	n.mu.Lock()
	n.state = state
	n.mu.Unlock()
}
