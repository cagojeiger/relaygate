package localbinding

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

func TestLiveBindingsAreStableAndRetirementRecoversCapacity(t *testing.T) {
	sessions := newTestSessions()
	listener := testSession("listener", "client-1", "listener-key")
	sessions.allow(listener.Ref)
	committer := &testCommitter{current: testControlSession()}
	manager := mustManager(t, 1, committer, sessions)
	first, err := manager.Bind(context.Background(), listener, "/z", "worker", testEndpoint{})
	if err != nil {
		t.Fatalf("Bind(first): %v", err)
	}
	if got := manager.LiveBindings(); len(got) != 1 || got[0] != first {
		t.Fatalf("LiveBindings() = %#v, want first binding", got)
	}
	if err := manager.Unbind(listener.Ref, first.Ref.ListenerBindingID); err != nil {
		t.Fatalf("Unbind(): %v", err)
	}
	waitFor(t, func() bool { return manager.ActiveCount() == 0 })
	if got := manager.LiveBindings(); len(got) != 0 {
		t.Fatalf("LiveBindings() after unbind = %#v", got)
	}
	second, err := manager.Bind(context.Background(), listener, "/a", "worker", testEndpoint{})
	if err != nil {
		t.Fatalf("Bind(after retire): %v", err)
	}
	if second.Ref.ListenerBindingID == first.Ref.ListenerBindingID {
		t.Fatal("rebind reused listener binding identity")
	}
}

func TestBindMapsCurrentDirectoryErrors(t *testing.T) {
	sessions := newTestSessions()
	listener := testSession("listener", "client-1", "listener-key")
	sessions.allow(listener.Ref)
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "conflict", err: routing.ErrConflict, want: ErrConflict},
		{name: "capacity", err: routing.ErrCapacity, want: ErrCapacity},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := mustManager(t, 1, &testCommitter{declareErr: test.err, current: testControlSession()}, sessions)
			_, err := manager.Bind(context.Background(), listener, "/events", "worker", testEndpoint{})
			if !errors.Is(err, test.want) || !errors.Is(err, test.err) {
				t.Fatalf("Bind() = %v, want %v wrapping %v", err, test.want, test.err)
			}
		})
	}
}

func mustManager(t *testing.T, max uint32, committer Committer, sessions SessionValidator) *Manager {
	t.Helper()
	manager, err := New("gateway-1", "instance-1", max, committer, sessions)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)
	return manager
}

type testCommitter struct {
	mu         sync.Mutex
	current    controlmodel.SessionRef
	ok         bool
	declareErr error
	declared   []routing.LiveBinding
	withdrawn  []routing.LiveBinding
}

type testEndpoint struct{}

func (testEndpoint) Offer(context.Context, Offer) error                { return nil }
func (testEndpoint) Confirm(context.Context, Confirmation) error       { return nil }
func (testEndpoint) Terminate(context.Context, Termination) error      { return nil }
func (testEndpoint) DeliverPayload(context.Context, PipePayload) error { return nil }

func (c *testCommitter) Declare(_ context.Context, binding routing.LiveBinding) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.declareErr != nil {
		return c.declareErr
	}
	c.declared = append(c.declared, binding)
	return nil
}
func (c *testCommitter) Withdraw(_ context.Context, binding routing.LiveBinding) error {
	c.mu.Lock()
	c.withdrawn = append(c.withdrawn, binding)
	c.mu.Unlock()
	return nil
}
func (c *testCommitter) CurrentSession() (controlmodel.SessionRef, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.ok || c.current.ControlSessionID != ""
}

type testSessions struct {
	mu      sync.Mutex
	allowed map[clientsession.Ref]bool
}

func newTestSessions() *testSessions { return &testSessions{allowed: make(map[clientsession.Ref]bool)} }
func (s *testSessions) allow(ref clientsession.Ref) {
	s.mu.Lock()
	s.allowed[ref] = true
	s.mu.Unlock()
}
func (s *testSessions) Require(ref clientsession.Ref) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.allowed[ref] {
		return errors.New("session is not active")
	}
	return nil
}
func (s *testSessions) Allowed(clientID, apiKeyID string) bool {
	return clientID == "client-1" && apiKeyID != "retired"
}

func testSession(id, clientID, keyID string) clientsession.Session {
	return clientsession.Session{Ref: clientsession.Ref{ClientSessionID: id, ClientID: clientID, APIKeyID: keyID, AuthRevision: "revision-1"}, Done: make(chan struct{})}
}

func testControlSession() controlmodel.SessionRef {
	return controlmodel.SessionRef{ClusterEpoch: "epoch-1", AuthorityID: "authority-1", ControlSessionID: "owner-session", GatewayID: "gateway-1", GatewayInstanceID: "instance-1"}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
