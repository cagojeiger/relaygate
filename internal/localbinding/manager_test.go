package localbinding

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
)

func TestBindLifecycleNamespaceAndNonDisclosingUnbind(t *testing.T) {
	sessions := newFakeSessions()
	first := newSession("session-1", "client-1", "key-1")
	second := newSession("session-2", "client-2", "key-2")
	third := newSession("session-3", "client-3", "key-3")
	sessions.allow(first.Ref)
	sessions.allow(second.Ref)
	sessions.allow(third.Ref)

	removeEntered := make(chan controlstate.BindingSlot, 2)
	allowRemove := make(chan struct{})
	committer := &fakeCommitter{
		install: exactInstall,
		remove: func(_ context.Context, slot controlstate.BindingSlot) error {
			removeEntered <- cloneSlot(slot)
			<-allowRemove
			return nil
		},
	}
	manager := mustManager(t, 2, committer, sessions)
	defer manager.Close()

	firstSlot, err := manager.Bind(context.Background(), first, "/events/*", "worker")
	if err != nil {
		t.Fatalf("first Bind(): %v", err)
	}
	if firstSlot.Key.ClientID != first.Ref.ClientID {
		t.Fatalf("binding ClientID = %q, want authenticated %q", firstSlot.Key.ClientID, first.Ref.ClientID)
	}
	firstID := firstSlot.Ref.ListenerBindingID
	if snapshot, ok := manager.Inspect(firstID); !ok || snapshot.State != StateLive {
		t.Fatalf("first state = %#v, found=%v", snapshot, ok)
	}
	if !manager.Eligible(firstID) {
		t.Fatal("committed binding is not eligible")
	}
	if _, err := manager.Bind(context.Background(), first, "/events/*", "worker"); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate Bind() error = %v, want ErrConflict", err)
	}

	secondSlot, err := manager.Bind(context.Background(), second, "/events/*", "worker")
	if err != nil {
		t.Fatalf("cross-client Bind(): %v", err)
	}
	if firstSlot.Key == secondSlot.Key || secondSlot.Key.ClientID != second.Ref.ClientID {
		t.Fatalf("strict namespace keys = first %#v, second %#v", firstSlot.Key, secondSlot.Key)
	}

	if err := manager.Unbind(second.Ref, firstID); err != nil {
		t.Fatalf("cross-session Unbind(): %v", err)
	}
	if !manager.Eligible(firstID) {
		t.Fatal("cross-session Unbind disclosed or changed the binding")
	}
	if err := manager.Unbind(first.Ref, "unknown"); err != nil {
		t.Fatalf("unknown Unbind(): %v", err)
	}
	if err := manager.Unbind(first.Ref, firstID); err != nil {
		t.Fatalf("Unbind(): %v", err)
	}
	if manager.Eligible(firstID) {
		t.Fatal("binding remained eligible after Unbind returned")
	}
	if snapshot, ok := manager.Inspect(firstID); !ok || snapshot.State != StateRetiring {
		t.Fatalf("state while cleanup blocked = %#v, found=%v", snapshot, ok)
	}
	exact := <-removeEntered
	if !slotsEqual(exact, firstSlot) {
		t.Fatalf("cleanup slot = %#v, want exact %#v", exact, firstSlot)
	}
	if manager.ActiveCount() != 2 {
		t.Fatalf("occupied count during cleanup = %d, want 2", manager.ActiveCount())
	}
	if _, err := manager.Bind(context.Background(), third, "/new", "worker"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Bind() while cleanup is blocked = %v, want ErrCapacity", err)
	}
	if err := manager.Unbind(first.Ref, firstID); err != nil {
		t.Fatalf("duplicate Unbind(): %v", err)
	}
	close(allowRemove)
	waitState(t, manager, firstID, StateRetired)
	if _, err := manager.Bind(context.Background(), third, "/new", "worker"); err != nil {
		t.Fatalf("Bind() after cleanup completed: %v", err)
	}
}

func TestCapacityAdmissionIsAtomicUnderConcurrency(t *testing.T) {
	const contenders = 24
	sessions := newFakeSessions()
	session := newSession("session-1", "client-1", "key-1")
	sessions.allow(session.Ref)
	installEntered := make(chan struct{}, 1)
	allowInstall := make(chan struct{})
	committer := &fakeCommitter{install: func(_ context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
		installEntered <- struct{}{}
		<-allowInstall
		return liveSlot(key, 1, ref), nil
	}}
	manager := mustManager(t, 1, committer, sessions)
	defer manager.Close()

	start := make(chan struct{})
	results := make(chan error, contenders)
	for index := range contenders {
		go func() {
			<-start
			_, err := manager.Bind(context.Background(), session, fmt.Sprintf("/endpoint/%d", index), "target")
			results <- err
		}()
	}
	close(start)
	<-installEntered

	for range contenders - 1 {
		if err := <-results; !errors.Is(err, ErrCapacity) {
			t.Fatalf("losing concurrent Bind() error = %v, want ErrCapacity", err)
		}
	}
	if manager.ActiveCount() != 1 {
		t.Fatalf("active count while registering = %d, want 1", manager.ActiveCount())
	}
	close(allowInstall)
	if err := <-results; err != nil {
		t.Fatalf("winning concurrent Bind(): %v", err)
	}
}

func TestCleanupPendingBindingRetainsCapacity(t *testing.T) {
	sessions := newFakeSessions()
	session := newSession("session-1", "client-1", "key-1")
	sessions.allow(session.Ref)
	committer := newCASCommitter()
	manager := mustManager(t, 1, committer, sessions)
	defer manager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	bindResult := make(chan error, 1)
	go func() {
		_, err := manager.Bind(ctx, session, "/events", "worker")
		bindResult <- err
	}()
	<-committer.firstInstall
	cancel()
	if err := <-bindResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Bind() error = %v, want context.Canceled", err)
	}
	if manager.ActiveCount() != 1 {
		t.Fatalf("active count while install outcome is unknown = %d, want 1", manager.ActiveCount())
	}
	if _, err := manager.Bind(context.Background(), session, "/replacement", "worker"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Bind() before late install resolves = %v, want ErrCapacity", err)
	}

	close(committer.allowFirstInstall)
	<-committer.removeEntered
	if _, err := manager.Bind(context.Background(), session, "/replacement", "worker"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Bind() while cleanup is pending = %v, want ErrCapacity", err)
	}
	close(committer.allowRemove)
	<-committer.removeFinished
	waitActiveCount(t, manager, 0)
	if _, err := manager.Bind(context.Background(), session, "/replacement", "worker"); err != nil {
		t.Fatalf("Bind() after exact cleanup: %v", err)
	}
}

func TestCallerCancellationRetiresBeforeLateInstallAndCleansExactSlot(t *testing.T) {
	sessions := newFakeSessions()
	session := newSession("session-1", "client-1", "key-1")
	sessions.allow(session.Ref)
	committer := newCASCommitter()
	manager := mustManager(t, 2, committer, sessions)
	defer manager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	bindResult := make(chan error, 1)
	go func() {
		_, err := manager.Bind(ctx, session, "/events", "worker")
		bindResult <- err
	}()
	firstRef := <-committer.firstInstall
	if snapshot, ok := manager.Inspect(firstRef.ListenerBindingID); !ok || snapshot.State != StateRegistering {
		t.Fatalf("state before cancel = %#v, found=%v", snapshot, ok)
	}
	cancel()
	if err := <-bindResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Bind() error = %v", err)
	}
	if manager.ActiveCount() != 1 || manager.Eligible(firstRef.ListenerBindingID) {
		t.Fatalf("late attempt capacity/eligibility = count=%d eligible=%v, want 1/false", manager.ActiveCount(), manager.Eligible(firstRef.ListenerBindingID))
	}
	if snapshot, ok := manager.Inspect(firstRef.ListenerBindingID); !ok || snapshot.State != StateRetired {
		t.Fatalf("state immediately after cancel = %#v, found=%v", snapshot, ok)
	}

	close(committer.allowFirstInstall)
	oldCleanup := <-committer.removeEntered
	if oldCleanup.Ref == nil || *oldCleanup.Ref != firstRef {
		t.Fatalf("late cleanup ref = %#v, want %#v", oldCleanup.Ref, firstRef)
	}

	newSlot, err := manager.Bind(context.Background(), session, "/events", "worker")
	if err != nil {
		t.Fatalf("rebind while old cleanup is delayed: %v", err)
	}
	if newSlot.Generation != oldCleanup.Generation+1 {
		t.Fatalf("rebind generation = %d, want %d", newSlot.Generation, oldCleanup.Generation+1)
	}
	close(committer.allowRemove)
	<-committer.removeFinished
	current := committer.currentSlot(newSlot.Key)
	if !slotsEqual(current, newSlot) {
		t.Fatalf("old conditional cleanup changed rebind: current %#v, want %#v", current, newSlot)
	}
}

func TestSessionAndCredentialRetirementAreImmediateAndIdempotent(t *testing.T) {
	sessions := newFakeSessions()
	first := newSession("session-1", "client-1", "key-1")
	second := newSession("session-2", "client-1", "key-2")
	thirdDone := make(chan struct{})
	third := sessionWithDone("session-3", "client-2", "key-3", thirdDone)
	for _, session := range []clientsession.Session{first, second, third} {
		sessions.allow(session.Ref)
	}
	committer := &fakeCommitter{install: exactInstall, remove: func(context.Context, controlstate.BindingSlot) error { return nil }}
	manager := mustManager(t, 3, committer, sessions)
	defer manager.Close()

	firstSlot := mustBind(t, manager, first, "/one")
	secondSlot := mustBind(t, manager, second, "/two")
	thirdSlot := mustBind(t, manager, third, "/three")

	if got := manager.RetireSession(first.Ref); got != 1 {
		t.Fatalf("RetireSession() = %d, want 1", got)
	}
	if got := manager.RetireSession(first.Ref); got != 0 {
		t.Fatalf("duplicate RetireSession() = %d, want 0", got)
	}
	if manager.Eligible(firstSlot.Ref.ListenerBindingID) {
		t.Fatal("session-retired binding remained eligible")
	}
	change := clientauth.ChangeSet{Removed: []clientauth.CredentialID{{ClientID: second.Ref.ClientID, APIKeyID: second.Ref.APIKeyID}}}
	if got := manager.Retire(change); got != 1 {
		t.Fatalf("Retire(change) = %d, want 1", got)
	}
	if got := manager.Retire(change); got != 0 {
		t.Fatalf("duplicate Retire(change) = %d, want 0", got)
	}
	if manager.Eligible(secondSlot.Ref.ListenerBindingID) {
		t.Fatal("credential-retired binding remained eligible")
	}

	close(thirdDone)
	waitInactive(t, manager, thirdSlot.Ref.ListenerBindingID)
	waitActiveCount(t, manager, 0)
}

func TestReloadRetirementCannotMissConcurrentInsertion(t *testing.T) {
	session := newSession("session-1", "client-1", "key-1")
	sessions := newBlockingSessions(session.Ref)
	installEntered := make(chan struct{})
	allowInstall := make(chan struct{})
	committer := &fakeCommitter{
		install: func(_ context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
			close(installEntered)
			<-allowInstall
			return liveSlot(key, 1, ref), nil
		},
		remove: func(context.Context, controlstate.BindingSlot) error { return nil },
	}
	manager := mustManager(t, 1, committer, sessions)
	defer manager.Close()

	bindDone := make(chan error, 1)
	go func() {
		_, err := manager.Bind(context.Background(), session, "/events", "worker")
		bindDone <- err
	}()
	<-sessions.requireEntered
	retireStarted := make(chan struct{})
	retireDone := make(chan int, 1)
	go func() {
		close(retireStarted)
		retireDone <- manager.Retire(clientauth.ChangeSet{Removed: []clientauth.CredentialID{{ClientID: "client-1", APIKeyID: "key-1"}}})
	}()
	<-retireStarted
	close(sessions.allowRequire)
	<-installEntered
	if got := <-retireDone; got != 1 {
		t.Fatalf("concurrent Retire() = %d, want inserted binding", got)
	}
	close(allowInstall)
	if err := <-bindDone; !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("retired in-flight Bind() error = %v, want ErrSessionEnded", err)
	}
}

func TestCommitErrorPreservesCauseAndLeavesNoLiveBinding(t *testing.T) {
	upstream := errors.New("quorum unavailable")
	sessions := newFakeSessions()
	session := newSession("session-1", "client-1", "key-1")
	sessions.allow(session.Ref)
	manager := mustManager(t, 1, &fakeCommitter{install: func(context.Context, controlstate.BindingKey, controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
		return controlstate.BindingSlot{}, upstream
	}}, sessions)
	defer manager.Close()

	_, err := manager.Bind(context.Background(), session, "/events", "worker")
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, upstream) {
		t.Fatalf("Bind() error = %v, want ErrUnavailable wrapping upstream", err)
	}
	if manager.ActiveCount() != 0 {
		t.Fatalf("active count after failed commit = %d", manager.ActiveCount())
	}
}

func TestCASMismatchIsAStableBindingConflict(t *testing.T) {
	sessions := newFakeSessions()
	session := newSession("session-1", "client-1", "key-1")
	sessions.allow(session.Ref)
	manager := mustManager(t, 1, &fakeCommitter{install: func(context.Context, controlstate.BindingKey, controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
		return controlstate.BindingSlot{}, controlstate.ErrCASMismatch
	}}, sessions)
	defer manager.Close()

	_, err := manager.Bind(context.Background(), session, "/events", "worker")
	if !errors.Is(err, ErrConflict) || !errors.Is(err, controlstate.ErrCASMismatch) {
		t.Fatalf("Bind() error = %v, want ErrConflict wrapping ErrCASMismatch", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("CAS mismatch was classified as unavailable: %v", err)
	}
}

func TestBindingLimitsAreStableErrors(t *testing.T) {
	sessions := newFakeSessions()
	session := newSession("session-1", "client-1", "key-1")
	sessions.allow(session.Ref)
	committer := &fakeCommitter{install: func(context.Context, controlstate.BindingKey, controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
		return controlstate.BindingSlot{}, controlstate.ErrBindingCapacity
	}}
	manager := mustManager(t, 1, committer, sessions)
	defer manager.Close()

	if _, err := manager.Bind(context.Background(), session, "/events", "worker"); !errors.Is(err, ErrCapacity) || !errors.Is(err, controlstate.ErrBindingCapacity) {
		t.Fatalf("Bind() capacity error = %v", err)
	}
	if _, err := manager.Bind(context.Background(), session, strings.Repeat("e", controlstate.MaxEndpointPatternBytes+1), "worker"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Bind() oversized endpoint error = %v, want ErrInvalid", err)
	}
	if _, err := New("gateway-1", "instance-1", controlstate.MaxListenerBindingsPerGateway+1, committer, sessions); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New() oversized capacity error = %v, want ErrInvalid", err)
	}
}

type fakeCommitter struct {
	install func(context.Context, controlstate.BindingKey, controlstate.ListenerBindingRef) (controlstate.BindingSlot, error)
	remove  func(context.Context, controlstate.BindingSlot) error
}

func (f *fakeCommitter) Install(ctx context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
	return f.install(ctx, key, ref)
}

func (f *fakeCommitter) Remove(ctx context.Context, slot controlstate.BindingSlot) error {
	if f.remove == nil {
		return nil
	}
	return f.remove(ctx, slot)
}

type fakeSessions struct {
	mu      sync.Mutex
	allowed map[clientsession.Ref]bool
}

func newFakeSessions() *fakeSessions {
	return &fakeSessions{allowed: make(map[clientsession.Ref]bool)}
}

func (f *fakeSessions) allow(ref clientsession.Ref) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowed[ref] = true
}

func (f *fakeSessions) Require(ref clientsession.Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.allowed[ref] {
		return clientsession.ErrStaleSession
	}
	return nil
}

type blockingSessions struct {
	ref            clientsession.Ref
	requireEntered chan struct{}
	allowRequire   chan struct{}
	once           sync.Once
}

func newBlockingSessions(ref clientsession.Ref) *blockingSessions {
	return &blockingSessions{ref: ref, requireEntered: make(chan struct{}), allowRequire: make(chan struct{})}
}

func (f *blockingSessions) Require(ref clientsession.Ref) error {
	if ref != f.ref {
		return clientsession.ErrStaleSession
	}
	f.once.Do(func() {
		close(f.requireEntered)
		<-f.allowRequire
	})
	return nil
}

type casCommitter struct {
	mu sync.Mutex

	current  map[controlstate.BindingKey]controlstate.BindingSlot
	installs int

	firstInstall      chan controlstate.ListenerBindingRef
	allowFirstInstall chan struct{}
	removeEntered     chan controlstate.BindingSlot
	allowRemove       chan struct{}
	removeFinished    chan struct{}
	removeOnce        sync.Once
}

func newCASCommitter() *casCommitter {
	return &casCommitter{
		current:           make(map[controlstate.BindingKey]controlstate.BindingSlot),
		firstInstall:      make(chan controlstate.ListenerBindingRef, 1),
		allowFirstInstall: make(chan struct{}),
		removeEntered:     make(chan controlstate.BindingSlot, 1),
		allowRemove:       make(chan struct{}),
		removeFinished:    make(chan struct{}),
	}
}

func (f *casCommitter) Install(_ context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
	f.mu.Lock()
	f.installs++
	first := f.installs == 1
	f.mu.Unlock()
	if first {
		f.firstInstall <- ref
		<-f.allowFirstInstall
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	generation := f.current[key].Generation + 1
	slot := liveSlot(key, generation, ref)
	f.current[key] = cloneSlot(slot)
	return slot, nil
}

func (f *casCommitter) Remove(_ context.Context, exact controlstate.BindingSlot) error {
	f.removeOnce.Do(func() {
		f.removeEntered <- cloneSlot(exact)
		<-f.allowRemove
		f.mu.Lock()
		if slotsEqual(f.current[exact.Key], exact) {
			f.current[exact.Key] = controlstate.BindingSlot{Key: exact.Key, Generation: exact.Generation + 1}
		}
		f.mu.Unlock()
		close(f.removeFinished)
	})
	return nil
}

func (f *casCommitter) currentSlot(key controlstate.BindingKey) controlstate.BindingSlot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneSlot(f.current[key])
}

func exactInstall(_ context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
	return liveSlot(key, 1, ref), nil
}

func liveSlot(key controlstate.BindingKey, generation uint64, ref controlstate.ListenerBindingRef) controlstate.BindingSlot {
	copy := ref
	return controlstate.BindingSlot{Key: key, Generation: generation, Ref: &copy}
}

func slotsEqual(left, right controlstate.BindingSlot) bool {
	if left.Key != right.Key || left.Generation != right.Generation {
		return false
	}
	if left.Ref == nil || right.Ref == nil {
		return left.Ref == nil && right.Ref == nil
	}
	return *left.Ref == *right.Ref
}

func newSession(id, clientID, apiKeyID string) clientsession.Session {
	done := make(chan struct{})
	return sessionWithDone(id, clientID, apiKeyID, done)
}

func sessionWithDone(id, clientID, apiKeyID string, done <-chan struct{}) clientsession.Session {
	return clientsession.Session{
		Ref: clientsession.Ref{
			ClientSessionID: id,
			ClientID:        clientID,
			APIKeyID:        apiKeyID,
			AuthRevision:    "revision-1",
		},
		Done: done,
	}
}

func mustManager(t *testing.T, max uint32, committer Committer, sessions SessionValidator) *Manager {
	t.Helper()
	manager, err := New("gateway-1", "instance-1", max, committer, sessions)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return manager
}

func mustBind(t *testing.T, manager *Manager, session clientsession.Session, endpoint string) controlstate.BindingSlot {
	t.Helper()
	slot, err := manager.Bind(context.Background(), session, endpoint, "worker")
	if err != nil {
		t.Fatalf("Bind(%q): %v", endpoint, err)
	}
	return slot
}

func waitState(t *testing.T, manager *Manager, bindingID string, want State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := manager.Inspect(bindingID); ok && snapshot.State == want {
			return
		}
		runtime.Gosched()
	}
	snapshot, ok := manager.Inspect(bindingID)
	t.Fatalf("binding state = %#v, found=%v, want %s", snapshot, ok, want)
}

func waitInactive(t *testing.T, manager *Manager, bindingID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !manager.Eligible(bindingID) {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("binding %q remained eligible", bindingID)
}

func waitActiveCount(t *testing.T, manager *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.ActiveCount() == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("active count = %d, want %d", manager.ActiveCount(), want)
}
