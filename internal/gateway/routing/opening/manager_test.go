package opening

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

type openHarness struct {
	manager      *Manager
	admitter     *fakeAdmitter
	store        *fakeStore
	endpoint     *scriptedEndpoint
	caller       clientsession.Session
	listener     clientsession.Session
	slot         routing.LiveBinding
	context      routing.OpenContext
	callerDone   chan struct{}
	listenerDone chan struct{}
}

func newHarness(t *testing.T, max uint32, timeout time.Duration, endpoint *scriptedEndpoint) *openHarness {
	t.Helper()
	callerDone := make(chan struct{})
	listenerDone := make(chan struct{})
	caller := testSession("caller", "client-1", "caller-key", callerDone)
	listener := testSession("listener", "client-1", "listener-key", listenerDone)
	slot := testSlot(caller.Ref.ClientID)
	open := newOpenContext(t, "attempt-1", caller.Ref, slot)
	admitter := &fakeAdmitter{context: open}
	store := &fakeStore{
		gatewayID:    "gateway-1",
		listener:     listener.Ref,
		listenerDone: listener.Done,
		endpoint:     endpoint,
	}
	return &openHarness{
		manager:      mustOpeningManager(t, max, timeout, admitter, store),
		admitter:     admitter,
		store:        store,
		endpoint:     endpoint,
		caller:       caller,
		listener:     listener,
		slot:         slot,
		context:      open,
		callerDone:   callerDone,
		listenerDone: listenerDone,
	}
}

func newSequenceHarness(t *testing.T, max uint32, endpoint *scriptedEndpoint) *openHarness {
	t.Helper()
	return newHarness(t, max, time.Second, endpoint)
}

type fakeAdmitter struct {
	mu      sync.Mutex
	context routing.OpenContext
	err     error
	calls   int
}

func (f *fakeAdmitter) AdmitOpen(context.Context, clientsession.Ref, string, string) (routing.OpenContext, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.context.Clone(), f.err
}

func (f *fakeAdmitter) setContext(open routing.OpenContext) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.context = open
}

func (f *fakeAdmitter) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeAdmitter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeStore struct {
	mu           sync.Mutex
	gatewayID    string
	listener     clientsession.Ref
	listenerDone <-chan struct{}
	endpoint     localbinding.ListenerEndpoint
	err          error
	calls        int
}

func (f *fakeStore) GatewayID() string { return f.gatewayID }

func (f *fakeStore) Reserve(open routing.OpenContext, caller clientsession.Ref) (localbinding.Reservation, error) {
	return f.reserve(open, caller, true)
}

func (f *fakeStore) ReserveForwarded(open routing.OpenContext, caller clientsession.Ref) (localbinding.Reservation, error) {
	return f.reserve(open, caller, false)
}

func (f *fakeStore) reserve(open routing.OpenContext, caller clientsession.Ref, consume bool) (localbinding.Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return localbinding.Reservation{}, f.err
	}
	if consume && !open.TryConsume() {
		return localbinding.Reservation{}, localbinding.ErrAttemptUsed
	}
	return localbinding.Reservation{
		Context:      open.Clone(),
		Caller:       caller,
		Binding:      cloneSlot(open.Binding),
		Listener:     f.listener,
		Endpoint:     f.endpoint,
		ListenerDone: f.listenerDone,
	}, nil
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeStore) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

type fakeRemoteOpener struct {
	mu             sync.Mutex
	result         RemoteResult
	err            error
	calls          int
	callerEndpoint localbinding.CallerEndpoint
	beforeReturn   func()
}

func (f *fakeRemoteOpener) Open(_ context.Context, _ routing.OpenContext, callerEndpoint localbinding.CallerEndpoint) (RemoteResult, error) {
	f.mu.Lock()
	f.calls++
	f.callerEndpoint = callerEndpoint
	result := f.result
	beforeReturn := f.beforeReturn
	result.Binding = cloneSlot(result.Binding)
	err := f.err
	f.mu.Unlock()
	if beforeReturn != nil {
		beforeReturn()
	}
	return result, err
}

func (f *fakeRemoteOpener) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type scriptedRemoteEndpoint struct {
	*scriptedEndpoint

	mu          sync.Mutex
	done        chan struct{}
	closeOnce   sync.Once
	activateErr error
	activations int
	closes      int
}

func newScriptedRemoteEndpoint() *scriptedRemoteEndpoint {
	return &scriptedRemoteEndpoint{scriptedEndpoint: &scriptedEndpoint{}, done: make(chan struct{})}
}

func (s *scriptedRemoteEndpoint) Activate(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activations++
	return s.activateErr
}

func (s *scriptedRemoteEndpoint) Close(context.Context) error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}

func (s *scriptedRemoteEndpoint) Done() <-chan struct{} { return s.done }

func (s *scriptedRemoteEndpoint) activationCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activations
}

func (s *scriptedRemoteEndpoint) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("remote endpoint was not closed")
	}
}

type scriptedEndpoint struct {
	mu            sync.Mutex
	deliver       func(context.Context, localbinding.PipePayload) error
	offer         func(context.Context, localbinding.Offer) error
	confirm       func(context.Context, localbinding.Confirmation) error
	terminate     func(context.Context, localbinding.Termination) error
	terminatePipe func(context.Context, string) error
	payloads      chan localbinding.PipePayload
	offers        []localbinding.Offer
	confirmations []localbinding.Confirmation
	terminations  chan localbinding.Termination
	pipeTerminals chan string
}

func (s *scriptedEndpoint) Offer(ctx context.Context, offer localbinding.Offer) error {
	s.mu.Lock()
	s.offers = append(s.offers, offer)
	fn := s.offer
	s.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx, offer)
}

func (s *scriptedEndpoint) DeliverPayload(ctx context.Context, payload localbinding.PipePayload) error {
	s.mu.Lock()
	if s.payloads == nil {
		s.payloads = make(chan localbinding.PipePayload, 32)
	}
	payloads := s.payloads
	fn := s.deliver
	s.mu.Unlock()
	payloads <- payload
	if fn == nil {
		return nil
	}
	return fn(ctx, payload)
}

func (s *scriptedEndpoint) Confirm(ctx context.Context, confirmation localbinding.Confirmation) error {
	s.mu.Lock()
	s.confirmations = append(s.confirmations, confirmation)
	fn := s.confirm
	s.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx, confirmation)
}

func (s *scriptedEndpoint) Terminate(ctx context.Context, termination localbinding.Termination) error {
	s.mu.Lock()
	if s.terminations == nil {
		s.terminations = make(chan localbinding.Termination, 16)
	}
	terminations := s.terminations
	fn := s.terminate
	s.mu.Unlock()
	terminations <- termination
	if fn == nil {
		return nil
	}
	return fn(ctx, termination)
}

func (s *scriptedEndpoint) TerminatePipe(ctx context.Context, pipeID string) error {
	s.mu.Lock()
	if s.pipeTerminals == nil {
		s.pipeTerminals = make(chan string, 16)
	}
	pipeTerminals := s.pipeTerminals
	fn := s.terminatePipe
	s.mu.Unlock()
	pipeTerminals <- pipeID
	if fn == nil {
		return nil
	}
	return fn(ctx, pipeID)
}

func (s *scriptedEndpoint) offerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.offers)
}

func (s *scriptedEndpoint) confirmCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.confirmations)
}

func receiveTermination(t *testing.T, endpoint *scriptedEndpoint) localbinding.Termination {
	t.Helper()
	endpoint.mu.Lock()
	if endpoint.terminations == nil {
		endpoint.terminations = make(chan localbinding.Termination, 16)
	}
	terminations := endpoint.terminations
	endpoint.mu.Unlock()
	select {
	case termination := <-terminations:
		return termination
	case <-time.After(time.Second):
		t.Fatal("listener did not receive termination")
		return localbinding.Termination{}
	}
}

func receivePayload(t *testing.T, endpoint *scriptedEndpoint) localbinding.PipePayload {
	t.Helper()
	endpoint.mu.Lock()
	if endpoint.payloads == nil {
		endpoint.payloads = make(chan localbinding.PipePayload, 32)
	}
	payloads := endpoint.payloads
	endpoint.mu.Unlock()
	select {
	case payload := <-payloads:
		return payload
	case <-time.After(time.Second):
		t.Fatal("endpoint did not receive payload")
		return localbinding.PipePayload{}
	}
}

func assertNoPayload(t *testing.T, endpoint *scriptedEndpoint, wait time.Duration) {
	t.Helper()
	endpoint.mu.Lock()
	if endpoint.payloads == nil {
		endpoint.payloads = make(chan localbinding.PipePayload, 32)
	}
	payloads := endpoint.payloads
	endpoint.mu.Unlock()
	select {
	case payload := <-payloads:
		t.Fatalf("unexpected payload: %#v", payload)
	case <-time.After(wait):
	}
}

func receivePipeTermination(t *testing.T, endpoint *scriptedEndpoint) string {
	t.Helper()
	endpoint.mu.Lock()
	if endpoint.pipeTerminals == nil {
		endpoint.pipeTerminals = make(chan string, 16)
	}
	pipeTerminals := endpoint.pipeTerminals
	endpoint.mu.Unlock()
	select {
	case pipeID := <-pipeTerminals:
		return pipeID
	case <-time.After(time.Second):
		t.Fatal("caller did not receive Pipe termination")
		return ""
	}
}

func assertNoPipeTermination(t *testing.T, endpoint *scriptedEndpoint, wait time.Duration) {
	t.Helper()
	endpoint.mu.Lock()
	if endpoint.pipeTerminals == nil {
		endpoint.pipeTerminals = make(chan string, 16)
	}
	pipeTerminals := endpoint.pipeTerminals
	endpoint.mu.Unlock()
	select {
	case pipeID := <-pipeTerminals:
		t.Fatalf("unexpected caller Pipe termination: %q", pipeID)
	case <-time.After(wait):
	}
}

func waitClosed(t *testing.T, done <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

func assertNoTermination(t *testing.T, endpoint *scriptedEndpoint, wait time.Duration) {
	t.Helper()
	endpoint.mu.Lock()
	if endpoint.terminations == nil {
		endpoint.terminations = make(chan localbinding.Termination, 16)
	}
	terminations := endpoint.terminations
	endpoint.mu.Unlock()
	select {
	case termination := <-terminations:
		t.Fatalf("unexpected duplicate termination: %#v", termination)
	case <-time.After(wait):
	}
}

func mustOpeningManager(t *testing.T, max uint32, timeout time.Duration, admitter Admitter, store ReservationStore) *Manager {
	t.Helper()
	manager, err := New(Config{ClusterEpoch: "epoch-1", MaxPipes: max, OpenTimeout: timeout}, admitter, store)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return manager
}

func testSession(id, clientID, keyID string, done <-chan struct{}) clientsession.Session {
	return clientsession.Session{Ref: clientsession.Ref{
		ClientSessionID: id,
		ClientID:        clientID,
		APIKeyID:        keyID,
		AuthRevision:    "revision-1",
	}, Done: done}
}

func testSlot(clientID string) routing.LiveBinding {
	ref := routing.ListenerBindingRef{
		GatewayID:         "gateway-1",
		GatewayInstanceID: "instance-1",
		ListenerBindingID: "binding-1",
	}
	return routing.LiveBinding{
		Key: routing.BindingKey{
			ClientID:        clientID,
			EndpointPattern: testEndpoint,
			TargetID:        testTarget,
		},
		Ref: ref,
	}
}

func newOpenContext(t *testing.T, attemptID string, caller clientsession.Ref, slot routing.LiveBinding) routing.OpenContext {
	t.Helper()
	return newForwardedOpenContext(t, attemptID, caller, slot, time.Now().Add(time.Minute))
}

func newForwardedOpenContext(
	t *testing.T,
	attemptID string,
	caller clientsession.Ref,
	slot routing.LiveBinding,
	expiresAt time.Time,
) routing.OpenContext {
	t.Helper()
	open, err := routing.NewForwardedOpenContext(
		"epoch-1",
		"authority-1",
		attemptID,
		routing.AuthContext{
			ClientSessionID: caller.ClientSessionID,
			ClientID:        caller.ClientID,
			APIKeyID:        caller.APIKeyID,
			AuthRevision:    caller.AuthRevision,
		},
		slot,
		routing.ForwardingContext{
			IngressGatewayID:         "gateway-1",
			IngressGatewayInstanceID: "instance-1",
			IngressControlSessionID:  "control-1",
			OwnerControlSessionID:    "owner-control-1",
			OwnerRelayAddress:        "127.0.0.1:27430",
			ExpiresAt:                expiresAt,
		},
	)
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	return open
}

func slotsEqual(left, right routing.LiveBinding) bool {
	return left == right
}

func gateAdmissionError(gates [6]bool) error {
	if !gates[1] || !gates[2] || !gates[4] {
		return routing.ErrOpenUnavailable
	}
	if !gates[3] {
		return routing.ErrRouteNotFound
	}
	return nil
}

func waitActiveCount(t *testing.T, manager *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.ActiveCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("ActiveCount() = %d, want %d", manager.ActiveCount(), want)
}

func waitPendingCount(t *testing.T, manager *Manager, want uint32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		got := manager.pending
		manager.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	manager.mu.Lock()
	got := manager.pending
	manager.mu.Unlock()
	t.Fatalf("pending termination slots = %d, want %d", got, want)
}
