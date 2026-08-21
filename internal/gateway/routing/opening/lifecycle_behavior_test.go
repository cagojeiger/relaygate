package opening

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

func TestSessionDoneTerminalizesAcceptedPipe(t *testing.T) {
	for _, test := range []struct {
		name string
		end  func(*openHarness)
	}{
		{name: "caller", end: func(h *openHarness) { close(h.callerDone) }},
		{name: "listener", end: func(h *openHarness) { close(h.listenerDone) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
			defer h.manager.Close()
			result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
			if err != nil {
				t.Fatalf("Open(): %v", err)
			}
			test.end(h)
			waitActiveCount(t, h.manager, 0)
			snapshot, ok := h.manager.Inspect(result.AttemptID)
			if !ok || snapshot.State != StateTerminal || !errors.Is(snapshot.Err, ErrUnknown) || !errors.Is(snapshot.Err, ErrSessionEnded) {
				t.Fatalf("session-ended snapshot = %#v, found=%v", snapshot, ok)
			}
		})
	}
}

func TestCallerListenerAndCredentialRetirementSynchronouslyTerminalize(t *testing.T) {
	for _, test := range []struct {
		name   string
		retire func(*openHarness) int
	}{
		{name: "caller", retire: func(h *openHarness) int { return h.manager.RetireSession(h.caller.Ref) }},
		{name: "listener", retire: func(h *openHarness) int { return h.manager.RetireSession(h.listener.Ref) }},
		{name: "caller credential", retire: func(h *openHarness) int {
			return h.manager.Retire(clientauth.ChangeSet{Removed: []clientauth.CredentialID{{ClientID: h.caller.Ref.ClientID, APIKeyID: h.caller.Ref.APIKeyID}}})
		}},
		{name: "listener credential", retire: func(h *openHarness) int {
			return h.manager.Retire(clientauth.ChangeSet{Removed: []clientauth.CredentialID{{ClientID: h.listener.Ref.ClientID, APIKeyID: h.listener.Ref.APIKeyID}}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
			defer h.manager.Close()
			result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
			if err != nil {
				t.Fatalf("Open(): %v", err)
			}
			if got := test.retire(h); got != 1 {
				t.Fatalf("retire() = %d, want 1", got)
			}
			snapshot, ok := h.manager.Inspect(result.AttemptID)
			if !ok || snapshot.State != StateTerminal || h.manager.ActiveCount() != 0 {
				t.Fatalf("retirement state = %#v, found=%v active=%d", snapshot, ok, h.manager.ActiveCount())
			}
			if got := test.retire(h); got != 0 {
				t.Fatalf("duplicate retire() = %d", got)
			}
		})
	}
}

func TestClosePipeIsExactParticipantOwnedAndIdempotent(t *testing.T) {
	endpoint := &scriptedEndpoint{}
	h := newSequenceHarness(t, 2, endpoint)
	defer h.manager.Close()

	first, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("first Open(): %v", err)
	}
	h.admitter.setContext(newOpenContext(t, "attempt-2", h.caller.Ref, h.slot))
	second, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("second Open(): %v", err)
	}

	foreign := h.caller.Ref
	foreign.ClientSessionID = "other-session"
	if h.manager.ClosePipe(foreign, first.PipeID) {
		t.Fatal("foreign session closed participant Pipe")
	}
	if h.manager.ClosePipe(h.listener.Ref, "unknown-pipe") {
		t.Fatal("listener session closed unknown Pipe")
	}
	if h.manager.ActiveCount() != 2 {
		t.Fatalf("foreign/unknown close changed active count to %d", h.manager.ActiveCount())
	}

	if !h.manager.ClosePipe(h.listener.Ref, first.PipeID) {
		t.Fatal("listener could not close participant Pipe")
	}
	if termination := receiveTermination(t, endpoint); termination.PipeID != first.PipeID {
		t.Fatalf("first termination = %#v", termination)
	}
	if snapshot, ok := h.manager.Inspect(second.AttemptID); !ok || snapshot.State != StateAccepted {
		t.Fatalf("second Pipe changed while closing first: %#v, found=%v", snapshot, ok)
	}
	if !h.manager.ClosePipe(h.listener.Ref, first.PipeID) {
		t.Fatal("duplicate listener close was not idempotent")
	}
	if !h.manager.ClosePipe(h.caller.Ref, first.PipeID) {
		t.Fatal("caller participant lost bounded terminal history")
	}
	assertNoTermination(t, endpoint, 20*time.Millisecond)

	if !h.manager.ClosePipe(h.caller.Ref, second.PipeID) {
		t.Fatal("caller could not close second Pipe")
	}
	if termination := receiveTermination(t, endpoint); termination.PipeID != second.PipeID {
		t.Fatalf("second termination = %#v", termination)
	}
	if h.manager.ActiveCount() != 0 {
		t.Fatalf("active count = %d, want 0", h.manager.ActiveCount())
	}
}

func TestClosePipeAndSessionRetirementHaveOneTerminalEffect(t *testing.T) {
	endpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, endpoint)
	defer h.manager.Close()
	result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		h.manager.ClosePipe(h.caller.Ref, result.PipeID)
	}()
	go func() {
		defer wait.Done()
		<-start
		h.manager.RetireSession(h.caller.Ref)
	}()
	close(start)
	wait.Wait()

	if termination := receiveTermination(t, endpoint); termination.PipeID != result.PipeID {
		t.Fatalf("termination = %#v", termination)
	}
	assertNoTermination(t, endpoint, 20*time.Millisecond)
	if snapshot, ok := h.manager.Inspect(result.AttemptID); !ok || snapshot.State != StateTerminal {
		t.Fatalf("terminal snapshot = %#v, found=%v", snapshot, ok)
	}
}

func TestOpenCapacityAndTerminalHistoryAreBounded(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	defer h.manager.Close()
	if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); err != nil {
		t.Fatalf("first Open(): %v", err)
	}
	if result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, ErrCapacity) || result.PipeID != "" {
		t.Fatalf("second Open() = %#v, %v, want capacity", result, err)
	}
	if h.admitter.callCount() != 1 || h.endpoint.offerCount() != 1 {
		t.Fatalf("capacity crossed admission: admit=%d offer=%d", h.admitter.callCount(), h.endpoint.offerCount())
	}

	endpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error { return localbinding.ErrOfferRejected }}
	terminalHarness := newSequenceHarness(t, 2, endpoint)
	defer terminalHarness.manager.Close()
	for index := 0; index < 7; index++ {
		fresh := newOpenContext(t, fmt.Sprintf("attempt-terminal-%d", index+1), terminalHarness.caller.Ref, terminalHarness.slot)
		terminalHarness.admitter.setContext(fresh)
		if _, err := terminalHarness.manager.Open(context.Background(), terminalHarness.caller, testEndpoint, testTarget); !errors.Is(err, ErrListenerRejected) {
			t.Fatalf("rejected Open %d = %v", index, err)
		}
	}
	terminalHarness.manager.mu.Lock()
	entries := len(terminalHarness.manager.entries)
	terminals := len(terminalHarness.manager.terminalOrder)
	attempts := len(terminalHarness.manager.byAttempt)
	pipes := len(terminalHarness.manager.byPipe)
	terminalHarness.manager.mu.Unlock()
	if entries > 2 || terminals > 2 || attempts > 2 || pipes > 2 {
		t.Fatalf("bounded terminal cache = entries %d terminals %d attempts %d pipes %d", entries, terminals, attempts, pipes)
	}
}

func TestOpenRejectsUnavailableRemoteRelayAndSameSession(t *testing.T) {
	t.Run("remote relay unavailable", func(t *testing.T) {
		h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
		defer h.manager.Close()
		remoteSlot := cloneSlot(h.slot)
		remoteSlot.Ref.GatewayID = "gateway-2"
		h.context = newOpenContext(t, "attempt-remote", h.caller.Ref, remoteSlot)
		h.admitter.setContext(h.context)
		if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, ErrRemoteRelayUnavailable) {
			t.Fatalf("Open(remote) = %v", err)
		}
		if h.store.callCount() != 0 || h.endpoint.offerCount() != 0 {
			t.Fatalf("remote owner reserved/offered = %d/%d", h.store.callCount(), h.endpoint.offerCount())
		}
		if !h.context.TryConsume() {
			t.Fatal("remote-owner rejection consumed the capability")
		}
	})

	t.Run("same session", func(t *testing.T) {
		h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
		defer h.manager.Close()
		h.store.listener = h.caller.Ref
		h.store.listenerDone = h.caller.Done
		if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Open(self) = %v", err)
		}
		if h.endpoint.offerCount() != 0 {
			t.Fatalf("self Open offered %d times", h.endpoint.offerCount())
		}
	})
}

func TestOpenRejectsExpiredLocalContextBeforeOffer(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	defer h.manager.Close()
	fixedNow := time.Unix(2_000_000_000, 0)
	h.manager.now = func() time.Time { return fixedNow }
	h.context = newForwardedOpenContext(t, "attempt-expired-local", h.caller.Ref, h.slot, fixedNow)
	h.admitter.setContext(h.context)

	if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, ErrContextExpired) {
		t.Fatalf("Open(expired local context) = %v, want ErrContextExpired", err)
	}
	if h.store.callCount() != 0 || h.endpoint.offerCount() != 0 {
		t.Fatalf("expired local context reserved/offered = %d/%d", h.store.callCount(), h.endpoint.offerCount())
	}
	if !h.context.TryConsume() {
		t.Fatal("expiry rejection consumed the capability")
	}
}

func TestSixGateAdmissionComposition(t *testing.T) {
	for mask := 0; mask < 64; mask++ {
		gates := [6]bool{}
		for index := range gates {
			gates[index] = mask&(1<<(5-index)) != 0
		}
		t.Run(fmt.Sprintf("%06b", mask), func(t *testing.T) {
			callerDone := make(chan struct{})
			caller := testSession("caller", "client-1", "caller-key", callerDone)
			if !gates[0] {
				close(callerDone)
			}
			listenerDone := make(chan struct{})
			listener := testSession("listener", "client-1", "listener-key", listenerDone)
			slot := testSlot(caller.Ref.ClientID)
			open := newOpenContext(t, "attempt-gates", caller.Ref, slot)
			endpoint := &scriptedEndpoint{}
			admitter := &fakeAdmitter{context: open, err: gateAdmissionError(gates)}
			store := &fakeStore{
				gatewayID:    "gateway-1",
				listener:     listener.Ref,
				listenerDone: listener.Done,
				endpoint:     endpoint,
			}
			if !gates[5] {
				store.err = localbinding.ErrNotFound
			}
			manager := mustOpeningManager(t, 1, time.Second, admitter, store)
			defer manager.Close()

			result, err := manager.Open(context.Background(), caller, testEndpoint, testTarget)
			admitted := mask == 63
			if admitted {
				if err != nil || result.PipeID == "" || endpoint.offerCount() != 1 {
					t.Fatalf("111111 result = %#v, err=%v offers=%d", result, err, endpoint.offerCount())
				}
				return
			}
			if err == nil || result.PipeID != "" || endpoint.offerCount() != 0 {
				t.Fatalf("not-admitted result = %#v, err=%v offers=%d", result, err, endpoint.offerCount())
			}
		})
	}
}

func TestCloseTerminalizesAcceptedAndTerminatesListener(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	h.manager.Close()
	if h.manager.ActiveCount() != 0 {
		t.Fatalf("ActiveCount() after Close = %d", h.manager.ActiveCount())
	}
	snapshot, ok := h.manager.Inspect(result.AttemptID)
	if !ok || snapshot.State != StateTerminal || !errors.Is(snapshot.Err, ErrUnknown) {
		t.Fatalf("closed snapshot = %#v, found=%v", snapshot, ok)
	}
	receiveTermination(t, h.endpoint)
}

func TestCloseCancelsAndJoinsTerminationWorkers(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	endpoint := &scriptedEndpoint{terminate: func(ctx context.Context, _ localbinding.Termination) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}}
	h := newHarness(t, 1, time.Second, endpoint)
	if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if got := h.manager.RetireAll(); got != 1 {
		t.Fatalf("RetireAll() = %d, want 1", got)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("termination worker did not start")
	}
	h.manager.Close()
	select {
	case <-stopped:
	default:
		t.Fatal("Close returned before the termination worker exited")
	}
}

func TestCloseJoinsInFlightOfferAndTerminatesLateProvisional(t *testing.T) {
	offerEntered := make(chan struct{})
	releaseOffer := make(chan struct{})
	endpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error {
		close(offerEntered)
		<-releaseOffer
		return nil
	}}
	h := newHarness(t, 1, time.Second, endpoint)
	openDone := make(chan error, 1)
	go func() {
		_, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
		openDone <- err
	}()
	<-offerEntered

	closeDone := make(chan struct{})
	go func() {
		h.manager.Close()
		close(closeDone)
	}()
	waitActiveCount(t, h.manager, 0)
	select {
	case <-closeDone:
		t.Fatal("Close returned while a listener Offer was still in flight")
	default:
	}

	close(releaseOffer)
	if err := <-openDone; !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnknown) {
		t.Fatalf("Open() after Close = %v, want known pre-accept unavailable", err)
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not join the in-flight Open")
	}
	if termination := receiveTermination(t, endpoint); termination.PipeID != "" {
		t.Fatalf("late provisional termination = %#v, want no PipeID", termination)
	}
	h.manager.mu.Lock()
	pending := h.manager.pending
	h.manager.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending termination slots after Close = %d, want 0", pending)
	}
	endpoint.mu.Lock()
	terminations := endpoint.terminations
	endpoint.mu.Unlock()
	select {
	case duplicate := <-terminations:
		t.Fatalf("duplicate late provisional termination = %#v", duplicate)
	default:
	}
	if endpoint.confirmCount() != 0 {
		t.Fatalf("late provisional accept sent %d confirmations", endpoint.confirmCount())
	}
}

func TestCanceledInFlightOfferErrorReleasesCleanupSlot(t *testing.T) {
	offerEntered := make(chan struct{})
	releaseOffer := make(chan struct{})
	endpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error {
		close(offerEntered)
		<-releaseOffer
		return localbinding.ErrEndpointUnavailable
	}}
	h := newHarness(t, 1, time.Second, endpoint)
	defer h.manager.Close()
	ctx, cancel := context.WithCancel(context.Background())
	openDone := make(chan error, 1)
	go func() {
		_, err := h.manager.Open(ctx, h.caller, testEndpoint, testTarget)
		openDone <- err
	}()
	<-offerEntered
	cancel()
	waitActiveCount(t, h.manager, 0)
	waitPendingCount(t, h.manager, 1)
	close(releaseOffer)
	if err := <-openDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() = %v, want cancellation that won before endpoint failure", err)
	}
	waitPendingCount(t, h.manager, 0)
	endpoint.mu.Lock()
	terminations := endpoint.terminations
	endpoint.mu.Unlock()
	if terminations != nil {
		select {
		case termination := <-terminations:
			t.Fatalf("failed Offer emitted termination = %#v", termination)
		default:
		}
	}
}

func TestPendingTerminationRetainsCapacityUntilCompletion(t *testing.T) {
	terminationStarted := make(chan struct{})
	releaseTermination := make(chan struct{})
	terminationDone := make(chan struct{}, 2)
	var started sync.Once
	endpoint := &scriptedEndpoint{terminate: func(ctx context.Context, _ localbinding.Termination) error {
		started.Do(func() { close(terminationStarted) })
		select {
		case <-releaseTermination:
		case <-ctx.Done():
			return ctx.Err()
		}
		terminationDone <- struct{}{}
		return nil
	}}
	h := newHarness(t, 1, time.Second, endpoint)
	defer h.manager.Close()

	if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); err != nil {
		t.Fatalf("first Open(): %v", err)
	}
	if got := h.manager.RetireAll(); got != 1 {
		t.Fatalf("RetireAll() = %d, want 1", got)
	}
	<-terminationStarted
	if h.manager.ActiveCount() != 0 {
		t.Fatalf("ActiveCount() while cleanup pending = %d, want terminal entries excluded", h.manager.ActiveCount())
	}
	h.manager.mu.Lock()
	pending := h.manager.pending
	h.manager.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pending termination slots = %d, want 1", pending)
	}

	second := newOpenContext(t, "attempt-2", h.caller.Ref, h.slot)
	h.admitter.setContext(second)
	admissions := h.admitter.callCount()
	if result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, ErrCapacity) || result.PipeID != "" {
		t.Fatalf("Open(cleanup pending) = %#v, %v, want capacity", result, err)
	}
	if h.admitter.callCount() != admissions || endpoint.offerCount() != 1 {
		t.Fatalf("cleanup capacity crossed admission/offer: admit=%d offer=%d", h.admitter.callCount(), endpoint.offerCount())
	}

	close(releaseTermination)
	select {
	case <-terminationDone:
	case <-time.After(time.Second):
		t.Fatal("listener termination did not complete")
	}
	waitPendingCount(t, h.manager, 0)
	if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); err != nil {
		t.Fatalf("Open() after cleanup completion: %v", err)
	}
}

func TestOpenPipeRequiresCallerEndpointBeforeAdmission(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	defer h.manager.Close()
	if _, err := h.manager.OpenPipe(context.Background(), h.caller, nil, testEndpoint, testTarget); !errors.Is(err, ErrInvalid) {
		t.Fatalf("OpenPipe(nil caller endpoint) = %v, want ErrInvalid", err)
	}
	if h.admitter.callCount() != 0 || h.manager.ActiveCount() != 0 {
		t.Fatalf("nil caller endpoint crossed admission: calls=%d active=%d", h.admitter.callCount(), h.manager.ActiveCount())
	}
}

func TestAcceptedPipeContinuesWhenFutureAdmissionIsUnavailable(t *testing.T) {
	listenerEndpoint := &scriptedEndpoint{}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 2, time.Second, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(first): %v", err)
	}
	if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ActivatePipe(first) rejected exact caller")
	}

	h.admitter.setError(routing.ErrOpenUnavailable)
	if second, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget); !errors.Is(err, ErrUnavailable) || second.PipeID != "" {
		t.Fatalf("OpenPipe(after authority loss) = %#v, %v, want unavailable without Pipe", second, err)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("failed future admission changed accepted Pipe count to %d", h.manager.ActiveCount())
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("still-live")); err != nil {
		t.Fatalf("RelayPayload(existing Pipe): %v", err)
	}
	payload := receivePayload(t, listenerEndpoint)
	if payload.PipeID != result.PipeID || string(payload.Data) != "still-live" {
		t.Fatalf("existing Pipe payload = %#v", payload)
	}
}
