package opening

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	"github.com/cagojeiger/relaygate/internal/localbinding"
)

func TestOpenExactSuccessAndAcceptedCapacityLifetime(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	defer h.manager.Close()

	result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if result.AttemptID != "attempt-1" || result.PipeID == "" || !slotsEqual(result.Binding, h.slot) {
		t.Fatalf("Open() result = %#v", result)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("ActiveCount() = %d, want accepted pipe capacity 1", h.manager.ActiveCount())
	}
	snapshot, ok := h.manager.Inspect(result.AttemptID)
	if !ok || snapshot.State != StateAccepted || snapshot.PipeID != result.PipeID {
		t.Fatalf("accepted snapshot = %#v, found=%v", snapshot, ok)
	}
	if h.endpoint.offerCount() != 1 || h.endpoint.confirmCount() != 1 {
		t.Fatalf("endpoint calls = offer %d confirm %d", h.endpoint.offerCount(), h.endpoint.confirmCount())
	}

	if got := h.manager.RetireSession(h.listener.Ref); got != 1 {
		t.Fatalf("RetireSession(listener) = %d, want 1", got)
	}
	if h.manager.ActiveCount() != 0 {
		t.Fatalf("ActiveCount() after listener retirement = %d", h.manager.ActiveCount())
	}
	snapshot, ok = h.manager.Inspect(result.AttemptID)
	if !ok || snapshot.State != StateTerminal || !errors.Is(snapshot.Err, ErrUnknown) || !errors.Is(snapshot.Err, ErrSessionEnded) {
		t.Fatalf("terminal snapshot = %#v, found=%v", snapshot, ok)
	}
	termination := receiveTermination(t, h.endpoint)
	if termination.AttemptID != result.AttemptID || termination.PipeID != result.PipeID {
		t.Fatalf("termination = %#v, want exact accepted pipe", termination)
	}
	if h.context.TryConsume() {
		t.Fatal("accepted capability became reusable after terminal retirement")
	}
}

func TestOpenListenerRejectAndDeadlineHaveNoPipe(t *testing.T) {
	t.Run("reject", func(t *testing.T) {
		endpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error {
			return localbinding.ErrOfferRejected
		}}
		h := newHarness(t, 1, time.Second, endpoint)
		defer h.manager.Close()

		result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
		if !errors.Is(err, ErrListenerRejected) || result.PipeID != "" {
			t.Fatalf("Open() = %#v, %v, want listener rejection without PipeID", result, err)
		}
		if endpoint.confirmCount() != 0 || h.manager.ActiveCount() != 0 {
			t.Fatalf("reject confirm/active = %d/%d", endpoint.confirmCount(), h.manager.ActiveCount())
		}
	})

	t.Run("deadline", func(t *testing.T) {
		endpoint := &scriptedEndpoint{offer: func(ctx context.Context, _ localbinding.Offer) error {
			<-ctx.Done()
			return context.Cause(ctx)
		}}
		h := newHarness(t, 1, 20*time.Millisecond, endpoint)
		defer h.manager.Close()

		result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
		if !errors.Is(err, ErrDeadline) || errors.Is(err, ErrUnknown) || result.PipeID != "" {
			t.Fatalf("Open() = %#v, %v, want pre-accept deadline", result, err)
		}
		if endpoint.confirmCount() != 0 || h.manager.ActiveCount() != 0 {
			t.Fatalf("deadline confirm/active = %d/%d", endpoint.confirmCount(), h.manager.ActiveCount())
		}
	})
}

func TestOfferEndpointFailureClassification(t *testing.T) {
	for _, test := range []struct {
		name     string
		endpoint error
		want     error
	}{
		{name: "transport unavailable", endpoint: localbinding.ErrEndpointUnavailable, want: ErrUnavailable},
		{name: "listener session ended", endpoint: localbinding.ErrSessionEnded, want: ErrSessionEnded},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error { return test.endpoint }}
			h := newHarness(t, 1, time.Second, endpoint)
			defer h.manager.Close()
			if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, test.want) || errors.Is(err, ErrListenerRejected) {
				t.Fatalf("Open() error = %v, want %v without listener rejection", err, test.want)
			}
		})
	}
}

func TestOpenAcceptVersusCancelBothOrders(t *testing.T) {
	t.Run("cancel first makes late accept no-op", func(t *testing.T) {
		offerEntered := make(chan struct{})
		allowLateAccept := make(chan struct{})
		endpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error {
			close(offerEntered)
			<-allowLateAccept
			return nil
		}}
		h := newHarness(t, 1, time.Second, endpoint)
		defer h.manager.Close()
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := h.manager.Open(ctx, h.caller, testEndpoint, testTarget)
			result <- err
		}()
		<-offerEntered
		cancel()
		waitActiveCount(t, h.manager, 0)
		close(allowLateAccept)
		if err := <-result; !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnknown) {
			t.Fatalf("cancel-first Open() error = %v", err)
		}
		if endpoint.confirmCount() != 0 {
			t.Fatalf("late accept sent %d confirmations", endpoint.confirmCount())
		}
		if termination := receiveTermination(t, endpoint); termination.PipeID != "" {
			t.Fatalf("late provisional termination = %#v, want no PipeID", termination)
		}
	})

	t.Run("accept first makes cancellation unknown", func(t *testing.T) {
		confirmEntered := make(chan struct{})
		endpoint := &scriptedEndpoint{confirm: func(ctx context.Context, _ localbinding.Confirmation) error {
			close(confirmEntered)
			<-ctx.Done()
			return context.Cause(ctx)
		}}
		h := newHarness(t, 1, time.Second, endpoint)
		defer h.manager.Close()
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := h.manager.Open(ctx, h.caller, testEndpoint, testTarget)
			result <- err
		}()
		<-confirmEntered
		cancel()
		if err := <-result; !errors.Is(err, ErrUnknown) || !errors.Is(err, context.Canceled) {
			t.Fatalf("accept-first Open() error = %v, want Unknown wrapping cancel", err)
		}
		termination := receiveTermination(t, endpoint)
		if termination.PipeID == "" {
			t.Fatalf("accepted cancellation termination = %#v, want PipeID", termination)
		}
	})
}

func TestLateOriginalAttemptDeadlineIsNoOpAfterAccepted(t *testing.T) {
	h := newHarness(t, 1, 100*time.Millisecond, &scriptedEndpoint{})
	defer h.manager.Close()
	result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	time.Sleep(2 * h.manager.config.OpenTimeout)
	snapshot, ok := h.manager.Inspect(result.AttemptID)
	if !ok || snapshot.State != StateAccepted || snapshot.PipeID != result.PipeID {
		t.Fatalf("late deadline changed accepted state: %#v, found=%v", snapshot, ok)
	}
}

func TestSuccessfulOpenDetachesAttemptContextFromPipeLifetime(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	defer h.manager.Close()
	ctx, cancel := context.WithCancel(context.Background())
	result, err := h.manager.Open(ctx, h.caller, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)

	snapshot, ok := h.manager.Inspect(result.AttemptID)
	if !ok || snapshot.State != StateAccepted || snapshot.PipeID != result.PipeID || h.manager.ActiveCount() != 1 {
		t.Fatalf("attempt context ended accepted Pipe: %#v, found=%v active=%d", snapshot, ok, h.manager.ActiveCount())
	}
	if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ClosePipe did not terminalize detached Pipe")
	}
	if termination := receiveTermination(t, h.endpoint); termination.PipeID != result.PipeID {
		t.Fatalf("termination = %#v", termination)
	}
}

func TestConfirmationLossIsUnknown(t *testing.T) {
	endpoint := &scriptedEndpoint{confirm: func(context.Context, localbinding.Confirmation) error {
		return localbinding.ErrEndpointUnavailable
	}}
	h := newHarness(t, 1, time.Second, endpoint)
	defer h.manager.Close()
	result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if !errors.Is(err, ErrUnknown) || !errors.Is(err, ErrUnavailable) || result.PipeID != "" {
		t.Fatalf("Open() = %#v, %v, want Unknown after owner accept", result, err)
	}
	termination := receiveTermination(t, endpoint)
	if termination.PipeID == "" {
		t.Fatalf("confirmation-loss termination = %#v, want accepted PipeID", termination)
	}
}

func TestConfirmationTimeoutIsUnknown(t *testing.T) {
	endpoint := &scriptedEndpoint{confirm: func(ctx context.Context, _ localbinding.Confirmation) error {
		<-ctx.Done()
		return context.Cause(ctx)
	}}
	h := newHarness(t, 1, 20*time.Millisecond, endpoint)
	defer h.manager.Close()
	result, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
	if !errors.Is(err, ErrUnknown) || !errors.Is(err, ErrDeadline) || result.PipeID != "" {
		t.Fatalf("Open() = %#v, %v, want Unknown confirmation timeout", result, err)
	}
	if termination := receiveTermination(t, endpoint); termination.PipeID == "" {
		t.Fatalf("confirmation-timeout termination = %#v, want accepted PipeID", termination)
	}
}

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

func TestClosePipeIsExactSessionOwnedAndIdempotent(t *testing.T) {
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
		t.Fatal("foreign session closed caller Pipe")
	}
	if h.manager.ActiveCount() != 2 {
		t.Fatalf("foreign close changed active count to %d", h.manager.ActiveCount())
	}

	if !h.manager.ClosePipe(h.caller.Ref, first.PipeID) {
		t.Fatal("caller could not close owned Pipe")
	}
	if termination := receiveTermination(t, endpoint); termination.PipeID != first.PipeID {
		t.Fatalf("first termination = %#v", termination)
	}
	if snapshot, ok := h.manager.Inspect(second.AttemptID); !ok || snapshot.State != StateAccepted {
		t.Fatalf("second Pipe changed while closing first: %#v, found=%v", snapshot, ok)
	}
	if !h.manager.ClosePipe(h.caller.Ref, first.PipeID) {
		t.Fatal("duplicate owned close was not idempotent")
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

func TestOpenRejectsRemoteOwnerAndSameSession(t *testing.T) {
	t.Run("remote", func(t *testing.T) {
		h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
		defer h.manager.Close()
		remoteSlot := cloneSlot(h.slot)
		remoteSlot.Ref.GatewayID = "gateway-2"
		h.context = newOpenContext(t, "attempt-remote", h.caller.Ref, remoteSlot)
		h.admitter.setContext(h.context)
		if _, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget); !errors.Is(err, ErrRemoteOwnerUnsupported) {
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

const (
	testEndpoint = "/events"
	testTarget   = "worker"
)

type openHarness struct {
	manager      *Manager
	admitter     *fakeAdmitter
	store        *fakeStore
	endpoint     *scriptedEndpoint
	caller       clientsession.Session
	listener     clientsession.Session
	slot         controlstate.BindingSlot
	context      authority.OpenContext
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
	context authority.OpenContext
	err     error
	calls   int
}

func (f *fakeAdmitter) AdmitOpen(context.Context, clientsession.Ref, string, string) (authority.OpenContext, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.context.Clone(), f.err
}

func (f *fakeAdmitter) setContext(open authority.OpenContext) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.context = open
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

func (f *fakeStore) Reserve(open authority.OpenContext, caller clientsession.Ref) (localbinding.Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return localbinding.Reservation{}, f.err
	}
	if !open.TryConsume() {
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

type scriptedEndpoint struct {
	mu            sync.Mutex
	offer         func(context.Context, localbinding.Offer) error
	confirm       func(context.Context, localbinding.Confirmation) error
	terminate     func(context.Context, localbinding.Termination) error
	offers        []localbinding.Offer
	confirmations []localbinding.Confirmation
	terminations  chan localbinding.Termination
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

func testSlot(clientID string) controlstate.BindingSlot {
	ref := controlstate.ListenerBindingRef{
		GatewayID:         "gateway-1",
		GatewayInstanceID: "instance-1",
		ListenerBindingID: "binding-1",
	}
	return controlstate.BindingSlot{
		Key: controlstate.BindingKey{
			ClientID:        clientID,
			EndpointPattern: testEndpoint,
			TargetID:        testTarget,
		},
		Generation: 1,
		Ref:        &ref,
	}
}

func newOpenContext(t *testing.T, attemptID string, caller clientsession.Ref, slot controlstate.BindingSlot) authority.OpenContext {
	t.Helper()
	open, err := authority.NewOpenContext("epoch-1", "authority-1", attemptID, authority.AuthContext{
		ClientSessionID: caller.ClientSessionID,
		ClientID:        caller.ClientID,
		APIKeyID:        caller.APIKeyID,
		AuthRevision:    caller.AuthRevision,
	}, slot)
	if err != nil {
		t.Fatalf("NewOpenContext(): %v", err)
	}
	return open
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

func gateAdmissionError(gates [6]bool) error {
	if !gates[1] || !gates[2] || !gates[4] {
		return authority.ErrOpenUnavailable
	}
	if !gates[3] {
		return authority.ErrRouteNotFound
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
