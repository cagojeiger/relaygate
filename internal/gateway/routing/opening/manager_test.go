package opening

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
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

func TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown(t *testing.T) {
	confirmEntered := make(chan struct{})
	endpoint := &scriptedEndpoint{confirm: func(ctx context.Context, _ localbinding.Confirmation) error {
		close(confirmEntered)
		<-ctx.Done()
		return context.Cause(ctx)
	}}
	h := newHarness(t, 1, time.Second, endpoint)
	result := make(chan error, 1)
	go func() {
		_, err := h.manager.Open(context.Background(), h.caller, testEndpoint, testTarget)
		result <- err
	}()
	<-confirmEntered

	// Listener acceptance already crossed the Open LP. Owner shutdown cannot
	// turn the lost confirmation into a stable failure or recover the Pipe.
	h.manager.Close()
	if err := <-result; !errors.Is(err, ErrUnknown) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Open() after accept/confirmation loss/owner shutdown = %v, want Unknown wrapping Unavailable", err)
	}
	termination := receiveTermination(t, endpoint)
	if termination.AttemptID != "attempt-1" || termination.PipeID == "" {
		t.Fatalf("termination = %#v, want exact accepted attempt with unrecoverable PipeID", termination)
	}
	if h.manager.ActiveCount() != 0 {
		t.Fatalf("ActiveCount() after owner shutdown = %d", h.manager.ActiveCount())
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

	h.admitter.setError(authority.ErrOpenUnavailable)
	if second, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget); !errors.Is(err, ErrUnavailable) || second.PipeID != "" {
		t.Fatalf("OpenPipe(after authority loss) = %#v, %v, want unavailable without Pipe", second, err)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("failed future admission changed accepted Pipe count to %d", h.manager.ActiveCount())
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("still-live")); err != nil {
		t.Fatalf("RelayPayload(existing Pipe): %v", err)
	}
	payload := receivePayload(t, listenerEndpoint)
	if payload.PipeID != result.PipeID || string(payload.Data) != "still-live" {
		t.Fatalf("existing Pipe payload = %#v", payload)
	}
}

func TestRelayPayloadRoutesBothDirectionsWithPerDirectionFIFO(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	listenerEndpoint := &scriptedEndpoint{deliver: func(ctx context.Context, payload localbinding.PipePayload) error {
		if string(payload.Data) != "caller-1" {
			return nil
		}
		close(firstEntered)
		select {
		case <-releaseFirst:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(): %v", err)
	}
	if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ActivatePipe() rejected exact caller")
	}

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("caller-1"))
	}()
	first := receivePayload(t, listenerEndpoint)
	<-firstEntered

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("caller-2"))
	}()
	reverseResult := make(chan error, 1)
	go func() {
		reverseResult <- h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, []byte("listener-1"))
	}()
	reverse := receivePayload(t, callerEndpoint)
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)
	close(releaseFirst)

	if err := <-firstResult; err != nil {
		t.Fatalf("first caller payload: %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second caller payload: %v", err)
	}
	if err := <-reverseResult; err != nil {
		t.Fatalf("listener payload: %v", err)
	}
	second := receivePayload(t, listenerEndpoint)
	if first.PipeID != result.PipeID || string(first.Data) != "caller-1" ||
		second.PipeID != result.PipeID || string(second.Data) != "caller-2" {
		t.Fatalf("caller-to-listener payloads = %#v, %#v", first, second)
	}
	if reverse.PipeID != result.PipeID || string(reverse.Data) != "listener-1" {
		t.Fatalf("listener-to-caller payload = %#v", reverse)
	}
}

func TestRelayPayloadWaitsForExactCallerActivation(t *testing.T) {
	listenerEndpoint := &scriptedEndpoint{}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(): %v", err)
	}
	started := make(chan struct{})
	relayed := make(chan error, 1)
	go func() {
		close(started)
		relayed <- h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, []byte("early"))
	}()
	<-started
	assertNoPayload(t, callerEndpoint, 20*time.Millisecond)
	select {
	case err := <-relayed:
		t.Fatalf("pre-activation payload returned early: %v", err)
	default:
	}

	foreign := h.caller.Ref
	foreign.AuthRevision = "other-revision"
	if h.manager.ActivatePipe(foreign, result.PipeID) {
		t.Fatal("foreign full session ref activated Pipe")
	}
	assertNoPayload(t, callerEndpoint, 20*time.Millisecond)
	firstActivation := h.manager.ActivatePipe(h.caller.Ref, result.PipeID)
	secondActivation := h.manager.ActivatePipe(h.caller.Ref, result.PipeID)
	if !firstActivation || !secondActivation {
		t.Fatal("exact activation was not idempotent")
	}
	if err := <-relayed; err != nil {
		t.Fatalf("gated RelayPayload(): %v", err)
	}
	payload := receivePayload(t, callerEndpoint)
	if payload.PipeID != result.PipeID || string(payload.Data) != "early" {
		t.Fatalf("activated payload = %#v", payload)
	}
}

func TestRelayPayloadActivationWaitIsBoundedAndTerminalizesPipe(t *testing.T) {
	listenerEndpoint := &scriptedEndpoint{}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, 30*time.Millisecond, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(): %v", err)
	}
	started := time.Now()
	if err := h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, []byte("early")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pre-activation RelayPayload() = %v, want ErrUnavailable", err)
	}
	if elapsed := time.Since(started); elapsed < h.manager.config.OpenTimeout || elapsed > time.Second {
		t.Fatalf("activation wait = %v, want bounded near %v", elapsed, h.manager.config.OpenTimeout)
	}
	if snapshot, ok := h.manager.Inspect(result.AttemptID); !ok || snapshot.State != StateTerminal || h.manager.ActiveCount() != 0 {
		t.Fatalf("activation timeout did not terminalize Pipe: %#v, found=%v active=%d", snapshot, ok, h.manager.ActiveCount())
	}
	assertNoPayload(t, callerEndpoint, 20*time.Millisecond)
	if termination := receiveTermination(t, listenerEndpoint); termination.PipeID != result.PipeID {
		t.Fatalf("listener termination = %#v, want PipeID %q", termination, result.PipeID)
	}
	if h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ActivatePipe() revived terminal Pipe")
	}
	if err := h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, []byte("late")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("terminal RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
}

func TestRelayPayloadRejectsForeignUnknownAndTerminalWithoutMutation(t *testing.T) {
	listenerEndpoint := &scriptedEndpoint{}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(): %v", err)
	}
	if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ActivatePipe() rejected exact caller")
	}
	foreign := h.caller.Ref
	foreign.APIKeyID = "foreign-key"
	if err := h.manager.RelayPayload(context.Background(), foreign, result.PipeID, []byte("foreign")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("foreign RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, "unknown-pipe", []byte("unknown")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("unknown RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("foreign/unknown payload changed active count to %d", h.manager.ActiveCount())
	}
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)

	if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ClosePipe() rejected exact caller")
	}
	if err := h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, []byte("terminal")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("terminal RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	if h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("terminal Pipe reactivated")
	}
	assertNoPayload(t, callerEndpoint, 20*time.Millisecond)
}

func TestRelayPayloadSizeBoundariesAndCopy(t *testing.T) {
	listenerEndpoint := &scriptedEndpoint{}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(): %v", err)
	}
	if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ActivatePipe() rejected exact caller")
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, nil); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("empty RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, make([]byte, localbinding.MaxPayloadBytes+1)); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("oversized RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	var nilContext context.Context
	if err := h.manager.RelayPayload(nilContext, h.caller.Ref, result.PipeID, []byte{1}); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("nil-context RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, "", []byte{1}); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("empty-PipeID RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, strings.Repeat("p", routing.MaxIdentityBytes+1), []byte{1}); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("oversized-PipeID RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("invalid payload changed active count to %d", h.manager.ActiveCount())
	}
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)

	maximum := make([]byte, localbinding.MaxPayloadBytes)
	maximum[0] = 'a'
	maximum[len(maximum)-1] = 'z'
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, maximum); err != nil {
		t.Fatalf("maximum RelayPayload(): %v", err)
	}
	maximum[0] = 'x'
	delivered := receivePayload(t, listenerEndpoint)
	if delivered.PipeID != result.PipeID || len(delivered.Data) != localbinding.MaxPayloadBytes || delivered.Data[0] != 'a' || delivered.Data[len(delivered.Data)-1] != 'z' {
		t.Fatalf("maximum payload was not copied exactly: id=%q len=%d edges=%q/%q", delivered.PipeID, len(delivered.Data), delivered.Data[0], delivered.Data[len(delivered.Data)-1])
	}
}

func TestPayloadDeliveryFailureTerminalizesOnceAndNotifiesBothEndpoints(t *testing.T) {
	for _, test := range []struct {
		name       string
		delivery   error
		wantStable error
	}{
		{name: "backpressure", delivery: localbinding.ErrPayloadBackpressure, wantStable: ErrPayloadBackpressure},
		{name: "unavailable", delivery: errors.New("transport write failed"), wantStable: ErrUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			listenerEndpoint := &scriptedEndpoint{deliver: func(context.Context, localbinding.PipePayload) error {
				return test.delivery
			}}
			callerEndpoint := &scriptedEndpoint{}
			h := newHarness(t, 1, time.Second, listenerEndpoint)
			defer h.manager.Close()

			result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
			if err != nil {
				t.Fatalf("OpenPipe(): %v", err)
			}
			if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
				t.Fatal("ActivatePipe() rejected exact caller")
			}
			if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("payload")); !errors.Is(err, test.wantStable) {
				t.Fatalf("RelayPayload() = %v, want %v", err, test.wantStable)
			}
			if snapshot, ok := h.manager.Inspect(result.AttemptID); !ok || snapshot.State != StateTerminal || !errors.Is(snapshot.Err, ErrUnknown) || !errors.Is(snapshot.Err, test.wantStable) {
				t.Fatalf("payload-failure snapshot = %#v, found=%v", snapshot, ok)
			}
			if h.manager.ActiveCount() != 0 {
				t.Fatalf("ActiveCount() = %d after payload failure", h.manager.ActiveCount())
			}
			if termination := receiveTermination(t, listenerEndpoint); termination.PipeID != result.PipeID {
				t.Fatalf("listener termination = %#v", termination)
			}
			if pipeID := receivePipeTermination(t, callerEndpoint); pipeID != result.PipeID {
				t.Fatalf("caller termination = %q, want %q", pipeID, result.PipeID)
			}
			if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("again")); !errors.Is(err, ErrPipeNotOwned) {
				t.Fatalf("terminal RelayPayload() = %v, want ErrPipeNotOwned", err)
			}
			if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
				t.Fatal("terminal ClosePipe() lost bounded owned history")
			}
			assertNoTermination(t, listenerEndpoint, 20*time.Millisecond)
			assertNoPipeTermination(t, callerEndpoint, 20*time.Millisecond)
		})
	}
}

func TestClosePipeCancelsInFlightPayloadAndBypassesDelivery(t *testing.T) {
	deliveryStarted := make(chan struct{})
	deliveryCanceled := make(chan struct{})
	listenerEndpoint := &scriptedEndpoint{deliver: func(ctx context.Context, _ localbinding.PipePayload) error {
		close(deliveryStarted)
		<-ctx.Done()
		close(deliveryCanceled)
		return ctx.Err()
	}}
	callerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, listenerEndpoint)
	defer h.manager.Close()

	result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("OpenPipe(): %v", err)
	}
	if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ActivatePipe() rejected exact caller")
	}
	relayed := make(chan error, 1)
	go func() {
		relayed <- h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("blocked"))
	}()
	waitClosed(t, deliveryStarted, "payload delivery did not start")
	if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ClosePipe() rejected exact caller")
	}
	waitClosed(t, deliveryCanceled, "ClosePipe did not cancel in-flight payload context")
	if err := <-relayed; !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("canceled in-flight RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	if termination := receiveTermination(t, listenerEndpoint); termination.PipeID != result.PipeID {
		t.Fatalf("listener termination = %#v", termination)
	}
	if pipeID := receivePipeTermination(t, callerEndpoint); pipeID != result.PipeID {
		t.Fatalf("caller termination = %q, want %q", pipeID, result.PipeID)
	}
}

func TestAcceptedPipeTerminationFanoutIsConcurrentAndPreAcceptDoesNotNotifyCaller(t *testing.T) {
	t.Run("accepted concurrent fanout", func(t *testing.T) {
		listenerStarted := make(chan struct{})
		callerStarted := make(chan struct{})
		release := make(chan struct{})
		listenerEndpoint := &scriptedEndpoint{terminate: func(ctx context.Context, _ localbinding.Termination) error {
			close(listenerStarted)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		callerEndpoint := &scriptedEndpoint{terminatePipe: func(ctx context.Context, _ string) error {
			close(callerStarted)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		h := newHarness(t, 1, time.Second, listenerEndpoint)
		defer h.manager.Close()
		result, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
		if err != nil {
			t.Fatalf("OpenPipe(): %v", err)
		}
		if !h.manager.ActivatePipe(h.caller.Ref, result.PipeID) {
			t.Fatal("ActivatePipe() rejected exact caller")
		}
		if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
			t.Fatal("ClosePipe() rejected exact caller")
		}
		waitClosed(t, listenerStarted, "listener termination did not start")
		waitClosed(t, callerStarted, "caller termination did not start concurrently")
		close(release)
		waitPendingCount(t, h.manager, 0)
	})

	t.Run("pre-accept failure", func(t *testing.T) {
		listenerEndpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error {
			return localbinding.ErrOfferRejected
		}}
		callerEndpoint := &scriptedEndpoint{}
		h := newHarness(t, 1, time.Second, listenerEndpoint)
		defer h.manager.Close()
		if _, err := h.manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget); !errors.Is(err, ErrListenerRejected) {
			t.Fatalf("OpenPipe() = %v, want ErrListenerRejected", err)
		}
		assertNoPipeTermination(t, callerEndpoint, 20*time.Millisecond)
	})
}

func TestTerminalPreActivationPayloadIsNotReplayedIntoNewPipe(t *testing.T) {
	listenerEndpoint := &scriptedEndpoint{}
	firstCallerEndpoint := &scriptedEndpoint{}
	h := newHarness(t, 1, time.Second, listenerEndpoint)
	defer h.manager.Close()

	first, err := h.manager.OpenPipe(context.Background(), h.caller, firstCallerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("first OpenPipe(): %v", err)
	}
	started := make(chan struct{})
	oldResult := make(chan error, 1)
	go func() {
		close(started)
		oldResult <- h.manager.RelayPayload(context.Background(), h.caller.Ref, first.PipeID, []byte("old"))
	}()
	<-started
	if !h.manager.ClosePipe(h.caller.Ref, first.PipeID) {
		t.Fatal("ClosePipe(first) rejected exact caller")
	}
	if err := <-oldResult; !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("pre-activation terminal RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	receiveTermination(t, listenerEndpoint)
	assertNoPipeTermination(t, firstCallerEndpoint, 20*time.Millisecond)
	if h.manager.ActivatePipe(h.caller.Ref, first.PipeID) {
		t.Fatal("terminal Pipe activated after caller-facing PipeOpened write")
	}
	assertNoPipeTermination(t, firstCallerEndpoint, 20*time.Millisecond)
	waitPendingCount(t, h.manager, 0)
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)

	h.admitter.setContext(newOpenContext(t, "attempt-2", h.caller.Ref, h.slot))
	secondCallerEndpoint := &scriptedEndpoint{}
	second, err := h.manager.OpenPipe(context.Background(), h.caller, secondCallerEndpoint, testEndpoint, testTarget)
	if err != nil {
		t.Fatalf("second OpenPipe(): %v", err)
	}
	if second.PipeID == first.PipeID {
		t.Fatal("new OpenPipe reused terminal PipeID")
	}
	if !h.manager.ActivatePipe(h.caller.Ref, second.PipeID) {
		t.Fatal("ActivatePipe(second) rejected exact caller")
	}
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, second.PipeID, []byte("new")); err != nil {
		t.Fatalf("new RelayPayload(): %v", err)
	}
	payload := receivePayload(t, listenerEndpoint)
	if payload.PipeID != second.PipeID || string(payload.Data) != "new" {
		t.Fatalf("new Pipe payload = %#v", payload)
	}
}

func TestForwardedOwnerSingleUseExpiryAndFailedGuard(t *testing.T) {
	fixedNow := time.Unix(2_000_000_000, 0)

	t.Run("exact reservation is single use", func(t *testing.T) {
		h := newHarness(t, 2, time.Second, &scriptedEndpoint{})
		defer h.manager.Close()
		h.manager.now = func() time.Time { return fixedNow }
		open := newForwardedOpenContext(t, "forwarded-1", h.caller.Ref, h.slot, fixedNow.Add(time.Minute))
		callerEndpoint := &scriptedEndpoint{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		result, err := h.manager.OpenForwarded(ctx, open, callerEndpoint)
		if err != nil || result.PipeID == "" {
			t.Fatalf("OpenForwarded() = %#v, %v", result, err)
		}
		duplicateCtx, cancelDuplicate := context.WithCancel(context.Background())
		defer cancelDuplicate()
		if _, err := h.manager.OpenForwarded(duplicateCtx, open.Clone(), callerEndpoint); !errors.Is(err, ErrAttemptReplay) {
			t.Fatalf("duplicate OpenForwarded() = %v, want ErrAttemptReplay", err)
		}
		if h.endpoint.offerCount() != 1 || h.endpoint.confirmCount() != 1 {
			t.Fatalf("duplicate forwarded attempt reached listener: offers=%d confirms=%d", h.endpoint.offerCount(), h.endpoint.confirmCount())
		}
	})

	t.Run("failed exact owner guard does not consume attempt", func(t *testing.T) {
		h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
		defer h.manager.Close()
		h.manager.now = func() time.Time { return fixedNow }
		open := newForwardedOpenContext(t, "forwarded-retry", h.caller.Ref, h.slot, fixedNow.Add(time.Minute))
		h.store.setError(localbinding.ErrNotFound)
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		if _, err := h.manager.OpenForwarded(firstCtx, open, &scriptedEndpoint{}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("first OpenForwarded() = %v, want ErrNotFound", err)
		}
		cancelFirst()
		h.store.setError(nil)
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		defer cancelSecond()
		if result, err := h.manager.OpenForwarded(secondCtx, open.Clone(), &scriptedEndpoint{}); err != nil || result.PipeID == "" {
			t.Fatalf("retry OpenForwarded() = %#v, %v", result, err)
		}
	})

	t.Run("listener rejection keeps serialized attempt consumed", func(t *testing.T) {
		listenerEndpoint := &scriptedEndpoint{offer: func(context.Context, localbinding.Offer) error {
			return localbinding.ErrOfferRejected
		}}
		h := newHarness(t, 1, time.Second, listenerEndpoint)
		defer h.manager.Close()
		h.manager.now = func() time.Time { return fixedNow }
		open := newForwardedOpenContext(t, "forwarded-rejected", h.caller.Ref, h.slot, fixedNow.Add(time.Minute))
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		if _, err := h.manager.OpenForwarded(firstCtx, open, &scriptedEndpoint{}); !errors.Is(err, ErrListenerRejected) {
			t.Fatalf("first OpenForwarded() = %v, want ErrListenerRejected", err)
		}
		cancelFirst()
		listenerEndpoint.mu.Lock()
		listenerEndpoint.offer = nil
		listenerEndpoint.mu.Unlock()
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		defer cancelSecond()
		if _, err := h.manager.OpenForwarded(secondCtx, open.Clone(), &scriptedEndpoint{}); !errors.Is(err, ErrAttemptReplay) {
			t.Fatalf("replayed rejected OpenForwarded() = %v, want ErrAttemptReplay", err)
		}
		if listenerEndpoint.offerCount() != 1 {
			t.Fatalf("replayed rejected attempt offered %d times, want 1", listenerEndpoint.offerCount())
		}
	})

	t.Run("expired and bounded replay cache fail closed", func(t *testing.T) {
		h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
		defer h.manager.Close()
		now := fixedNow
		h.manager.now = func() time.Time { return now }
		first := newForwardedOpenContext(t, "forwarded-cache-1", h.caller.Ref, h.slot, fixedNow.Add(time.Minute))
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		result, err := h.manager.OpenForwarded(firstCtx, first, &scriptedEndpoint{})
		if err != nil {
			t.Fatalf("first OpenForwarded(): %v", err)
		}
		if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
			t.Fatal("ClosePipe(first forwarded) failed")
		}
		cancelFirst()
		receiveTermination(t, h.endpoint)
		waitPendingCount(t, h.manager, 0)

		second := newForwardedOpenContext(t, "forwarded-cache-2", h.caller.Ref, h.slot, fixedNow.Add(2*time.Minute))
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		defer cancelSecond()
		if _, err := h.manager.OpenForwarded(secondCtx, second, &scriptedEndpoint{}); !errors.Is(err, ErrCapacity) {
			t.Fatalf("OpenForwarded(full replay cache) = %v, want ErrCapacity", err)
		}

		now = fixedNow.Add(90 * time.Second)
		secondResult, err := h.manager.OpenForwarded(secondCtx, second.Clone(), &scriptedEndpoint{})
		if err != nil || secondResult.PipeID == "" {
			t.Fatalf("OpenForwarded(after expiry prune) = %#v, %v", secondResult, err)
		}
		if !h.manager.ClosePipe(h.caller.Ref, secondResult.PipeID) {
			t.Fatal("ClosePipe(second forwarded) failed")
		}
		receiveTermination(t, h.endpoint)
		waitPendingCount(t, h.manager, 0)
		expired := newForwardedOpenContext(t, "forwarded-expired", h.caller.Ref, h.slot, fixedNow.Add(time.Minute))
		expiredCtx, cancelExpired := context.WithCancel(context.Background())
		defer cancelExpired()
		if _, err := h.manager.OpenForwarded(expiredCtx, expired, &scriptedEndpoint{}); !errors.Is(err, ErrContextExpired) {
			t.Fatalf("OpenForwarded(expired) = %v, want ErrContextExpired", err)
		}
	})

	t.Run("response loss plus duplicate across expiry never replays outcome", func(t *testing.T) {
		endpoint := &scriptedEndpoint{confirm: func(context.Context, localbinding.Confirmation) error {
			return localbinding.ErrEndpointUnavailable
		}}
		h := newHarness(t, 1, time.Second, endpoint)
		defer h.manager.Close()
		now := fixedNow
		h.manager.now = func() time.Time { return now }
		open := newForwardedOpenContext(t, "forwarded-response-loss", h.caller.Ref, h.slot, fixedNow.Add(time.Minute))

		firstCtx, cancelFirst := context.WithCancel(context.Background())
		defer cancelFirst()
		if _, err := h.manager.OpenForwarded(firstCtx, open, &scriptedEndpoint{}); !errors.Is(err, ErrUnknown) {
			t.Fatalf("OpenForwarded(response loss) = %v, want ErrUnknown", err)
		}
		termination := receiveTermination(t, endpoint)
		if termination.AttemptID != open.AttemptID || termination.PipeID == "" {
			t.Fatalf("response-loss termination = %#v", termination)
		}
		waitPendingCount(t, h.manager, 0)
		duplicateCtx, cancelDuplicate := context.WithCancel(context.Background())
		defer cancelDuplicate()
		if _, err := h.manager.OpenForwarded(duplicateCtx, open.Clone(), &scriptedEndpoint{}); !errors.Is(err, ErrAttemptReplay) {
			t.Fatalf("OpenForwarded(duplicate before expiry) = %v, want ErrAttemptReplay", err)
		}
		if endpoint.offerCount() != 1 || endpoint.confirmCount() != 1 {
			t.Fatalf("response-loss duplicate reached listener: offers=%d confirms=%d", endpoint.offerCount(), endpoint.confirmCount())
		}

		now = fixedNow.Add(time.Minute)
		expiredCtx, cancelExpired := context.WithCancel(context.Background())
		defer cancelExpired()
		if _, err := h.manager.OpenForwarded(expiredCtx, open.Clone(), &scriptedEndpoint{}); !errors.Is(err, ErrAttemptReplay) {
			t.Fatalf("OpenForwarded(duplicate at expiry) = %v, want retained terminal replay rejection", err)
		}
		if endpoint.offerCount() != 1 || endpoint.confirmCount() != 1 {
			t.Fatalf("expired attempt replayed prior outcome: offers=%d confirms=%d", endpoint.offerCount(), endpoint.confirmCount())
		}
	})
}

func TestRemoteIngressActivationPayloadAndClose(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	remoteSlot := cloneSlot(h.slot)
	remoteSlot.Ref.GatewayID = "gateway-2"
	remoteSlot.Ref.GatewayInstanceID = "instance-2"
	remote := newScriptedRemoteEndpoint()
	remoteOpener := &fakeRemoteOpener{result: RemoteResult{
		AttemptID: "remote-attempt",
		PipeID:    "remote-pipe",
		Binding:   remoteSlot,
		Endpoint:  remote,
	}}
	manager, err := New(Config{ClusterEpoch: "epoch-1", MaxPipes: 1, OpenTimeout: time.Second}, h.admitter, h.store, remoteOpener)
	if err != nil {
		t.Fatalf("New(remote): %v", err)
	}
	defer manager.Close()
	h.admitter.setContext(newForwardedOpenContext(t, "remote-attempt", h.caller.Ref, remoteSlot, time.Now().Add(time.Minute)))
	callerEndpoint := &scriptedEndpoint{}

	result, err := manager.OpenPipe(context.Background(), h.caller, callerEndpoint, testEndpoint, testTarget)
	if err != nil || result.PipeID != "remote-pipe" || !slotsEqual(result.Binding, remoteSlot) {
		t.Fatalf("remote OpenPipe() = %#v, %v", result, err)
	}
	if h.store.callCount() != 0 || remoteOpener.callCount() != 1 {
		t.Fatalf("remote dispatch calls: local reserve=%d remote=%d", h.store.callCount(), remoteOpener.callCount())
	}
	if err := manager.RelayPayload(context.Background(), clientsession.Ref{}, result.PipeID, []byte("forged")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("RelayPayload(zero remote sender) = %v, want ErrPipeNotOwned", err)
	}
	assertNoPayload(t, remote.scriptedEndpoint, 20*time.Millisecond)
	activated := manager.ActivatePipe(h.caller.Ref, result.PipeID)
	if !activated || remote.activationCount() != 1 {
		t.Fatalf("remote activation = ok/count %v/%d", activated, remote.activationCount())
	}
	if err := manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, []byte("across-gateways")); err != nil {
		t.Fatalf("RelayPayload(remote): %v", err)
	}
	payload := receivePayload(t, remote.scriptedEndpoint)
	if payload.PipeID != result.PipeID || string(payload.Data) != "across-gateways" {
		t.Fatalf("remote payload = %#v", payload)
	}
	if !manager.ClosePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ClosePipe(remote) rejected exact caller")
	}
	if pipeID := receivePipeTermination(t, callerEndpoint); pipeID != result.PipeID {
		t.Fatalf("caller terminal PipeID = %q", pipeID)
	}
	remote.waitClosed(t)
	waitPendingCount(t, manager, 0)
}

func TestRemoteOwnerAcceptedThenIngressTerminalIsUnknown(t *testing.T) {
	h := newHarness(t, 1, time.Second, &scriptedEndpoint{})
	remoteSlot := cloneSlot(h.slot)
	remoteSlot.Ref.GatewayID = "gateway-2"
	remoteSlot.Ref.GatewayInstanceID = "instance-2"
	remote := newScriptedRemoteEndpoint()
	remoteOpener := &fakeRemoteOpener{result: RemoteResult{
		AttemptID: "remote-race",
		PipeID:    "remote-race-pipe",
		Binding:   remoteSlot,
		Endpoint:  remote,
	}}
	manager, err := New(Config{ClusterEpoch: "epoch-1", MaxPipes: 1, OpenTimeout: time.Second}, h.admitter, h.store, remoteOpener)
	if err != nil {
		t.Fatalf("New(remote): %v", err)
	}
	defer manager.Close()
	h.admitter.setContext(newForwardedOpenContext(t, "remote-race", h.caller.Ref, remoteSlot, time.Now().Add(time.Minute)))
	remoteOpener.beforeReturn = func() {
		if got := manager.RetireSession(h.caller.Ref); got != 1 {
			t.Errorf("RetireSession() = %d, want in-flight ingress entry", got)
		}
	}

	if _, err := manager.OpenPipe(context.Background(), h.caller, &scriptedEndpoint{}, testEndpoint, testTarget); !errors.Is(err, ErrUnknown) || !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("OpenPipe(owner accepted then ingress terminal) = %v, want Unknown wrapping session end", err)
	}
	remote.waitClosed(t)
	waitActiveCount(t, manager, 0)
}

func TestRemoteIngressActivationFailureAndMalformedResultTerminalize(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*RemoteResult, *scriptedRemoteEndpoint)
		activate  bool
	}{
		{name: "activation failure", activate: true, configure: func(_ *RemoteResult, endpoint *scriptedRemoteEndpoint) {
			endpoint.activateErr = errors.New("hop activation failed")
		}},
		{name: "non exact owner result", configure: func(result *RemoteResult, _ *scriptedRemoteEndpoint) {
			result.Binding.Ref.ListenerBindingID = "different-binding"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, 1, 50*time.Millisecond, &scriptedEndpoint{})
			remoteSlot := cloneSlot(h.slot)
			remoteSlot.Ref.GatewayID = "gateway-2"
			remoteSlot.Ref.GatewayInstanceID = "instance-2"
			endpoint := newScriptedRemoteEndpoint()
			remoteResult := RemoteResult{AttemptID: "remote-attempt", PipeID: "remote-pipe", Binding: remoteSlot, Endpoint: endpoint}
			test.configure(&remoteResult, endpoint)
			remoteOpener := &fakeRemoteOpener{result: remoteResult}
			manager, err := New(Config{ClusterEpoch: "epoch-1", MaxPipes: 1, OpenTimeout: 50 * time.Millisecond}, h.admitter, h.store, remoteOpener)
			if err != nil {
				t.Fatalf("New(remote): %v", err)
			}
			defer manager.Close()
			h.admitter.setContext(newForwardedOpenContext(t, "remote-attempt", h.caller.Ref, remoteSlot, time.Now().Add(time.Minute)))
			result, openErr := manager.OpenPipe(context.Background(), h.caller, &scriptedEndpoint{}, testEndpoint, testTarget)
			if test.activate {
				if openErr != nil {
					t.Fatalf("OpenPipe(): %v", openErr)
				}
				if manager.ActivatePipe(h.caller.Ref, result.PipeID) {
					t.Fatal("failed remote activation reported success")
				}
			} else if !errors.Is(openErr, ErrUnknown) {
				t.Fatalf("OpenPipe(non-exact result) = %v, want ErrUnknown", openErr)
			}
			endpoint.waitClosed(t)
			waitActiveCount(t, manager, 0)
			waitPendingCount(t, manager, 0)
		})
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
	slot         routing.LiveBinding
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

func (f *fakeStore) Reserve(open authority.OpenContext, caller clientsession.Ref) (localbinding.Reservation, error) {
	return f.reserve(open, caller, true)
}

func (f *fakeStore) ReserveForwarded(open authority.OpenContext, caller clientsession.Ref) (localbinding.Reservation, error) {
	return f.reserve(open, caller, false)
}

func (f *fakeStore) reserve(open authority.OpenContext, caller clientsession.Ref, consume bool) (localbinding.Reservation, error) {
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

func (f *fakeRemoteOpener) Open(_ context.Context, _ authority.OpenContext, callerEndpoint localbinding.CallerEndpoint) (RemoteResult, error) {
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

func newOpenContext(t *testing.T, attemptID string, caller clientsession.Ref, slot routing.LiveBinding) authority.OpenContext {
	t.Helper()
	return newForwardedOpenContext(t, attemptID, caller, slot, time.Now().Add(time.Minute))
}

func newForwardedOpenContext(
	t *testing.T,
	attemptID string,
	caller clientsession.Ref,
	slot routing.LiveBinding,
	expiresAt time.Time,
) authority.OpenContext {
	t.Helper()
	open, err := authority.NewForwardedOpenContext(
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
		authority.ForwardingContext{
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
