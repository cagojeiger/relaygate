package opening

import (
	"context"
	"errors"
	"testing"
	"time"

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
