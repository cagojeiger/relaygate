package opening

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

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
		firstResult <- h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("caller-1"))
	}()
	first := receivePayload(t, listenerEndpoint)
	<-firstEntered

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("caller-2"))
	}()
	reverseResult := make(chan error, 1)
	go func() {
		reverseResult <- h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, "payload-id", []byte("listener-1"))
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
		relayed <- h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, "payload-id", []byte("early"))
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
	if err := h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, "payload-id", []byte("early")); !errors.Is(err, ErrUnavailable) {
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
	if err := h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, "payload-id", []byte("late")); !errors.Is(err, ErrPipeNotOwned) {
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
	if err := h.manager.RelayPayload(context.Background(), foreign, result.PipeID, "payload-id", []byte("foreign")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("foreign RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, "unknown-pipe", "payload-id", []byte("unknown")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("unknown RelayPayload() = %v, want ErrPipeNotOwned", err)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("foreign/unknown payload changed active count to %d", h.manager.ActiveCount())
	}
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)

	if !h.manager.ClosePipe(h.caller.Ref, result.PipeID) {
		t.Fatal("ClosePipe() rejected exact caller")
	}
	if err := h.manager.RelayPayload(context.Background(), h.listener.Ref, result.PipeID, "payload-id", []byte("terminal")); !errors.Is(err, ErrPipeNotOwned) {
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
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", nil); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("empty RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", make([]byte, localbinding.MaxPayloadBytes+1)); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("oversized RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	var nilContext context.Context
	if err := h.manager.RelayPayload(nilContext, h.caller.Ref, result.PipeID, "payload-id", []byte{1}); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("nil-context RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, "", "payload-id", []byte{1}); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("empty-PipeID RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, strings.Repeat("p", routing.MaxIdentityBytes+1), "payload-id", []byte{1}); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("oversized-PipeID RelayPayload() = %v, want ErrPayloadInvalid", err)
	}
	if h.manager.ActiveCount() != 1 {
		t.Fatalf("invalid payload changed active count to %d", h.manager.ActiveCount())
	}
	assertNoPayload(t, listenerEndpoint, 20*time.Millisecond)

	maximum := make([]byte, localbinding.MaxPayloadBytes)
	maximum[0] = 'a'
	maximum[len(maximum)-1] = 'z'
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", maximum); err != nil {
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
			if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("payload")); !errors.Is(err, test.wantStable) {
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
			if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("again")); !errors.Is(err, ErrPipeNotOwned) {
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
		relayed <- h.manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("blocked"))
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
		oldResult <- h.manager.RelayPayload(context.Background(), h.caller.Ref, first.PipeID, "payload-id", []byte("old"))
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
	if err := h.manager.RelayPayload(context.Background(), h.caller.Ref, second.PipeID, "payload-id", []byte("new")); err != nil {
		t.Fatalf("new RelayPayload(): %v", err)
	}
	payload := receivePayload(t, listenerEndpoint)
	if payload.PipeID != second.PipeID || string(payload.Data) != "new" {
		t.Fatalf("new Pipe payload = %#v", payload)
	}
}
