package opening

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

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
	if err := manager.RelayPayload(context.Background(), clientsession.Ref{}, result.PipeID, "payload-id", []byte("forged")); !errors.Is(err, ErrPipeNotOwned) {
		t.Fatalf("RelayPayload(zero remote sender) = %v, want ErrPipeNotOwned", err)
	}
	assertNoPayload(t, remote.scriptedEndpoint, 20*time.Millisecond)
	activated := manager.ActivatePipe(h.caller.Ref, result.PipeID)
	if !activated || remote.activationCount() != 1 {
		t.Fatalf("remote activation = ok/count %v/%d", activated, remote.activationCount())
	}
	if err := manager.RelayPayload(context.Background(), h.caller.Ref, result.PipeID, "payload-id", []byte("across-gateways")); err != nil {
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
