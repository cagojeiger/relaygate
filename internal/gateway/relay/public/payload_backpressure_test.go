package relaygrpc

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
)

func TestStreamCoordinatorOrdersPreActivationTerminalAfterPipeOpened(t *testing.T) {
	stream := newGateRecordingRelayStream(false)
	actor := newOutboundActor(stream, make(chan struct{}, 8), time.Second)
	defer actor.close()
	pipeEndpoint := newStreamPipeEndpoint(actor, time.Second)
	opener := &testOpener{
		open: func(context.Context, clientsession.Session, string, string) (opening.Result, error) {
			return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1"}, nil
		},
		activatePipe: func(clientsession.Ref, string) bool { return false },
	}
	session := testPayloadSession()
	service := &Service{opener: opener}
	coordinator := newStreamCoordinator(context.Background(), session, opener, pipeEndpoint, actor, make(chan struct{}, 1))
	if response := coordinator.startOpen(context.Background(), service, &relayv1.Open{RequestId: "request-1", Endpoint: "one", TargetId: "worker"}); response != nil {
		t.Fatalf("startOpen() = %#v", response)
	}
	first := receiveWithin(t, stream.sent, "PipeOpened")
	second := receiveWithin(t, stream.sent, "PipeTerminated")
	if first.GetPipeOpened().GetPipeId() != "pipe-1" || second.GetPipeTerminated().GetPipeId() != "pipe-1" {
		t.Fatalf("responses = %#v then %#v, want PipeOpened then exact PipeTerminated", first, second)
	}
	coordinator.close()
}

func TestOutboundActorPayloadPressureAndControlBypass(t *testing.T) {
	stream := newGateRecordingRelayStream(true)
	slots := make(chan struct{}, 64)
	actor := newOutboundActor(stream, slots, 10*payloadTestTimeout)
	defer actor.close()

	payloadResults := make(chan error, 1+outboundPayloadQueueCapacity)
	go func() {
		payloadResults <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("in-flight")))
	}()
	receiveWithin(t, stream.entered, "blocked payload Send")
	actor.payloadTimeout = payloadTestTimeout
	for index := 0; index < cap(actor.payloadQueue); index++ {
		go func(value byte) {
			payloadResults <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte{value}))
		}(byte(index))
	}
	waitForCondition(t, func() bool { return len(actor.payloadQueue) == cap(actor.payloadQueue) }, "full per-stream payload lane")

	controlDone := make(chan error, 1)
	go func() {
		controlDone <- actor.send(context.Background(), &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeTerminated{
			PipeTerminated: &relayv1.PipeTerminated{PipeId: "pipe-1"},
		}})
	}()
	waitForCondition(t, func() bool { return len(actor.queue) == 1 }, "queued terminal control")

	started := time.Now()
	overflowResults := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			overflowResults <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("overflow")))
		}()
	}
	for index := 0; index < 2; index++ {
		if err := receiveWithin(t, overflowResults, "bounded payload overflow"); !errors.Is(err, localbinding.ErrPayloadBackpressure) {
			t.Fatalf("overflow sendPayload error = %v, want ErrPayloadBackpressure", err)
		}
	}
	if elapsed := time.Since(started); elapsed > 10*payloadTestTimeout {
		t.Fatalf("queue and enqueue-gate waits = %v, want one bounded payload timeout", elapsed)
	}

	close(stream.releaseFirst)
	first := receiveWithin(t, stream.sent, "first payload")
	second := receiveWithin(t, stream.sent, "bypassing terminal")
	if !bytes.Equal(first.GetPipePayload().GetPayload(), []byte("in-flight")) || second.GetPipeTerminated().GetPipeId() != "pipe-1" {
		t.Fatalf("first two sends = %#v then %#v, want payload then terminal control", first, second)
	}
	if err := receiveWithin(t, controlDone, "terminal control completion"); err != nil {
		t.Fatalf("terminal control send: %v", err)
	}
	for index := 0; index < 1+cap(actor.payloadQueue); index++ {
		_ = receiveWithin(t, payloadResults, "payload completion or bounded timeout")
	}
	waitForCondition(t, func() bool { return len(slots) == 0 }, "payload slot drain")
}

func TestOutboundActorSharesGlobalPayloadBudget(t *testing.T) {
	slots := make(chan struct{}, 1)
	blockedStream := newGateRecordingRelayStream(true)
	blocked := newOutboundActor(blockedStream, slots, 10*payloadTestTimeout)
	defer blocked.close()
	otherStream := newGateRecordingRelayStream(false)
	other := newOutboundActor(otherStream, slots, payloadTestTimeout)
	defer other.close()

	holderResult := make(chan error, 1)
	go func() {
		holderResult <- blocked.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("hold")))
	}()
	receiveWithin(t, blockedStream.entered, "global payload slot holder")
	if err := other.sendPayload(context.Background(), payloadResponse("pipe-2", []byte("blocked"))); !errors.Is(err, localbinding.ErrPayloadBackpressure) {
		t.Fatalf("cross-stream sendPayload error = %v, want ErrPayloadBackpressure", err)
	}
	close(blockedStream.releaseFirst)
	receiveWithin(t, blockedStream.sent, "global slot holder completion")
	_ = receiveWithin(t, holderResult, "global slot holder result")
	waitForCondition(t, func() bool { return len(slots) == 0 }, "global payload slot release")
}

func TestX07BackpressureCancelAndParticipantCrashReleaseAllPayloadSlots(t *testing.T) {
	stream := newGateRecordingRelayStream(true)
	slots := make(chan struct{}, 128)
	actor := newOutboundActor(stream, slots, time.Second)

	results := make(chan error, 1+outboundPayloadQueueCapacity+8)
	go func() {
		results <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("in-flight")))
	}()
	receiveWithin(t, stream.entered, "blocked payload Send")
	for index := 0; index < cap(actor.payloadQueue); index++ {
		go func(value byte) {
			results <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte{value}))
		}(byte(index))
	}
	waitForCondition(t, func() bool { return len(actor.payloadQueue) == cap(actor.payloadQueue) }, "full payload lane before close")

	const waiters = 8
	for index := 0; index < waiters; index++ {
		go func(value byte) {
			results <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte{value}))
		}(byte(index))
	}
	waitForCondition(t, func() bool {
		return len(slots) == 1+cap(actor.payloadQueue)+waiters
	}, "queued and gate-waiting payload slots")

	actor.close()
	waitForCondition(t, func() bool { return len(slots) == 1 }, "queued and waiting slot release while one Send remains in flight")
	close(stream.releaseFirst)
	for index := 0; index < 1+cap(actor.payloadQueue)+waiters; index++ {
		_ = receiveWithin(t, results, "payload result after actor close")
	}
	waitForCondition(t, func() bool { return len(slots) == 0 }, "all payload slots after actor close")
}

func TestOutboundActorSkipsCanceledQueuedPayloadWithoutReplay(t *testing.T) {
	stream := newGateRecordingRelayStream(true)
	slots := make(chan struct{}, 1)
	actor := newOutboundActor(stream, slots, time.Second)
	defer actor.close()

	controlDone := make(chan error, 1)
	go func() {
		controlDone <- actor.send(context.Background(), &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
			ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "control"},
		}})
	}()
	receiveWithin(t, stream.entered, "blocked control Send")
	pipeCtx, cancelPipe := context.WithCancel(context.Background())
	oldResult := make(chan error, 1)
	go func() {
		oldResult <- actor.sendPayload(pipeCtx, payloadResponse("old-pipe", []byte("must-not-replay")))
	}()
	waitForCondition(t, func() bool { return len(actor.payloadQueue) == 1 }, "queued old payload")
	cancelPipe()
	if err := receiveWithin(t, oldResult, "canceled old payload"); !errors.Is(err, context.Canceled) {
		t.Fatalf("old payload result = %v, want context.Canceled", err)
	}
	close(stream.releaseFirst)
	if response := receiveWithin(t, stream.sent, "control Send"); response.GetListenerUnbound() == nil {
		t.Fatalf("first response = %#v, want control", response)
	}
	if err := receiveWithin(t, controlDone, "control completion"); err != nil {
		t.Fatalf("control send: %v", err)
	}
	waitForCondition(t, func() bool { return len(slots) == 0 }, "canceled queued payload slot")
	select {
	case response := <-stream.sent:
		t.Fatalf("terminal queued payload was replayed: %#v", response)
	case <-time.After(20 * time.Millisecond):
	}

	if err := actor.sendPayload(context.Background(), payloadResponse("new-pipe", []byte("new"))); err != nil {
		t.Fatalf("sendPayload(new): %v", err)
	}
	if response := receiveWithin(t, stream.sent, "new payload"); !bytes.Equal(response.GetPipePayload().GetPayload(), []byte("new")) {
		t.Fatalf("new payload response = %#v", response)
	}
}

func TestOutboundActorTimeoutCancelsQueuedPayloadBeforeReturning(t *testing.T) {
	stream := newGateRecordingRelayStream(true)
	slots := make(chan struct{}, 2)
	actor := newOutboundActor(stream, slots, payloadTestTimeout)
	defer actor.close()

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("in-flight")))
	}()
	receiveWithin(t, stream.entered, "blocked first payload Send")

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("timed-out-queued")))
	}()
	waitForCondition(t, func() bool { return len(actor.payloadQueue) == 1 }, "queued second payload")
	if err := receiveWithin(t, secondResult, "second payload timeout"); !errors.Is(err, localbinding.ErrPayloadBackpressure) {
		t.Fatalf("second payload result = %v, want ErrPayloadBackpressure", err)
	}

	close(stream.releaseFirst)
	if response := receiveWithin(t, stream.sent, "in-flight first payload"); !bytes.Equal(response.GetPipePayload().GetPayload(), []byte("in-flight")) {
		t.Fatalf("first response = %#v", response)
	}
	_ = receiveWithin(t, firstResult, "first payload result")
	waitForCondition(t, func() bool { return len(slots) == 0 }, "timed-out queued payload slot drain")
	select {
	case response := <-stream.sent:
		t.Fatalf("timed-out queued payload was sent late: %#v", response)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestOutboundActorInFlightPayloadTimeoutFailsStreamBeforeReturning(t *testing.T) {
	stream := newNthSendGateRecordingRelayStream(2)
	slots := make(chan struct{}, 2)
	actor := newOutboundActor(stream, slots, payloadTestTimeout)
	defer actor.close()

	if err := actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("first"))); err != nil {
		t.Fatalf("sendPayload(first): %v", err)
	}
	if response := receiveWithin(t, stream.sent, "first payload"); !bytes.Equal(response.GetPipePayload().GetPayload(), []byte("first")) {
		t.Fatalf("first response = %#v", response)
	}

	go func() {
		<-actor.failures()
		stream.cancel()
	}()
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- actor.sendPayload(context.Background(), payloadResponse("pipe-1", []byte("timed-out-in-flight")))
	}()
	receiveWithin(t, stream.entered, "blocked second payload Send")
	if err := receiveWithin(t, secondResult, "in-flight payload timeout"); err == nil {
		t.Fatal("in-flight payload timeout returned nil")
	}
	waitForCondition(t, func() bool { return len(slots) == 0 }, "in-flight payload slot drain")
	select {
	case response := <-stream.sent:
		t.Fatalf("timed-out in-flight payload was sent: %#v", response)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStreamPipeTerminalQueuePressureFailsActor(t *testing.T) {
	stream := newGateRecordingRelayStream(true)
	actor := newOutboundActor(stream, make(chan struct{}, 8), payloadTestTimeout)
	response := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
		ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "fill"},
	}}
	actor.queue <- outboundMessage{ctx: context.Background(), response: response}
	receiveWithin(t, stream.entered, "blocked control Send")
	for index := 0; index < cap(actor.queue); index++ {
		actor.queue <- outboundMessage{ctx: context.Background(), response: response}
	}

	endpoint := newStreamPipeEndpoint(actor, payloadTestTimeout)
	if err := endpoint.TerminatePipe(context.Background(), "pipe-1"); err == nil {
		t.Fatal("TerminatePipe() succeeded with a full control lane")
	}
	select {
	case <-actor.done:
	case <-time.After(time.Second):
		t.Fatal("bounded PipeTerminated delivery failure did not fail the actor")
	}
	close(stream.releaseFirst)
}

func TestBlockedPipeOpenedTerminalFallbackRetiresSession(t *testing.T) {
	session := testPayloadSession()
	sessions := &recordingPayloadSessionManager{session: session, ended: make(chan clientsession.Ref, 1)}
	bindingsRetired := make(chan clientsession.Ref, 1)
	bindings := &testBindingManager{retire: func(ref clientsession.Ref) int {
		bindingsRetired <- ref
		return 0
	}}
	endpointSeen := make(chan localbinding.CallerEndpoint, 1)
	openerRetired := make(chan clientsession.Ref, 1)
	closedPipe := make(chan string, 1)
	var activations atomic.Int32
	opener := &testOpener{
		openPipe: func(_ context.Context, _ clientsession.Session, endpoint localbinding.CallerEndpoint, _, _ string) (opening.Result, error) {
			endpointSeen <- endpoint
			return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1"}, nil
		},
		activatePipe: func(clientsession.Ref, string) bool {
			activations.Add(1)
			return true
		},
		closePipe: func(_ clientsession.Ref, pipeID string) bool {
			closedPipe <- pipeID
			return true
		},
		retire: func(ref clientsession.Ref) int {
			openerRetired <- ref
			return 0
		},
	}
	service, err := NewService(sessions, bindings, opener, time.Second, payloadTestTimeout, 1)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	stream := newConnectBlockingRelayStream()
	connectDone := make(chan error, 1)
	go func() { connectDone <- service.Connect(stream) }()

	stream.requests <- authenticateRequest("client-a", "key-a", "secret-a")
	if response := receiveWithin(t, stream.sent, "ClientSessionOpened"); response.GetClientSessionOpened() == nil {
		t.Fatalf("first response = %#v, want ClientSessionOpened", response)
	}
	stream.requests <- openRequest("request-1", "one", "worker")
	endpoint := receiveWithin(t, endpointSeen, "OpenPipe caller endpoint")
	receiveWithin(t, stream.pipeOpenedEntered, "blocked PipeOpened Send")

	if err := endpoint.TerminatePipe(context.Background(), "pipe-1"); err == nil {
		t.Fatal("TerminatePipe() succeeded while PipeOpened Send was blocked")
	}
	if err := receiveWithin(t, connectDone, "Connect failure after terminal fallback"); err == nil {
		t.Fatal("Connect() returned nil after outbound actor failure")
	}
	if activations.Load() != 0 {
		t.Fatalf("ActivatePipe calls = %d, want 0 after failed PipeOpened Send", activations.Load())
	}
	if pipeID := receiveWithin(t, closedPipe, "failed PipeOpened cleanup"); pipeID != "pipe-1" {
		t.Fatalf("closed Pipe = %q", pipeID)
	}
	for label, retired := range map[string]<-chan clientsession.Ref{
		"opening": openerRetired,
		"binding": bindingsRetired,
		"session": sessions.ended,
	} {
		if ref := receiveWithin(t, retired, label+" session retirement"); ref != session.Ref {
			t.Fatalf("%s retired session = %#v, want %#v", label, ref, session.Ref)
		}
	}
	close(stream.releasePipeOpened)
	stream.cancel()
}

func payloadResponse(pipeID string, payload []byte) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayload{
		PipePayload: &relayv1.PipePayload{PipeId: pipeID, PayloadId: "payload-test", Payload: payload},
	}}
}

func testPayloadSession() clientsession.Session {
	return clientsession.Session{Ref: clientsession.Ref{
		ClientSessionID: "session-1",
		ClientID:        "client-a",
		APIKeyID:        "key-a",
		AuthRevision:    "revision-1",
	}, Done: make(chan struct{})}
}

func waitForCondition(t *testing.T, condition func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
