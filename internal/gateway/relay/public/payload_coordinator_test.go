package relaygrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewServiceCapsProcessPayloadSlots(t *testing.T) {
	store, err := clientauth.NewStore(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	sessions, err := clientsession.NewManager(store, 1)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	defer sessions.Close()

	for _, test := range []struct {
		name string
		max  uint32
		want int
	}{
		{name: "below cap", max: 7, want: 7},
		{name: "above cap", max: 2048, want: maxGlobalPayloadSlots},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewService(sessions, &testBindingManager{}, &testOpener{}, time.Second, time.Second, test.max)
			if err != nil {
				t.Fatalf("NewService(): %v", err)
			}
			if got := cap(service.payloadSlots); got != test.want {
				t.Fatalf("payload slot capacity = %d, want %d", got, test.want)
			}
		})
	}
}

func TestStreamPipeEndpointReceiptCorrelationIsExactAndBounded(t *testing.T) {
	endpoint := &streamPipeEndpoint{
		pending: make(map[payloadReceiptKey]pendingPayloadReceipt),
		history: make(map[payloadReceiptKey]payloadReceiptOutcome),
	}
	key := payloadReceiptKey{pipeID: "pipe-1", payloadID: "payload-1"}
	result := make(chan error, 1)
	fingerprint := sha256.Sum256([]byte("same"))
	endpoint.pending[key] = pendingPayloadReceipt{result: result, hash: fingerprint}
	if err := endpoint.acknowledge(key.pipeID, key.payloadID); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("receipt result: %v", err)
	}
	if err := endpoint.acknowledge(key.pipeID, key.payloadID); err != nil {
		t.Fatalf("exact receipt replay: %v", err)
	}
	if err := endpoint.reject(key.pipeID, key.payloadID, relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE); err == nil {
		t.Fatal("conflicting rejection replay succeeded")
	}
	if err := endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{
		PipeID: key.pipeID, PayloadID: key.payloadID, Data: []byte("same"),
	}); err != nil {
		t.Fatalf("exact payload replay: %v", err)
	}
	if err := endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{
		PipeID: key.pipeID, PayloadID: key.payloadID, Data: []byte("different"),
	}); err == nil {
		t.Fatal("conflicting payload replay succeeded")
	}
	unknownKey := payloadReceiptKey{pipeID: "pipe-unknown", payloadID: "payload-unknown"}
	endpoint.rememberLocked(unknownKey, payloadReceiptOutcome{unknown: true, hash: fingerprint})
	if err := endpoint.acknowledge(unknownKey.pipeID, unknownKey.payloadID); err != nil {
		t.Fatalf("late receipt after Unknown: %v", err)
	}
	if outcome := endpoint.history[unknownKey]; !outcome.unknown || outcome.received {
		t.Fatalf("late receipt changed Unknown outcome: %#v", outcome)
	}
	if err := endpoint.reject(unknownKey.pipeID, unknownKey.payloadID, relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE); err != nil {
		t.Fatalf("late rejection after Unknown: %v", err)
	}
	if outcome := endpoint.history[unknownKey]; !outcome.unknown || outcome.failure != relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNSPECIFIED {
		t.Fatalf("late rejection changed Unknown outcome: %#v", outcome)
	}

	for index := 0; index <= maxPayloadReceiptHistory; index++ {
		entry := payloadReceiptKey{pipeID: "pipe-bounded", payloadID: fmt.Sprintf("payload-%d", index)}
		endpoint.rememberLocked(entry, payloadReceiptOutcome{received: true})
	}
	if len(endpoint.history) != maxPayloadReceiptHistory || len(endpoint.order) != maxPayloadReceiptHistory {
		t.Fatalf("receipt history = %d/%d, want %d", len(endpoint.history), len(endpoint.order), maxPayloadReceiptHistory)
	}
}

func TestPipeTerminalWaitsForPayloadOutcomeResponse(t *testing.T) {
	stream := newGateRecordingRelayStream(false)
	defer stream.cancel()
	actor := newOutboundActor(stream, make(chan struct{}, maxGlobalPayloadSlots), time.Second)
	defer actor.close()
	endpoint := newStreamPipeEndpoint(actor, time.Second)
	if err := endpoint.beginOutcome(context.Background()); err != nil {
		t.Fatalf("beginOutcome(): %v", err)
	}
	receipt := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadReceived{
		PipePayloadReceived: &relayv1.PipePayloadReceived{PipeId: "pipe-1", PayloadId: "payload-1"},
	}}
	if err := actor.send(context.Background(), receipt); err != nil {
		t.Fatalf("send payload outcome: %v", err)
	}
	if got := receiveWithin(t, stream.sent, "payload outcome"); got.GetPipePayloadReceived() == nil {
		t.Fatalf("first response = %#v, want payload outcome", got)
	}

	terminalResult := make(chan error, 1)
	go func() {
		terminalResult <- endpoint.TerminatePipe(context.Background(), "pipe-1")
	}()
	select {
	case response := <-stream.sent:
		t.Fatalf("terminal overtook payload outcome completion: %#v", response)
	case <-time.After(50 * time.Millisecond):
	}
	endpoint.endOutcome()
	if got := receiveWithin(t, stream.sent, "Pipe terminal"); got.GetPipeTerminated() == nil {
		t.Fatalf("second response = %#v, want Pipe terminal", got)
	}
	if err := receiveWithin(t, terminalResult, "Pipe terminal completion"); err != nil {
		t.Fatalf("TerminatePipe(): %v", err)
	}
}

func TestListenerTerminalWaitsForPayloadOutcomeResponse(t *testing.T) {
	stream := newGateRecordingRelayStream(false)
	defer stream.cancel()
	actor := newOutboundActor(stream, make(chan struct{}, maxGlobalPayloadSlots), time.Second)
	defer actor.close()
	pipeEndpoint := newStreamPipeEndpoint(actor, time.Second)
	listener := newStreamListenerEndpoint(context.Background(), actor, pipeEndpoint, time.Second)
	listener.attempts["attempt-1"] = &listenerAttempt{
		phase:        listenerOpen,
		pipeID:       "pipe-1",
		decision:     make(chan bool, 1),
		confirmation: make(chan struct{}, 1),
		terminal:     make(chan struct{}),
	}

	if err := pipeEndpoint.beginOutcome(context.Background()); err != nil {
		t.Fatalf("beginOutcome(): %v", err)
	}
	receipt := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadReceived{
		PipePayloadReceived: &relayv1.PipePayloadReceived{PipeId: "pipe-1", PayloadId: "payload-1"},
	}}
	if err := actor.send(context.Background(), receipt); err != nil {
		t.Fatalf("send payload outcome: %v", err)
	}
	if got := receiveWithin(t, stream.sent, "payload outcome"); got.GetPipePayloadReceived() == nil {
		t.Fatalf("first response = %#v, want payload outcome", got)
	}

	terminalResult := make(chan error, 1)
	terminalParent, cancelTerminal := context.WithCancel(context.Background())
	cancelTerminal()
	go func() {
		terminalResult <- listener.Terminate(terminalParent, localbinding.Termination{
			AttemptID: "attempt-1",
			PipeID:    "pipe-1",
		})
	}()
	select {
	case response := <-stream.sent:
		t.Fatalf("listener terminal overtook payload outcome completion: %#v", response)
	case <-time.After(50 * time.Millisecond):
	}
	pipeEndpoint.endOutcome()
	if got := receiveWithin(t, stream.sent, "Listener terminal"); got.GetListenerTerminated() == nil {
		t.Fatalf("second response = %#v, want Listener terminal", got)
	}
	if err := receiveWithin(t, terminalResult, "Listener terminal completion"); err != nil {
		t.Fatalf("Terminate(): %v", err)
	}
}

func TestConnectPayloadRejectionsAreStableAndOwnershipPrivate(t *testing.T) {
	called := make(chan localbinding.PipePayload, 1)
	closed := make(chan string, 2)
	opener := &testOpener{relayPayload: func(_ context.Context, _ clientsession.Ref, pipeID string, payload []byte) error {
		switch pipeID {
		case "owned":
			called <- localbinding.PipePayload{PipeID: pipeID, PayloadID: "payload-test", Data: append([]byte(nil), payload...)}
			return nil
		case "backpressure":
			return opening.ErrPayloadBackpressure
		case "unavailable":
			return opening.ErrUnavailable
		default:
			return opening.ErrPipeNotOwned
		}
	}, closePipe: func(_ clientsession.Ref, pipeID string) bool {
		if pipeID == "owned" {
			closed <- pipeID
			return true
		}
		return false
	}}
	_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStreamWithDeadline(t, connection)

	assertRejected := func(pipeID string, payload []byte, want relayv1.PipePayloadFailure) {
		t.Helper()
		payloadID := sendPipePayload(t, stream, pipeID, payload)
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv(PipePayloadRejected): %v", err)
		}
		rejected := response.GetPipePayloadRejected()
		if rejected.GetPipeId() != pipeID || rejected.GetPayloadId() != payloadID || rejected.GetFailure() != want {
			t.Fatalf("PipePayloadRejected = %#v, want pipe %q failure %s", rejected, pipeID, want)
		}
	}

	assertRejected("unknown", []byte("x"), relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED)
	assertRejected("foreign", []byte("x"), relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED)
	assertRejected("terminal", []byte("x"), relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED)
	assertRejected("backpressure", []byte("x"), relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE)
	assertRejected("unavailable", []byte("x"), relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE)
	want := bytes.Repeat([]byte{0x6d}, localbinding.MaxPayloadBytes)
	ownedPayloadID := sendPipePayload(t, stream, "owned", want)
	if got := receiveWithin(t, called, "maximum legal RelayPayload call"); got.PipeID != "owned" || !bytes.Equal(got.Data, want) {
		t.Fatalf("RelayPayload call = pipe %q, %d bytes", got.PipeID, len(got.Data))
	}
	if response, err := stream.Recv(); err != nil || response.GetPipePayloadReceived().GetPayloadId() != ownedPayloadID {
		t.Fatalf("Recv(PipePayloadReceived) = %#v, %v", response, err)
	}
	assertRejected("owned", make([]byte, localbinding.MaxPayloadBytes+1), relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST)
	if got := receiveWithin(t, closed, "owned invalid payload close"); got != "owned" {
		t.Fatalf("ClosePipe invalid payload = %q", got)
	}
	assertRejected("unknown", nil, relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST)
	select {
	case got := <-closed:
		t.Fatalf("invalid unknown payload changed pipe %q", got)
	default:
	}
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
		ClosePipe: &relayv1.ClosePipe{PipeId: "after-success"},
	}}); err != nil {
		t.Fatalf("Send(ClosePipe after payload): %v", err)
	}
	response, err := stream.Recv()
	if err != nil || response.GetPipeCloseAcknowledged().GetPipeId() != "after-success" {
		t.Fatalf("Recv(PipeCloseAcknowledged after payload) = %#v, %v", response, err)
	}
}

func TestConnectOrdersCloseAfterBlockingPayload(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	closeCalled := make(chan struct{}, 1)
	opener := &testOpener{
		relayPayload: func(context.Context, clientsession.Ref, string, []byte) error {
			close(started)
			<-release
			return nil
		},
		closePipe: func(clientsession.Ref, string) bool {
			closeCalled <- struct{}{}
			return true
		},
	}
	_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStreamWithDeadline(t, connection)

	sendPipePayload(t, stream, "pipe-1", []byte("blocked"))
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
		ClosePipe: &relayv1.ClosePipe{PipeId: "pipe-1"},
	}}); err != nil {
		t.Fatalf("Send(ClosePipe): %v", err)
	}
	<-started
	select {
	case <-closeCalled:
		t.Fatal("source stream processed ClosePipe while RelayPayload was still blocked")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	receiveWithin(t, closeCalled, "ClosePipe after RelayPayload release")
	if response, err := stream.Recv(); err != nil || response.GetPipePayloadReceived() == nil {
		t.Fatalf("Recv(PipePayloadReceived) = %#v, %v", response, err)
	}
	if response, err := stream.Recv(); err != nil || !response.GetPipeCloseAcknowledged().GetOwned() {
		t.Fatalf("Recv(PipeCloseAcknowledged) = %#v, %v", response, err)
	}
}

func TestStreamCoordinatorPipeWorkPressureFailsClosedWithoutBlockingReceive(t *testing.T) {
	started := make(chan struct{})
	opener := &testOpener{relayPayload: func(ctx context.Context, _ clientsession.Ref, _ string, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	service := &Service{opener: opener}
	stream := newGateRecordingRelayStream(false)
	actor := newOutboundActor(stream, make(chan struct{}, 2), time.Second)
	defer actor.close()
	coordinator := newStreamCoordinator(
		context.Background(),
		testPayloadSession(),
		opener,
		newStreamPipeEndpoint(actor, time.Second),
		actor,
		make(chan struct{}, 1),
	)
	defer coordinator.close()

	first := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayload{
		PipePayload: &relayv1.PipePayload{PipeId: "pipe-1", PayloadId: "payload-blocked", Payload: []byte("blocked")},
	}}
	if err := coordinator.enqueuePipeWork(service, first); err != nil {
		t.Fatalf("enqueuePipeWork(first): %v", err)
	}
	receiveWithin(t, started, "blocked Pipe worker")

	for index := 0; index < streamPipeWorkQueueCapacity; index++ {
		if err := coordinator.enqueuePipeWork(service, &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
			ClosePipe: &relayv1.ClosePipe{PipeId: "pipe-1"},
		}}); err != nil {
			t.Fatalf("enqueuePipeWork(%d): %v", index, err)
		}
	}
	startedAt := time.Now()
	err := coordinator.enqueuePipeWork(service, &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
		ClosePipe: &relayv1.ClosePipe{PipeId: "pipe-1"},
	}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("overflow enqueuePipeWork() = %v, want ResourceExhausted", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("overflow enqueue blocked receive loop for %v", elapsed)
	}
}

func TestStreamCoordinatorCloseCancelsPipeWorkerWithoutTransportJoinCycle(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	opener := &testOpener{relayPayload: func(ctx context.Context, _ clientsession.Ref, _ string, _ []byte) error {
		close(started)
		<-release
		return ctx.Err()
	}}
	service := &Service{opener: opener}
	stream := newGateRecordingRelayStream(false)
	actor := newOutboundActor(stream, make(chan struct{}, 2), time.Second)
	defer actor.close()
	coordinator := newStreamCoordinator(
		context.Background(),
		testPayloadSession(),
		opener,
		newStreamPipeEndpoint(actor, time.Second),
		actor,
		make(chan struct{}, 1),
	)

	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayload{
		PipePayload: &relayv1.PipePayload{PipeId: "pipe-1", PayloadId: "payload-blocked", Payload: []byte("blocked")},
	}}
	if err := coordinator.enqueuePipeWork(service, request); err != nil {
		t.Fatalf("enqueuePipeWork(): %v", err)
	}
	receiveWithin(t, started, "blocked Pipe worker")

	closed := make(chan struct{})
	go func() {
		coordinator.close()
		close(closed)
	}()
	receiveWithin(t, closed, "coordinator close without Pipe worker join")
	select {
	case <-coordinator.pipeDone:
		t.Fatal("Pipe worker exited before blocked transport result")
	default:
	}
	close(release)
	receiveWithin(t, coordinator.pipeDone, "Pipe worker after transport result")
}

func TestStreamCoordinatorActivatesOnlyAfterPipeOpenedSend(t *testing.T) {
	stream := newGateRecordingRelayStream(true)
	actor := newOutboundActor(stream, make(chan struct{}, 8), time.Second)
	defer actor.close()
	pipeEndpoint := newStreamPipeEndpoint(actor, time.Second)
	activated := make(chan string, 1)
	opener := &testOpener{
		openPipe: func(_ context.Context, _ clientsession.Session, got localbinding.CallerEndpoint, _, _ string) (opening.Result, error) {
			if got != pipeEndpoint {
				t.Errorf("OpenPipe caller endpoint = %T %p, want %p", got, got, pipeEndpoint)
			}
			return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1"}, nil
		},
		activatePipe: func(_ clientsession.Ref, pipeID string) bool {
			activated <- pipeID
			return true
		},
	}
	session := testPayloadSession()
	service := &Service{opener: opener}
	coordinator := newStreamCoordinator(context.Background(), session, opener, pipeEndpoint, actor, make(chan struct{}, 1))
	if response := coordinator.startOpen(context.Background(), service, &relayv1.Open{RequestId: "request-1", Endpoint: "one", TargetId: "worker"}); response != nil {
		t.Fatalf("startOpen() = %#v", response)
	}
	receiveWithin(t, stream.entered, "blocked PipeOpened Send")
	select {
	case pipeID := <-activated:
		t.Fatalf("ActivatePipe(%q) ran before PipeOpened Send completed", pipeID)
	case <-time.After(20 * time.Millisecond):
	}
	close(stream.releaseFirst)
	if response := receiveWithin(t, stream.sent, "PipeOpened Send"); response.GetPipeOpened().GetPipeId() != "pipe-1" {
		t.Fatalf("first response = %#v, want PipeOpened", response)
	}
	if pipeID := receiveWithin(t, activated, "post-Send activation"); pipeID != "pipe-1" {
		t.Fatalf("activated Pipe = %q", pipeID)
	}
	coordinator.close()
}
