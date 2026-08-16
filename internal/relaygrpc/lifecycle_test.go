package relaygrpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"github.com/cagojeiger/relaygate/internal/localbinding"
	"github.com/cagojeiger/relaygate/internal/opening"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestConnectRunsTwoOpensConcurrentlyOnOneStream(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	opener := &testOpener{open: func(ctx context.Context, _ clientsession.Session, endpoint, _ string) (opening.Result, error) {
		started <- endpoint
		select {
		case <-release:
			return opening.Result{AttemptID: "attempt-" + endpoint, PipeID: "pipe-" + endpoint}, nil
		case <-ctx.Done():
			return opening.Result{}, ctx.Err()
		}
	}}
	_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStreamWithDeadline(t, connection)

	if err := stream.Send(openRequest("request-1", "one", "worker")); err != nil {
		t.Fatalf("Send(first Open): %v", err)
	}
	if err := stream.Send(openRequest("request-2", "two", "worker")); err != nil {
		t.Fatalf("Send(second Open): %v", err)
	}
	seenStarts := map[string]bool{}
	for len(seenStarts) < 2 {
		seenStarts[receiveWithin(t, started, "concurrent Open start")] = true
	}
	close(release)

	seenResults := map[string]bool{}
	for len(seenResults) < 2 {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv(PipeOpened): %v", err)
		}
		opened := response.GetPipeOpened()
		if opened == nil {
			t.Fatalf("response = %#v, want PipeOpened", response)
		}
		seenResults[opened.GetRequestId()] = true
	}
	if !seenResults["request-1"] || !seenResults["request-2"] {
		t.Fatalf("Open results = %#v", seenResults)
	}
}

func TestConnectBoundsOpenWorkersAcrossStreams(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	opener := &testOpener{open: func(ctx context.Context, _ clientsession.Session, _, _ string) (opening.Result, error) {
		started <- struct{}{}
		select {
		case <-release:
			return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1"}, nil
		case <-ctx.Done():
			return opening.Result{}, ctx.Err()
		}
	}}
	_, _, server := startTestServerWithRuntimeLimits(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	}, 3, 1, time.Second, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	first := authenticateTestStreamWithDeadline(t, connection)
	second := authenticateTestStreamWithDeadline(t, connection)

	if err := first.Send(openRequest("request-1", "one", "worker")); err != nil {
		t.Fatalf("Send(first Open): %v", err)
	}
	receiveWithin(t, started, "first global Open slot")
	if err := second.Send(openRequest("request-2", "two", "worker")); err != nil {
		t.Fatalf("Send(second Open): %v", err)
	}
	response, err := second.Recv()
	if err != nil {
		t.Fatalf("Recv(capacity): %v", err)
	}
	if failed := response.GetPipeOpenFailed(); failed.GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED {
		t.Fatalf("capacity response = %#v", response)
	}
	select {
	case <-started:
		t.Fatal("second stream spawned an Open worker beyond the process-wide bound")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if response, err := first.Recv(); err != nil || response.GetPipeOpened().GetRequestId() != "request-1" {
		t.Fatalf("Recv(first PipeOpened) = %#v, %v", response, err)
	}
}

func TestConnectRejectsDuplicateInFlightRequestIDWithoutSecondOutcome(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	opener := &testOpener{open: func(ctx context.Context, _ clientsession.Session, _, _ string) (opening.Result, error) {
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-release:
			return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1"}, nil
		case <-ctx.Done():
			return opening.Result{}, ctx.Err()
		}
	}}
	_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStreamWithDeadline(t, connection)

	if err := stream.Send(openRequest("request-1", "one", "worker")); err != nil {
		t.Fatalf("Send(first Open): %v", err)
	}
	receiveWithin(t, started, "first Open")
	if err := stream.Send(openRequest("request-1", "different", "worker")); err != nil {
		t.Fatalf("Send(duplicate Open): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(OpenRequestRejected): %v", err)
	}
	rejected := response.GetOpenRequestRejected()
	if rejected.GetRequestId() != "request-1" || rejected.GetFailure() != relayv1.OpenRequestFailure_OPEN_REQUEST_FAILURE_DUPLICATE_IN_FLIGHT {
		t.Fatalf("duplicate rejection = %#v", response)
	}
	close(release)
	response, err = stream.Recv()
	if err != nil || response.GetPipeOpened().GetRequestId() != "request-1" {
		t.Fatalf("original outcome = %#v, %v", response, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Open calls = %d, want 1", calls.Load())
	}
}

func TestConnectCancelOpenBothSidesOfAccept(t *testing.T) {
	t.Run("cancel before accept is a stable cancellation", func(t *testing.T) {
		started := make(chan struct{})
		finish := make(chan struct{})
		opener := &testOpener{open: func(ctx context.Context, _ clientsession.Session, _, _ string) (opening.Result, error) {
			close(started)
			<-ctx.Done()
			<-finish
			return opening.Result{}, ctx.Err()
		}}
		_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
		connection := dialTestServer(t, server.Address())
		stream := authenticateTestStreamWithDeadline(t, connection)
		if err := stream.Send(openRequest("request-cancel", "one", "worker")); err != nil {
			t.Fatalf("Send(Open): %v", err)
		}
		<-started
		cancel := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_CancelOpen{CancelOpen: &relayv1.CancelOpen{RequestId: "request-cancel"}}}
		if err := stream.Send(cancel); err != nil {
			t.Fatalf("Send(first CancelOpen): %v", err)
		}
		if err := stream.Send(cancel); err != nil {
			t.Fatalf("Send(second CancelOpen): %v", err)
		}
		for index := 0; index < 2; index++ {
			response, err := stream.Recv()
			if err != nil {
				t.Fatalf("Recv(OpenCancelAcknowledged): %v", err)
			}
			ack := response.GetOpenCancelAcknowledged()
			if ack.GetRequestId() != "request-cancel" || !ack.GetWasPending() {
				t.Fatalf("cancel acknowledgement = %#v", response)
			}
		}
		close(finish)
		response, err := stream.Recv()
		if err != nil || response.GetPipeOpenFailed().GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_CANCELLED {
			t.Fatalf("cancelled outcome = %#v, %v", response, err)
		}
	})

	t.Run("cancel wins over a computed stable failure before response commit", func(t *testing.T) {
		failureComputed := make(chan struct{})
		allowReturn := make(chan struct{})
		opener := &testOpener{open: func(context.Context, clientsession.Session, string, string) (opening.Result, error) {
			close(failureComputed)
			<-allowReturn
			return opening.Result{}, opening.ErrNotFound
		}}
		_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
		connection := dialTestServer(t, server.Address())
		stream := authenticateTestStreamWithDeadline(t, connection)
		if err := stream.Send(openRequest("request-failure-race", "one", "worker")); err != nil {
			t.Fatalf("Send(Open): %v", err)
		}
		<-failureComputed
		if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_CancelOpen{
			CancelOpen: &relayv1.CancelOpen{RequestId: "request-failure-race"},
		}}); err != nil {
			t.Fatalf("Send(CancelOpen): %v", err)
		}
		response, err := stream.Recv()
		if err != nil || !response.GetOpenCancelAcknowledged().GetWasPending() {
			t.Fatalf("cancel acknowledgement = %#v, %v", response, err)
		}
		close(allowReturn)
		response, err = stream.Recv()
		if err != nil || response.GetPipeOpenFailed().GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_CANCELLED {
			t.Fatalf("cancel-won outcome = %#v, %v", response, err)
		}
	})

	t.Run("cancel after accept is unknown", func(t *testing.T) {
		accepted := make(chan struct{})
		opener := &testOpener{open: func(ctx context.Context, _ clientsession.Session, _, _ string) (opening.Result, error) {
			close(accepted)
			<-ctx.Done()
			return opening.Result{}, fmt.Errorf("%w: %v", opening.ErrUnknown, ctx.Err())
		}}
		_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
		connection := dialTestServer(t, server.Address())
		stream := authenticateTestStreamWithDeadline(t, connection)
		if err := stream.Send(openRequest("request-accepted", "one", "worker")); err != nil {
			t.Fatalf("Send(Open): %v", err)
		}
		<-accepted
		if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_CancelOpen{
			CancelOpen: &relayv1.CancelOpen{RequestId: "request-accepted"},
		}}); err != nil {
			t.Fatalf("Send(CancelOpen): %v", err)
		}
		seenAck := false
		seenUnknown := false
		for !seenAck || !seenUnknown {
			response, err := stream.Recv()
			if err != nil {
				t.Fatalf("Recv(cancel result): %v", err)
			}
			if ack := response.GetOpenCancelAcknowledged(); ack != nil {
				seenAck = ack.GetWasPending()
			}
			if unknown := response.GetPipeOpenUnknown(); unknown != nil {
				seenUnknown = unknown.GetRequestId() == "request-accepted"
			}
		}
	})
}

func TestConnectClosePipeIsExactParticipantOwned(t *testing.T) {
	var mu sync.Mutex
	type pipeParticipants struct {
		caller   clientsession.Ref
		listener clientsession.Ref
	}
	participants := make(map[string]pipeParticipants)
	closed := make(map[string]int)
	var listenerSession clientsession.Ref
	bindings := &testBindingManager{bind: func(_ context.Context, session clientsession.Session, endpoint, targetID string, _ localbinding.ListenerEndpoint) (controlstate.BindingSlot, error) {
		mu.Lock()
		listenerSession = session.Ref
		mu.Unlock()
		return testListenerSlot(session.Ref.ClientID, endpoint, targetID), nil
	}}
	opener := &testOpener{
		open: func(_ context.Context, session clientsession.Session, endpoint, _ string) (opening.Result, error) {
			pipeID := "pipe-" + endpoint
			mu.Lock()
			participants[pipeID] = pipeParticipants{caller: session.Ref, listener: listenerSession}
			mu.Unlock()
			return opening.Result{AttemptID: "attempt-" + endpoint, PipeID: pipeID}, nil
		},
		closePipe: func(session clientsession.Ref, pipeID string) bool {
			mu.Lock()
			defer mu.Unlock()
			ownedBy := participants[pipeID]
			if ownedBy.caller != session && ownedBy.listener != session {
				return false
			}
			if closed[pipeID] == 0 {
				closed[pipeID]++
			}
			return true
		},
	}
	_, _, server := startTestServerWithDependencies(t, bindings, opener)
	connection := dialTestServer(t, server.Address())
	listener := authenticateTestStreamWithDeadline(t, connection)
	owner := authenticateTestStreamWithDeadline(t, connection)
	foreign := authenticateTestStreamWithDeadline(t, connection)
	bindListener(t, listener, "/close", "listener")

	if err := owner.Send(openRequest("request-1", "one", "worker")); err != nil {
		t.Fatalf("Send(first Open): %v", err)
	}
	if err := owner.Send(openRequest("request-2", "two", "worker")); err != nil {
		t.Fatalf("Send(second Open): %v", err)
	}
	for index := 0; index < 2; index++ {
		if response, err := owner.Recv(); err != nil || response.GetPipeOpened() == nil {
			t.Fatalf("Recv(PipeOpened) = %#v, %v", response, err)
		}
	}

	closeRequest := func(stream grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse], pipeID string) *relayv1.PipeCloseAcknowledged {
		t.Helper()
		if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
			ClosePipe: &relayv1.ClosePipe{PipeId: pipeID},
		}}); err != nil {
			t.Fatalf("Send(ClosePipe): %v", err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv(PipeCloseAcknowledged): %v", err)
		}
		return response.GetPipeCloseAcknowledged()
	}
	if ack := closeRequest(foreign, "pipe-one"); ack.GetOwned() {
		t.Fatalf("foreign close acknowledgement = %#v", ack)
	}
	if ack := closeRequest(foreign, "unknown-pipe"); ack.GetOwned() {
		t.Fatalf("unknown close acknowledgement = %#v", ack)
	}
	if ack := closeRequest(listener, "pipe-one"); !ack.GetOwned() {
		t.Fatalf("listener close acknowledgement = %#v", ack)
	}
	if ack := closeRequest(listener, "pipe-one"); !ack.GetOwned() {
		t.Fatalf("duplicate listener close acknowledgement = %#v", ack)
	}
	if ack := closeRequest(owner, "pipe-one"); !ack.GetOwned() {
		t.Fatalf("caller retained-history acknowledgement = %#v", ack)
	}

	mu.Lock()
	firstCloses := closed["pipe-one"]
	secondCloses := closed["pipe-two"]
	mu.Unlock()
	if firstCloses != 1 || secondCloses != 0 {
		t.Fatalf("close effects = first %d second %d", firstCloses, secondCloses)
	}
	if ack := closeRequest(owner, "pipe-two"); !ack.GetOwned() {
		t.Fatalf("second Pipe close acknowledgement = %#v", ack)
	}
}

func TestCrossOpeningStreamsDoNotHeadOfLineBlockListenerDecisions(t *testing.T) {
	var mu sync.Mutex
	endpoints := make(map[string]localbinding.ListenerEndpoint)
	slots := make(map[string]controlstate.BindingSlot)
	bindings := &testBindingManager{bind: func(_ context.Context, session clientsession.Session, endpoint, targetID string, listener localbinding.ListenerEndpoint) (controlstate.BindingSlot, error) {
		ref := controlstate.ListenerBindingRef{
			GatewayID:         "gateway-a",
			GatewayInstanceID: "instance-a",
			ListenerBindingID: "binding-" + targetID,
		}
		slot := controlstate.BindingSlot{
			Key:        controlstate.BindingKey{ClientID: session.Ref.ClientID, EndpointPattern: endpoint, TargetID: targetID},
			Generation: 1,
			Ref:        &ref,
		}
		mu.Lock()
		endpoints[targetID] = listener
		slots[targetID] = slot
		mu.Unlock()
		return slot, nil
	}}
	var sequence atomic.Int32
	opener := &testOpener{open: func(ctx context.Context, caller clientsession.Session, _, targetID string) (opening.Result, error) {
		mu.Lock()
		listener := endpoints[targetID]
		slot := slots[targetID]
		mu.Unlock()
		index := sequence.Add(1)
		attemptID := fmt.Sprintf("attempt-%d", index)
		pipeID := fmt.Sprintf("pipe-%d", index)
		if err := listener.Offer(ctx, localbinding.Offer{AttemptID: attemptID, Caller: caller.Ref, Binding: slot}); err != nil {
			return opening.Result{}, err
		}
		if err := listener.Confirm(ctx, localbinding.Confirmation{AttemptID: attemptID, PipeID: pipeID}); err != nil {
			return opening.Result{}, fmt.Errorf("%w: %v", opening.ErrUnknown, err)
		}
		return opening.Result{AttemptID: attemptID, PipeID: pipeID, Binding: slot}, nil
	}}
	_, _, server := startTestServerWithDependencies(t, bindings, opener)
	connection := dialTestServer(t, server.Address())
	left := authenticateTestStreamWithDeadline(t, connection)
	right := authenticateTestStreamWithDeadline(t, connection)
	bindListener(t, left, "/left", "left")
	bindListener(t, right, "/right", "right")

	if err := left.Send(openRequest("left-to-right", "/right", "right")); err != nil {
		t.Fatalf("Send(left Open): %v", err)
	}
	if err := right.Send(openRequest("right-to-left", "/left", "left")); err != nil {
		t.Fatalf("Send(right Open): %v", err)
	}
	leftOffer, err := left.Recv()
	if err != nil || leftOffer.GetListenerOffer() == nil {
		t.Fatalf("Recv(left ListenerOffer) = %#v, %v", leftOffer, err)
	}
	rightOffer, err := right.Recv()
	if err != nil || rightOffer.GetListenerOffer() == nil {
		t.Fatalf("Recv(right ListenerOffer) = %#v, %v", rightOffer, err)
	}
	for stream, offer := range map[grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse]]*relayv1.ListenerOffer{
		left: leftOffer.GetListenerOffer(), right: rightOffer.GetListenerOffer(),
	} {
		if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{
			ListenerAccept: &relayv1.ListenerAccept{AttemptId: offer.GetAttemptId()},
		}}); err != nil {
			t.Fatalf("Send(ListenerAccept): %v", err)
		}
	}
	leftEstablished, err := left.Recv()
	if err != nil || leftEstablished.GetListenerEstablished() == nil {
		t.Fatalf("Recv(left ListenerEstablished) = %#v, %v", leftEstablished, err)
	}
	rightEstablished, err := right.Recv()
	if err != nil || rightEstablished.GetListenerEstablished() == nil {
		t.Fatalf("Recv(right ListenerEstablished) = %#v, %v", rightEstablished, err)
	}
	establishedByStream := map[grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse]]*relayv1.ListenerEstablished{
		left: leftEstablished.GetListenerEstablished(), right: rightEstablished.GetListenerEstablished(),
	}
	for stream, established := range establishedByStream {
		if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
			ListenerConfirmed: &relayv1.ListenerConfirmed{AttemptId: established.GetAttemptId(), PipeId: established.GetPipeId()},
		}}); err != nil {
			t.Fatalf("Send(ListenerConfirmed): %v", err)
		}
	}
	for stream, expected := range map[grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse]]struct {
		established *relayv1.ListenerEstablished
		requestID   string
	}{
		left:  {established: establishedByStream[left], requestID: "left-to-right"},
		right: {established: establishedByStream[right], requestID: "right-to-left"},
	} {
		seenAcknowledged := false
		seenOpened := false
		for !seenAcknowledged || !seenOpened {
			response, err := stream.Recv()
			if err != nil {
				t.Fatalf("Recv(confirmation/Open outcome): %v", err)
			}
			if acknowledged := response.GetListenerConfirmationAcknowledged(); acknowledged != nil {
				if acknowledged.GetAttemptId() != expected.established.GetAttemptId() || acknowledged.GetPipeId() != expected.established.GetPipeId() {
					t.Fatalf("ListenerConfirmationAcknowledged = %#v", acknowledged)
				}
				seenAcknowledged = true
				continue
			}
			if opened := response.GetPipeOpened(); opened != nil {
				if opened.GetRequestId() != expected.requestID {
					t.Fatalf("PipeOpened = %#v, want request %q", opened, expected.requestID)
				}
				seenOpened = true
				continue
			}
			t.Fatalf("unexpected confirmation/Open response = %#v", response)
		}
	}
}

func TestConnectCloseSendCancelsAndJoinsOpenWorkers(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	opener := &testOpener{open: func(ctx context.Context, _ clientsession.Session, _, _ string) (opening.Result, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return opening.Result{}, ctx.Err()
	}}
	_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStreamWithDeadline(t, connection)
	if err := stream.Send(openRequest("request-1", "one", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	<-started
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend(): %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv(after CloseSend) = %v, want EOF", err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Connect returned without joining its Open worker")
	}
}

func TestStreamCoordinatorCloseInterruptsBlockedOpenResponse(t *testing.T) {
	stream := newBlockingRelayStream()
	actor := newOutboundActor(stream, make(chan struct{}, maxGlobalPayloadSlots), time.Second)
	closedPipe := make(chan string, 1)
	opener := &testOpener{
		open: func(context.Context, clientsession.Session, string, string) (opening.Result, error) {
			return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1"}, nil
		},
		closePipe: func(_ clientsession.Ref, pipeID string) bool {
			closedPipe <- pipeID
			return true
		},
	}
	session := clientsession.Session{Ref: clientsession.Ref{
		ClientSessionID: "session-1",
		ClientID:        "client-a",
		APIKeyID:        "key-a",
		AuthRevision:    "revision-1",
	}, Done: make(chan struct{})}
	service := &Service{opener: opener}
	pipeEndpoint := newStreamPipeEndpoint(actor, time.Second)
	coordinator := newStreamCoordinator(context.Background(), session, opener, pipeEndpoint, actor, make(chan struct{}, 1))
	if response := coordinator.startOpen(context.Background(), service, &relayv1.Open{RequestId: "request-1", Endpoint: "one", TargetId: "worker"}); response != nil {
		t.Fatalf("startOpen() = %#v", response)
	}
	receiveWithin(t, stream.entered, "blocked PipeOpened Send")

	done := make(chan struct{})
	go func() {
		coordinator.close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator close did not interrupt and join blocked response worker")
	}
	if pipeID := receiveWithin(t, closedPipe, "accepted Pipe cleanup"); pipeID != "pipe-1" {
		t.Fatalf("closed Pipe = %q", pipeID)
	}
	close(stream.release)
	actor.close()
}

func TestListenerTerminalQueuePressureFailsStreamInsteadOfDropping(t *testing.T) {
	for _, test := range []struct {
		name string
		send func(*streamListenerEndpoint, *listenerAttempt)
	}{
		{
			name: "local cancellation",
			send: func(endpoint *streamListenerEndpoint, attempt *listenerAttempt) {
				endpoint.cancelAttempt(context.Background(), "attempt-1", attempt, "pipe-1")
			},
		},
		{
			name: "opening manager termination",
			send: func(endpoint *streamListenerEndpoint, _ *listenerAttempt) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				_ = endpoint.Terminate(ctx, localbinding.Termination{AttemptID: "attempt-1", PipeID: "pipe-1"})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := newBlockingRelayStream()
			actor := newOutboundActor(stream, make(chan struct{}, maxGlobalPayloadSlots), time.Second)
			response := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
				ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "fill"},
			}}
			actor.queue <- outboundMessage{ctx: context.Background(), response: response}
			receiveWithin(t, stream.entered, "blocked outbound Send")
			for index := 0; index < cap(actor.queue); index++ {
				actor.queue <- outboundMessage{ctx: context.Background(), response: response}
			}

			pipeEndpoint := newStreamPipeEndpoint(actor, 20*time.Millisecond)
			endpoint := newStreamListenerEndpoint(context.Background(), actor, pipeEndpoint, 20*time.Millisecond)
			attempt := &listenerAttempt{terminal: make(chan struct{})}
			endpoint.attempts["attempt-1"] = attempt
			test.send(endpoint, attempt)
			select {
			case <-actor.done:
			case <-time.After(time.Second):
				t.Fatal("terminal queue pressure neither delivered terminal nor failed the stream")
			}
			close(stream.release)
			actor.close()
		})
	}
}

func authenticateTestStreamWithDeadline(t *testing.T, connection *grpc.ClientConn) grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	stream, err := relayv1.NewRelayClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(authenticateRequest("client-a", "key-a", "secret-a")); err != nil {
		t.Fatalf("Send(authenticate): %v", err)
	}
	if response, err := stream.Recv(); err != nil || response.GetClientSessionOpened() == nil {
		t.Fatalf("Recv(ClientSessionOpened) = %#v, %v", response, err)
	}
	return stream
}

type blockingRelayStream struct {
	ctx     context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingRelayStream() *blockingRelayStream {
	return &blockingRelayStream{
		ctx:     context.Background(),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingRelayStream) Send(*relayv1.ConnectResponse) error {
	s.once.Do(func() { s.entered <- struct{}{} })
	<-s.release
	return nil
}

func (*blockingRelayStream) Recv() (*relayv1.ConnectRequest, error) { return nil, io.EOF }
func (*blockingRelayStream) SetHeader(metadata.MD) error            { return nil }
func (*blockingRelayStream) SendHeader(metadata.MD) error           { return nil }
func (*blockingRelayStream) SetTrailer(metadata.MD)                 {}
func (s *blockingRelayStream) Context() context.Context             { return s.ctx }
func (*blockingRelayStream) SendMsg(any) error                      { return nil }
func (*blockingRelayStream) RecvMsg(any) error                      { return io.EOF }
