package relaygrpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
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
	bindings := &testBindingManager{bind: func(_ context.Context, session clientsession.Session, endpoint, targetID string, _ localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
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
