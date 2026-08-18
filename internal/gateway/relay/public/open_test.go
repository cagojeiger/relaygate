package relaygrpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
)

func TestConnectOpenAcknowledgesListenerConfirmationOnlyAfterExactApply(t *testing.T) {
	bound := make(chan bindingCall, 1)
	endpointForOpen := make(chan localbinding.ListenerEndpoint, 1)
	slot := testListenerSlot("client-a", "/jobs/exact", "worker")
	bindings := testOpenBindingManager(bound, slot)
	opener := &testOpener{open: func(ctx context.Context, caller clientsession.Session, endpoint, targetID string) (opening.Result, error) {
		listener := <-endpointForOpen
		offer := localbinding.Offer{AttemptID: "attempt-1", Caller: caller.Ref, Binding: slot}
		if err := listener.Offer(ctx, offer); err != nil {
			return opening.Result{}, err
		}
		confirmation := localbinding.Confirmation{AttemptID: "attempt-1", PipeID: "pipe-1"}
		if err := listener.Confirm(ctx, confirmation); err != nil {
			return opening.Result{}, fmt.Errorf("%w: %v", opening.ErrUnknown, err)
		}
		return opening.Result{AttemptID: "attempt-1", PipeID: "pipe-1", Binding: slot}, nil
	}}
	_, _, server := startTestServerWithDependencies(t, bindings, opener)
	connection := dialTestServer(t, server.Address())
	listener := authenticateTestStream(t, connection)

	bindListener(t, listener, "/jobs/exact", "worker")
	call := receiveWithin(t, bound, "Bind")
	if call.endpoint == nil {
		t.Fatal("Bind() listener endpoint is nil")
	}
	endpointForOpen <- call.endpoint

	caller := authenticateTestStream(t, connection)
	if err := caller.Send(openRequest("request-1", "/jobs/exact", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	offerResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerOffer): %v", err)
	}
	offer := offerResponse.GetListenerOffer()
	if offer.GetAttemptId() != "attempt-1" || offer.GetListenerBindingId() != "listener-a" ||
		offer.GetEndpoint() != "/jobs/exact" || offer.GetTargetId() != "worker" || offer.GetCallerSessionId() == "" {
		t.Fatalf("ListenerOffer = %#v", offer)
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{
		ListenerAccept: &relayv1.ListenerAccept{AttemptId: offer.GetAttemptId()},
	}}); err != nil {
		t.Fatalf("Send(ListenerAccept): %v", err)
	}
	establishedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerEstablished): %v", err)
	}
	established := establishedResponse.GetListenerEstablished()
	if established.GetAttemptId() != "attempt-1" || established.GetPipeId() != "pipe-1" {
		t.Fatalf("ListenerEstablished = %#v", established)
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
		ListenerConfirmed: &relayv1.ListenerConfirmed{AttemptId: established.GetAttemptId(), PipeId: "wrong-pipe"},
	}}); err != nil {
		t.Fatalf("Send(inexact ListenerConfirmed): %v", err)
	}
	rejectedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerDecisionRejected): %v", err)
	}
	if rejected := rejectedResponse.GetListenerDecisionRejected(); rejected.GetAttemptId() != established.GetAttemptId() ||
		rejected.GetFailure() != relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE ||
		rejectedResponse.GetListenerConfirmationAcknowledged() != nil {
		t.Fatalf("inexact confirmation response = %#v", rejectedResponse)
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
		ListenerConfirmed: &relayv1.ListenerConfirmed{AttemptId: established.GetAttemptId(), PipeId: established.GetPipeId()},
	}}); err != nil {
		t.Fatalf("Send(ListenerConfirmed): %v", err)
	}
	requireListenerConfirmationAcknowledged(t, listener, established.GetAttemptId(), established.GetPipeId())
	openedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpened): %v", err)
	}
	opened := openedResponse.GetPipeOpened()
	if opened.GetRequestId() != "request-1" || opened.GetAttemptId() != "attempt-1" || opened.GetPipeId() != "pipe-1" ||
		opened.GetEndpoint() != "/jobs/exact" || opened.GetTargetId() != "worker" {
		t.Fatalf("PipeOpened = %#v", opened)
	}
}

func requireListenerConfirmationAcknowledged(t *testing.T, stream interface {
	Recv() (*relayv1.ConnectResponse, error)
}, attemptID, pipeID string) {
	t.Helper()
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerConfirmationAcknowledged): %v", err)
	}
	acknowledged := response.GetListenerConfirmationAcknowledged()
	if acknowledged.GetAttemptId() != attemptID || acknowledged.GetPipeId() != pipeID {
		t.Fatalf("ListenerConfirmationAcknowledged = %#v", response)
	}
}

func TestConnectSendsListenerBoundBeforeConcurrentOffer(t *testing.T) {
	slot := testListenerSlot("client-a", "/jobs/order", "worker")
	offerResult := make(chan error, 1)
	bindings := &testBindingManager{bind: func(_ context.Context, session clientsession.Session, endpointPattern, targetID string, endpoint localbinding.ListenerEndpoint) (routing.LiveBinding, error) {
		go func() {
			offerResult <- endpoint.Offer(context.Background(), localbinding.Offer{
				AttemptID: "attempt-order",
				Caller:    session.Ref,
				Binding:   slot,
			})
		}()
		return slot, nil
	}}
	_, _, server := startTestServerWithDependencies(t, bindings, &testOpener{})
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStream(t, connection)

	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: "/jobs/order", TargetId: "worker"},
	}}); err != nil {
		t.Fatalf("Send(BindListener): %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(first): %v", err)
	}
	if first.GetListenerBound() == nil {
		t.Fatalf("first response = %#v, want ListenerBound", first)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(second): %v", err)
	}
	if second.GetListenerOffer().GetAttemptId() != "attempt-order" {
		t.Fatalf("second response = %#v, want ListenerOffer", second)
	}
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerReject{
		ListenerReject: &relayv1.ListenerReject{AttemptId: "attempt-order"},
	}}); err != nil {
		t.Fatalf("Send(ListenerReject): %v", err)
	}
	if err := receiveWithin(t, offerResult, "Offer rejection"); !errors.Is(err, localbinding.ErrOfferRejected) {
		t.Fatalf("Offer() error = %v, want ErrOfferRejected", err)
	}
}

func TestConnectOpenReturnsStableFailureWhenListenerRejects(t *testing.T) {
	bound := make(chan bindingCall, 1)
	endpointForOpen := make(chan localbinding.ListenerEndpoint, 1)
	slot := testListenerSlot("client-a", "/jobs/reject", "worker")
	bindings := testOpenBindingManager(bound, slot)
	opener := &testOpener{open: func(ctx context.Context, caller clientsession.Session, _, _ string) (opening.Result, error) {
		err := (<-endpointForOpen).Offer(ctx, localbinding.Offer{AttemptID: "attempt-reject", Caller: caller.Ref, Binding: slot})
		if errors.Is(err, localbinding.ErrOfferRejected) {
			return opening.Result{}, opening.ErrListenerRejected
		}
		return opening.Result{}, err
	}}
	_, _, server := startTestServerWithDependencies(t, bindings, opener)
	connection := dialTestServer(t, server.Address())
	listener := authenticateTestStream(t, connection)
	bindListener(t, listener, "/jobs/reject", "worker")
	endpointForOpen <- receiveWithin(t, bound, "Bind").endpoint
	caller := authenticateTestStream(t, connection)

	if err := caller.Send(openRequest("request-reject", "/jobs/reject", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	offerResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerOffer): %v", err)
	}
	attemptID := offerResponse.GetListenerOffer().GetAttemptId()
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerReject{
		ListenerReject: &relayv1.ListenerReject{AttemptId: attemptID},
	}}); err != nil {
		t.Fatalf("Send(ListenerReject): %v", err)
	}
	response, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpenFailed): %v", err)
	}
	failed := response.GetPipeOpenFailed()
	if failed.GetRequestId() != "request-reject" || failed.GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_LISTENER_REJECTED {
		t.Fatalf("PipeOpenFailed = %#v", failed)
	}
}

func TestConnectOpenDeadlineTerminatesListenerOffer(t *testing.T) {
	bound := make(chan bindingCall, 1)
	endpointForOpen := make(chan localbinding.ListenerEndpoint, 1)
	slot := testListenerSlot("client-a", "/jobs/slow", "worker")
	bindings := testOpenBindingManager(bound, slot)
	opener := &testOpener{open: func(ctx context.Context, caller clientsession.Session, _, _ string) (opening.Result, error) {
		attemptCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err := (<-endpointForOpen).Offer(attemptCtx, localbinding.Offer{AttemptID: "attempt-slow", Caller: caller.Ref, Binding: slot})
		if errors.Is(err, context.DeadlineExceeded) {
			return opening.Result{}, opening.ErrDeadline
		}
		return opening.Result{}, err
	}}
	_, _, server := startTestServerWithDependencies(t, bindings, opener)
	connection := dialTestServer(t, server.Address())
	listener := authenticateTestStream(t, connection)
	bindListener(t, listener, "/jobs/slow", "worker")
	endpointForOpen <- receiveWithin(t, bound, "Bind").endpoint
	caller := authenticateTestStream(t, connection)

	if err := caller.Send(openRequest("request-slow", "/jobs/slow", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	if response, err := listener.Recv(); err != nil || response.GetListenerOffer().GetAttemptId() != "attempt-slow" {
		t.Fatalf("Recv(ListenerOffer) = %#v, %v", response, err)
	}
	terminatedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerTerminated): %v", err)
	}
	if terminated := terminatedResponse.GetListenerTerminated(); terminated.GetAttemptId() != "attempt-slow" || terminated.GetPipeId() != "" {
		t.Fatalf("ListenerTerminated = %#v", terminated)
	}
	failedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpenFailed): %v", err)
	}
	if failed := failedResponse.GetPipeOpenFailed(); failed.GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_DEADLINE_EXCEEDED {
		t.Fatalf("PipeOpenFailed = %#v", failed)
	}
}

func TestConnectOpenConfirmationLossReturnsUnknown(t *testing.T) {
	bound := make(chan bindingCall, 1)
	endpointForOpen := make(chan localbinding.ListenerEndpoint, 1)
	slot := testListenerSlot("client-a", "/jobs/unknown", "worker")
	bindings := testOpenBindingManager(bound, slot)
	opener := &testOpener{open: func(ctx context.Context, caller clientsession.Session, _, _ string) (opening.Result, error) {
		listener := <-endpointForOpen
		if err := listener.Offer(ctx, localbinding.Offer{AttemptID: "attempt-unknown", Caller: caller.Ref, Binding: slot}); err != nil {
			return opening.Result{}, err
		}
		if err := listener.Confirm(ctx, localbinding.Confirmation{AttemptID: "attempt-unknown", PipeID: "pipe-unknown"}); err != nil {
			return opening.Result{}, fmt.Errorf("%w: %v", opening.ErrUnknown, err)
		}
		return opening.Result{}, errors.New("confirmation unexpectedly succeeded")
	}}
	_, _, server := startTestServerWithDependencies(t, bindings, opener)
	listenerConnection := dialTestServer(t, server.Address())
	listener := authenticateTestStream(t, listenerConnection)
	bindListener(t, listener, "/jobs/unknown", "worker")
	endpointForOpen <- receiveWithin(t, bound, "Bind").endpoint
	callerConnection := dialTestServer(t, server.Address())
	caller := authenticateTestStream(t, callerConnection)

	if err := caller.Send(openRequest("request-unknown", "/jobs/unknown", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	offerResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerOffer): %v", err)
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{
		ListenerAccept: &relayv1.ListenerAccept{AttemptId: offerResponse.GetListenerOffer().GetAttemptId()},
	}}); err != nil {
		t.Fatalf("Send(ListenerAccept): %v", err)
	}
	if response, err := listener.Recv(); err != nil || response.GetListenerEstablished().GetPipeId() != "pipe-unknown" {
		t.Fatalf("Recv(ListenerEstablished) = %#v, %v", response, err)
	}
	if err := listener.CloseSend(); err != nil {
		t.Fatalf("CloseSend(listener): %v", err)
	}
	response, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpenUnknown): %v", err)
	}
	unknown := response.GetPipeOpenUnknown()
	if unknown.GetRequestId() != "request-unknown" || unknown.GetEndpoint() != "/jobs/unknown" || unknown.GetTargetId() != "worker" {
		t.Fatalf("PipeOpenUnknown = %#v", unknown)
	}
}

func TestConnectOpenFailuresAreRedactedAndDoNotEndStream(t *testing.T) {
	opener := &testOpener{open: func(context.Context, clientsession.Session, string, string) (opening.Result, error) {
		return opening.Result{}, fmt.Errorf("secret owner address: %w", opening.ErrRemoteRelayUnavailable)
	}}
	_, _, server := startTestServerWithDependencies(t, &testBindingManager{}, opener)
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStream(t, connection)

	if err := stream.Send(openRequest("request-redacted", "/jobs/redacted", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpenFailed): %v", err)
	}
	if strings.Contains(response.String(), "secret") || response.GetPipeOpenFailed().GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE {
		t.Fatalf("redacted response = %v", response)
	}

	if err := stream.Send(openRequest("request-invalid", "/jobs/redacted", "")); err != nil {
		t.Fatalf("Send(second Open): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(second PipeOpenFailed): %v", err)
	}
	if response.GetPipeOpenFailed().GetFailure() != relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST {
		t.Fatalf("second response = %v", response)
	}
}

func TestConnectRejectsStaleListenerDecisionWithoutEndingStream(t *testing.T) {
	bound := make(chan bindingCall, 1)
	slot := testListenerSlot("client-a", "/jobs/live", "worker")
	_, _, server := startTestServerWithDependencies(t, testOpenBindingManager(bound, slot), &testOpener{})
	connection := dialTestServer(t, server.Address())
	stream := authenticateTestStream(t, connection)

	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{
		ListenerAccept: &relayv1.ListenerAccept{AttemptId: "missing-attempt"},
	}}); err != nil {
		t.Fatalf("Send(stale ListenerAccept): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerDecisionRejected): %v", err)
	}
	rejected := response.GetListenerDecisionRejected()
	if rejected.GetAttemptId() != "missing-attempt" || rejected.GetFailure() != relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING {
		t.Fatalf("ListenerDecisionRejected = %#v", rejected)
	}
	bindListener(t, stream, "/jobs/live", "worker")
	if receiveWithin(t, bound, "Bind after stale decision").endpoint == nil {
		t.Fatal("listener endpoint is nil after stale decision")
	}
}
