package relaygate

import (
	"context"
	"errors"
	"testing"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newPendingOpenTestClient(requestID string) (*Client, *openCall) {
	pipeSlots := make(chan struct{}, 1)
	pipeSlots <- struct{}{}
	call := &openCall{
		requestID: requestID,
		endpoint:  "/service",
		target:    "target",
		result:    make(chan openResult, 1),
		reserved:  true,
		retired:   make(chan struct{}),
	}
	return &Client{
		authenticated:  true,
		opens:          map[string]*openCall{requestID: call},
		openTombstones: make(map[string]openTombstone),
		pipeSlots:      pipeSlots,
	}, call
}

func pipeOpenFailedResponse(requestID string, failure relayv1.OpenFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenFailed{
		PipeOpenFailed: &relayv1.PipeOpenFailed{
			RequestId: requestID,
			Endpoint:  "/service",
			TargetId:  "target",
			Failure:   failure,
		},
	}}
}

func openRequestRejectedResponse(requestID string, failure relayv1.OpenRequestFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenRequestRejected{
		OpenRequestRejected: &relayv1.OpenRequestRejected{RequestId: requestID, Failure: failure},
	}}
}

func acceptingOfferTestClient(attemptID string) (*Client, *Offer) {
	client := &Client{
		authenticated:   true,
		offers:          make(map[string]*Offer),
		offerTombstones: make(map[string]string),
		pipes:           make(map[string]*Pipe),
		pipeTombstones:  make(map[string]*Pipe),
	}
	listener := &Listener{client: client, id: "listener", endpoint: "/service", target: "target"}
	offer := newOffer(listener, attemptID, "caller-session")
	offer.state = offerAccepting
	client.offers[attemptID] = offer
	return client, offer
}

func acceptingReservedOfferTestClient(attemptID string) (*Client, *Offer) {
	client, offer := acceptingOfferTestClient(attemptID)
	client.pipeSlots = make(chan struct{}, maxPipes)
	client.pipeSlots <- struct{}{}
	offer.reserved = true
	return client, offer
}

func listenerDecisionRejectedResponse(attemptID string, failure relayv1.ListenerDecisionFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerDecisionRejected{
		ListenerDecisionRejected: &relayv1.ListenerDecisionRejected{AttemptId: attemptID, Failure: failure},
	}}
}

func TestPipeOpenFailedStrictEnumDecoding(t *testing.T) {
	known := []struct {
		name        string
		wire        relayv1.OpenFailure
		failure     OpenFailure
		outcome     OpenOutcome
		isCancelled bool
	}{
		{name: "invalid request", wire: relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST, failure: OpenFailureInvalidRequest, outcome: OpenOutcomeFailed},
		{name: "route not found", wire: relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND, failure: OpenFailureRouteNotFound, outcome: OpenOutcomeFailed},
		{name: "unavailable", wire: relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE, failure: OpenFailureUnavailable, outcome: OpenOutcomeFailed},
		{name: "capacity reached", wire: relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED, failure: OpenFailureCapacityReached, outcome: OpenOutcomeFailed},
		{name: "listener rejected", wire: relayv1.OpenFailure_OPEN_FAILURE_LISTENER_REJECTED, failure: OpenFailureListenerRejected, outcome: OpenOutcomeFailed},
		{name: "deadline exceeded", wire: relayv1.OpenFailure_OPEN_FAILURE_DEADLINE_EXCEEDED, failure: OpenFailureDeadlineExceeded, outcome: OpenOutcomeFailed},
		{name: "cancelled", wire: relayv1.OpenFailure_OPEN_FAILURE_CANCELLED, failure: OpenFailureCancelled, outcome: OpenOutcomeCancelled, isCancelled: true},
	}
	for _, test := range known {
		t.Run("known/"+test.name, func(t *testing.T) {
			client, call := newPendingOpenTestClient("request-known")
			if err := client.dispatch(pipeOpenFailedResponse(call.requestID, test.wire)); err != nil {
				t.Fatalf("dispatch known PipeOpenFailed: %v", err)
			}
			result := <-call.result
			var openErr *OpenError
			if !errors.As(result.err, &openErr) || openErr.Outcome != test.outcome || openErr.Failure != test.failure {
				t.Fatalf("known PipeOpenFailed result = %#v, %v", openErr, result.err)
			}
			if errors.Is(result.err, ErrOpenCancelled) != test.isCancelled {
				t.Fatalf("errors.Is(ErrOpenCancelled) = %t, want %t", errors.Is(result.err, ErrOpenCancelled), test.isCancelled)
			}
		})
	}

	for _, test := range []struct {
		name string
		wire relayv1.OpenFailure
	}{
		{name: "unspecified", wire: relayv1.OpenFailure_OPEN_FAILURE_UNSPECIFIED},
		{name: "unknown", wire: relayv1.OpenFailure(99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, call := newPendingOpenTestClient("request-invalid")
			err := client.dispatch(pipeOpenFailedResponse(call.requestID, test.wire))
			if !errors.Is(err, errProtocol) {
				t.Fatalf("dispatch PipeOpenFailed(%d) = %v, want protocol failure", test.wire, err)
			}
			if client.opens[call.requestID] != call || len(client.pipeSlots) != 1 {
				t.Fatal("invalid PipeOpenFailed consumed the pending Open")
			}
			select {
			case result := <-call.result:
				t.Fatalf("invalid PipeOpenFailed completed the Open: %#v", result)
			default:
			}
		})
	}

	t.Run("foreign", func(t *testing.T) {
		client := &Client{authenticated: true, opens: make(map[string]*openCall), openTombstones: make(map[string]openTombstone)}
		err := client.dispatch(pipeOpenFailedResponse("foreign-request", relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE))
		if !errors.Is(err, errProtocol) {
			t.Fatalf("foreign PipeOpenFailed = %v, want protocol failure", err)
		}
	})
}

func TestOpenRequestRejectedStrictEnumAndTypedOutcome(t *testing.T) {
	t.Run("known duplicate in flight", func(t *testing.T) {
		client, call := newPendingOpenTestClient("request-duplicate")
		response := openRequestRejectedResponse(call.requestID, relayv1.OpenRequestFailure_OPEN_REQUEST_FAILURE_DUPLICATE_IN_FLIGHT)
		if err := client.dispatch(response); err != nil {
			t.Fatalf("dispatch OpenRequestRejected: %v", err)
		}
		result := <-call.result
		var openErr *OpenError
		if !errors.Is(result.err, ErrOpenDuplicateInFlight) || errors.Is(result.err, ErrOpenFailed) ||
			!errors.As(result.err, &openErr) || openErr.Outcome != OpenOutcomeRejected || openErr.Failure != 0 {
			t.Fatalf("duplicate-in-flight result = %#v, %v", openErr, result.err)
		}
		if err := client.dispatch(response); err != nil {
			t.Fatalf("duplicate OpenRequestRejected was not idempotent: %v", err)
		}
	})

	for _, test := range []struct {
		name string
		wire relayv1.OpenRequestFailure
	}{
		{name: "unspecified", wire: relayv1.OpenRequestFailure_OPEN_REQUEST_FAILURE_UNSPECIFIED},
		{name: "unknown", wire: relayv1.OpenRequestFailure(99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, call := newPendingOpenTestClient("request-invalid-rejection")
			err := client.dispatch(openRequestRejectedResponse(call.requestID, test.wire))
			if !errors.Is(err, errProtocol) {
				t.Fatalf("dispatch OpenRequestRejected(%d) = %v, want protocol failure", test.wire, err)
			}
			if client.opens[call.requestID] != call || len(client.pipeSlots) != 1 {
				t.Fatal("invalid OpenRequestRejected consumed the pending Open")
			}
			select {
			case result := <-call.result:
				t.Fatalf("invalid OpenRequestRejected completed the Open: %#v", result)
			default:
			}
		})
	}

	t.Run("foreign", func(t *testing.T) {
		client := &Client{authenticated: true, opens: make(map[string]*openCall), openTombstones: make(map[string]openTombstone)}
		err := client.dispatch(openRequestRejectedResponse("foreign-request", relayv1.OpenRequestFailure_OPEN_REQUEST_FAILURE_DUPLICATE_IN_FLIGHT))
		if !errors.Is(err, errProtocol) {
			t.Fatalf("foreign OpenRequestRejected = %v, want protocol failure", err)
		}
	})
}

func TestListenerDecisionRejectedStrictEnumDecoding(t *testing.T) {
	for _, failure := range []relayv1.ListenerDecisionFailure{
		relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST,
		relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING,
		relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE,
	} {
		t.Run("known/"+failure.String(), func(t *testing.T) {
			client, offer := acceptingOfferTestClient("attempt-known")
			if err := client.dispatch(listenerDecisionRejectedResponse(offer.attemptID, failure)); err != nil {
				t.Fatalf("dispatch known ListenerDecisionRejected: %v", err)
			}
			select {
			case err := <-offer.failure:
				if err == nil {
					t.Fatal("known ListenerDecisionRejected produced nil error")
				}
			default:
				t.Fatal("known ListenerDecisionRejected did not reject the Offer")
			}
		})
	}

	for _, test := range []struct {
		name string
		wire relayv1.ListenerDecisionFailure
	}{
		{name: "unspecified", wire: relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_UNSPECIFIED},
		{name: "unknown", wire: relayv1.ListenerDecisionFailure(99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, offer := acceptingOfferTestClient("attempt-invalid")
			err := client.dispatch(listenerDecisionRejectedResponse(offer.attemptID, test.wire))
			if !errors.Is(err, errProtocol) {
				t.Fatalf("dispatch ListenerDecisionRejected(%d) = %v, want protocol failure", test.wire, err)
			}
			if !offer.isAccepting() {
				t.Fatal("invalid ListenerDecisionRejected changed the Offer phase")
			}
			select {
			case result := <-offer.failure:
				t.Fatalf("invalid ListenerDecisionRejected completed the Offer: %v", result)
			default:
			}
		})
	}

	t.Run("foreign", func(t *testing.T) {
		client := &Client{authenticated: true, offers: make(map[string]*Offer)}
		err := client.dispatch(listenerDecisionRejectedResponse("foreign-attempt", relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE))
		if !errors.Is(err, errProtocol) {
			t.Fatalf("foreign ListenerDecisionRejected = %v, want protocol failure", err)
		}
	})

	t.Run("invalid identity", func(t *testing.T) {
		client, offer := acceptingOfferTestClient("attempt-current")
		err := client.dispatch(listenerDecisionRejectedResponse("", relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST))
		if !errors.Is(err, errProtocol) || client.offers[offer.attemptID] != offer || !offer.isAccepting() {
			t.Fatalf("invalid-identity ListenerDecisionRejected = %v, offer=%#v", err, client.offers[offer.attemptID])
		}
	})

	t.Run("wrong phase", func(t *testing.T) {
		client, offer := acceptingOfferTestClient("attempt-pending")
		offer.state = offerPending
		err := client.dispatch(listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE))
		if !errors.Is(err, errProtocol) || client.offers[offer.attemptID] != offer {
			t.Fatalf("wrong-phase ListenerDecisionRejected = %v, offer=%#v", err, client.offers[offer.attemptID])
		}
	})

	t.Run("retired", func(t *testing.T) {
		client, offer := acceptingOfferTestClient("attempt-retired")
		response := listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING)
		if err := client.dispatch(response); err != nil {
			t.Fatalf("first ListenerDecisionRejected: %v", err)
		}
		if err := client.dispatch(response); !errors.Is(err, errProtocol) {
			t.Fatalf("retired ListenerDecisionRejected = %v, want protocol failure", err)
		}
	})
}

func TestListenerDecisionRejectedRetiresExactOfferAndReservation(t *testing.T) {
	client, offer := acceptingReservedOfferTestClient("attempt-rejected")
	unrelatedOffer := newOffer(offer.listener, "attempt-unrelated", "caller-unrelated")
	client.offers[unrelatedOffer.attemptID] = unrelatedOffer
	unrelatedPipe := newPipe(client, "pipe-unrelated", "attempt-unrelated-pipe", "/other", "other-target")
	client.pipes[unrelatedPipe.id] = unrelatedPipe
	client.pipeSlots <- struct{}{}

	if err := client.dispatch(listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST)); err != nil {
		t.Fatalf("dispatch ListenerDecisionRejected: %v", err)
	}
	if _, exists := client.offers[offer.attemptID]; exists {
		t.Fatal("rejected Offer remained active")
	}
	if client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe {
		t.Fatal("rejection changed unrelated Offer or Pipe state")
	}
	if pipeID, exists := client.offerTombstones[offer.attemptID]; !exists || pipeID != "" {
		t.Fatalf("retired Offer history = %q, %t, want exact empty Pipe identity", pipeID, exists)
	}
	if len(client.pipeSlots) != 1 {
		t.Fatalf("Pipe slots after rejection = %d, want only unrelated Pipe slot", len(client.pipeSlots))
	}
	select {
	case err := <-offer.failure:
		if err == nil {
			t.Fatal("rejected Offer produced nil error")
		}
	default:
		t.Fatal("rejected Offer did not publish its failure")
	}
	select {
	case <-unrelatedPipe.Done():
		t.Fatalf("unrelated Pipe became terminal: %v", unrelatedPipe.Err())
	default:
	}

	terminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: offer.attemptID},
	}}
	for range 2 {
		if err := client.dispatch(terminal); err != nil {
			t.Fatalf("exact terminal after rejection was not idempotent: %v", err)
		}
	}
}

func TestListenerDecisionRejectedCleansExactProvisionalPipe(t *testing.T) {
	client, offer := acceptingReservedOfferTestClient("attempt-rejected")
	unrelatedOffer := newOffer(offer.listener, "attempt-unrelated", "caller-unrelated")
	client.offers[unrelatedOffer.attemptID] = unrelatedOffer
	unrelatedPipe := newPipe(client, "pipe-unrelated", "attempt-unrelated-pipe", "/other", "other-target")
	client.pipes[unrelatedPipe.id] = unrelatedPipe
	client.pipeSlots <- struct{}{}

	established := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerEstablished{
		ListenerEstablished: &relayv1.ListenerEstablished{AttemptId: offer.attemptID, PipeId: "pipe-rejected"},
	}}
	if err := client.dispatch(established); err != nil {
		t.Fatalf("dispatch ListenerEstablished: %v", err)
	}
	provisional := client.pipes["pipe-rejected"]
	if provisional == nil || offer.provisional != provisional || len(client.pipeSlots) != 2 {
		t.Fatalf("provisional Pipe registration = %#v, offer=%#v, slots=%d", provisional, offer.provisional, len(client.pipeSlots))
	}

	if err := client.dispatch(listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE)); err != nil {
		t.Fatalf("dispatch ListenerDecisionRejected: %v", err)
	}
	if _, exists := client.offers[offer.attemptID]; exists {
		t.Fatal("rejected Offer remained active")
	}
	if _, exists := client.pipes[provisional.id]; exists {
		t.Fatal("rejected provisional Pipe remained active")
	}
	if client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe {
		t.Fatal("provisional cleanup changed unrelated Offer or Pipe state")
	}
	if pipeID := client.offerTombstones[offer.attemptID]; pipeID != provisional.id {
		t.Fatalf("retired Offer Pipe identity = %q, want %q", pipeID, provisional.id)
	}
	if client.pipeTombstones[provisional.id] != provisional {
		t.Fatal("provisional Pipe terminal history was not retained")
	}
	select {
	case <-provisional.Done():
		if provisional.Err() == nil {
			t.Fatal("provisional Pipe terminal error is nil")
		}
	default:
		t.Fatal("provisional Pipe was not terminalized")
	}
	if len(client.pipeSlots) != 1 {
		t.Fatalf("Pipe slots after provisional cleanup = %d, want only unrelated Pipe slot", len(client.pipeSlots))
	}
	select {
	case <-unrelatedPipe.Done():
		t.Fatalf("unrelated Pipe became terminal: %v", unrelatedPipe.Err())
	default:
	}

	terminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: offer.attemptID, PipeId: provisional.id},
	}}
	for range 2 {
		if err := client.dispatch(terminal); err != nil {
			t.Fatalf("exact provisional terminal was not idempotent: %v", err)
		}
	}
	foreignTerminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: offer.attemptID, PipeId: "pipe-foreign"},
	}}
	if err := client.dispatch(foreignTerminal); !errors.Is(err, errProtocol) {
		t.Fatalf("foreign terminal after rejection = %v, want protocol failure", err)
	}
}

func TestPublicOpenOutcomeNumbersRemainCompatible(t *testing.T) {
	if OpenOutcomeFailed != 1 || OpenOutcomeCancelled != 2 || OpenOutcomeUnknown != 3 || OpenOutcomeRejected != 4 {
		t.Fatalf("OpenOutcome values = %d, %d, %d, %d", OpenOutcomeFailed, OpenOutcomeCancelled, OpenOutcomeUnknown, OpenOutcomeRejected)
	}
}

func TestPipeCloseNotOwnedHasTypedBackwardCompatibleError(t *testing.T) {
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		open := request.GetOpen()
		if open == nil {
			return status.Error(codes.FailedPrecondition, "Open required")
		}
		if err := stream.Send(pipeOpened(open, "attempt-not-owned", "pipe-not-owned")); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetClosePipe().GetPipeId() != "pipe-not-owned" {
			return status.Error(codes.FailedPrecondition, "ClosePipe required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{
			PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-not-owned", Owned: false},
		}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	client := connectTestClient(t, address)
	pipe, err := client.Open(context.Background(), "/service", "target")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = pipe.Close(context.Background())
	if !errors.Is(err, ErrPipeNotOwned) || !errors.Is(err, ErrPipeClosed) {
		t.Fatalf("Close not-owned = %v, want ErrPipeNotOwned and ErrPipeClosed compatibility", err)
	}
	if !errors.Is(pipe.Err(), ErrPipeNotOwned) || !errors.Is(pipe.Err(), ErrPipeClosed) {
		t.Fatalf("Pipe terminal = %v, want ErrPipeNotOwned and ErrPipeClosed compatibility", pipe.Err())
	}
}
