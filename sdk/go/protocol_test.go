package relaygate

import (
	"context"
	"errors"
	"fmt"
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
		offerTombstones: make(map[string]offerTombstone),
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

func listenerOfferResponse(attemptID string) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{
		ListenerOffer: &relayv1.ListenerOffer{
			AttemptId: attemptID, ListenerBindingId: "listener", Endpoint: "/service", TargetId: "target", CallerSessionId: "caller-session",
		},
	}}
}

func listenerConfirmationAcknowledgedResponse(attemptID, pipeID string) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerConfirmationAcknowledged{
		ListenerConfirmationAcknowledged: &relayv1.ListenerConfirmationAcknowledged{AttemptId: attemptID, PipeId: pipeID},
	}}
}

func TestListenerConfirmationAcknowledgedReplayIsExactAndBounded(t *testing.T) {
	client, offer := acceptingReservedOfferTestClient("attempt-confirmed")
	established := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerEstablished{
		ListenerEstablished: &relayv1.ListenerEstablished{AttemptId: offer.attemptID, PipeId: "pipe-confirmed"},
	}}
	if err := client.dispatch(established); err != nil {
		t.Fatalf("dispatch ListenerEstablished: %v", err)
	}
	acknowledged := listenerConfirmationAcknowledgedResponse(offer.attemptID, "pipe-confirmed")
	if err := client.dispatch(acknowledged); err != nil {
		t.Fatalf("dispatch first ListenerConfirmationAcknowledged: %v", err)
	}
	if _, exists := client.offers[offer.attemptID]; exists {
		t.Fatal("acknowledged Offer remained active")
	}
	if len(offer.ack) != 1 {
		t.Fatalf("first acknowledgement deliveries = %d, want 1", len(offer.ack))
	}
	for replay := 0; replay < 2; replay++ {
		if err := client.dispatch(acknowledged); err != nil {
			t.Fatalf("exact ListenerConfirmationAcknowledged replay %d = %v, want no-op", replay+1, err)
		}
	}
	if len(offer.ack) != 1 {
		t.Fatalf("acknowledgement deliveries after replay = %d, want 1", len(offer.ack))
	}

	conflicting := listenerConfirmationAcknowledgedResponse(offer.attemptID, "pipe-conflicting")
	if err := client.dispatch(conflicting); !errors.Is(err, errProtocol) {
		t.Fatalf("conflicting ListenerConfirmationAcknowledged = %v, want protocol failure", err)
	}
	if client.pipes["pipe-confirmed"] == nil || len(offer.ack) != 1 {
		t.Fatal("conflicting acknowledgement changed the live Pipe or redelivered the ACK")
	}

	rejected := &Client{
		authenticated: true,
		offers:        make(map[string]*Offer),
		offerTombstones: map[string]offerTombstone{
			"attempt-rejected": {decisionFailure: relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE},
		},
	}
	if err := rejected.dispatch(listenerConfirmationAcknowledgedResponse("attempt-rejected", "pipe-rejected")); !errors.Is(err, errProtocol) {
		t.Fatalf("confirmation after decision rejection = %v, want protocol failure", err)
	}

	bounded := &Client{authenticated: true, offers: make(map[string]*Offer), offerTombstones: make(map[string]offerTombstone)}
	bounded.mu.Lock()
	for index := 0; index <= maxPendingOffers; index++ {
		bounded.addOfferTombstoneLocked(fmt.Sprintf("attempt-%d", index), offerTombstone{pipeID: fmt.Sprintf("pipe-%d", index)})
	}
	bounded.mu.Unlock()
	if len(bounded.offerTombstones) != maxPendingOffers || len(bounded.offerHistory) != maxPendingOffers {
		t.Fatalf("confirmation replay history = %d records, %d order entries, want %d", len(bounded.offerTombstones), len(bounded.offerHistory), maxPendingOffers)
	}
	newest := maxPendingOffers
	if err := bounded.dispatch(listenerConfirmationAcknowledgedResponse(fmt.Sprintf("attempt-%d", newest), fmt.Sprintf("pipe-%d", newest))); err != nil {
		t.Fatalf("exact acknowledgement in bounded history = %v, want no-op", err)
	}
	if err := bounded.dispatch(listenerConfirmationAcknowledgedResponse("attempt-0", "pipe-0")); !errors.Is(err, errProtocol) {
		t.Fatalf("acknowledgement after bounded-history eviction = %v, want protocol failure", err)
	}
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

func TestListenerOfferCannotReviveRetiredAttempt(t *testing.T) {
	for _, test := range []struct {
		name    string
		retired offerTombstone
	}{
		{name: "terminal", retired: offerTombstone{pipeID: "pipe-retired"}},
		{name: "decision rejection", retired: offerTombstone{decisionFailure: relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const attemptID = "attempt-retired"
			client := &Client{
				authenticated:   true,
				listeners:       make(map[string]*Listener),
				offers:          make(map[string]*Offer),
				offerTombstones: map[string]offerTombstone{attemptID: test.retired},
			}
			listener := newListener(client, "listener", "/service", "target")
			client.listeners[listener.id] = listener

			if err := client.dispatch(listenerOfferResponse(attemptID)); !errors.Is(err, errProtocol) {
				t.Fatalf("ListenerOffer for retired attempt = %v, want protocol failure", err)
			}
			if len(client.offers) != 0 || client.offerTombstones[attemptID] != test.retired {
				t.Fatal("retired ListenerOffer changed active or terminal attempt state")
			}
			select {
			case revived := <-listener.offers:
				t.Fatalf("retired attempt became live: %#v", revived)
			default:
			}
		})
	}
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

	t.Run("retired by another outcome", func(t *testing.T) {
		client := &Client{
			authenticated: true,
			offers:        make(map[string]*Offer),
			offerTombstones: map[string]offerTombstone{
				"attempt-terminal": {pipeID: "pipe-terminal"},
			},
		}
		err := client.dispatch(listenerDecisionRejectedResponse("attempt-terminal", relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE))
		if !errors.Is(err, errProtocol) {
			t.Fatalf("ListenerDecisionRejected after another terminal outcome = %v, want protocol failure", err)
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

	t.Run("exact replay and conflicting replay", func(t *testing.T) {
		client, offer := acceptingOfferTestClient("attempt-retired")
		response := listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING)
		if err := client.dispatch(response); err != nil {
			t.Fatalf("first ListenerDecisionRejected: %v", err)
		}
		<-offer.failure
		if err := client.dispatch(response); err != nil {
			t.Fatalf("exact ListenerDecisionRejected replay = %v, want no-op", err)
		}
		select {
		case extra := <-offer.failure:
			t.Fatalf("exact ListenerDecisionRejected replay published another failure: %v", extra)
		default:
		}
		conflict := listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE)
		if err := client.dispatch(conflict); !errors.Is(err, errProtocol) {
			t.Fatalf("conflicting ListenerDecisionRejected replay = %v, want protocol failure", err)
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

	failure := relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST
	response := listenerDecisionRejectedResponse(offer.attemptID, failure)
	if err := client.dispatch(response); err != nil {
		t.Fatalf("dispatch ListenerDecisionRejected: %v", err)
	}
	if _, exists := client.offers[offer.attemptID]; exists {
		t.Fatal("rejected Offer remained active")
	}
	if client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe {
		t.Fatal("rejection changed unrelated Offer or Pipe state")
	}
	if retired, exists := client.offerTombstones[offer.attemptID]; !exists || retired.pipeID != "" || retired.decisionFailure != failure {
		t.Fatalf("retired Offer history = %#v, %t, want exact rejection", retired, exists)
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
	if err := client.dispatch(response); err != nil {
		t.Fatalf("exact ListenerDecisionRejected replay = %v, want no-op", err)
	}
	if client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe || len(client.pipeSlots) != 1 {
		t.Fatal("exact rejection replay changed unrelated state or released another Pipe slot")
	}

	terminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: offer.attemptID},
	}}
	retired := client.offerTombstones[offer.attemptID]
	if err := client.dispatch(terminal); !errors.Is(err, errProtocol) {
		t.Fatalf("ListenerTerminated after decision rejection = %v, want protocol failure", err)
	}
	if client.offerTombstones[offer.attemptID] != retired || client.offers[unrelatedOffer.attemptID] != unrelatedOffer ||
		client.pipes[unrelatedPipe.id] != unrelatedPipe || len(client.pipeSlots) != 1 {
		t.Fatal("ListenerTerminated after decision rejection changed terminal or unrelated state")
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

	failure := relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE
	response := listenerDecisionRejectedResponse(offer.attemptID, failure)
	if err := client.dispatch(response); err != nil {
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
	if retired := client.offerTombstones[offer.attemptID]; retired.pipeID != provisional.id || retired.decisionFailure != failure {
		t.Fatalf("retired Offer history = %#v, want Pipe %q and failure %s", retired, provisional.id, failure)
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
	if err := client.dispatch(response); err != nil {
		t.Fatalf("exact provisional rejection replay = %v, want no-op", err)
	}
	if client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe || len(client.pipeSlots) != 1 {
		t.Fatal("exact provisional rejection replay changed unrelated state or released another Pipe slot")
	}
	conflict := listenerDecisionRejectedResponse(offer.attemptID, relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING)
	if err := client.dispatch(conflict); !errors.Is(err, errProtocol) {
		t.Fatalf("conflicting provisional rejection replay = %v, want protocol failure", err)
	}
	if client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe || len(client.pipeSlots) != 1 {
		t.Fatal("conflicting provisional rejection replay changed unrelated state")
	}

	terminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: offer.attemptID, PipeId: provisional.id},
	}}
	retired := client.offerTombstones[offer.attemptID]
	if err := client.dispatch(terminal); !errors.Is(err, errProtocol) {
		t.Fatalf("ListenerTerminated after provisional decision rejection = %v, want protocol failure", err)
	}
	if client.offerTombstones[offer.attemptID] != retired || client.pipeTombstones[provisional.id] != provisional ||
		client.offers[unrelatedOffer.attemptID] != unrelatedOffer || client.pipes[unrelatedPipe.id] != unrelatedPipe || len(client.pipeSlots) != 1 {
		t.Fatal("ListenerTerminated after provisional decision rejection changed terminal or unrelated state")
	}
	foreignTerminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: offer.attemptID, PipeId: "pipe-foreign"},
	}}
	if err := client.dispatch(foreignTerminal); !errors.Is(err, errProtocol) {
		t.Fatalf("foreign terminal after rejection = %v, want protocol failure", err)
	}
}

func TestListenerDecisionRejectedReplayHistoryIsBounded(t *testing.T) {
	client := &Client{
		authenticated:   true,
		offers:          make(map[string]*Offer),
		offerTombstones: make(map[string]offerTombstone),
	}
	failure := relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING
	client.mu.Lock()
	for index := 0; index <= maxPendingOffers; index++ {
		attemptID := fmt.Sprintf("attempt-%d", index)
		client.addOfferTombstoneLocked(attemptID, offerTombstone{decisionFailure: failure})
	}
	client.mu.Unlock()
	if len(client.offerTombstones) != maxPendingOffers || len(client.offerHistory) != maxPendingOffers {
		t.Fatalf("rejection replay history = %d records, %d order entries, want %d", len(client.offerTombstones), len(client.offerHistory), maxPendingOffers)
	}
	if _, exists := client.offerTombstones["attempt-0"]; exists {
		t.Fatal("oldest rejection replay history was not evicted")
	}
	newest := fmt.Sprintf("attempt-%d", maxPendingOffers)
	if err := client.dispatch(listenerDecisionRejectedResponse(newest, failure)); err != nil {
		t.Fatalf("exact replay in bounded history = %v, want no-op", err)
	}
	if err := client.dispatch(listenerDecisionRejectedResponse("attempt-0", failure)); !errors.Is(err, errProtocol) {
		t.Fatalf("replay after bounded-history eviction = %v, want protocol failure", err)
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
