package relaygate

import (
	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
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
