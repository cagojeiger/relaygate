package relaygate

import (
	"errors"
	"testing"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

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
