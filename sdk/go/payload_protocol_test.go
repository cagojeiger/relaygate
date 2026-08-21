package relaygate

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

func TestPayloadFingerprintAndDeliveryOutcomeCorrelationAreExact(t *testing.T) {
	client := &Client{pipeSlots: make(chan struct{}, 1)}
	client.pipeSlots <- struct{}{}
	pipe := newPipe(client, "pipe-1", "attempt-1", "/service", "target")
	if accepted, duplicate, conflict := pipe.deliver("payload-1", []byte("same")); accepted == nil || duplicate || conflict {
		t.Fatalf("first delivery = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	if accepted, duplicate, conflict := pipe.deliver("payload-1", []byte("same")); accepted != nil || !duplicate || conflict {
		t.Fatalf("exact replay = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	if accepted, duplicate, conflict := pipe.deliver("payload-1", []byte("different")); accepted != nil || duplicate || !conflict {
		t.Fatalf("conflicting replay = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	if accepted, duplicate, conflict := pipe.deliver("payload-2", []byte("second")); accepted == nil || duplicate || conflict {
		t.Fatalf("interleaved delivery = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	if accepted, duplicate, conflict := pipe.deliver("payload-1", []byte("same")); accepted != nil || !duplicate || conflict {
		t.Fatalf("non-adjacent exact replay = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	if accepted, duplicate, conflict := pipe.deliver("payload-1", []byte("different")); accepted != nil || duplicate || !conflict {
		t.Fatalf("non-adjacent conflicting replay = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	if got := len(pipe.payloads); got != 2 {
		t.Fatalf("receive queue length = %d, want exactly two", got)
	}

	call := &deliveryCall{payloadID: "payload-out", result: make(chan error, 1)}
	pipe.delivery = call
	if matched, duplicate := pipe.finishDelivery("payload-out", DeliveryReceived, 0); !matched || duplicate {
		t.Fatalf("first receipt = matched %t duplicate %t", matched, duplicate)
	}
	if err := <-call.result; err != nil {
		t.Fatalf("Send result: %v", err)
	}
	if matched, duplicate := pipe.finishDelivery("payload-out", DeliveryReceived, 0); !matched || !duplicate {
		t.Fatalf("receipt replay = matched %t duplicate %t", matched, duplicate)
	}
	if matched, _ := pipe.finishDelivery("payload-out", DeliveryRejected, PipePayloadBackpressure); matched {
		t.Fatal("conflicting rejection matched received outcome")
	}

	unknownCall := &deliveryCall{payloadID: "payload-unknown", result: make(chan error, 1)}
	pipe.delivery = unknownCall
	if matched, duplicate := pipe.finishDelivery("payload-unknown", DeliveryUnknown, 0); !matched || duplicate {
		t.Fatalf("unknown outcome = matched %t duplicate %t", matched, duplicate)
	}
	<-unknownCall.result
	if matched, duplicate := pipe.finishDelivery("payload-unknown", DeliveryReceived, 0); !matched || !duplicate {
		t.Fatalf("late receipt = matched %t duplicate %t", matched, duplicate)
	}
	if pipe.lastDeliveryOutcome != DeliveryUnknown {
		t.Fatalf("late receipt changed Unknown to %v", pipe.lastDeliveryOutcome)
	}

	bounded := newPipe(client, "pipe-bounded", "attempt-2", "/service", "target")
	for index := 0; index <= maxReceivedPayloads; index++ {
		payloadID := fmt.Sprintf("payload-bounded-%d", index)
		accepted, duplicate, conflict := bounded.deliver(payloadID, []byte(payloadID))
		if accepted == nil || duplicate || conflict {
			t.Fatalf("bounded delivery %d = accepted %v duplicate %t conflict %t", index, accepted != nil, duplicate, conflict)
		}
		close(accepted.acknowledged)
		if _, err := bounded.Recv(context.Background()); err != nil {
			t.Fatalf("bounded Recv(%d): %v", index, err)
		}
	}
	if len(bounded.received) != maxReceivedPayloads || len(bounded.receivedOrder) != maxReceivedPayloads {
		t.Fatalf("received history = %d/%d, want %d", len(bounded.received), len(bounded.receivedOrder), maxReceivedPayloads)
	}
}

func TestRetiredPipePayloadRejectionRequiresExactDeliveryCorrelation(t *testing.T) {
	client := &Client{pipes: make(map[string]*Pipe), pipeTombstones: make(map[string]*Pipe)}
	pipe := newPipe(client, "pipe-retired", "attempt-1", "/service", "target")
	pipe.lastDeliveryID = "payload-1"
	pipe.lastDeliveryOutcome = DeliveryRejected
	pipe.lastDeliveryFailure = PipePayloadBackpressure
	client.pipeTombstones[pipe.id] = pipe

	exact := &relayv1.PipePayloadRejected{PipeId: pipe.id, PayloadId: "payload-1", Failure: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE}
	if err := client.dispatchPipePayloadRejected(exact); err != nil {
		t.Fatalf("exact retired rejection: %v", err)
	}
	wrongPayload := &relayv1.PipePayloadRejected{PipeId: pipe.id, PayloadId: "payload-2", Failure: exact.Failure}
	if err := client.dispatchPipePayloadRejected(wrongPayload); !errors.Is(err, errProtocol) {
		t.Fatalf("wrong-payload retired rejection = %v, want protocol error", err)
	}
	wrongFailure := &relayv1.PipePayloadRejected{PipeId: pipe.id, PayloadId: "payload-1", Failure: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE}
	if err := client.dispatchPipePayloadRejected(wrongFailure); !errors.Is(err, errProtocol) {
		t.Fatalf("wrong-failure retired rejection = %v, want protocol error", err)
	}
}

func TestPayloadIsNotExposedBeforeReceiptHandoff(t *testing.T) {
	client := &Client{pipeSlots: make(chan struct{}, 1)}
	client.pipeSlots <- struct{}{}
	pipe := newPipe(client, "pipe-1", "attempt-1", "/service", "target")
	accepted, duplicate, conflict := pipe.deliver("payload-1", []byte("first"))
	if accepted == nil || duplicate || conflict {
		t.Fatalf("delivery = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if payload, err := pipe.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) || payload != nil {
		t.Fatalf("Recv before receipt handoff = %q, %v", payload, err)
	}

	accepted, duplicate, conflict = pipe.deliver("payload-2", []byte("second"))
	if accepted == nil || duplicate || conflict {
		t.Fatalf("second delivery = accepted %v duplicate %t conflict %t", accepted != nil, duplicate, conflict)
	}
	close(accepted.acknowledged)
	payload, err := pipe.Recv(context.Background())
	if err != nil || string(payload) != "second" {
		t.Fatalf("Recv after receipt handoff = %q, %v", payload, err)
	}
}

func TestPayloadDeadlineBeforeWriterOwnershipIsNotSent(t *testing.T) {
	client := &Client{
		sendQueue: make(chan sendCommand, 1),
		pipeSlots: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	client.pipeSlots <- struct{}{}
	pipe := newPipe(client, "pipe-queued", "attempt-queued", "/service", "target")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := pipe.Send(ctx, []byte("queued"))
	if !errors.Is(err, ErrDeliveryNotSent) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send = %v, want NotSent deadline", err)
	}
	command := <-client.sendQueue
	if state := command.state.Load(); state != sendCanceled {
		t.Fatalf("queued command state = %d, want canceled-before-write", state)
	}
	select {
	case <-client.done:
		t.Fatal("pre-write deadline terminated Client")
	default:
	}
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
