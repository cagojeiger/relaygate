package relaygate

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBindDeadlineClosesSessionBeforeLateAcknowledgement(t *testing.T) {
	bindReceived := make(chan struct{})
	lateSend := make(chan error, 1)
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		bind := request.GetBindListener()
		if bind == nil {
			return status.Error(codes.FailedPrecondition, "Bind required")
		}
		close(bindReceived)
		<-stream.Context().Done()
		lateSend <- stream.Send(listenerBound("late-binding", bind.GetEndpointPattern(), bind.GetTargetId()))
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Bind(ctx, "/late", "worker")
		result <- err
	}()
	<-bindReceived
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Bind after cancellation = %v", err)
	}
	<-client.Done()
	if err := <-lateSend; err == nil {
		t.Fatal("late ListenerBound was written after cancelled Bind returned")
	}
}

func TestBlockedStreamSendCancellationFailsSessionBeforeReturn(t *testing.T) {
	clientCtx, stop := context.WithCancelCause(context.Background())
	stream := &blockingRelayClientStream{ctx: clientCtx, started: make(chan struct{})}
	client := &Client{
		ctx: clientCtx, cancel: stop, stream: stream,
		sendQueue: make(chan sendCommand, sendQueueCapacity), pipeSlots: make(chan struct{}, maxPipes), done: make(chan struct{}),
		listeners: make(map[string]*Listener), offers: make(map[string]*Offer), opens: make(map[string]*openCall), pipes: make(map[string]*Pipe),
	}
	client.tasks.Add(1)
	go client.runSender()
	go client.supervise()
	callCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.send(callCtx, &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{BindListener: &relayv1.BindListener{EndpointPattern: "/blocked", TargetId: "worker"}}})
	}()
	<-stream.started
	cancel()
	err := <-result
	var uncertain *sendUncertainError
	if !errors.As(err, &uncertain) || !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked send error = %v", err)
	}
	select {
	case <-client.Done():
	default:
		t.Fatal("blocked Send returned before the Client session closed")
	}
}

func TestPipeCloseDeadlineDoesNotCrossInFlightDelivery(t *testing.T) {
	clientCtx, stop := context.WithCancelCause(context.Background())
	stream := &blockingRelayClientStream{
		ctx: clientCtx, started: make(chan struct{}), requests: make(chan *relayv1.ConnectRequest, 1),
	}
	client := &Client{
		ctx: clientCtx, cancel: stop, stream: stream,
		sendQueue: make(chan sendCommand, sendQueueCapacity), pipeSlots: make(chan struct{}, maxPipes), done: make(chan struct{}),
		listeners: make(map[string]*Listener), offers: make(map[string]*Offer), opens: make(map[string]*openCall),
		pipes: make(map[string]*Pipe), closeCalls: make(map[string]*closeCall),
	}
	if !client.reservePipeSlot() {
		t.Fatal("reserve Pipe slot")
	}
	pipe := newPipe(client, "pipe-blocked", "attempt-blocked", "/blocked", "worker")
	client.pipes[pipe.id] = pipe
	client.tasks.Add(1)
	go client.runSender()
	go client.supervise()
	t.Cleanup(func() {
		client.stop(errExplicitClose)
		<-client.Done()
	})

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- pipe.Send(context.Background(), []byte("first-payload"))
	}()
	<-stream.started
	first := <-stream.requests
	if string(first.GetPipePayload().GetPayload()) != "first-payload" {
		t.Fatalf("first wire request = %T, want first PipePayload", first.GetMessage())
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelClose()
	closeResult := make(chan error, 1)
	go func() { closeResult <- pipe.Close(closeCtx) }()
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		client.stop(errors.New("test: Close remained blocked behind payload Send"))
		<-client.Done()
		t.Fatal("Close did not honor its deadline while payload Send was blocked")
	}

	select {
	case queued := <-client.sendQueue:
		t.Fatalf("Close crossed in-flight delivery: %T", queued.request.GetMessage())
	default:
	}
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelSecond()
	if err := pipe.Send(secondCtx, []byte("second-payload")); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrDeliveryNotSent) {
		t.Fatalf("second Send = %v, want NotSent deadline before admission", err)
	}
	client.stop(errors.New("test: end blocked delivery"))
	<-client.Done()
	if err := <-sendResult; !errors.Is(err, ErrDeliveryUnknown) {
		t.Fatalf("blocked payload Send = %v, want ErrDeliveryUnknown", err)
	}
}

func TestForeignMessageFailsClosedAndBoundsAreFixed(t *testing.T) {
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		return stream.Send(pipePayload("foreign-pipe", "foreign-payload", []byte("payload")))
	})
	client := connectTestClient(t, address)
	<-client.Done()
	if !errors.Is(client.Err(), errProtocol) {
		t.Fatalf("Client Err = %v, want protocol failure", client.Err())
	}

	bounded := &Client{pipeSlots: make(chan struct{}, maxPipes)}
	for index := 0; index < maxPipes; index++ {
		if !bounded.reservePipeSlot() {
			t.Fatalf("reservation %d rejected before bound", index)
		}
	}
	if bounded.reservePipeSlot() {
		t.Fatal("reservation beyond combined Pipe bound succeeded")
	}
	bounded.releasePipeSlot()
	if !bounded.reservePipeSlot() {
		t.Fatal("released Pipe reservation was not reusable")
	}

	history := &Client{bindingRecords: make(map[string]bindingRecord)}
	for index := 0; index < maxPendingOffers; index++ {
		record := bindingRecord{id: fmt.Sprintf("binding-%d", index), endpoint: "/bounded", target: "worker", unbound: index >= maxListeners}
		if !history.addBindingRecordLocked(record) {
			t.Fatalf("initial binding history rejected at %d", index)
		}
	}
	for index := 0; index < maxPendingOffers*2; index++ {
		record := bindingRecord{id: fmt.Sprintf("retired-%d", index), endpoint: "/bounded", target: "worker", unbound: true}
		if !history.addBindingRecordLocked(record) {
			t.Fatalf("binding history churn rejected at %d", index)
		}
		if len(history.bindingRecords) > maxPendingOffers || len(history.bindingHistory) > maxPendingOffers {
			t.Fatalf("binding history exceeded bound: records=%d order=%d", len(history.bindingRecords), len(history.bindingHistory))
		}
	}
	for index := 0; index < maxListeners; index++ {
		if _, exists := history.bindingRecords[fmt.Sprintf("binding-%d", index)]; !exists {
			t.Fatalf("live binding %d was evicted during retired-history churn", index)
		}
	}
}

func TestInsecureRequiresLoopback(t *testing.T) {
	_, err := Connect(context.Background(), NewConfig("example.com:443", "client", "key", "secret").WithInsecureLocal())
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Connect(non-loopback Insecure) = %v", err)
	}
}

func TestConfigRedactsSecretAndClonesTLSConfig(t *testing.T) {
	const secret = "never-format-this-api-key"
	baseTLS := &tls.Config{ServerName: "relay.example", MinVersion: tls.VersionTLS13}
	config := NewConfig("relay.example:443", "client", "key", secret).WithTLSConfig(baseTLS)
	baseTLS.ServerName = "mutated.example"
	if config.TLSConfig == baseTLS || config.TLSConfig.ServerName != "relay.example" {
		t.Fatalf("WithTLSConfig did not own a clone: %#v", config.TLSConfig)
	}
	for _, formatted := range []string{
		fmt.Sprintf("%v", config),
		fmt.Sprintf("%+v", config),
		fmt.Sprintf("%#v", config),
		fmt.Sprintf("%v", &config),
		fmt.Sprintf("%#v", &config),
	} {
		if strings.Contains(formatted, secret) || !strings.Contains(formatted, "redacted") {
			t.Fatalf("formatted Config was not redacted: %s", formatted)
		}
	}
	if err := validateConfig(NewConfig("relay.example:443", "client", "key", secret)); err != nil {
		t.Fatalf("default system-roots TLS config rejected: %v", err)
	}
}

func listenerBound(id, endpoint, target string) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerBound{ListenerBound: &relayv1.ListenerBound{Binding: &relayv1.ListenerBinding{
		ListenerBindingId: id, EndpointPattern: endpoint, TargetId: target,
	}}}}
}

func pipePayload(pipeID, payloadID string, payload []byte) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayload{PipePayload: &relayv1.PipePayload{PipeId: pipeID, PayloadId: payloadID, Payload: payload}}}
}

func pipeOpened(open *relayv1.Open, attemptID, pipeID string) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpened{PipeOpened: &relayv1.PipeOpened{
		RequestId: open.GetRequestId(), AttemptId: attemptID, PipeId: pipeID, Endpoint: open.GetEndpoint(), TargetId: open.GetTargetId(),
	}}}
}
