package relaygrpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"github.com/cagojeiger/relaygate/internal/localbinding"
	"github.com/cagojeiger/relaygate/internal/opening"
)

func TestExactSameGatewayOpenAcrossRealBindingOpeningAndRelayLayers(t *testing.T) {
	authStore, err := clientauth.NewStore(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	sessions, err := clientsession.NewManager(authStore, 10)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	committer := &openIntegrationCommitter{}
	bindings, err := localbinding.New("gateway-a", "instance-a", 10, committer, sessions)
	if err != nil {
		t.Fatalf("localbinding.New(): %v", err)
	}
	opener, err := opening.New(opening.Config{
		ClusterEpoch: "epoch-1",
		MaxPipes:     10,
		OpenTimeout:  time.Second,
	}, &openIntegrationAdmitter{committer: committer}, bindings)
	if err != nil {
		t.Fatalf("opening.New(): %v", err)
	}
	service, err := NewService(sessions, bindings, opener, time.Second, time.Second, 10)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	server, err := Start(context.Background(), Config{BindAddress: "127.0.0.1:0", MaxConcurrentStreams: 10}, service)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
		opener.Close()
		bindings.Close()
		sessions.Close()
	})

	connection := dialTestServer(t, server.Address())
	listener := authenticateTestStream(t, connection)
	caller := authenticateTestStream(t, connection)
	bindListener(t, listener, "/jobs/one", "worker")

	if err := caller.Send(openRequest("request-1", "/jobs/one", "worker")); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	offerResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerOffer): %v", err)
	}
	offer := offerResponse.GetListenerOffer()
	if offer.GetAttemptId() == "" || offer.GetEndpoint() != "/jobs/one" || offer.GetTargetId() != "worker" {
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
	if established.GetAttemptId() != offer.GetAttemptId() || established.GetPipeId() == "" {
		t.Fatalf("ListenerEstablished = %#v", established)
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
		ListenerConfirmed: &relayv1.ListenerConfirmed{AttemptId: established.GetAttemptId(), PipeId: established.GetPipeId()},
	}}); err != nil {
		t.Fatalf("Send(ListenerConfirmed): %v", err)
	}
	requireListenerConfirmationAcknowledged(t, listener, established.GetAttemptId(), established.GetPipeId())
	preActivationPayload := []byte{0x00, 0x01, 0xfe, 0xff}
	sendPipePayload(t, listener, established.GetPipeId(), preActivationPayload)
	openedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpened): %v", err)
	}
	opened := openedResponse.GetPipeOpened()
	if opened.GetRequestId() != "request-1" || opened.GetAttemptId() != offer.GetAttemptId() || opened.GetPipeId() != established.GetPipeId() {
		t.Fatalf("PipeOpened = %#v", opened)
	}
	requirePipePayload(t, caller, established.GetPipeId(), preActivationPayload)

	callerFrames := [][]byte{
		[]byte("caller-frame-one"),
		{0x00, 0x7f, 0x80, 0xff},
	}
	for _, payload := range callerFrames {
		sendPipePayload(t, caller, established.GetPipeId(), payload)
	}
	for _, payload := range callerFrames {
		requirePipePayload(t, listener, established.GetPipeId(), payload)
	}

	listenerFrames := [][]byte{
		[]byte("listener-frame-one"),
		{0xff, 0x80, 0x7f, 0x00},
	}
	for _, payload := range listenerFrames {
		sendPipePayload(t, listener, established.GetPipeId(), payload)
	}
	for _, payload := range listenerFrames {
		requirePipePayload(t, caller, established.GetPipeId(), payload)
	}

	maximumPayload := bytes.Repeat([]byte{0xa5}, localbinding.MaxPayloadBytes)
	sendPipePayload(t, caller, established.GetPipeId(), maximumPayload)
	requirePipePayload(t, listener, established.GetPipeId(), maximumPayload)
	sendPipePayload(t, caller, established.GetPipeId(), append(maximumPayload, 0x5a))
	rejectedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(maximum+1 PipePayloadRejected): %v", err)
	}
	if rejected := rejectedResponse.GetPipePayloadRejected(); rejected.GetPipeId() != established.GetPipeId() ||
		rejected.GetFailure() != relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST {
		t.Fatalf("maximum+1 PipePayloadRejected = %#v", rejectedResponse)
	}
	if opener.ActiveCount() != 1 {
		t.Fatalf("active pipes = %d, want 1", opener.ActiveCount())
	}

	slot := committer.current()
	if slot.Ref == nil {
		t.Fatal("binding disappeared before explicit Unbind")
	}
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_UnbindListener{
		UnbindListener: &relayv1.UnbindListener{ListenerBindingId: slot.Ref.ListenerBindingID},
	}}); err != nil {
		t.Fatalf("Send(UnbindListener): %v", err)
	}
	if response, err := listener.Recv(); err != nil || response.GetListenerUnbound() == nil {
		t.Fatalf("Recv(ListenerUnbound) = %#v, %v", response, err)
	}
	waitForCount(t, bindings.ActiveCount, 0, "active bindings")
	if opener.ActiveCount() != 1 {
		t.Fatalf("explicit Unbind ended accepted pipe: active=%d", opener.ActiveCount())
	}

	if err := caller.CloseSend(); err != nil {
		t.Fatalf("CloseSend(caller): %v", err)
	}
	if _, err := caller.Recv(); err != io.EOF {
		t.Fatalf("Recv(caller close) = %v, want EOF", err)
	}
	waitForCount(t, opener.ActiveCount, 0, "active pipes")
	terminatedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerTerminated): %v", err)
	}
	terminated := terminatedResponse.GetListenerTerminated()
	if terminated.GetAttemptId() != offer.GetAttemptId() || terminated.GetPipeId() != established.GetPipeId() {
		t.Fatalf("ListenerTerminated = %#v", terminated)
	}
}

func sendPipePayload(t *testing.T, stream interface {
	Send(*relayv1.ConnectRequest) error
}, pipeID string, payload []byte) {
	t.Helper()
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayload{
		PipePayload: &relayv1.PipePayload{PipeId: pipeID, Payload: payload},
	}}); err != nil {
		t.Fatalf("Send(PipePayload): %v", err)
	}
}

func requirePipePayload(t *testing.T, stream interface {
	Recv() (*relayv1.ConnectResponse, error)
}, pipeID string, want []byte) {
	t.Helper()
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(PipePayload): %v", err)
	}
	payload := response.GetPipePayload()
	if payload.GetPipeId() != pipeID || !bytes.Equal(payload.GetPayload(), want) {
		t.Fatalf("PipePayload = %#v, want pipe %q and %d exact bytes", payload, pipeID, len(want))
	}
}

type openIntegrationCommitter struct {
	mu   sync.Mutex
	slot controlstate.BindingSlot
}

func (c *openIntegrationCommitter) Install(_ context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slot.Ref != nil {
		return controlstate.BindingSlot{}, controlstate.ErrCASMismatch
	}
	c.slot = controlstate.BindingSlot{Key: key, Generation: 1, Ref: cloneIntegrationRef(&ref)}
	return cloneIntegrationSlot(c.slot), nil
}

func (c *openIntegrationCommitter) Remove(_ context.Context, slot controlstate.BindingSlot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !integrationSlotsEqual(c.slot, slot) {
		return controlstate.ErrCASMismatch
	}
	c.slot = controlstate.BindingSlot{Key: slot.Key, Generation: slot.Generation + 1}
	return nil
}

func (c *openIntegrationCommitter) current() controlstate.BindingSlot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneIntegrationSlot(c.slot)
}

type openIntegrationAdmitter struct {
	committer *openIntegrationCommitter
	sequence  atomic.Uint64
}

func (a *openIntegrationAdmitter) AdmitOpen(_ context.Context, caller clientsession.Ref, endpoint, targetID string) (authority.OpenContext, error) {
	slot := a.committer.current()
	key := controlstate.BindingKey{ClientID: caller.ClientID, EndpointPattern: endpoint, TargetID: targetID}
	if slot.Ref == nil || slot.Key != key {
		return authority.OpenContext{}, authority.ErrRouteNotFound
	}
	return authority.NewOpenContext(
		"epoch-1",
		"authority-1",
		fmt.Sprintf("attempt-%d", a.sequence.Add(1)),
		authority.AuthContext{
			ClientSessionID: caller.ClientSessionID,
			ClientID:        caller.ClientID,
			APIKeyID:        caller.APIKeyID,
			AuthRevision:    caller.AuthRevision,
		},
		slot,
	)
}

func cloneIntegrationSlot(slot controlstate.BindingSlot) controlstate.BindingSlot {
	copy := slot
	copy.Ref = cloneIntegrationRef(slot.Ref)
	return copy
}

func cloneIntegrationRef(ref *controlstate.ListenerBindingRef) *controlstate.ListenerBindingRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	return &copy
}

func integrationSlotsEqual(left, right controlstate.BindingSlot) bool {
	if left.Key != right.Key || left.Generation != right.Generation {
		return false
	}
	if left.Ref == nil || right.Ref == nil {
		return left.Ref == nil && right.Ref == nil
	}
	return *left.Ref == *right.Ref
}

func waitForCount(t *testing.T, count func() int, want int, label string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if count() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s = %d, want %d", label, count(), want)
}
