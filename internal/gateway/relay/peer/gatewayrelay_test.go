package gatewayrelay

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testTimeout = 2 * time.Second

type testOwner struct {
	open         func(context.Context, routing.OpenContext, localbinding.CallerEndpoint) (opening.Result, error)
	activate     func(clientsession.Ref, string) bool
	relayPayload func(context.Context, clientsession.Ref, string, string, []byte) error
	closePipe    func(clientsession.Ref, string) bool
}

func (o *testOwner) OpenForwarded(ctx context.Context, open routing.OpenContext, endpoint localbinding.CallerEndpoint) (opening.Result, error) {
	if o.open != nil {
		return o.open(ctx, open, endpoint)
	}
	return opening.Result{}, opening.ErrUnavailable
}

func (o *testOwner) ActivatePipe(caller clientsession.Ref, pipeID string) bool {
	return o.activate == nil || o.activate(caller, pipeID)
}

func (o *testOwner) RelayPayload(ctx context.Context, caller clientsession.Ref, pipeID, payloadID string, payload []byte) error {
	if o.relayPayload != nil {
		return o.relayPayload(ctx, caller, pipeID, payloadID, payload)
	}
	return nil
}

func (o *testOwner) ClosePipe(caller clientsession.Ref, pipeID string) bool {
	return o.closePipe == nil || o.closePipe(caller, pipeID)
}

type testCallerEndpoint struct {
	deliver func(context.Context, localbinding.PipePayload) error
	term    func(context.Context, string) error
}

func TestNewClientRequiresSeparatePositiveTimeouts(t *testing.T) {
	for _, test := range []struct {
		name           string
		connectTimeout time.Duration
		openTimeout    time.Duration
		maxPipes       uint32
	}{
		{name: "connect timeout", openTimeout: time.Second, maxPipes: 1},
		{name: "Open timeout", connectTimeout: time.Second, maxPipes: 1},
		{name: "Pipe capacity", connectTimeout: time.Second, openTimeout: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewClient(test.connectTimeout, test.openTimeout, test.maxPipes); err == nil {
				t.Fatal("NewClient() succeeded with invalid limits")
			}
		})
	}
	client, err := NewClient(25*time.Millisecond, time.Second, 1)
	if err != nil {
		t.Fatalf("NewClient(valid): %v", err)
	}
	defer client.Close()
	if client.connectTimeout != 25*time.Millisecond || client.openTimeout != time.Second {
		t.Fatalf("client timeouts = %s/%s", client.connectTimeout, client.openTimeout)
	}
}

func (e *testCallerEndpoint) DeliverPayload(ctx context.Context, payload localbinding.PipePayload) error {
	if e.deliver != nil {
		return e.deliver(ctx, payload)
	}
	return nil
}

func (e *testCallerEndpoint) TerminatePipe(ctx context.Context, pipeID string) error {
	if e.term != nil {
		return e.term(ctx, pipeID)
	}
	return nil
}

func TestGatewayRelayRoundTripActivationPayloadAndClose(t *testing.T) {
	ownerEndpoint := make(chan localbinding.CallerEndpoint, 1)
	ownerLifetime := make(chan context.Context, 1)
	activated := make(chan clientsession.Ref, 1)
	ingressPayload := make(chan localbinding.PipePayload, 1)
	closed := make(chan clientsession.Ref, 1)
	var closeCount atomic.Int32
	owner := &testOwner{}
	service, server := startGatewayRelay(t, owner, 4)
	_ = service
	open := validForwardedOpen(t, server.Address(), "attempt-round-trip")
	owner.open = func(ctx context.Context, got routing.OpenContext, endpoint localbinding.CallerEndpoint) (opening.Result, error) {
		assertSameOpenContext(t, got, open)
		ownerEndpoint <- endpoint
		ownerLifetime <- ctx
		return opening.Result{AttemptID: got.AttemptID, PipeID: "pipe-round-trip", Binding: got.Binding}, nil
	}
	owner.activate = func(caller clientsession.Ref, pipeID string) bool {
		if pipeID != "pipe-round-trip" {
			t.Errorf("ActivatePipe pipe = %q", pipeID)
		}
		activated <- caller
		return true
	}
	owner.relayPayload = func(_ context.Context, caller clientsession.Ref, pipeID, payloadID string, payload []byte) error {
		if caller != callerRef(open.Auth) || pipeID != "pipe-round-trip" {
			t.Errorf("RelayPayload caller/pipe = %#v/%q", caller, pipeID)
		}
		ingressPayload <- localbinding.PipePayload{PipeID: pipeID, PayloadID: payloadID, Data: append([]byte(nil), payload...)}
		return nil
	}
	owner.closePipe = func(caller clientsession.Ref, pipeID string) bool {
		closeCount.Add(1)
		if pipeID != "pipe-round-trip" {
			t.Errorf("ClosePipe pipe = %q", pipeID)
		}
		select {
		case closed <- caller:
		default:
		}
		return true
	}

	ownerPayload := make(chan localbinding.PipePayload, 1)
	callerEndpoint := &testCallerEndpoint{deliver: func(_ context.Context, payload localbinding.PipePayload) error {
		ownerPayload <- payload
		return nil
	}}
	client := newTestClient(t, 4)
	result, err := client.Open(context.Background(), open, callerEndpoint)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if result.AttemptID != open.AttemptID || result.PipeID != "pipe-round-trip" || !sameBinding(result.Binding, open.Binding) {
		t.Fatalf("Open result = %#v", result)
	}
	lifetime := receive(t, ownerLifetime)
	select {
	case <-lifetime.Done():
		t.Fatal("owner Pipe lifetime ended immediately after ForwardAccepted")
	default:
	}
	if err := result.Endpoint.Activate(context.Background()); err != nil {
		t.Fatalf("Activate(): %v", err)
	}
	if got := receive(t, activated); got != callerRef(open.Auth) {
		t.Fatalf("Activate caller = %#v", got)
	}

	fromIngress := []byte("ingress-to-owner")
	if err := result.Endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{PipeID: result.PipeID, PayloadID: "payload-ingress", Data: fromIngress}); err != nil {
		t.Fatalf("DeliverPayload(ingress): %v", err)
	}
	if got := receive(t, ingressPayload); !bytes.Equal(got.Data, fromIngress) {
		t.Fatalf("owner payload = %q", got.Data)
	}

	fromOwner := []byte("owner-to-ingress")
	if err := receive(t, ownerEndpoint).DeliverPayload(context.Background(), localbinding.PipePayload{PipeID: result.PipeID, PayloadID: "payload-owner", Data: fromOwner}); err != nil {
		t.Fatalf("DeliverPayload(owner): %v", err)
	}
	if got := receive(t, ownerPayload); !bytes.Equal(got.Data, fromOwner) {
		t.Fatalf("caller payload = %q", got.Data)
	}

	if err := result.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	select {
	case <-result.Endpoint.Done():
	case <-time.After(testTimeout):
		t.Fatal("remote endpoint did not become terminal")
	}
	if got := receive(t, closed); got != callerRef(open.Auth) {
		t.Fatalf("Close caller = %#v", got)
	}
	if got := closeCount.Load(); got != 1 {
		t.Fatalf("ClosePipe calls = %d, want one exact close", got)
	}
	select {
	case <-lifetime.Done():
	case <-time.After(testTimeout):
		t.Fatal("owner Pipe lifetime did not end after Close")
	}
}

func TestPeerReceiptStateRejectsConflictingOutcomeAndPayload(t *testing.T) {
	var state peerReceiptState
	payload := localbinding.PipePayload{PipeID: "pipe-1", PayloadID: "payload-1", Data: []byte("same")}
	result, duplicate, err := state.begin(payload)
	if err != nil || duplicate {
		t.Fatalf("begin = duplicate %t, err %v", duplicate, err)
	}
	if err := state.acknowledge(payload.PayloadID); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("receipt result: %v", err)
	}
	if _, duplicate, err := state.begin(payload); err != nil || !duplicate {
		t.Fatalf("exact payload replay = duplicate %t, err %v", duplicate, err)
	}
	conflicting := payload
	conflicting.Data = []byte("different")
	if _, _, err := state.begin(conflicting); err == nil {
		t.Fatal("conflicting payload replay succeeded")
	}
	if err := state.reject(payload.PayloadID, gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE); err == nil {
		t.Fatal("conflicting rejection after receipt succeeded")
	}

	var unknown peerReceiptState
	result, duplicate, err = unknown.begin(payload)
	if err != nil || duplicate {
		t.Fatalf("unknown begin = duplicate %t, err %v", duplicate, err)
	}
	unknown.retireUnknown(payload.PayloadID, result)
	if err := unknown.acknowledge(payload.PayloadID); err != nil {
		t.Fatalf("late receipt after Unknown: %v", err)
	}
	if unknown.lastOutcome != peerReceiptUnknown {
		t.Fatalf("late receipt changed Unknown to %v", unknown.lastOutcome)
	}
	if err := unknown.reject(payload.PayloadID, gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE); err != nil {
		t.Fatalf("late rejection after Unknown: %v", err)
	}
	if unknown.lastOutcome != peerReceiptUnknown {
		t.Fatalf("late rejection changed Unknown to %v", unknown.lastOutcome)
	}
}

type dropGatewayRelay struct {
	gatewayv1.UnimplementedGatewayRelayServer
	opens atomic.Int32
}

func (s *dropGatewayRelay) Forward(stream grpc.BidiStreamingServer[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetForwardOpen() == nil {
		return status.Error(codes.InvalidArgument, "ForwardOpen required")
	}
	s.opens.Add(1)
	return status.Error(codes.Unavailable, "transport dropped")
}

func startGatewayRelay(t *testing.T, owner Owner, maxPipes uint32) (*Service, *Server) {
	t.Helper()
	service, err := NewService(owner, testTimeout/2, maxPipes)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	server, err := Start(context.Background(), Config{
		BindAddress: "127.0.0.1:0",
		OpenTimeout: testTimeout / 2,
		MaxPipes:    maxPipes,
	}, service)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
	})
	return service, server
}

func startRawGatewayRelay(t *testing.T, service gatewayv1.GatewayRelayServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen(): %v", err)
	}
	server := grpc.NewServer()
	gatewayv1.RegisterGatewayRelayServer(server, service)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

func newTestClient(t *testing.T, maxPipes uint32) *Client {
	t.Helper()
	client, err := NewClient(testTimeout/4, testTimeout/2, maxPipes)
	if err != nil {
		t.Fatalf("NewClient(): %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func validForwardedOpen(t *testing.T, address, attemptID string) routing.OpenContext {
	t.Helper()
	auth := routing.AuthContext{
		ClientSessionID: "caller-session",
		ClientID:        "client-a",
		APIKeyID:        "key-a",
		AuthRevision:    "revision-a",
	}
	binding := routing.LiveBinding{
		Key: routing.BindingKey{
			ClientID:        auth.ClientID,
			EndpointPattern: "/relay/test",
			TargetID:        "target-a",
		},
		Ref: routing.ListenerBindingRef{
			GatewayID:         "gateway-owner",
			GatewayInstanceID: "owner-instance",
			ListenerBindingID: "listener-binding",
		},
	}
	open, err := routing.NewForwardedOpenContext(
		"epoch-a",
		"authority-a",
		attemptID,
		auth,
		binding,
		routing.ForwardingContext{
			IngressGatewayID:         "gateway-ingress",
			IngressGatewayInstanceID: "ingress-instance",
			IngressControlSessionID:  "control-session",
			OwnerControlSessionID:    "owner-control-session",
			OwnerRelayAddress:        address,
			ExpiresAt:                time.Now().Add(5 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	return open
}

func assertSameOpenContext(t *testing.T, got, want routing.OpenContext) {
	t.Helper()
	if got.ClusterEpoch != want.ClusterEpoch || got.AuthorityID != want.AuthorityID || got.AttemptID != want.AttemptID ||
		got.Auth != want.Auth || !sameBinding(got.Binding, want.Binding) ||
		got.IngressGatewayID != want.IngressGatewayID || got.IngressGatewayInstanceID != want.IngressGatewayInstanceID ||
		got.IngressControlSessionID != want.IngressControlSessionID || got.OwnerControlSessionID != want.OwnerControlSessionID ||
		got.OwnerRelayAddress != want.OwnerRelayAddress ||
		got.ExpiresAt.UnixMilli() != want.ExpiresAt.UnixMilli() {
		t.Fatalf("forwarded Open context mismatch:\n got %#v\nwant %#v", got, want)
	}
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for test event")
		var zero T
		return zero
	}
}
