package gatewayrelay

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
	"github.com/cagojeiger/relaygate/internal/localbinding"
	"github.com/cagojeiger/relaygate/internal/opening"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testTimeout = 2 * time.Second

type testOwner struct {
	open         func(context.Context, authority.OpenContext, localbinding.CallerEndpoint) (opening.Result, error)
	activate     func(clientsession.Ref, string) bool
	relayPayload func(context.Context, clientsession.Ref, string, []byte) error
	closePipe    func(clientsession.Ref, string) bool
}

func (o *testOwner) OpenForwarded(ctx context.Context, open authority.OpenContext, endpoint localbinding.CallerEndpoint) (opening.Result, error) {
	if o.open != nil {
		return o.open(ctx, open, endpoint)
	}
	return opening.Result{}, opening.ErrUnavailable
}

func (o *testOwner) ActivatePipe(caller clientsession.Ref, pipeID string) bool {
	return o.activate == nil || o.activate(caller, pipeID)
}

func (o *testOwner) RelayPayload(ctx context.Context, caller clientsession.Ref, pipeID string, payload []byte) error {
	if o.relayPayload != nil {
		return o.relayPayload(ctx, caller, pipeID, payload)
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
	owner.open = func(ctx context.Context, got authority.OpenContext, endpoint localbinding.CallerEndpoint) (opening.Result, error) {
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
	owner.relayPayload = func(_ context.Context, caller clientsession.Ref, pipeID string, payload []byte) error {
		if caller != callerRef(open.Auth) || pipeID != "pipe-round-trip" {
			t.Errorf("RelayPayload caller/pipe = %#v/%q", caller, pipeID)
		}
		ingressPayload <- localbinding.PipePayload{PipeID: pipeID, Data: append([]byte(nil), payload...)}
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
	if err := result.Endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{PipeID: result.PipeID, Data: fromIngress}); err != nil {
		t.Fatalf("DeliverPayload(ingress): %v", err)
	}
	if got := receive(t, ingressPayload); !bytes.Equal(got.Data, fromIngress) {
		t.Fatalf("owner payload = %q", got.Data)
	}

	fromOwner := []byte("owner-to-ingress")
	if err := receive(t, ownerEndpoint).DeliverPayload(context.Background(), localbinding.PipePayload{PipeID: result.PipeID, Data: fromOwner}); err != nil {
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

func TestGatewayRelayMapsStableOpenFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "capacity", err: opening.ErrCapacity},
		{name: "replay", err: opening.ErrAttemptReplay},
		{name: "expired", err: opening.ErrContextExpired},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &testOwner{open: func(context.Context, authority.OpenContext, localbinding.CallerEndpoint) (opening.Result, error) {
				return opening.Result{}, test.err
			}}
			_, server := startGatewayRelay(t, owner, 1)
			client := newTestClient(t, 1)
			_, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-"+test.name), &testCallerEndpoint{})
			if !errors.Is(err, test.err) {
				t.Fatalf("Open() = %v, want %v", err, test.err)
			}
		})
	}
}

func TestGatewayRelayTransportLossAfterForwardOpenIsUnknownAndNotRetried(t *testing.T) {
	drop := &dropGatewayRelay{}
	address, stop := startRawGatewayRelay(t, drop)
	defer stop()
	client := newTestClient(t, 2)
	_, err := client.Open(context.Background(), validForwardedOpen(t, address, "attempt-drop"), &testCallerEndpoint{})
	if !errors.Is(err, opening.ErrUnknown) {
		t.Fatalf("Open() = %v, want ErrUnknown", err)
	}
	if got := drop.opens.Load(); got != 1 {
		t.Fatalf("Forward streams = %d, want one without retry", got)
	}
}

func TestGatewayRelayClientCapacityIsProcessWide(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	owner := &testOwner{open: func(ctx context.Context, open authority.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
		close(started)
		select {
		case <-release:
			return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-one", Binding: open.Binding}, nil
		case <-ctx.Done():
			return opening.Result{}, ctx.Err()
		}
	}}
	_, server := startGatewayRelay(t, owner, 2)
	client := newTestClient(t, 1)
	firstOpen := validForwardedOpen(t, server.Address(), "attempt-one")
	firstDone := make(chan error, 1)
	go func() {
		result, err := client.Open(context.Background(), firstOpen, &testCallerEndpoint{})
		if err == nil {
			_ = result.Endpoint.Close(context.Background())
		}
		firstDone <- err
	}()
	receive(t, started)
	_, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-two"), &testCallerEndpoint{})
	if !errors.Is(err, opening.ErrCapacity) {
		t.Fatalf("second Open() = %v, want ErrCapacity", err)
	}
	close(release)
	if err := receive(t, firstDone); err != nil {
		t.Fatalf("first Open() = %v", err)
	}
}

func TestGatewayRelayServerCapacityIsProcessWideAcrossClients(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	owner := &testOwner{open: func(ctx context.Context, open authority.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
		if open.AttemptID == "attempt-server-one" {
			close(started)
			select {
			case <-release:
				return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-server-one", Binding: open.Binding}, nil
			case <-ctx.Done():
				return opening.Result{}, ctx.Err()
			}
		}
		return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-server-two", Binding: open.Binding}, nil
	}}
	_, server := startGatewayRelay(t, owner, 1)
	firstClient := newTestClient(t, 1)
	secondClient := newTestClient(t, 1)
	firstOpen := validForwardedOpen(t, server.Address(), "attempt-server-one")
	firstDone := make(chan error, 1)
	go func() {
		result, err := firstClient.Open(context.Background(), firstOpen, &testCallerEndpoint{})
		if err == nil {
			_ = result.Endpoint.Close(context.Background())
		}
		firstDone <- err
	}()
	receive(t, started)
	_, err := secondClient.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-server-two"), &testCallerEndpoint{})
	if !errors.Is(err, opening.ErrCapacity) {
		t.Fatalf("second client Open() = %v, want server ErrCapacity", err)
	}
	close(release)
	if err := receive(t, firstDone); err != nil {
		t.Fatalf("first Open() = %v", err)
	}
}

func TestGatewayRelayClientCloseCancelsAndJoinsActivePipe(t *testing.T) {
	ownerLifetime := make(chan context.Context, 1)
	owner := &testOwner{open: func(ctx context.Context, open authority.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
		ownerLifetime <- ctx
		return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-client-close", Binding: open.Binding}, nil
	}}
	_, server := startGatewayRelay(t, owner, 1)
	client, err := NewClient(testTimeout/4, testTimeout/2, 1)
	if err != nil {
		t.Fatalf("NewClient(): %v", err)
	}
	result, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-client-close"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	lifetime := receive(t, ownerLifetime)
	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	receive(t, closed)
	select {
	case <-result.Endpoint.Done():
	default:
		t.Fatal("Client.Close returned before endpoint workers joined")
	}
	select {
	case <-lifetime.Done():
	case <-time.After(testTimeout):
		t.Fatal("Client.Close did not end owner stream lifetime")
	}
}

func TestGatewayRelayTerminalCancelsBlockedInboundDeliveryAndJoins(t *testing.T) {
	ownerEndpoint := make(chan localbinding.CallerEndpoint, 1)
	owner := &testOwner{open: func(_ context.Context, open authority.OpenContext, endpoint localbinding.CallerEndpoint) (opening.Result, error) {
		ownerEndpoint <- endpoint
		return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-terminal", Binding: open.Binding}, nil
	}}
	_, server := startGatewayRelay(t, owner, 1)
	deliveryStarted := make(chan struct{})
	deliveryCanceled := make(chan struct{})
	callerEndpoint := &testCallerEndpoint{deliver: func(ctx context.Context, _ localbinding.PipePayload) error {
		close(deliveryStarted)
		<-ctx.Done()
		close(deliveryCanceled)
		return ctx.Err()
	}}
	client := newTestClient(t, 1)
	result, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-terminal"), callerEndpoint)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if err := result.Endpoint.Activate(context.Background()); err != nil {
		t.Fatalf("Activate(): %v", err)
	}
	endpoint := receive(t, ownerEndpoint)
	if err := endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{PipeID: result.PipeID, Data: []byte("blocked")}); err != nil {
		t.Fatalf("owner DeliverPayload(): %v", err)
	}
	receive(t, deliveryStarted)
	if err := endpoint.TerminatePipe(context.Background(), result.PipeID); err != nil {
		t.Fatalf("TerminatePipe(): %v", err)
	}
	receive(t, deliveryCanceled)
	select {
	case <-result.Endpoint.Done():
	case <-time.After(testTimeout):
		t.Fatal("Done did not wait for canceled delivery worker")
	}
}

func TestGatewayRelayOwnerTerminalCancelsBlockedOwnerPayload(t *testing.T) {
	ownerEndpoint := make(chan localbinding.CallerEndpoint, 1)
	relayStarted := make(chan struct{})
	relayCanceled := make(chan struct{})
	owner := &testOwner{
		open: func(_ context.Context, open authority.OpenContext, endpoint localbinding.CallerEndpoint) (opening.Result, error) {
			ownerEndpoint <- endpoint
			return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-owner-blocked", Binding: open.Binding}, nil
		},
		relayPayload: func(ctx context.Context, _ clientsession.Ref, _ string, _ []byte) error {
			close(relayStarted)
			<-ctx.Done()
			close(relayCanceled)
			return ctx.Err()
		},
	}
	_, server := startGatewayRelay(t, owner, 1)
	client := newTestClient(t, 1)
	result, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-owner-blocked"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if err := result.Endpoint.Activate(context.Background()); err != nil {
		t.Fatalf("Activate(): %v", err)
	}
	endpoint := receive(t, ownerEndpoint)
	if err := result.Endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{PipeID: result.PipeID, Data: []byte("blocked")}); err != nil {
		t.Fatalf("DeliverPayload(): %v", err)
	}
	receive(t, relayStarted)
	if err := endpoint.TerminatePipe(context.Background(), result.PipeID); err != nil {
		t.Fatalf("TerminatePipe(): %v", err)
	}
	receive(t, relayCanceled)
	select {
	case <-result.Endpoint.Done():
	case <-time.After(testTimeout):
		t.Fatal("remote endpoint did not join after owner payload cancellation")
	}
}

func TestGatewayRelayRejectsOversizedPayloadBeforeOwner(t *testing.T) {
	var relayed atomic.Int32
	owner := &testOwner{
		open: func(_ context.Context, open authority.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
			return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-limit", Binding: open.Binding}, nil
		},
		relayPayload: func(context.Context, clientsession.Ref, string, []byte) error {
			relayed.Add(1)
			return nil
		},
	}
	_, server := startGatewayRelay(t, owner, 1)
	client := newTestClient(t, 1)
	result, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "attempt-limit"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if err := result.Endpoint.Activate(context.Background()); err != nil {
		t.Fatalf("Activate(): %v", err)
	}
	err = result.Endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{
		PipeID: result.PipeID,
		Data:   make([]byte, localbinding.MaxPayloadBytes+1),
	})
	if err == nil {
		t.Fatal("oversized DeliverPayload succeeded")
	}
	if got := relayed.Load(); got != 0 {
		t.Fatalf("owner RelayPayload calls = %d", got)
	}
	_ = result.Endpoint.Close(context.Background())
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

func validForwardedOpen(t *testing.T, address, attemptID string) authority.OpenContext {
	t.Helper()
	auth := authority.AuthContext{
		ClientSessionID: "caller-session",
		ClientID:        "client-a",
		APIKeyID:        "key-a",
		AuthRevision:    "revision-a",
	}
	binding := controlstate.BindingSlot{
		Key: controlstate.BindingKey{
			ClientID:        auth.ClientID,
			EndpointPattern: "/relay/test",
			TargetID:        "target-a",
		},
		Generation: 1,
		Ref: &controlstate.ListenerBindingRef{
			GatewayID:         "gateway-owner",
			GatewayInstanceID: "owner-instance",
			ListenerBindingID: "listener-binding",
		},
	}
	open, err := authority.NewForwardedOpenContext(
		"epoch-a",
		"authority-a",
		attemptID,
		auth,
		binding,
		authority.ForwardingContext{
			IngressGatewayID:         "gateway-ingress",
			IngressGatewayInstanceID: "ingress-instance",
			IngressControlSessionID:  "control-session",
			OwnerRelayAddress:        address,
			ExpiresAt:                time.Now().Add(5 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	return open
}

func assertSameOpenContext(t *testing.T, got, want authority.OpenContext) {
	t.Helper()
	if got.ClusterEpoch != want.ClusterEpoch || got.AuthorityID != want.AuthorityID || got.AttemptID != want.AttemptID ||
		got.Auth != want.Auth || !sameBinding(got.Binding, want.Binding) ||
		got.IngressGatewayID != want.IngressGatewayID || got.IngressGatewayInstanceID != want.IngressGatewayInstanceID ||
		got.IngressControlSessionID != want.IngressControlSessionID || got.OwnerRelayAddress != want.OwnerRelayAddress ||
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
