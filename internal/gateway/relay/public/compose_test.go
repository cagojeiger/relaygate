package relaygrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	composeClientID = "local-development"
	composeAPIKeyID = "primary"
	composeAPIKey   = "relaygate-local-development-key"
)

func TestComposePublicRelaySmoke(t *testing.T) {
	address := os.Getenv("RELAYGATE_COMPOSE_RELAY_ADDR")
	if address == "" {
		t.Skip("RELAYGATE_COMPOSE_RELAY_ADDR is not set")
	}
	runComposeRelaySmoke(t, address, address)
}

func TestComposeCrossGatewayRelaySmoke(t *testing.T) {
	callerAddress := os.Getenv("RELAYGATE_COMPOSE_CALLER_RELAY_ADDR")
	listenerAddress := os.Getenv("RELAYGATE_COMPOSE_LISTENER_RELAY_ADDR")
	if callerAddress == "" || listenerAddress == "" {
		t.Skip("RELAYGATE_COMPOSE_CALLER_RELAY_ADDR and RELAYGATE_COMPOSE_LISTENER_RELAY_ADDR are not set")
	}
	runComposeRelaySmoke(t, callerAddress, listenerAddress)
}

func TestComposePortClosed(t *testing.T) {
	address := os.Getenv("RELAYGATE_COMPOSE_CLOSED_ADDR")
	if address == "" {
		t.Skip("RELAYGATE_COMPOSE_CLOSED_ADDR is not set")
	}
	connection, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		t.Fatalf("unexpected listener on %s", address)
	}
}

// TestComposeFailoverRedeclaresLiveBinding is launched on a Gateway that the
// smoke script keeps alive. It binds before the current leader is stopped,
// then retries exact Opens until the surviving Gateway has reconnected and
// full-redeclared that same local binding to the replacement authority.
func TestComposeFailoverRedeclaresLiveBinding(t *testing.T) {
	address := os.Getenv("RELAYGATE_COMPOSE_RELAY_ADDR")
	if address == "" {
		t.Skip("RELAYGATE_COMPOSE_RELAY_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	defer connection.Close()
	listener, _ := authenticateComposeStream(t, ctx, relayv1.NewRelayClient(connection))
	caller, _ := authenticateComposeStream(t, ctx, relayv1.NewRelayClient(connection))
	runID := time.Now().UnixNano()
	endpoint := fmt.Sprintf("/compose/failover/%d", runID)
	targetID := "survivor"
	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: endpoint, TargetId: targetID},
	}}); err != nil {
		t.Fatalf("Send(BindListener): %v", err)
	}
	boundResponse, err := listener.Recv()
	if err != nil || boundResponse.GetListenerBound().GetBinding().GetListenerBindingId() == "" {
		t.Fatalf("Recv(ListenerBound) = %#v, %v", boundResponse, err)
	}

	listenerErr := make(chan error, 1)
	go serveComposeOffers(listener, listenerErr)
	// This marker is intentionally written directly to stdout so the external
	// smoke orchestrator stops the old leader only after the binding is live.
	_, _ = fmt.Fprintln(os.Stdout, "compose failover binding ready")
	time.Sleep(2 * time.Second)

	deadline := time.Now().Add(60 * time.Second)
	for sequence := 1; time.Now().Before(deadline); sequence++ {
		requestID := fmt.Sprintf("failover-open-%d-%d", runID, sequence)
		if err := caller.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Open{
			Open: &relayv1.Open{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
		}}); err != nil {
			t.Fatalf("Send(Open): %v", err)
		}
		response, err := caller.Recv()
		if err != nil {
			t.Fatalf("Recv(Open outcome): %v", err)
		}
		if opened := response.GetPipeOpened(); opened != nil {
			if opened.GetRequestId() != requestID || opened.GetPipeId() == "" || opened.GetEndpoint() != endpoint || opened.GetTargetId() != targetID {
				t.Fatalf("PipeOpened = %#v", opened)
			}
			select {
			case listenerFailure := <-listenerErr:
				t.Fatalf("listener offer loop: %v", listenerFailure)
			default:
			}
			_, _ = fmt.Fprintln(os.Stdout, "compose failover open succeeded")
			// Keep the listener and its current directory entry alive long
			// enough for the external status oracle to observe bindings=1.
			time.Sleep(5 * time.Second)
			return
		}
		if failed := response.GetPipeOpenFailed(); failed == nil {
			t.Fatalf("Open outcome before redeclare = %#v", response)
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("wait for current binding redeclare: %v", ctx.Err())
		case listenerFailure := <-listenerErr:
			timer.Stop()
			t.Fatalf("listener offer loop: %v", listenerFailure)
		case <-timer.C:
		}
	}
	t.Fatal("live binding was not redeclared to the replacement authority")
}

func serveComposeOffers(listener relayv1.Relay_ConnectClient, failed chan<- error) {
	for {
		response, err := listener.Recv()
		if err != nil {
			failed <- err
			return
		}
		if offer := response.GetListenerOffer(); offer != nil {
			if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerAccept{
				ListenerAccept: &relayv1.ListenerAccept{AttemptId: offer.GetAttemptId()},
			}}); err != nil {
				failed <- err
				return
			}
			continue
		}
		if established := response.GetListenerEstablished(); established != nil {
			if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ListenerConfirmed{
				ListenerConfirmed: &relayv1.ListenerConfirmed{AttemptId: established.GetAttemptId(), PipeId: established.GetPipeId()},
			}}); err != nil {
				failed <- err
				return
			}
			continue
		}
		if response.GetListenerConfirmationAcknowledged() != nil || response.GetListenerTerminated() != nil {
			continue
		}
		failed <- fmt.Errorf("unexpected listener response %T", response.GetMessage())
		return
	}
}

func runComposeRelaySmoke(t *testing.T, callerAddress, listenerAddress string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listenerConnection, err := grpc.NewClient(
		"passthrough:///"+listenerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient(listener): %v", err)
	}
	defer listenerConnection.Close()
	callerConnection, err := grpc.NewClient(
		"passthrough:///"+callerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient(caller): %v", err)
	}
	defer callerConnection.Close()

	listener, _ := authenticateComposeStream(t, ctx, relayv1.NewRelayClient(listenerConnection))
	caller, callerSessionID := authenticateComposeStream(t, ctx, relayv1.NewRelayClient(callerConnection))
	runID := time.Now().UnixNano()
	endpoint := fmt.Sprintf("/compose/relay/%d", runID)
	targetID := "smoke"
	requestID := fmt.Sprintf("compose-open-%d", runID)

	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{
		BindListener: &relayv1.BindListener{EndpointPattern: endpoint, TargetId: targetID},
	}}); err != nil {
		t.Fatalf("Send(BindListener): %v", err)
	}
	boundResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerBound): %v", err)
	}
	bound := boundResponse.GetListenerBound().GetBinding()
	if bound.GetListenerBindingId() == "" || bound.GetEndpointPattern() != endpoint || bound.GetTargetId() != targetID {
		t.Fatalf("ListenerBound = %#v", bound)
	}

	if err := caller.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Open{
		Open: &relayv1.Open{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
	}}); err != nil {
		t.Fatalf("Send(Open): %v", err)
	}
	offerResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerOffer): %v", err)
	}
	offer := offerResponse.GetListenerOffer()
	if offer.GetAttemptId() == "" || offer.GetListenerBindingId() != bound.GetListenerBindingId() ||
		offer.GetEndpoint() != endpoint || offer.GetTargetId() != targetID || offer.GetCallerSessionId() != callerSessionID {
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
		ListenerConfirmed: &relayv1.ListenerConfirmed{
			AttemptId: established.GetAttemptId(),
			PipeId:    established.GetPipeId(),
		},
	}}); err != nil {
		t.Fatalf("Send(ListenerConfirmed): %v", err)
	}
	requireListenerConfirmationAcknowledged(t, listener, established.GetAttemptId(), established.GetPipeId())

	openedResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(PipeOpened): %v", err)
	}
	opened := openedResponse.GetPipeOpened()
	if opened.GetRequestId() != requestID || opened.GetAttemptId() != established.GetAttemptId() ||
		opened.GetPipeId() != established.GetPipeId() || opened.GetEndpoint() != endpoint || opened.GetTargetId() != targetID {
		t.Fatalf("PipeOpened = %#v", opened)
	}

	callerPayload := []byte{0x00, 0x01, 0xfe, 0xff}
	sendPipePayload(t, caller, opened.GetPipeId(), callerPayload)
	listenerPayloadResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(caller-to-listener PipePayload): %v", err)
	}
	if payload := listenerPayloadResponse.GetPipePayload(); payload.GetPipeId() != opened.GetPipeId() || !bytes.Equal(payload.GetPayload(), callerPayload) {
		t.Fatalf("caller-to-listener PipePayload = %#v", payload)
	}

	listenerPayload := []byte("listener-to-caller")
	sendPipePayload(t, listener, opened.GetPipeId(), listenerPayload)
	callerPayloadResponse, err := caller.Recv()
	if err != nil {
		t.Fatalf("Recv(listener-to-caller PipePayload): %v", err)
	}
	if payload := callerPayloadResponse.GetPipePayload(); payload.GetPipeId() != opened.GetPipeId() || !bytes.Equal(payload.GetPayload(), listenerPayload) {
		t.Fatalf("listener-to-caller PipePayload = %#v", payload)
	}

	if err := caller.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_ClosePipe{
		ClosePipe: &relayv1.ClosePipe{PipeId: opened.GetPipeId()},
	}}); err != nil {
		t.Fatalf("Send(ClosePipe): %v", err)
	}
	var closed *relayv1.PipeCloseAcknowledged
	callerTerminated := false
	for closed == nil || !callerTerminated {
		response, receiveErr := caller.Recv()
		if receiveErr != nil {
			t.Fatalf("Recv(caller Pipe close outcome): %v", receiveErr)
		}
		if acknowledgement := response.GetPipeCloseAcknowledged(); acknowledgement != nil {
			closed = acknowledgement
		}
		if terminated := response.GetPipeTerminated(); terminated != nil {
			callerTerminated = terminated.GetPipeId() == opened.GetPipeId()
		}
	}
	if closed.GetPipeId() != opened.GetPipeId() || !closed.GetOwned() {
		t.Fatalf("PipeCloseAcknowledged = %#v", closed)
	}
	terminatedResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerTerminated): %v", err)
	}
	terminated := terminatedResponse.GetListenerTerminated()
	if terminated.GetAttemptId() != opened.GetAttemptId() || terminated.GetPipeId() != opened.GetPipeId() {
		t.Fatalf("ListenerTerminated = %#v", terminated)
	}

	if err := listener.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_UnbindListener{
		UnbindListener: &relayv1.UnbindListener{ListenerBindingId: bound.GetListenerBindingId()},
	}}); err != nil {
		t.Fatalf("Send(UnbindListener): %v", err)
	}
	unboundResponse, err := listener.Recv()
	if err != nil {
		t.Fatalf("Recv(ListenerUnbound): %v", err)
	}
	if got := unboundResponse.GetListenerUnbound().GetListenerBindingId(); got != bound.GetListenerBindingId() {
		t.Fatalf("ListenerUnbound binding ID = %q, want %q", got, bound.GetListenerBindingId())
	}
}

// TestComposeTCPProxy exposes a loopback-only public Relay listener inside one
// RelayGate container network namespace. CI uses it only as a bounded sidecar;
// the production listener remains bound to 127.0.0.1 and is never published.
func TestComposeTCPProxy(t *testing.T) {
	listenAddress := os.Getenv("RELAYGATE_COMPOSE_PROXY_LISTEN_ADDR")
	targetAddress := os.Getenv("RELAYGATE_COMPOSE_PROXY_TARGET_ADDR")
	if listenAddress == "" || targetAddress == "" {
		t.Skip("RELAYGATE_COMPOSE_PROXY_LISTEN_ADDR and RELAYGATE_COMPOSE_PROXY_TARGET_ADDR are not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		t.Fatalf("net.Listen(%q): %v", listenAddress, err)
	}
	defer listener.Close()
	t.Logf("compose proxy listening on %s for %s", listener.Addr(), targetAddress)
	_, _ = fmt.Fprintln(os.Stdout, "compose proxy listening")

	downstream, err := acceptComposeProxyConnection(ctx, listener)
	if err != nil {
		t.Fatalf("accept proxy connection: %v", err)
	}
	defer downstream.Close()
	upstream, err := net.DialTimeout("tcp", targetAddress, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy target %q: %v", targetAddress, err)
	}
	defer upstream.Close()

	if err := relayComposeProxy(ctx, downstream, upstream); err != nil {
		t.Fatalf("relay proxy connection: %v", err)
	}
}

func acceptComposeProxyConnection(ctx context.Context, listener net.Listener) (net.Conn, error) {
	type result struct {
		connection net.Conn
		err        error
	}
	accepted := make(chan result, 1)
	go func() {
		connection, err := listener.Accept()
		accepted <- result{connection: connection, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = listener.Close()
		return nil, context.Cause(ctx)
	case result := <-accepted:
		return result.connection, result.err
	}
}

func relayComposeProxy(ctx context.Context, downstream, upstream net.Conn) error {
	type copyResult struct {
		err error
	}
	results := make(chan copyResult, 2)
	var copies sync.WaitGroup
	copies.Add(2)
	copyDirection := func(destination, source net.Conn) {
		defer copies.Done()
		_, err := io.Copy(destination, source)
		if tcp, ok := destination.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		results <- copyResult{err: err}
	}
	go copyDirection(upstream, downstream)
	go copyDirection(downstream, upstream)

	var first copyResult
	select {
	case <-ctx.Done():
		first.err = context.Cause(ctx)
	case first = <-results:
	}
	_ = downstream.Close()
	_ = upstream.Close()
	copies.Wait()

	if first.err != nil && !errors.Is(first.err, net.ErrClosed) && !errors.Is(first.err, io.EOF) &&
		!errors.Is(first.err, syscall.ECONNRESET) {
		return first.err
	}
	return nil
}

func authenticateComposeStream(t *testing.T, ctx context.Context, client relayv1.RelayClient) (relayv1.Relay_ConnectClient, string) {
	t.Helper()
	stream, err := client.Connect(ctx, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(&relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Authenticate{
		Authenticate: &relayv1.Authenticate{
			ClientId: composeClientID,
			ApiKeyId: composeAPIKeyID,
			ApiKey:   composeAPIKey,
		},
	}}); err != nil {
		t.Fatalf("Send(Authenticate): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ClientSessionOpened): %v", err)
	}
	session := response.GetClientSessionOpened().GetSession()
	if session.GetClientSessionId() == "" || session.GetClientId() != composeClientID ||
		session.GetApiKeyId() != composeAPIKeyID || session.GetAuthRevision() == "" {
		t.Fatalf("ClientSessionOpened = %#v", session)
	}
	return stream, session.GetClientSessionId()
}
