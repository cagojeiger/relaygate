package relaygate

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func openManagedSession(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse], number int32) error {
	request, err := recvRequest(stream)
	if err != nil {
		return err
	}
	authenticate := request.GetAuthenticate()
	if authenticate == nil {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	return stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
		ClientSessionOpened: &relayv1.ClientSessionOpened{Session: &relayv1.ClientSessionRef{
			ClientSessionId: fmt.Sprintf("managed-session-%d", number),
			ClientId:        authenticate.GetClientId(),
			ApiKeyId:        authenticate.GetApiKeyId(),
			AuthRevision:    "managed-revision",
		}},
	}})
}

func waitManagedState(t *testing.T, client *ManagedClient, state ManagedState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if client.State() == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("ManagedClient state = %s, want %s", client.State(), state)
}

func TestManagedClientReconnectsAndRedeclaresCurrentListenerOnly(t *testing.T) {
	var sessions atomic.Int32
	firstDropped := make(chan struct{})
	dropFirst := make(chan struct{})
	secondBound := make(chan struct{})
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		number := sessions.Add(1)
		if err := openManagedSession(stream, number); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		bind := request.GetBindListener()
		if bind == nil || bind.GetEndpointPattern() != "/echo" || bind.GetTargetId() != "server" {
			return status.Error(codes.FailedPrecondition, "expected exact echo BindListener")
		}
		if err := stream.Send(listenerBound(fmt.Sprintf("managed-binding-%d", number), "/echo", "server")); err != nil {
			return err
		}
		if number == 1 {
			request, err := recvRequest(stream)
			if err != nil {
				return err
			}
			open := request.GetOpen()
			if open == nil {
				return status.Error(codes.FailedPrecondition, "Open required after first Bind")
			}
			if err := stream.Send(pipeOpened(open, "managed-first-attempt", "managed-first-pipe")); err != nil {
				return err
			}
			<-dropFirst
			close(firstDropped)
			return status.Error(codes.Unavailable, "injected session loss")
		}
		close(secondBound)
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{
			ListenerOffer: &relayv1.ListenerOffer{
				AttemptId: "managed-attempt", ListenerBindingId: "managed-binding-2",
				Endpoint: "/echo", TargetId: "server", CallerSessionId: "caller-session",
			},
		}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := ConnectManaged(ctx, NewConfig(address, "client-1", "key-1", "secret").WithInsecureLocal())
	if err != nil {
		t.Fatalf("ConnectManaged: %v", err)
	}
	defer client.Close() //nolint:errcheck
	listener, err := client.Bind(ctx, "/echo", "server")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	pipe, err := client.Open(ctx, "/echo", "server")
	if err != nil {
		t.Fatalf("Open on first Ready session: %v", err)
	}
	close(dropFirst)

	<-firstDropped
	select {
	case <-pipe.Done():
	case <-ctx.Done():
		t.Fatal("old Pipe did not become terminal with its session")
	}
	waitManagedState(t, client, ManagedBackoff)
	if _, err := client.Open(ctx, "/echo", "server"); !errors.Is(err, ErrManagedNotReady) {
		t.Fatalf("Open during Backoff = %v, want ErrManagedNotReady", err)
	}
	select {
	case <-secondBound:
	case <-ctx.Done():
		t.Fatal("listener was not rebound on the fresh session")
	}
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after reconnect: %v", err)
	}
	offer, err := listener.Next(ctx)
	if err != nil {
		t.Fatalf("Next after reconnect: %v", err)
	}
	if offer.AttemptID() != "managed-attempt" || offer.ListenerID() != "managed-binding-2" {
		t.Fatalf("Offer after reconnect = %#v", offer)
	}
	if sessions.Load() != 2 {
		t.Fatalf("authenticated sessions = %d, want 2", sessions.Load())
	}
}

func TestManagedClientUnbindDuringBackoffDoesNotRedeclare(t *testing.T) {
	var sessions atomic.Int32
	firstDropped := make(chan struct{})
	secondConnected := make(chan struct{})
	unexpected := make(chan *relayv1.ConnectRequest, 1)
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		number := sessions.Add(1)
		if err := openManagedSession(stream, number); err != nil {
			return err
		}
		if number == 1 {
			request, err := recvRequest(stream)
			if err != nil {
				return err
			}
			bind := request.GetBindListener()
			if bind == nil {
				return status.Error(codes.FailedPrecondition, "BindListener required")
			}
			if err := stream.Send(listenerBound("managed-binding-1", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
				return err
			}
			close(firstDropped)
			return status.Error(codes.Unavailable, "injected session loss")
		}
		close(secondConnected)
		request, err := stream.Recv()
		if err == nil {
			unexpected <- request
		}
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := ConnectManaged(ctx, NewConfig(address, "client-1", "key-1", "secret").WithInsecureLocal())
	if err != nil {
		t.Fatalf("ConnectManaged: %v", err)
	}
	listener, err := client.Bind(ctx, "/temporary", "server")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	<-firstDropped
	waitManagedState(t, client, ManagedBackoff)
	if err := listener.Unbind(ctx); err != nil {
		t.Fatalf("Unbind during Backoff: %v", err)
	}
	select {
	case <-secondConnected:
	case <-ctx.Done():
		t.Fatal("fresh session did not connect")
	}
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	select {
	case request := <-unexpected:
		t.Fatalf("removed Listener was redeclared: %T", request.GetMessage())
	case <-time.After(100 * time.Millisecond):
	}
	started := time.Now()
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("Close took %s; supervisor did not cancel promptly", elapsed)
	}
}

func TestManagedClientCloseCancelsBackoff(t *testing.T) {
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if err := openManagedSession(stream, 1); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "force backoff")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := ConnectManaged(ctx, NewConfig(address, "client-1", "key-1", "secret").WithInsecureLocal())
	if err != nil {
		t.Fatalf("ConnectManaged: %v", err)
	}
	waitManagedState(t, client, ManagedBackoff)
	started := time.Now()
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("Close took %s; backoff was not cancelled promptly", elapsed)
	}
}

func TestManagedClientStopsOnPermanentAuthenticationFailure(t *testing.T) {
	var sessions atomic.Int32
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		number := sessions.Add(1)
		if number == 1 {
			if err := openManagedSession(stream, number); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "force reconnect")
		}
		if _, err := recvRequest(stream); err != nil {
			return err
		}
		return status.Error(codes.Unauthenticated, "credential revoked")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := ConnectManaged(ctx, NewConfig(address, "client-1", "key-1", "secret").WithInsecureLocal())
	if err != nil {
		t.Fatalf("ConnectManaged: %v", err)
	}
	select {
	case <-client.Done():
	case <-ctx.Done():
		t.Fatal("permanent authentication failure did not stop supervisor")
	}
	if client.State() != ManagedFailed || client.Err() == nil {
		t.Fatalf("terminal managed state = %s, err = %v", client.State(), client.Err())
	}
	if sessions.Load() != 2 {
		t.Fatalf("authentication attempts = %d, want 2", sessions.Load())
	}
}

func TestManagedClientStopsOnProtocolFailureWithoutReconnect(t *testing.T) {
	var sessions atomic.Int32
	inject := make(chan struct{})
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		number := sessions.Add(1)
		if err := openManagedSession(stream, number); err != nil {
			return err
		}
		<-inject
		return stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
			ClientSessionOpened: &relayv1.ClientSessionOpened{Session: &relayv1.ClientSessionRef{
				ClientSessionId: "duplicate", ClientId: "client-1", ApiKeyId: "key-1", AuthRevision: "revision",
			}},
		}})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := ConnectManaged(ctx, NewConfig(address, "client-1", "key-1", "secret").WithInsecureLocal())
	if err != nil {
		t.Fatalf("ConnectManaged: %v", err)
	}
	close(inject)
	select {
	case <-client.Done():
	case <-ctx.Done():
		t.Fatal("protocol failure did not stop supervisor")
	}
	if client.State() != ManagedFailed || !errors.Is(client.Err(), errProtocol) {
		t.Fatalf("protocol terminal = %s, %v", client.State(), client.Err())
	}
	if sessions.Load() != 1 {
		t.Fatalf("sessions after protocol failure = %d, want 1", sessions.Load())
	}
}
