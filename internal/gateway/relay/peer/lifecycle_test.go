package gatewayrelay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
)

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
