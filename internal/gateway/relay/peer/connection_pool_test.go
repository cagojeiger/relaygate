package gatewayrelay

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func TestGatewayRelaySharesOneConnectionAcrossOwnerPipes(t *testing.T) {
	owner := &testOwner{open: func(_ context.Context, open routing.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
		return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-" + open.AttemptID, Binding: open.Binding}, nil
	}}
	_, server := startGatewayRelay(t, owner, 4)
	client := newTestClient(t, 4)
	var created atomic.Int32
	client.newConnection = func(address string) (*grpc.ClientConn, error) {
		created.Add(1)
		return newPeerConnection(address)
	}

	first, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "one"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	second, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "two"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(second): %v", err)
	}
	if got := created.Load(); got != 1 {
		t.Fatalf("created connections = %d, want 1", got)
	}
	if first.Endpoint.(*remoteEndpoint).connection.entry != second.Endpoint.(*remoteEndpoint).connection.entry {
		t.Fatal("Pipes for one exact owner did not share a connection")
	}

	if err := first.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(first): %v", err)
	}
	if err := second.Endpoint.Activate(context.Background()); err != nil {
		t.Fatalf("Activate(second after first closed): %v", err)
	}
	if err := second.Endpoint.DeliverPayload(context.Background(), localbinding.PipePayload{
		PipeID: second.PipeID, PayloadID: "payload-after-sibling-close", Data: []byte("still-live"),
	}); err != nil {
		t.Fatalf("DeliverPayload(second after first closed): %v", err)
	}
	if err := second.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	third, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "three"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(third): %v", err)
	}
	if got := created.Load(); got != 1 {
		t.Fatalf("created connections after idle reuse = %d, want 1", got)
	}
	if err := third.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(third): %v", err)
	}
}

func TestGatewayRelayReplacesChangedOwnerIdentityAfterOldPipesDrain(t *testing.T) {
	owner := &testOwner{open: func(_ context.Context, open routing.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
		return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-" + open.AttemptID, Binding: open.Binding}, nil
	}}
	_, server := startGatewayRelay(t, owner, 4)
	_, movedServer := startGatewayRelay(t, owner, 4)
	client := newTestClient(t, 4)
	var created atomic.Int32
	client.newConnection = func(address string) (*grpc.ClientConn, error) {
		created.Add(1)
		return newPeerConnection(address)
	}

	oldResult, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "old"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(old): %v", err)
	}
	oldEndpoint := oldResult.Endpoint.(*remoteEndpoint)
	oldEntry := oldEndpoint.connection.entry

	newOpen := validForwardedOpen(t, server.Address(), "new")
	newOpen.Binding.Ref.GatewayInstanceID = "owner-instance-new"
	newResult, err := client.Open(context.Background(), newOpen, &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(new): %v", err)
	}
	newEndpoint := newResult.Endpoint.(*remoteEndpoint)
	if got := created.Load(); got != 2 {
		t.Fatalf("created connections = %d, want 2 after owner replacement", got)
	}
	if !oldEntry.retiring || oldEntry == newEndpoint.connection.entry {
		t.Fatal("changed owner identity did not retire the previous connection")
	}
	if oldEntry.connection.GetState() == connectivity.Shutdown {
		t.Fatal("old connection closed while its Pipe was still active")
	}

	if err := oldResult.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(old): %v", err)
	}
	if state := oldEntry.connection.GetState(); state != connectivity.Shutdown {
		t.Fatalf("old connection state after last Pipe = %s, want Shutdown", state)
	}
	movedOpen := validForwardedOpen(t, movedServer.Address(), "moved")
	movedOpen.Binding.Ref.GatewayInstanceID = newOpen.Binding.Ref.GatewayInstanceID
	movedResult, err := client.Open(context.Background(), movedOpen, &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(moved): %v", err)
	}
	if got := created.Load(); got != 3 {
		t.Fatalf("created connections = %d, want 3 after owner address change", got)
	}
	if !newEndpoint.connection.entry.retiring {
		t.Fatal("changed owner address did not retire the previous connection")
	}
	if err := newResult.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(new): %v", err)
	}
	if state := newEndpoint.connection.entry.connection.GetState(); state != connectivity.Shutdown {
		t.Fatalf("replaced-address connection state after last Pipe = %s, want Shutdown", state)
	}
	if err := movedResult.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(moved): %v", err)
	}
}

func TestGatewayRelayIdleConnectionCacheIsBounded(t *testing.T) {
	owner := &testOwner{open: func(_ context.Context, open routing.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
		return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-" + open.AttemptID, Binding: open.Binding}, nil
	}}
	_, server := startGatewayRelay(t, owner, 3)
	client := newTestClient(t, 3)
	client.maxIdleConnections = 1
	var created atomic.Int32
	client.newConnection = func(address string) (*grpc.ClientConn, error) {
		created.Add(1)
		return newPeerConnection(address)
	}

	firstOpen := validForwardedOpen(t, server.Address(), "idle-one")
	first, err := client.Open(context.Background(), firstOpen, &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	firstConnection := first.Endpoint.(*remoteEndpoint).connection.entry.connection
	if err := first.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	secondOpen := validForwardedOpen(t, server.Address(), "idle-two")
	secondOpen.Binding.Ref.GatewayID = "gateway-owner-two"
	second, err := client.Open(context.Background(), secondOpen, &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(second): %v", err)
	}
	if err := second.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if state := firstConnection.GetState(); state != connectivity.Shutdown {
		t.Fatalf("evicted idle connection state = %s, want Shutdown", state)
	}

	third, err := client.Open(context.Background(), validForwardedOpen(t, server.Address(), "idle-three"), &testCallerEndpoint{})
	if err != nil {
		t.Fatalf("Open(third): %v", err)
	}
	if got := created.Load(); got != 3 {
		t.Fatalf("created connections = %d, want 3 after bounded idle eviction", got)
	}
	if err := third.Endpoint.Close(context.Background()); err != nil {
		t.Fatalf("Close(third): %v", err)
	}
}
