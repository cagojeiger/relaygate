package gatewayrelay

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
)

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

func TestGatewayRelayRejectsOversizedPayloadBeforeOwner(t *testing.T) {
	var relayed atomic.Int32
	owner := &testOwner{
		open: func(_ context.Context, open authority.OpenContext, _ localbinding.CallerEndpoint) (opening.Result, error) {
			return opening.Result{AttemptID: open.AttemptID, PipeID: "pipe-limit", Binding: open.Binding}, nil
		},
		relayPayload: func(_ context.Context, _ clientsession.Ref, _ string, _ string, _ []byte) error {
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
		PipeID:    result.PipeID,
		PayloadID: "payload-oversized",
		Data:      make([]byte, localbinding.MaxPayloadBytes+1),
	})
	if err == nil {
		t.Fatal("oversized DeliverPayload succeeded")
	}
	if got := relayed.Load(); got != 0 {
		t.Fatalf("owner RelayPayload calls = %d", got)
	}
	_ = result.Endpoint.Close(context.Background())
}
