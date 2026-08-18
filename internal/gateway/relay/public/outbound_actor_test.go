package relaygrpc

import (
	"context"
	"sync"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
)

func TestOutboundActorNeverCallsSendConcurrently(t *testing.T) {
	stream := newTrackingRelayStream()
	actor := newOutboundActor(stream, make(chan struct{}, maxGlobalPayloadSlots), time.Second)
	defer actor.close()

	var wg sync.WaitGroup
	errorsSeen := make(chan error, 64)
	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := actor.send(context.Background(), &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
				ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "binding"},
			}}); err != nil {
				errorsSeen <- err
			}
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("send failed: %v", err)
	}
	if stream.concurrent.Load() {
		t.Fatal("stream.Send was called concurrently")
	}
	if got := stream.sent.Load(); got != 64 {
		t.Fatalf("sent = %d, want 64", got)
	}
}
