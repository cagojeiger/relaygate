package relaygate

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestEventLoopStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	shutdown := make(chan struct{}, 1)
	loop, _ := newTestEventLoop()
	loop.onShutdown = func() { shutdown <- struct{}{} }
	cancel()
	if err := loop.wait(ctx); err != nil {
		t.Fatalf("wait() = %v, want nil", err)
	}
	select {
	case <-shutdown:
	default:
		t.Fatal("shutdown callback was not called")
	}
}

func TestEventLoopReloadsAndContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reloaded := make(chan struct{}, 1)
	loop, events := newTestEventLoop()
	loop.onReload = func() {
		reloaded <- struct{}{}
		cancel()
	}
	events.reload <- syscall.SIGHUP
	if err := loop.wait(ctx); err != nil {
		t.Fatalf("wait() = %v, want nil", err)
	}
	select {
	case <-reloaded:
	default:
		t.Fatal("reload callback was not called")
	}
}

func TestEventLoopReturnsServerFailure(t *testing.T) {
	want := errors.New("relay failed")
	loop, events := newTestEventLoop()
	events.relay <- want
	if got := loop.wait(context.Background()); !errors.Is(got, want) {
		t.Fatalf("wait() = %v, want %v", got, want)
	}
}

func TestEventLoopRejectsUnexpectedServerStop(t *testing.T) {
	loop, events := newTestEventLoop()
	events.control <- nil
	if got := loop.wait(context.Background()); got == nil || got.Error() != "control server stopped unexpectedly" {
		t.Fatalf("wait() = %v", got)
	}
}

type testEventChannels struct {
	reload       chan os.Signal
	admin        chan error
	control      chan error
	relay        chan error
	gatewayRelay chan error
}

func newTestEventLoop() (eventLoop, testEventChannels) {
	events := testEventChannels{
		reload:       make(chan os.Signal, 1),
		admin:        make(chan error, 1),
		control:      make(chan error, 1),
		relay:        make(chan error, 1),
		gatewayRelay: make(chan error, 1),
	}
	return eventLoop{
		reloadSignals:      events.reload,
		adminErrors:        events.admin,
		controlErrors:      events.control,
		relayErrors:        events.relay,
		gatewayRelayErrors: events.gatewayRelay,
		onShutdown:         func() {},
		onReload:           func() {},
	}, events
}
