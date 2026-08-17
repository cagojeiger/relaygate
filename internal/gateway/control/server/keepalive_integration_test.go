package controlgrpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientsession "github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	gatewaycontrol "github.com/cagojeiger/relaygate/internal/gateway/control/client"
	controltransport "github.com/cagojeiger/relaygate/internal/gateway/control/transport"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

type liveSnapshot []routing.LiveBinding

func (s liveSnapshot) LiveBindings() []routing.LiveBinding {
	return append([]routing.LiveBinding(nil), s...)
}

// blackholeProxy leaves established TCP sockets open while discarding both
// directions. New connections are rejected until Restore, so route deletion
// can be observed independently from the client's fresh reconnect.
type blackholeProxy struct {
	listener net.Listener
	target   string
	once     sync.Once
	wg       sync.WaitGroup

	mu         sync.Mutex
	blackholed bool
	pairs      map[*proxyPair]struct{}
}

type proxyPair struct {
	client net.Conn
	server net.Conn

	blocked atomic.Bool
	close   sync.Once
}

func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for blackhole proxy: %v", err)
	}
	proxy := &blackholeProxy{
		listener: listener,
		target:   target,
		pairs:    make(map[*proxyPair]struct{}),
	}
	proxy.wg.Add(1)
	go proxy.accept()
	t.Cleanup(proxy.Close)
	return proxy
}

func (p *blackholeProxy) Address() string { return p.listener.Addr().String() }

func (p *blackholeProxy) Blackhole() {
	p.mu.Lock()
	p.blackholed = true
	for pair := range p.pairs {
		pair.blocked.Store(true)
	}
	p.mu.Unlock()
}

func (p *blackholeProxy) Restore() {
	p.mu.Lock()
	p.blackholed = false
	p.mu.Unlock()
}

func (p *blackholeProxy) Close() {
	p.once.Do(func() {
		_ = p.listener.Close()
		p.mu.Lock()
		for pair := range p.pairs {
			pair.closeBoth()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
}

func (p *blackholeProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.serve(client)
	}
}

func (p *blackholeProxy) serve(client net.Conn) {
	defer p.wg.Done()
	server, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	pair := &proxyPair{client: client, server: server}

	p.mu.Lock()
	if p.blackholed {
		p.mu.Unlock()
		pair.closeBoth()
		return
	}
	p.pairs[pair] = struct{}{}
	p.mu.Unlock()
	defer func() {
		pair.closeBoth()
		p.mu.Lock()
		delete(p.pairs, pair)
		p.mu.Unlock()
	}()

	completed := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(discardWhenBlocked{pair: pair, destination: pair.server}, pair.client)
		completed <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(discardWhenBlocked{pair: pair, destination: pair.client}, pair.server)
		completed <- struct{}{}
	}()
	<-completed
	if !pair.blocked.Load() {
		pair.closeBoth()
	}
	<-completed
}

type discardWhenBlocked struct {
	pair        *proxyPair
	destination net.Conn
}

func (w discardWhenBlocked) Write(payload []byte) (int, error) {
	if w.pair.blocked.Load() {
		return len(payload), nil
	}
	return w.destination.Write(payload)
}

func (p *proxyPair) closeBoth() {
	p.close.Do(func() {
		_ = p.client.Close()
		_ = p.server.Close()
	})
}

func TestControlKeepaliveBlackholeDeletesAndRedeclaresCurrentRoutes(t *testing.T) {
	service, manager := newLiveService(t)
	server, err := Start(context.Background(), Config{BindAddress: "127.0.0.1:0"}, service)
	if err != nil {
		t.Fatalf("Start(control server): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown(control server): %v", err)
		}
	})

	proxy := startBlackholeProxy(t, server.Address())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := gatewaycontrol.New(gatewaycontrol.Config{
		ClusterEpoch:     "epoch-a",
		GatewayID:        "gateway-a",
		RelayAddress:     "127.0.0.1:27430",
		ControlEndpoints: []string{proxy.Address()},
		ConnectTimeout:   500 * time.Millisecond,
		RetryInterval:    50 * time.Millisecond,
	}, logger)
	if err != nil {
		t.Fatalf("New(control client): %v", err)
	}
	instanceID := client.Status().GatewayInstanceID
	binding := routing.LiveBinding{
		Key: routing.BindingKey{ClientID: "client-a", EndpointPattern: "/jobs", TargetID: "worker"},
		Ref: routing.ListenerBindingRef{GatewayID: "gateway-a", GatewayInstanceID: instanceID, ListenerBindingID: "listener-a"},
	}
	if err := client.AttachSnapshotProvider(liveSnapshot{binding}); err != nil {
		t.Fatalf("AttachSnapshotProvider(): %v", err)
	}

	runContext, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		client.Run(runContext)
		close(runDone)
	}()
	t.Cleanup(func() {
		cancelRun()
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("control client did not stop")
		}
	})

	waitForControlState(t, 5*time.Second, func() bool {
		presence := manager.Presence()
		return client.Status().Ready() && presence.Sessions == 1 && presence.Revalidated == 1 && presence.Bindings == 1
	}, "initial revalidated route")
	oldSession, ok := client.CurrentSession()
	if !ok {
		t.Fatal("initial current session was not published")
	}

	// One complete healthy keepalive interval proves that an idle but responsive
	// control stream is retained before the same TCP path is blackholed.
	time.Sleep(controltransport.KeepaliveTime + controltransport.KeepaliveTimeout + time.Second)
	if !client.Status().Ready() || manager.Presence().Bindings != 1 {
		t.Fatalf("healthy idle stream ended: status=%#v presence=%#v", client.Status(), manager.Presence())
	}

	proxy.Blackhole()
	blackholedAt := time.Now()
	blackholeBudget := 2*controltransport.KeepaliveTime + 2*controltransport.KeepaliveTimeout
	waitForControlState(t, blackholeBudget, func() bool {
		presence := manager.Presence()
		return !client.Status().Ready() && presence.Sessions == 0 && presence.Revalidated == 0 && presence.Bindings == 0
	}, "blackholed session and route deletion")
	if elapsed := time.Since(blackholedAt); elapsed >= blackholeBudget {
		t.Fatalf("blackhole detection took %s, budget %s", elapsed, blackholeBudget)
	}
	if err := manager.RequireRevalidated(oldSession); !errors.Is(err, authority.ErrStaleSession) {
		t.Fatalf("RequireRevalidated(old session) = %v, want ErrStaleSession", err)
	}
	admitContext, cancelAdmit := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err = client.AdmitOpen(admitContext, clientsession.Ref{
		ClientSessionID: "caller-session",
		ClientID:        "client-a",
		APIKeyID:        "caller-key",
		AuthRevision:    "revision-a",
	}, "/jobs", "worker")
	cancelAdmit()
	if !errors.Is(err, gatewaycontrol.ErrOpenUnavailable) {
		t.Fatalf("AdmitOpen(blackholed) = %v, want ErrOpenUnavailable", err)
	}

	proxy.Restore()
	waitForControlState(t, 5*time.Second, func() bool {
		presence := manager.Presence()
		return client.Status().Ready() && presence.Sessions == 1 && presence.Revalidated == 1 && presence.Bindings == 1
	}, "fresh reconnect and full redeclare")
	newSession, ok := client.CurrentSession()
	if !ok || newSession.ControlSessionID == oldSession.ControlSessionID {
		t.Fatalf("reconnected session = %#v, old = %#v", newSession, oldSession)
	}
	admitContext, cancelAdmit = context.WithTimeout(context.Background(), time.Second)
	open, err := client.AdmitOpen(admitContext, clientsession.Ref{
		ClientSessionID: "caller-session",
		ClientID:        "client-a",
		APIKeyID:        "caller-key",
		AuthRevision:    "revision-a",
	}, "/jobs", "worker")
	cancelAdmit()
	if err != nil || open.Binding != binding || open.OwnerControlSessionID != newSession.ControlSessionID {
		t.Fatalf("AdmitOpen(redeclared) = %#v, %v", open, err)
	}
}

func waitForControlState(t *testing.T, timeout time.Duration, condition func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
