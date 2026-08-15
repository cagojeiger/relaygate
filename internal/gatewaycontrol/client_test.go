package gatewaycontrol

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientRotatesEndpointsAndRevalidates(t *testing.T) {
	rejected := make(chan *controlv1.Hello, 1)
	rejectAddress := startControlServer(t, rejectingControlServer{hello: rejected})
	accepted := make(chan acceptedSession, 1)
	acceptAddress := startControlServer(t, &acceptingControlServer{accepted: accepted})

	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		ControlEndpoints: []string{rejectAddress, acceptAddress},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	rejectedHello := receiveTestValue(t, rejected)
	acceptedSession := receiveTestValue(t, accepted)
	if rejectedHello.GetGatewayInstanceId() != "instance-1" || acceptedSession.hello.GetGatewayInstanceId() != "instance-1" {
		t.Fatalf("instance IDs changed across endpoints: rejected=%q accepted=%q", rejectedHello.GetGatewayInstanceId(), acceptedSession.hello.GetGatewayInstanceId())
	}
	status := waitForClientState(t, client, StateRevalidated)
	if status.Endpoint != acceptAddress || status.GatewayGeneration != 1 || !status.Ready() {
		t.Fatalf("client status = %#v", status)
	}
}

func TestClientReconnectKeepsProcessInstanceAndUsesNewSession(t *testing.T) {
	accepted := make(chan acceptedSession, 2)
	address := startControlServer(t, &acceptingControlServer{accepted: accepted, closeFirst: true})
	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		ControlEndpoints: []string{address},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	first := receiveTestValue(t, accepted)
	second := receiveTestValue(t, accepted)
	if first.hello.GetGatewayInstanceId() != second.hello.GetGatewayInstanceId() {
		t.Fatalf("gateway instance changed across reconnect: first=%q second=%q", first.hello.GetGatewayInstanceId(), second.hello.GetGatewayInstanceId())
	}
	if first.session.GetControlSessionId() == second.session.GetControlSessionId() {
		t.Fatalf("control session was reused: %q", first.session.GetControlSessionId())
	}
	status := waitForClientState(t, client, StateRevalidated)
	if status.ControlSessionID != second.session.GetControlSessionId() {
		t.Fatalf("client status = %#v, want session %q", status, second.session.GetControlSessionId())
	}
}

func TestClientLeavesRevalidatedStateWhenControlTransportStalls(t *testing.T) {
	accepted := make(chan acceptedSession, 1)
	serverAddress := startControlServer(t, &acceptingControlServer{accepted: accepted})
	proxy := startBlackholeProxy(t, serverAddress)
	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		ControlEndpoints: []string{proxy.Address()},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	client.keepalive.Timeout = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	_ = receiveTestValue(t, accepted)
	_ = waitForClientState(t, client, StateRevalidated)
	proxy.Blackhole()
	waitForClientToLeaveState(t, client, StateRevalidated, controlKeepaliveTime+5*time.Second)
}

func TestNewClientRejectsIncompleteConfig(t *testing.T) {
	_, err := newClient(Config{}, nil, "instance-1")
	if err == nil {
		t.Fatal("newClient() succeeded with empty config")
	}
	_, err = newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		ControlEndpoints: []string{"127.0.0.1:7100"},
		ConnectTimeout:   time.Second,
		RetryInterval:    time.Second,
	}, nil, "")
	if err == nil {
		t.Fatal("newClient() succeeded with empty instance ID")
	}
}

type rejectingControlServer struct {
	controlv1.UnimplementedGatewayControlServer
	hello chan<- *controlv1.Hello
}

func (s rejectingControlServer) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetHello() == nil {
		return status.Error(codes.InvalidArgument, "hello required")
	}
	s.hello <- request.GetHello()
	return status.Error(codes.Unavailable, "not authority")
}

type acceptedSession struct {
	hello   *controlv1.Hello
	session *controlv1.SessionRef
}

type acceptingControlServer struct {
	controlv1.UnimplementedGatewayControlServer

	mu         sync.Mutex
	count      int
	accepted   chan<- acceptedSession
	closeFirst bool
}

func (s *acceptingControlServer) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	helloRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := helloRequest.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "hello required")
	}
	s.mu.Lock()
	s.count++
	count := s.count
	s.mu.Unlock()
	session := &controlv1.SessionRef{
		ClusterEpoch:      hello.GetClusterEpoch(),
		AuthorityId:       fmt.Sprintf("authority-%d", count),
		ControlSessionId:  fmt.Sprintf("session-%d", count),
		GatewayId:         hello.GetGatewayId(),
		GatewayInstanceId: hello.GetGatewayInstanceId(),
	}
	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SessionOpened{
		SessionOpened: &controlv1.SessionOpened{Session: session, GatewayGeneration: uint64(count)},
	}}); err != nil {
		return err
	}
	snapshotRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	snapshot := snapshotRequest.GetFullSnapshot()
	if snapshot == nil || snapshot.GetSession().GetControlSessionId() != session.GetControlSessionId() || len(snapshot.GetBindings()) != 0 {
		return status.Error(codes.InvalidArgument, "exact empty snapshot required")
	}
	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SnapshotAccepted{
		SnapshotAccepted: &controlv1.SnapshotAccepted{Presence: controlv1.PresenceState_PRESENCE_STATE_COMPLETE},
	}}); err != nil {
		return err
	}
	s.accepted <- acceptedSession{hello: hello, session: session}
	if s.closeFirst && count == 1 {
		return status.Error(codes.Unavailable, "authority changed")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startControlServer(t *testing.T, service controlv1.GatewayControlServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(): %v", err)
	}
	server := grpc.NewServer()
	controlv1.RegisterGatewayControlServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func receiveTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control session")
		var zero T
		return zero
	}
}

func waitForClientState(t *testing.T, client *Client, state State) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := client.Status()
		if status.State == state {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("client state = %q, want %q", client.Status().State, state)
	return Status{}
}

func waitForClientToLeaveState(t *testing.T, client *Client, state State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if client.Status().State != state {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("client state remained %q after %s", state, timeout)
}

type blackholeProxy struct {
	listener net.Listener
	target   string
	drop     atomic.Bool
	closed   atomic.Bool

	mu          sync.Mutex
	connections []net.Conn
}

func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for blackhole proxy: %v", err)
	}
	proxy := &blackholeProxy{listener: listener, target: target}
	go proxy.accept()
	t.Cleanup(proxy.Close)
	return proxy
}

func (p *blackholeProxy) Address() string {
	return p.listener.Addr().String()
}

func (p *blackholeProxy) Blackhole() {
	p.drop.Store(true)
}

func (p *blackholeProxy) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range p.connections {
		_ = connection.Close()
	}
}

func (p *blackholeProxy) accept() {
	for {
		downstream, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = downstream.Close()
			continue
		}
		if p.closed.Load() {
			_ = downstream.Close()
			_ = upstream.Close()
			return
		}
		p.mu.Lock()
		p.connections = append(p.connections, downstream, upstream)
		p.mu.Unlock()
		go p.forward(upstream, downstream)
		go p.forward(downstream, upstream)
	}
}

func (p *blackholeProxy) forward(destination, source net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		read, err := source.Read(buffer)
		if read > 0 && !p.drop.Load() {
			if _, writeErr := destination.Write(buffer[:read]); writeErr != nil {
				return
			}
		}
		if err != nil {
			if !p.drop.Load() {
				_ = destination.Close()
			}
			return
		}
	}
}
