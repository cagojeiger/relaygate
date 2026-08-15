package controlgrpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

func TestThreeNodeLeaderFailoverRequiresNewControlSessionAndSnapshot(t *testing.T) {
	const epoch = "integration-epoch-1"
	raftAddresses := []string{reserveTCPAddress(t), reserveTCPAddress(t), reserveTCPAddress(t)}
	voters := make([]raftnode.BootstrapVoter, len(raftAddresses))
	for index, address := range raftAddresses {
		voters[index] = raftnode.BootstrapVoter{NodeID: fmt.Sprintf("node-%d", index+1), Address: address}
	}

	nodes := make([]*raftnode.Node, len(voters))
	for _, index := range []int{1, 2, 0} {
		config := integrationRaftConfig(t, voters[index], index == 0, voters)
		node, err := raftnode.Open(config, hclog.NewNullLogger(), prometheus.NewRegistry())
		if err != nil {
			t.Fatalf("raftnode.Open(node-%d): %v", index+1, err)
		}
		nodes[index] = node
	}
	allNodes := append([]*raftnode.Node(nil), nodes...)
	t.Cleanup(func() {
		for _, node := range allNodes {
			if node != nil {
				_ = node.Close()
			}
		}
	})
	for _, node := range nodes {
		waitForRaftLeader(t, node)
	}
	for _, node := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := node.EnsureEpoch(ctx, epoch, 100, 16)
		cancel()
		if err != nil || !result.Applied() {
			t.Fatalf("EnsureEpoch() = %#v, %v", result, err)
		}
	}

	managers := make([]*authority.Manager, len(nodes))
	servers := make([]*Server, len(nodes))
	for index, node := range nodes {
		manager, err := authority.New(authority.Config{
			ClusterEpoch:        epoch,
			ProbeInterval:       25 * time.Millisecond,
			ProbeTimeout:        250 * time.Millisecond,
			RevalidationTimeout: time.Second,
		}, node)
		if err != nil {
			t.Fatalf("authority.New(node-%d): %v", index+1, err)
		}
		manager.Start(context.Background())
		service, err := NewService(epoch, node, manager)
		if err != nil {
			t.Fatalf("NewService(node-%d): %v", index+1, err)
		}
		server, err := Start(context.Background(), Config{BindAddress: "127.0.0.1:0"}, service)
		if err != nil {
			t.Fatalf("Start(node-%d): %v", index+1, err)
		}
		managers[index] = manager
		servers[index] = server
	}
	t.Cleanup(func() {
		for _, manager := range managers {
			if manager != nil {
				manager.Close()
			}
		}
		for _, server := range servers {
			if server != nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = server.Shutdown(ctx)
				cancel()
			}
		}
	})

	oldLeaderIndex := currentLeaderIndex(t, nodes)
	followerIndex := (oldLeaderIndex + 1) % len(nodes)
	followerConnection, err := grpc.NewClient(
		"passthrough:///"+servers[followerIndex].Address(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient(follower): %v", err)
	}
	followerContext, cancelFollower := context.WithTimeout(context.Background(), 5*time.Second)
	followerStream, err := controlv1.NewGatewayControlClient(followerConnection).Connect(followerContext)
	if err != nil {
		t.Fatalf("Connect(follower): %v", err)
	}
	if err := followerStream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{
		Hello: &controlv1.Hello{ClusterEpoch: epoch, GatewayId: "gateway-a", GatewayInstanceId: "instance-a"},
	}}); err != nil {
		t.Fatalf("Send(follower hello): %v", err)
	}
	if _, err := followerStream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("follower hello error = %v, want Unavailable", err)
	}
	cancelFollower()
	_ = followerConnection.Close()

	oldConnection, oldStream, oldSession := connectAndSync(t, servers[oldLeaderIndex].Address(), epoch, "gateway-a", "instance-a")
	t.Cleanup(func() { _ = oldConnection.Close() })

	oldTerm := nodes[oldLeaderIndex].Status().Term
	if err := nodes[oldLeaderIndex].Close(); err != nil {
		t.Fatalf("Close(old leader): %v", err)
	}
	nodes[oldLeaderIndex] = nil
	if _, err := oldStream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("old leader stream error = %v, want Unavailable", err)
	}

	var newLeaderIndex int
	waitForCondition(t, 10*time.Second, func() bool {
		leaders := 0
		for index, node := range nodes {
			if node != nil && node.Status().Role == "Leader" && node.Status().Term > oldTerm {
				newLeaderIndex = index
				leaders++
			}
		}
		return leaders == 1
	})
	newConnection, newStream, newSession := connectAndSync(t, servers[newLeaderIndex].Address(), epoch, "gateway-a", "instance-a")
	t.Cleanup(func() { _ = newConnection.Close() })
	if oldSession.GetAuthorityId() == newSession.GetAuthorityId() {
		t.Fatal("new Raft leader reused the old authority ID")
	}
	if slot := nodes[newLeaderIndex].LookupGateway("gateway-a"); slot.Generation != 1 {
		t.Fatalf("reconnect changed the same instance registration: %#v", slot)
	}

	key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
	if err := newStream.Send(installRequest(newSession, key, 0, nil, ref)); err != nil {
		t.Fatalf("Send(install after failover): %v", err)
	}
	response, err := newStream.Recv()
	if err != nil {
		t.Fatalf("Recv(install after failover): %v", err)
	}
	if code := response.GetMutationResult().GetCode(); code != controlv1.MutationCode_MUTATION_CODE_APPLIED {
		t.Fatalf("mutation code after failover = %v", code)
	}

	durableKey := controlstate.BindingKey{ClientID: "client-a", EndpointPattern: "/jobs/*", TargetID: "worker"}
	waitForCondition(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			slot := node.Lookup(durableKey)
			if slot.Generation != 1 || slot.Ref == nil || slot.Ref.GatewayInstanceID != "instance-a" {
				return false
			}
		}
		return true
	})
}

func integrationRaftConfig(t *testing.T, voter raftnode.BootstrapVoter, bootstrap bool, voters []raftnode.BootstrapVoter) raftnode.Config {
	t.Helper()
	config := raftnode.Config{
		NodeID:            voter.NodeID,
		BindAddress:       voter.Address,
		AdvertiseAddress:  voter.Address,
		DataDir:           t.TempDir(),
		Bootstrap:         bootstrap,
		ApplyTimeout:      3 * time.Second,
		TransportTimeout:  500 * time.Millisecond,
		ShutdownTimeout:   3 * time.Second,
		SnapshotRetain:    2,
		SnapshotThreshold: 64,
		SnapshotInterval:  30 * time.Second,
		MaxPool:           3,
		MaxCommandBytes:   64 << 10,
	}
	if bootstrap {
		config.BootstrapVoters = voters
	}
	return config
}

func connectAndSync(t *testing.T, address, epoch, gatewayID, instanceID string) (*grpc.ClientConn, grpc.BidiStreamingClient[controlv1.ControlRequest, controlv1.ControlResponse], *controlv1.SessionRef) {
	t.Helper()
	connection, err := grpc.NewClient(
		"passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := controlv1.NewGatewayControlClient(connection).Connect(ctx)
	if err != nil {
		_ = connection.Close()
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{
		Hello: &controlv1.Hello{ClusterEpoch: epoch, GatewayId: gatewayID, GatewayInstanceId: instanceID},
	}}); err != nil {
		_ = connection.Close()
		t.Fatalf("Send(hello): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		_ = connection.Close()
		t.Fatalf("Recv(session): %v", err)
	}
	session := response.GetSessionOpened().GetSession()
	if session == nil {
		_ = connection.Close()
		t.Fatalf("session response = %#v", response)
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: session},
	}}); err != nil {
		_ = connection.Close()
		t.Fatalf("Send(snapshot): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		_ = connection.Close()
		t.Fatalf("Recv(snapshot): %v", err)
	}
	if accepted := response.GetSnapshotAccepted(); accepted == nil || accepted.GetPresence() != controlv1.PresenceState_PRESENCE_STATE_COMPLETE {
		_ = connection.Close()
		t.Fatalf("snapshot response = %#v", response)
	}
	return connection, stream, session
}

func currentLeaderIndex(t *testing.T, nodes []*raftnode.Node) int {
	t.Helper()
	leader := -1
	for index, node := range nodes {
		if node.Status().Role == "Leader" {
			if leader >= 0 {
				t.Fatal("multiple Raft leaders")
			}
			leader = index
		}
	}
	if leader < 0 {
		t.Fatal("Raft cluster has no leader")
	}
	return leader
}

func waitForRaftLeader(t *testing.T, node *raftnode.Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := node.WaitForLeader(ctx); err != nil {
		t.Fatalf("WaitForLeader(): %v", err)
	}
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen(): %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	return address
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
