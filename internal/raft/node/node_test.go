package raftnode

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftmembership "github.com/cagojeiger/relaygate/internal/raft/membership"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

func TestThreeNodeDurableCohortSurvivingQuorumReelectsLeaderAndPreservesEpoch(t *testing.T) {
	configs := threeNodeConfigs(t)
	nodes := openThreeNodeCluster(t, configs)
	t.Cleanup(func() { closeNodes(t, nodes) })

	ensureCluster(t, oneLeaderNode(t, nodes))
	waitForCondition(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			if node.ClusterEpoch() != "epoch-1" {
				return false
			}
		}
		return true
	})

	leader, leaderIndex := oneLeader(t, nodes)
	initialTerm := leader.Status().Term
	if err := leader.Close(); err != nil {
		t.Fatalf("Close(leader): %v", err)
	}
	nodes[leaderIndex] = nil

	waitForCondition(t, 10*time.Second, func() bool {
		candidate, _ := oneLeaderOrNil(nodes)
		return candidate != nil && candidate.Status().Term > initialTerm
	})
	for _, node := range nodes {
		if node == nil {
			continue
		}
		status := node.Status()
		if status.PeerCount != 2 || status.ClusterEpoch != "epoch-1" || !status.Ready {
			t.Fatalf("surviving voter status = %#v", status)
		}
	}
}

func TestSameStoreRestartRestoresStateWithoutBootstrap(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	node := openTestNode(t, config)
	waitForLeader(t, node)
	ensureCluster(t, node)
	wantGateway, wantRoute := commitCurrentRoute(t, node)
	if err := node.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	reopened := openTestNode(t, config)
	t.Cleanup(func() { _ = reopened.Close() })
	waitForLeader(t, reopened)
	waitForCondition(t, 5*time.Second, func() bool { return reopened.ClusterEpoch() == "epoch-1" })
	if got := reopened.ClusterEpoch(); got != "epoch-1" {
		t.Fatalf("recovered epoch = %q, want epoch-1", got)
	}
	if got, ok := reopened.LookupGateway(wantGateway.GatewayID); !ok || got != wantGateway {
		t.Fatalf("recovered Gateway = %#v, %v; want %#v", got, ok, wantGateway)
	}
	if got, ok := reopened.LookupRoute(wantRoute.Key); !ok || got != wantRoute {
		t.Fatalf("recovered route = %#v, %v; want %#v", got, ok, wantRoute)
	}
}

func TestExistingStoreRejectsDifferentNodeID(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	node := openTestNode(t, config)
	if err := node.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	config.NodeID = "node-replacement"
	config.Bootstrap = false
	config.BootstrapVoters = nil
	_, err := Open(config, hclog.NewNullLogger(), prometheus.NewRegistry())
	if err == nil || !strings.Contains(err.Error(), "durable node identity") {
		t.Fatalf("Open(identity mismatch) error = %v, want durable node identity", err)
	}
}

func TestCorruptStoreFailsClosed(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	if err := os.WriteFile(filepath.Join(config.DataDir, "raft.db"), []byte("not-a-bolt-database"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt raft.db): %v", err)
	}
	_, err := Open(config, hclog.NewNullLogger(), prometheus.NewRegistry())
	if err == nil {
		t.Fatal("Open(corrupt store) succeeded")
	}
}

func TestSnapshotRecoveryRestoresCurrentState(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	config.SnapshotThreshold = 4
	config.SnapshotInterval = time.Hour
	node := openTestNode(t, config)
	waitForLeader(t, node)
	ensureCluster(t, node)
	wantGateway, wantRoute := commitCurrentRoute(t, node)

	command := mustInitializeClusterCommand(t, "epoch-1")
	for range 8 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := node.Apply(ctx, command)
		cancel()
		if err != nil {
			t.Fatalf("Apply(): %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := node.Snapshot(ctx)
	cancel()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(config.DataDir, "snapshots"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("snapshot directory = %#v, %v", entries, err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	reopened := openTestNode(t, config)
	t.Cleanup(func() { _ = reopened.Close() })
	waitForLeader(t, reopened)
	waitForCondition(t, 5*time.Second, func() bool { return reopened.ClusterEpoch() == "epoch-1" })
	if got := reopened.ClusterEpoch(); got != "epoch-1" {
		t.Fatalf("snapshot-recovered epoch = %q, want epoch-1", got)
	}
	if got, ok := reopened.LookupGateway(wantGateway.GatewayID); !ok || got != wantGateway {
		t.Fatalf("snapshot-recovered Gateway = %#v, %v; want %#v", got, ok, wantGateway)
	}
	if got, ok := reopened.LookupRoute(wantRoute.Key); !ok || got != wantRoute {
		t.Fatalf("snapshot-recovered route = %#v, %v; want %#v", got, ok, wantRoute)
	}
}

func TestAddCatchUpAndRemoveVoter(t *testing.T) {
	configs := threeNodeConfigs(t)
	nodes := openThreeNodeCluster(t, configs)
	t.Cleanup(func() { closeNodes(t, nodes) })
	leader := oneLeaderNode(t, nodes)
	ensureCluster(t, leader)
	wantGateway, wantRoute := commitCurrentRoute(t, leader)
	operatorServer, err := raftmembership.Start(context.Background(), leader.config.DataDir, leader)
	if err != nil {
		t.Fatalf("start membership operator: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := operatorServer.Shutdown(ctx); err != nil {
			t.Errorf("shutdown membership operator: %v", err)
		}
	})
	dialContext, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
	operatorClient, err := raftmembership.Dial(dialContext, raftmembership.SocketPath(leader.config.DataDir))
	cancelDial()
	if err != nil {
		t.Fatalf("dial membership operator: %v", err)
	}
	t.Cleanup(func() { _ = operatorClient.Close() })

	address := reserveAddress(t)
	joining := testConfig(t, address)
	joining.NodeID = "node-4"
	joining.Bootstrap = false
	joining.BootstrapVoters = nil
	node4 := openTestNode(t, joining)
	t.Cleanup(func() { _ = node4.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	added, err := operatorClient.Add(ctx, joining.NodeID, joining.AdvertiseAddress)
	cancel()
	if err != nil {
		t.Fatalf("membership Add(): %v", err)
	}
	if !added.Changed {
		t.Fatalf("membership Add() = %#v, want changed", added)
	}
	waitForCondition(t, 10*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		configuration, err := leader.GetConfiguration(ctx)
		return err == nil && isExistingVoter(configuration, raft.ServerID(joining.NodeID))
	})
	waitForCondition(t, 10*time.Second, func() bool {
		if node4.ClusterEpoch() != "epoch-1" {
			return false
		}
		gateway, gatewayOK := node4.LookupGateway(wantGateway.GatewayID)
		route, routeOK := node4.LookupRoute(wantRoute.Key)
		return gatewayOK && gateway == wantGateway && routeOK && route == wantRoute
	})
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	retriedAdd, err := operatorClient.Add(ctx, joining.NodeID, joining.AdvertiseAddress)
	cancel()
	if err != nil || retriedAdd.Changed {
		t.Fatalf("membership Add(retry) = %#v, %v; want unchanged", retriedAdd, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = operatorClient.Add(ctx, joining.NodeID, reserveAddress(t))
	cancel()
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("membership Add(reused identity) error = %v, want AlreadyExists", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = operatorClient.Add(ctx, "node-5", joining.AdvertiseAddress)
	cancel()
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("membership Add(reused address) error = %v, want AlreadyExists", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	removed, err := operatorClient.Remove(ctx, joining.NodeID)
	cancel()
	if err != nil {
		t.Fatalf("membership Remove(): %v", err)
	}
	if !removed.Changed {
		t.Fatalf("membership Remove() = %#v, want changed", removed)
	}
	waitForCondition(t, 10*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		configuration, err := leader.GetConfiguration(ctx)
		if err != nil {
			return false
		}
		for _, server := range configuration.Servers {
			if server.ID == raft.ServerID(joining.NodeID) {
				return false
			}
		}
		return true
	})
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	retriedRemove, err := operatorClient.Remove(ctx, joining.NodeID)
	cancel()
	if err != nil || retriedRemove.Changed {
		t.Fatalf("membership Remove(retry) = %#v, %v; want unchanged", retriedRemove, err)
	}
}

func TestConfigRejectsMoreThanSevenVoters(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	config.BootstrapVoters = []BootstrapVoter{{NodeID: config.NodeID, Address: address}}
	for index := 2; index <= 8; index++ {
		config.BootstrapVoters = append(config.BootstrapVoters, BootstrapVoter{
			NodeID:  fmt.Sprintf("node-%d", index),
			Address: fmt.Sprintf("127.0.0.1:%d", 28000+index),
		})
	}

	if err := config.validate(); err == nil || !strings.Contains(err.Error(), "at most 7 voters") {
		t.Fatalf("validate() error = %v, want max voter error", err)
	}
}

func TestConfigRequiresExplicitInitialBootstrapManifest(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	config.BootstrapVoters = nil

	if err := config.validate(); err == nil || !strings.Contains(err.Error(), "bootstrap voter manifest") {
		t.Fatalf("validate() error = %v, want explicit bootstrap manifest error", err)
	}
}

func TestConfigAllowsNonBootstrapReplacementOutsideInitialManifest(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	config.NodeID = "node-replacement"
	config.Bootstrap = false

	if err := config.validate(); err != nil {
		t.Fatalf("validate(non-bootstrap replacement): %v", err)
	}
}

func testConfig(t *testing.T, address string) Config {
	t.Helper()
	return Config{
		NodeID:            "node-1",
		BindAddress:       address,
		AdvertiseAddress:  address,
		DataDir:           t.TempDir(),
		Bootstrap:         true,
		BootstrapVoters:   []BootstrapVoter{{NodeID: "node-1", Address: address}},
		ApplyTimeout:      3 * time.Second,
		TransportTimeout:  500 * time.Millisecond,
		ShutdownTimeout:   3 * time.Second,
		SnapshotRetain:    2,
		SnapshotThreshold: 64,
		SnapshotInterval:  30 * time.Second,
		MaxPool:           3,
		MaxCommandBytes:   64 << 10,
	}
}

func openTestNode(t *testing.T, config Config) *Node {
	t.Helper()
	node, err := Open(config, hclog.NewNullLogger(), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	return node
}

func waitForLeader(t *testing.T, node *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := node.WaitForLeader(ctx); err != nil {
		t.Fatalf("WaitForLeader(): %v", err)
	}
}

func ensureCluster(t *testing.T, node *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := node.EnsureCluster(ctx, controlstate.InitializeCluster{
		ClusterEpoch:          "epoch-1",
		MaxGatewaySessions:    controlstate.DefaultMaxGatewaySessions,
		MaxRoutes:             controlstate.DefaultMaxRoutes,
		MaxBindingsPerGateway: controlstate.MaxListenerBindingsPerGateway,
	})
	if err != nil {
		t.Fatalf("EnsureCluster(): %v", err)
	}
	if !result.Applied() {
		t.Fatalf("EnsureCluster() = %#v", result)
	}
}

func commitCurrentRoute(t *testing.T, node *Node) (controlstate.GatewaySessionRef, controlstate.Route) {
	t.Helper()
	gateway := controlstate.GatewaySessionRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1"}
	binding := controlstate.Binding{
		Key:               controlstate.BindingKey{ClientID: "client-1", EndpointPattern: "/events", TargetID: "worker"},
		ListenerBindingID: "listener-1",
	}
	for _, encode := range []func() ([]byte, error){
		func() ([]byte, error) {
			return controlstate.EncodeRegisterGateway(controlstate.RegisterGateway{ClusterEpoch: "epoch-1", Gateway: gateway})
		},
		func() ([]byte, error) {
			return controlstate.EncodeDeclareRoute(controlstate.DeclareRoute{ClusterEpoch: "epoch-1", Gateway: gateway, Binding: binding})
		},
	} {
		command, err := encode()
		if err != nil {
			t.Fatalf("encode current-state command: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result, err := node.Apply(ctx, command)
		cancel()
		if err != nil || !result.Applied() {
			t.Fatalf("commit current-state command = %#v, %v", result, err)
		}
	}
	return gateway, controlstate.Route{Key: binding.Key, Owner: gateway, ListenerBindingID: binding.ListenerBindingID}
}

func mustInitializeClusterCommand(t *testing.T, epoch string) []byte {
	t.Helper()
	command, err := controlstate.EncodeInitializeCluster(controlstate.InitializeCluster{
		ClusterEpoch:          epoch,
		MaxGatewaySessions:    controlstate.DefaultMaxGatewaySessions,
		MaxRoutes:             controlstate.DefaultMaxRoutes,
		MaxBindingsPerGateway: controlstate.MaxListenerBindingsPerGateway,
	})
	if err != nil {
		t.Fatalf("EncodeInitializeCluster(): %v", err)
	}
	return command
}

func threeNodeConfigs(t *testing.T) []Config {
	t.Helper()
	addresses := []string{reserveAddress(t), reserveAddress(t), reserveAddress(t)}
	voters := make([]BootstrapVoter, 0, len(addresses))
	for index, address := range addresses {
		voters = append(voters, BootstrapVoter{NodeID: fmt.Sprintf("node-%d", index+1), Address: address})
	}
	configs := make([]Config, len(addresses))
	for index, address := range addresses {
		configs[index] = testConfig(t, address)
		configs[index].NodeID = voters[index].NodeID
		configs[index].Bootstrap = index == 0
		configs[index].BootstrapVoters = voters
	}
	return configs
}

func openThreeNodeCluster(t *testing.T, configs []Config) []*Node {
	t.Helper()
	nodes := make([]*Node, len(configs))
	for _, index := range []int{1, 2, 0} {
		nodes[index] = openTestNode(t, configs[index])
	}
	for _, node := range nodes {
		waitForLeader(t, node)
	}
	return nodes
}

func closeNodes(t *testing.T, nodes []*Node) {
	t.Helper()
	for index, node := range nodes {
		if node == nil {
			continue
		}
		if err := node.Close(); err != nil {
			t.Errorf("Close(node %d): %v", index, err)
		}
		nodes[index] = nil
	}
}

func oneLeaderNode(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	leader, _ := oneLeader(t, nodes)
	return leader
}

func oneLeader(t *testing.T, nodes []*Node) (*Node, int) {
	t.Helper()
	leader, index := oneLeaderOrNil(nodes)
	if leader == nil {
		t.Fatal("cluster has no leader")
	}
	return leader, index
}

func oneLeaderOrNil(nodes []*Node) (*Node, int) {
	var leader *Node
	leaderIndex := -1
	for index, node := range nodes {
		if node == nil || node.Status().Role != "Leader" {
			continue
		}
		if leader != nil {
			return nil, -1
		}
		leader = node
		leaderIndex = index
	}
	return leader, leaderIndex
}

func reserveAddress(t *testing.T) string {
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
