package raftnode

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
)

func TestThreeNodeStaticBootstrapPreservesElectionMembershipAndEpoch(t *testing.T) {
	configs := threeNodeConfigs(t)
	nodes := openThreeNodeCluster(t, configs)
	t.Cleanup(func() { closeNodes(t, nodes) })

	type epochOutcome struct {
		resultCode string
		err        error
	}
	outcomes := make(chan epochOutcome, len(nodes))
	var group sync.WaitGroup
	for _, node := range nodes {
		group.Add(1)
		go func(node *Node) {
			defer group.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := node.EnsureEpoch(ctx, "epoch-1")
			outcomes <- epochOutcome{resultCode: string(result.Code), err: err}
		}(node)
	}
	group.Wait()
	close(outcomes)
	for outcome := range outcomes {
		if outcome.err != nil || (outcome.resultCode != "applied" && outcome.resultCode != "already_applied") {
			t.Fatalf("EnsureEpoch() = %#v", outcome)
		}
	}

	leader, leaderIndex := oneLeader(t, nodes)
	for _, node := range nodes {
		status := node.Status()
		if status.PeerCount != 2 || !status.Ready || status.ClusterEpoch != "epoch-1" {
			t.Fatalf("node status = %#v", status)
		}
	}

	oldTerm := leader.Status().Term
	if err := leader.Close(); err != nil {
		t.Fatalf("Close(leader): %v", err)
	}
	nodes[leaderIndex] = nil

	var replacement *Node
	waitForCondition(t, 10*time.Second, func() bool {
		replacement = nil
		for _, node := range nodes {
			if node != nil && node.Status().Role == "Leader" {
				if replacement != nil {
					return false
				}
				replacement = node
			}
		}
		return replacement != nil && replacement.Status().Term > oldTerm
	})
	if status := replacement.Status(); status.PeerCount != 2 || status.ClusterEpoch != "epoch-1" {
		t.Fatalf("replacement status = %#v", status)
	}

	nodes[leaderIndex] = openTestNode(t, configs[leaderIndex])
	waitForLeader(t, nodes[leaderIndex])
	ensureEpoch(t, nodes[leaderIndex])
	waitForCondition(t, 5*time.Second, func() bool {
		status := nodes[leaderIndex].Status()
		return status.ClusterEpoch == "epoch-1" && status.PeerCount == 2
	})
}

func TestC10C12QuorumLossDoesNotChangeEpochAndFullyFencedResetStartsFreshEpoch(t *testing.T) {
	oldConfigs := threeNodeConfigs(t)
	oldNodes := openThreeNodeCluster(t, oldConfigs)
	t.Cleanup(func() { closeNodes(t, oldNodes) })
	oldLeader, oldLeaderIndex := oneLeader(t, oldNodes)
	ensureEpoch(t, oldLeader)
	waitForCondition(t, 5*time.Second, func() bool {
		for _, node := range oldNodes {
			if node.ClusterEpoch() != "epoch-1" {
				return false
			}
		}
		return true
	})
	snapshotContext, cancelSnapshot := context.WithTimeout(context.Background(), 5*time.Second)
	if err := oldLeader.Snapshot(snapshotContext); err != nil {
		cancelSnapshot()
		t.Fatalf("Snapshot(old leader): %v", err)
	}
	cancelSnapshot()
	closeNodes(t, oldNodes)

	// An intact voter restarted alone retains the three-voter membership and
	// epoch marker. Bootstrap=true cannot collapse it into a one-node cluster,
	// and an epoch change is rejected without consulting a new quorum.
	stranded := openTestNode(t, oldConfigs[oldLeaderIndex])
	t.Cleanup(func() { _ = stranded.Close() })
	waitForCondition(t, 5*time.Second, func() bool {
		return stranded.ClusterEpoch() == "epoch-1"
	})
	verifyContext, cancelVerify := context.WithTimeout(context.Background(), 2*time.Second)
	verifyErr := stranded.VerifyLeader(verifyContext)
	cancelVerify()
	if verifyErr == nil || stranded.Status().Role == "Leader" || stranded.Status().Ready {
		t.Fatalf("stranded voter admitted authority: status=%#v verify=%v", stranded.Status(), verifyErr)
	}
	epochContext, cancelEpoch := context.WithTimeout(context.Background(), time.Second)
	_, epochErr := stranded.EnsureEpoch(epochContext, "epoch-2")
	cancelEpoch()
	if epochErr == nil {
		t.Fatal("stranded voter changed its durable epoch")
	}
	if err := stranded.Close(); err != nil {
		t.Fatalf("Close(stranded voter): %v", err)
	}

	// The test owns every old process and has closed them all. A separate set of
	// identities, addresses, and stores can therefore bootstrap a fresh epoch;
	// nothing from the old runtime or directory is recovered.
	freshConfigs := threeNodeConfigs(t)
	for index := range freshConfigs {
		freshConfigs[index].NodeID = fmt.Sprintf("fresh-node-%d", index+1)
		if index == 0 {
			freshConfigs[index].BootstrapVoters = []BootstrapVoter{
				{NodeID: "fresh-node-1", Address: freshConfigs[0].AdvertiseAddress},
				{NodeID: "fresh-node-2", Address: freshConfigs[1].AdvertiseAddress},
				{NodeID: "fresh-node-3", Address: freshConfigs[2].AdvertiseAddress},
			}
		}
	}
	freshNodes := openThreeNodeCluster(t, freshConfigs)
	t.Cleanup(func() { closeNodes(t, freshNodes) })
	freshLeader, _ := oneLeader(t, freshNodes)
	ensureEpochValue(t, freshLeader, "epoch-2")
	waitForCondition(t, 5*time.Second, func() bool {
		for _, node := range freshNodes {
			status := node.Status()
			if status.ClusterEpoch != "epoch-2" || status.PeerCount != 2 {
				return false
			}
		}
		return true
	})
}

func TestSingleNodeSnapshotAndDurableEpochRestart(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)

	node := openTestNode(t, config)
	waitForLeader(t, node)
	ensureEpoch(t, node)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := node.Snapshot(ctx); err != nil {
		cancel()
		t.Fatalf("Snapshot(): %v", err)
	}
	cancel()
	if err := node.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	restarted := openTestNode(t, config)
	t.Cleanup(func() { _ = restarted.Close() })
	waitForLeader(t, restarted)
	ensureEpoch(t, restarted)
	if status := restarted.Status(); !status.Ready || status.ClusterEpoch != "epoch-1" || status.PeerCount != 0 {
		t.Fatalf("restarted status = %#v", status)
	}
}

func TestNodeOpenFailsClosedOnSemanticallyInvalidSafetySnapshot(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)

	node := openTestNode(t, config)
	waitForLeader(t, node)
	ensureEpoch(t, node)
	if err := node.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	snapshots, err := raft.NewFileSnapshotStoreWithLogger(config.DataDir, config.SnapshotRetain, hclog.NewNullLogger())
	if err != nil {
		t.Fatalf("NewFileSnapshotStoreWithLogger(): %v", err)
	}
	_, transport := raft.NewInmemTransport(raft.NewInmemAddr())
	t.Cleanup(func() { _ = transport.Close() })
	sink, err := snapshots.Create(
		raft.SnapshotVersionMax,
		10_000,
		1,
		raft.Configuration{Servers: []raft.Server{{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(config.NodeID),
			Address:  raft.ServerAddress(config.AdvertiseAddress),
		}}},
		1,
		transport,
	)
	if err != nil {
		t.Fatalf("Create(corrupt snapshot): %v", err)
	}
	if _, err := sink.Write([]byte(`{"version":1,"state":{"cluster_epoch":""}}`)); err != nil {
		_ = sink.Cancel()
		t.Fatalf("Write(corrupt snapshot): %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close(corrupt snapshot): %v", err)
	}

	restarted, err := Open(config, hclog.NewNullLogger(), prometheus.NewRegistry())
	if err == nil {
		_ = restarted.Close()
		t.Fatal("Open() restored a semantically invalid safety snapshot")
	}
	if !strings.Contains(err.Error(), "failed to load any existing snapshots") {
		t.Fatalf("Open() error = %v, want snapshot restore failure", err)
	}
}

func TestDurableNodeIdentityMismatchFailsClosed(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)
	node := openTestNode(t, config)
	waitForLeader(t, node)
	ensureEpoch(t, node)
	if err := node.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	config.NodeID = "node-2"
	if _, err := Open(config, hclog.NewNullLogger(), prometheus.NewRegistry()); err == nil {
		t.Fatal("Open() succeeded with a different durable node identity")
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := node.WaitForLeader(ctx); err != nil {
		t.Fatalf("WaitForLeader(): %v", err)
	}
}

func ensureEpoch(t *testing.T, node *Node) {
	t.Helper()
	ensureEpochValue(t, node, "epoch-1")
}

func ensureEpochValue(t *testing.T, node *Node, epoch string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := node.EnsureEpoch(ctx, epoch)
	if err != nil {
		t.Fatalf("EnsureEpoch(): %v", err)
	}
	if !result.Applied() {
		t.Fatalf("EnsureEpoch() = %#v", result)
	}
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
		configs[index].DataDir = t.TempDir()
		configs[index].Bootstrap = index == 0
		if index == 0 {
			configs[index].BootstrapVoters = voters
		}
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

func oneLeader(t *testing.T, nodes []*Node) (*Node, int) {
	t.Helper()
	var leader *Node
	leaderIndex := -1
	for index, node := range nodes {
		if node.Status().Role != "Leader" {
			continue
		}
		if leader != nil {
			t.Fatal("multiple leaders in one term")
		}
		leader = node
		leaderIndex = index
	}
	if leader == nil {
		t.Fatal("cluster has no leader")
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
