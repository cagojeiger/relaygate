package raftnode

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
)

func TestThreeNodeStaticBootstrapPreservesElectionMembershipAndEpoch(t *testing.T) {
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

	nodes := make([]*Node, len(configs))
	for _, index := range []int{1, 2, 0} {
		nodes[index] = openTestNode(t, configs[index])
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	})
	for _, node := range nodes {
		waitForLeader(t, node)
	}

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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := node.EnsureEpoch(ctx, "epoch-1")
	if err != nil {
		t.Fatalf("EnsureEpoch(): %v", err)
	}
	if !result.Applied() {
		t.Fatalf("EnsureEpoch() = %#v", result)
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
