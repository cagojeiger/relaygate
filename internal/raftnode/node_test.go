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

	"github.com/cagojeiger/relaygate/internal/controlstate"
)

func TestThreeNodeStaticBootstrapReplicatesControlState(t *testing.T) {
	addresses := []string{reserveAddress(t), reserveAddress(t), reserveAddress(t)}
	voters := make([]BootstrapVoter, 0, len(addresses))
	for index, address := range addresses {
		voters = append(voters, BootstrapVoter{
			NodeID:  fmt.Sprintf("node-%d", index+1),
			Address: address,
		})
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
		result controlstate.ApplyResult
		err    error
	}
	outcomes := make(chan epochOutcome, len(nodes))
	var group sync.WaitGroup
	for _, node := range nodes {
		group.Add(1)
		go func(node *Node) {
			defer group.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, err := node.EnsureEpoch(ctx, "epoch-1", 100, 16)
			outcomes <- epochOutcome{result: result, err: err}
		}(node)
	}
	group.Wait()
	close(outcomes)
	for outcome := range outcomes {
		if outcome.err != nil || !outcome.result.Applied() {
			t.Fatalf("EnsureEpoch() = %#v, %v", outcome.result, outcome.err)
		}
	}

	var leader *Node
	leaderIndex := -1
	for index, node := range nodes {
		status := node.Status()
		if status.PeerCount != 2 || !status.Ready {
			t.Fatalf("node status = %#v", status)
		}
		if status.Role == "Leader" {
			if leader != nil {
				t.Fatal("multiple leaders in one term")
			}
			leader = node
			leaderIndex = index
		}
	}
	if leader == nil {
		t.Fatal("cluster has no leader")
	}

	key := controlstate.BindingKey{ClientID: "client-a", EndpointPattern: "/service/*", TargetID: "primary"}
	ref := controlstate.ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-1"}
	command, err := controlstate.EncodeInstallBinding(controlstate.InstallBinding{
		ClusterEpoch: "epoch-1",
		Key:          key,
		NewRef:       ref,
	})
	if err != nil {
		t.Fatalf("EncodeInstallBinding(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	result, err := leader.Apply(ctx, command)
	cancel()
	if err != nil || result.Code != controlstate.ResultApplied {
		t.Fatalf("Apply() = %#v, %v", result, err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			slot := node.Lookup(key)
			if slot.Generation != 1 || slot.Ref == nil || *slot.Ref != ref {
				return false
			}
		}
		return true
	})

	oldTerm := leader.Status().Term
	if err := leader.Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	nodes[leaderIndex] = nil

	var replacementLeader *Node
	waitForCondition(t, 10*time.Second, func() bool {
		replacementLeader = nil
		for _, node := range nodes {
			if node != nil && node.Status().Role == "Leader" {
				if replacementLeader != nil {
					return false
				}
				replacementLeader = node
			}
		}
		return replacementLeader != nil && replacementLeader.Status().Term > oldTerm
	})

	removeCommand, err := controlstate.EncodeRemoveBinding(controlstate.RemoveBinding{
		ClusterEpoch:       "epoch-1",
		Key:                key,
		ExpectedGeneration: 1,
		ExpectedRef:        ref,
	})
	if err != nil {
		t.Fatalf("EncodeRemoveBinding(): %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	result, err = replacementLeader.Apply(ctx, removeCommand)
	cancel()
	if err != nil || result.Code != controlstate.ResultApplied {
		t.Fatalf("remove Apply() = %#v, %v", result, err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			slot := node.Lookup(key)
			if slot.Generation != 2 || !slot.IsTombstone() {
				return false
			}
		}
		return true
	})

	nodes[leaderIndex] = openTestNode(t, configs[leaderIndex])
	waitForLeader(t, nodes[leaderIndex])
	ensureEpoch(t, nodes[leaderIndex])
	waitForCondition(t, 5*time.Second, func() bool {
		slot := nodes[leaderIndex].Lookup(key)
		status := nodes[leaderIndex].Status()
		return slot.Generation == 2 && slot.IsTombstone() && status.ClusterEpoch == "epoch-1" && status.PeerCount == 2
	})
}

func TestSingleNodeSnapshotAndDurableRestart(t *testing.T) {
	address := reserveAddress(t)
	config := testConfig(t, address)

	node := openTestNode(t, config)
	waitForLeader(t, node)
	ensureEpoch(t, node)

	key := controlstate.BindingKey{ClientID: "client-a", EndpointPattern: "/service/*", TargetID: "primary"}
	ref := controlstate.ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-1"}
	command, err := controlstate.EncodeInstallBinding(controlstate.InstallBinding{
		ClusterEpoch: "epoch-1",
		Key:          key,
		NewRef:       ref,
	})
	if err != nil {
		t.Fatalf("EncodeInstallBinding(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	result, err := node.Apply(ctx, command)
	cancel()
	if err != nil {
		t.Fatalf("Apply(): %v", err)
	}
	if result.Code != controlstate.ResultApplied {
		t.Fatalf("Apply() = %#v", result)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
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

	slot := restarted.Lookup(key)
	if slot.Generation != 1 || slot.Ref == nil || *slot.Ref != ref {
		t.Fatalf("restarted slot = %#v", slot)
	}
	if status := restarted.Status(); !status.Ready || status.ClusterEpoch != "epoch-1" {
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
	_, err := Open(config, hclog.NewNullLogger(), prometheus.NewRegistry())
	if err == nil {
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
	result, err := node.EnsureEpoch(ctx, "epoch-1", 100, 16)
	if err != nil {
		t.Fatalf("EnsureEpoch(): %v", err)
	}
	if !result.Applied() {
		t.Fatalf("EnsureEpoch() = %#v", result)
	}
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
