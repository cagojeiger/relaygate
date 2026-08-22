package raftnode

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
	"github.com/hashicorp/raft"
)

func (n *Node) WaitForLeader(ctx context.Context) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		address, _ := n.raft.LeaderWithID()
		if address != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for raft leader: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (n *Node) VerifyLeader(ctx context.Context) error {
	if n.draining.Load() {
		return fmt.Errorf("raft node is shutting down")
	}
	if err := waitFuture(ctx, n.raft.VerifyLeader().Error); err != nil {
		return fmt.Errorf("verify raft leader: %w", err)
	}
	// VerifyLeader establishes current leadership; Barrier additionally waits
	// until this member has applied every committed log entry before a caller
	// reads the local FSM. The pair is the linearizable read fence used by
	// control-plane resolution and presence views.
	if err := waitFuture(ctx, n.raft.Barrier(n.applyTimeout(ctx)).Error); err != nil {
		return fmt.Errorf("barrier raft leader read: %w", err)
	}
	return nil
}

// EnsureCluster initializes the immutable cluster epoch and current-state
// capacity limits exactly once. Later calls must match the replicated values.
func (n *Node) EnsureCluster(ctx context.Context, cluster controlstate.InitializeCluster) (controlstate.ApplyResult, error) {
	command, err := controlstate.EncodeInitializeCluster(cluster)
	if err != nil {
		return controlstate.ApplyResult{}, err
	}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if current := n.fsm.State(); current.ClusterEpoch != "" {
			if current.ClusterEpoch != cluster.ClusterEpoch ||
				current.MaxGatewaySessions != cluster.MaxGatewaySessions ||
				current.MaxRoutes != cluster.MaxRoutes ||
				current.MaxBindingsPerGateway != cluster.MaxBindingsPerGateway {
				return controlstate.ApplyResult{}, fmt.Errorf("configured control cluster differs from active cohort state")
			}
			return controlstate.ApplyResult{Code: controlstate.ResultAlreadyApplied}, nil
		}
		if n.raft.State() == raft.Leader {
			result, applyErr := n.Apply(ctx, command)
			if applyErr == nil {
				return result, nil
			}
			if !errors.Is(applyErr, raft.ErrNotLeader) && !errors.Is(applyErr, raft.ErrLeadershipLost) {
				return controlstate.ApplyResult{}, applyErr
			}
		}
		select {
		case <-ctx.Done():
			return controlstate.ApplyResult{}, fmt.Errorf("wait for control epoch: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// ClusterEpoch returns only the cluster epoch held by the local current-state
// FSM. It does not expose Gateway, route, listener, or Pipe state.
func (n *Node) ClusterEpoch() string {
	return n.fsm.ClusterEpoch()
}

// Apply commits one bounded control-state command through the current leader.
// It is intentionally generic so the FSM can evolve without coupling its
// command vocabulary to the Raft transport package.
func (n *Node) Apply(ctx context.Context, command []byte) (controlstate.ApplyResult, error) {
	if n.draining.Load() {
		return controlstate.ApplyResult{}, fmt.Errorf("raft node is shutting down")
	}
	if len(command) == 0 {
		return controlstate.ApplyResult{}, fmt.Errorf("raft control-state command is empty")
	}
	if len(command) > n.config.MaxCommandBytes {
		return controlstate.ApplyResult{}, fmt.Errorf("raft control-state command is %d bytes; limit is %d", len(command), n.config.MaxCommandBytes)
	}

	started := time.Now()
	future := n.raft.Apply(command, n.applyTimeout(ctx))
	type outcome struct {
		response any
		err      error
	}
	completed := make(chan outcome, 1)
	go func() {
		err := future.Error()
		completed <- outcome{response: future.Response(), err: err}
	}()

	select {
	case <-ctx.Done():
		n.metrics.observeProposal("unknown", time.Since(started))
		return controlstate.ApplyResult{}, fmt.Errorf("wait for raft apply: %w", ctx.Err())
	case result := <-completed:
		if result.err != nil {
			n.metrics.observeProposal("error", time.Since(started))
			return controlstate.ApplyResult{}, fmt.Errorf("apply raft control command: %w", result.err)
		}
		response, ok := result.response.(controlstate.ApplyResult)
		if !ok {
			n.metrics.observeProposal("error", time.Since(started))
			return controlstate.ApplyResult{}, fmt.Errorf("unexpected raft FSM response %T", result.response)
		}
		metricResult := "committed"
		if !response.Applied() {
			metricResult = "rejected"
		}
		n.metrics.observeProposal(metricResult, time.Since(started))
		return response, nil
	}
}

// State returns a copy of the locally applied replicated control state.
func (n *Node) State() controlstate.State {
	return n.fsm.State()
}

// LookupGateway returns one current durable gateway incarnation, if any.
// Callers that need a linearizable read must call VerifyLeader first.
func (n *Node) LookupGateway(gatewayID string) (controlstate.GatewaySessionRef, bool) {
	return n.fsm.LookupGateway(gatewayID)
}

// LookupRoute returns one current exact route, if any. Pattern selection stays
// outside the replicated FSM; callers that need a linearizable read must call
// VerifyLeader first.
func (n *Node) LookupRoute(key controlstate.BindingKey) (controlstate.Route, bool) {
	return n.fsm.LookupRoute(key)
}

// GetConfiguration returns the latest Raft membership configuration. It does
// not mutate membership and may be served by any local member.
func (n *Node) GetConfiguration(ctx context.Context) (raft.Configuration, error) {
	future := n.raft.GetConfiguration()
	if err := waitFuture(ctx, future.Error); err != nil {
		return raft.Configuration{}, fmt.Errorf("get raft configuration: %w", err)
	}
	return future.Configuration(), nil
}

// AddVoter starts the standard Raft staging/catch-up/promotion flow. The
// caller must execute it on the leader; membership changes are serialized per
// local node because HashiCorp Raft accepts only one configuration change at a
// time. Replacing lost storage must use a fresh NodeID before this call.
func (n *Node) AddVoter(ctx context.Context, nodeID, address string) error {
	if err := validateMember(nodeID, address); err != nil {
		return err
	}
	if n.draining.Load() {
		return fmt.Errorf("raft node is shutting down")
	}

	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()

	configuration, err := n.GetConfiguration(ctx)
	if err != nil {
		return err
	}
	for _, server := range configuration.Servers {
		if server.ID == raft.ServerID(nodeID) && server.Address != raft.ServerAddress(address) {
			return fmt.Errorf("raft member %q already exists at %q; an erased member identity must not be reused", nodeID, server.Address)
		}
		if server.ID != raft.ServerID(nodeID) && server.Address == raft.ServerAddress(address) {
			return fmt.Errorf("raft address %q already belongs to member %q", address, server.ID)
		}
	}
	if !isExistingVoter(configuration, raft.ServerID(nodeID)) && voterCount(configuration) >= maxRaftVoters {
		return fmt.Errorf("raft voter limit is %d", maxRaftVoters)
	}
	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, n.applyTimeout(ctx))
	if err := waitFuture(ctx, future.Error); err != nil {
		return fmt.Errorf("add raft voter %q: %w", nodeID, err)
	}
	return nil
}

// RemoveServer removes a member through the normal Raft configuration-change
// path. It never performs recovery or force-reset behavior.
func (n *Node) RemoveServer(ctx context.Context, nodeID string) error {
	if strings.TrimSpace(nodeID) == "" {
		return fmt.Errorf("raft member node ID is required")
	}
	if n.draining.Load() {
		return fmt.Errorf("raft node is shutting down")
	}

	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()

	future := n.raft.RemoveServer(raft.ServerID(nodeID), 0, n.applyTimeout(ctx))
	if err := waitFuture(ctx, future.Error); err != nil {
		return fmt.Errorf("remove raft server %q: %w", nodeID, err)
	}
	return nil
}

func (n *Node) Snapshot(ctx context.Context) error {
	started := time.Now()
	future := n.raft.Snapshot()
	completed := make(chan error, 1)
	go func() { completed <- future.Error() }()
	select {
	case <-ctx.Done():
		n.metrics.observeSnapshot("unknown", time.Since(started))
		return fmt.Errorf("wait for raft snapshot: %w", ctx.Err())
	case err := <-completed:
		if err != nil {
			n.metrics.observeSnapshot("error", time.Since(started))
			return fmt.Errorf("create raft snapshot: %w", err)
		}
		n.metrics.observeSnapshot("success", time.Since(started))
		return nil
	}
}

func (n *Node) Status() Status {
	stats := n.raft.Stats()
	leaderAddress, leaderID := n.raft.LeaderWithID()
	status := Status{
		NodeID:             n.config.NodeID,
		Role:               n.raft.State().String(),
		LeaderID:           string(leaderID),
		LeaderAddress:      string(leaderAddress),
		Term:               parseStat(stats, "term"),
		CommitIndex:        parseStat(stats, "commit_index"),
		AppliedIndex:       parseStat(stats, "applied_index"),
		LastSnapshotIndex:  parseStat(stats, "last_snapshot_index"),
		PendingFSMCommands: parseStat(stats, "fsm_pending"),
		PeerCount:          parseStat(stats, "num_peers"),
		ClusterEpoch:       n.fsm.ClusterEpoch(),
	}
	status.Ready = !n.draining.Load() && status.LeaderAddress != "" && status.ClusterEpoch != ""
	return status
}
