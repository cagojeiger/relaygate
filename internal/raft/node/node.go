package raftnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/bbolt"

	"github.com/cagojeiger/relaygate/internal/raft/state"
)

const (
	raftMaxAppendEntries = 64
	maxRaftVoters        = 7
	storeOpenTimeout     = 5 * time.Second
)

var nodeIDKey = []byte("relaygate/node-id/v1")

type Config struct {
	NodeID           string
	BindAddress      string
	AdvertiseAddress string
	// DataDir is the durable identity, log, stable-state, and snapshot root for
	// this Raft member. A replacement member must use a new NodeID and a new
	// directory; it must never reuse an erased member identity.
	DataDir string
	// Bootstrap is a one-shot operator action for a brand-new cluster. It is
	// ignored for an intact store, but must not remain enabled on a replacement
	// whose former store was lost.
	Bootstrap         bool
	BootstrapVoters   []BootstrapVoter
	ApplyTimeout      time.Duration
	TransportTimeout  time.Duration
	ShutdownTimeout   time.Duration
	SnapshotRetain    int
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	MaxPool           int
	MaxCommandBytes   int
}

type BootstrapVoter struct {
	NodeID  string
	Address string
}

func (c Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if c.BindAddress == "" || c.AdvertiseAddress == "" {
		return fmt.Errorf("bind and advertise addresses are required")
	}
	if _, _, err := net.SplitHostPort(c.BindAddress); err != nil {
		return fmt.Errorf("invalid bind address: %w", err)
	}
	if _, _, err := net.SplitHostPort(c.AdvertiseAddress); err != nil {
		return fmt.Errorf("invalid advertise address: %w", err)
	}
	if len(c.BootstrapVoters) > maxRaftVoters {
		return fmt.Errorf("cohort voter manifest must contain at most %d voters", maxRaftVoters)
	}
	if c.Bootstrap && len(c.BootstrapVoters) == 0 {
		return fmt.Errorf("bootstrap voter manifest is required for initial bootstrap")
	}
	seenIDs := make(map[string]struct{}, len(c.BootstrapVoters))
	seenAddresses := make(map[string]struct{}, len(c.BootstrapVoters))
	localFound := false
	for _, voter := range c.BootstrapVoters {
		if voter.NodeID == "" {
			return fmt.Errorf("bootstrap voter node ID is required")
		}
		if _, _, err := net.SplitHostPort(voter.Address); err != nil {
			return fmt.Errorf("invalid bootstrap voter address %q: %w", voter.Address, err)
		}
		if _, exists := seenIDs[voter.NodeID]; exists {
			return fmt.Errorf("duplicate bootstrap voter node ID %q", voter.NodeID)
		}
		if _, exists := seenAddresses[voter.Address]; exists {
			return fmt.Errorf("duplicate bootstrap voter address %q", voter.Address)
		}
		seenIDs[voter.NodeID] = struct{}{}
		seenAddresses[voter.Address] = struct{}{}
		if voter.NodeID == c.NodeID {
			if voter.Address != c.AdvertiseAddress {
				return fmt.Errorf("local bootstrap voter address differs from advertise address")
			}
			localFound = true
		}
	}
	if c.Bootstrap && !localFound {
		return fmt.Errorf("bootstrap voters must contain local node ID %q", c.NodeID)
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data directory is required")
	}
	if c.ApplyTimeout <= 0 || c.TransportTimeout <= 0 || c.ShutdownTimeout <= 0 {
		return fmt.Errorf("timeouts must be positive")
	}
	if c.SnapshotRetain < 1 || c.SnapshotThreshold == 0 || c.SnapshotInterval <= 0 {
		return fmt.Errorf("snapshot settings must be positive")
	}
	if c.MaxPool < 1 || c.MaxCommandBytes < 1 {
		return fmt.Errorf("pool and command limits must be positive")
	}
	return nil
}

type Node struct {
	config    Config
	raft      *raft.Raft
	fsm       *controlstate.FSM
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport
	metrics   *metrics

	draining  atomic.Bool
	closeOnce sync.Once
	closeErr  error

	membershipMu sync.Mutex
}

type Status struct {
	NodeID             string `json:"node_id"`
	Role               string `json:"role"`
	LeaderID           string `json:"leader_id,omitempty"`
	LeaderAddress      string `json:"leader_address,omitempty"`
	Term               uint64 `json:"term"`
	CommitIndex        uint64 `json:"commit_index"`
	AppliedIndex       uint64 `json:"applied_index"`
	LastSnapshotIndex  uint64 `json:"last_snapshot_index"`
	PendingFSMCommands uint64 `json:"pending_fsm_commands"`
	PeerCount          uint64 `json:"peer_count"`
	ClusterEpoch       string `json:"cluster_epoch,omitempty"`
	Ready              bool   `json:"ready"`
}

func Open(config Config, logger hclog.Logger, registerer prometheus.Registerer) (*Node, error) {
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("validate raft node config: %w", err)
	}
	if logger == nil {
		logger = hclog.NewNullLogger()
	}
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create raft data directory: %w", err)
	}
	store, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(config.DataDir, "raft.db"),
		BoltOptions: &bbolt.Options{
			Timeout: storeOpenTimeout,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open raft store: %w", err)
	}
	cleanupStore := true
	defer func() {
		if cleanupStore {
			_ = store.Close()
		}
	}()

	snapshots, err := raft.NewFileSnapshotStoreWithLogger(config.DataDir, config.SnapshotRetain, logger.Named("snapshot"))
	if err != nil {
		return nil, fmt.Errorf("open raft snapshot store: %w", err)
	}
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, fmt.Errorf("inspect raft state: %w", err)
	}
	if err := ensureNodeIdentity(store, config.NodeID, existing); err != nil {
		return nil, err
	}

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", config.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("open raft transport: %w", err)
	}
	stream := &tcpStreamLayer{
		Listener:  listener,
		advertise: serverAddress(config.AdvertiseAddress),
	}
	transport := raft.NewNetworkTransportWithLogger(
		stream,
		config.MaxPool,
		config.TransportTimeout,
		logger.Named("transport"),
	)
	cleanupTransport := true
	defer func() {
		if cleanupTransport {
			_ = transport.Close()
		}
	}()

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.Logger = logger.Named("core")
	raftConfig.SnapshotThreshold = config.SnapshotThreshold
	raftConfig.SnapshotInterval = config.SnapshotInterval
	raftConfig.TrailingLogs = trailingLogEntries(config.SnapshotThreshold)
	raftConfig.MaxAppendEntries = raftMaxAppendEntries
	raftConfig.NoLegacyTelemetry = true

	fsm := controlstate.NewFSM()
	raftInstance, err := raft.NewRaft(raftConfig, fsm, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	cleanupRaft := true
	defer func() {
		if cleanupRaft {
			_ = raftInstance.Shutdown().Error()
		}
	}()

	if config.Bootstrap && !existing {
		future := raftInstance.BootstrapCluster(bootstrapConfiguration(config))
		if err := future.Error(); err != nil {
			return nil, fmt.Errorf("bootstrap raft cluster: %w", err)
		}
	}

	node := &Node{
		config:    config,
		raft:      raftInstance,
		fsm:       fsm,
		store:     store,
		transport: transport,
	}
	metrics, err := newMetrics(registerer, node)
	if err != nil {
		return nil, err
	}
	node.metrics = metrics

	cleanupRaft = false
	cleanupTransport = false
	cleanupStore = false
	return node, nil
}

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

// ClusterEpoch returns the constant-size safety marker held by the local FSM.
// It never exposes Gateway, route, listener, or Pipe state.
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
		return controlstate.ApplyResult{}, fmt.Errorf("raft safety command is empty")
	}
	if len(command) > n.config.MaxCommandBytes {
		return controlstate.ApplyResult{}, fmt.Errorf("raft safety command is %d bytes; limit is %d", len(command), n.config.MaxCommandBytes)
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

func (n *Node) BeginShutdown() {
	n.draining.Store(true)
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.BeginShutdown()
		shutdown := n.raft.Shutdown()
		completed := make(chan error, 1)
		go func() { completed <- shutdown.Error() }()

		timer := time.NewTimer(n.config.ShutdownTimeout)
		defer timer.Stop()
		select {
		case err := <-completed:
			if err != nil {
				n.closeErr = errors.Join(n.closeErr, fmt.Errorf("shutdown raft: %w", err))
			}
		case <-timer.C:
			n.closeErr = errors.Join(n.closeErr, fmt.Errorf("shutdown raft: timeout after %s", n.config.ShutdownTimeout))
			if err := n.transport.Close(); err != nil {
				n.closeErr = errors.Join(n.closeErr, fmt.Errorf("close raft transport after timeout: %w", err))
			}
			// Raft may still access the durable store after an unconfirmed
			// shutdown, so do not close it underneath its goroutines.
			return
		}
		if err := n.transport.Close(); err != nil {
			n.closeErr = errors.Join(n.closeErr, fmt.Errorf("close raft transport: %w", err))
		}
		if err := n.store.Close(); err != nil {
			n.closeErr = errors.Join(n.closeErr, fmt.Errorf("close raft store: %w", err))
		}
	})
	return n.closeErr
}

func (n *Node) applyTimeout(ctx context.Context) time.Duration {
	timeout := n.config.ApplyTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		return time.Nanosecond
	}
	return timeout
}

func parseStat(stats map[string]string, key string) uint64 {
	value, _ := strconv.ParseUint(stats[key], 10, 64)
	return value
}

func trailingLogEntries(snapshotThreshold uint64) uint64 {
	if snapshotThreshold < 4 {
		return 1
	}
	return snapshotThreshold / 4
}

func ensureNodeIdentity(store *raftboltdb.BoltStore, nodeID string, existingRaftState bool) error {
	stored, err := store.Get(nodeIDKey)
	if err == nil {
		if string(stored) != nodeID {
			return fmt.Errorf("durable node identity is %q, configured node_id is %q", string(stored), nodeID)
		}
		return nil
	}
	if !errors.Is(err, raftboltdb.ErrKeyNotFound) {
		return fmt.Errorf("read durable node identity: %w", err)
	}
	if existingRaftState {
		return fmt.Errorf("raft state exists without RelayGate node identity; refuse unsafe reuse")
	}
	// An empty directory has no retained evidence with which to distinguish a
	// new voter from a deleted member. It may join only through the normal
	// AddVoter workflow; operators must give replacements a never-reused NodeID.
	if err := store.Set(nodeIDKey, []byte(nodeID)); err != nil {
		return fmt.Errorf("persist node identity: %w", err)
	}
	return nil
}

func waitFuture(ctx context.Context, await func() error) error {
	completed := make(chan error, 1)
	go func() { completed <- await() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-completed:
		return err
	}
}

func validateMember(nodeID, address string) error {
	if strings.TrimSpace(nodeID) == "" {
		return fmt.Errorf("raft member node ID is required")
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("invalid raft member address %q: %w", address, err)
	}
	return nil
}

func voterCount(configuration raft.Configuration) int {
	count := 0
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			count++
		}
	}
	return count
}

func isExistingVoter(configuration raft.Configuration, id raft.ServerID) bool {
	for _, server := range configuration.Servers {
		if server.ID == id {
			return server.Suffrage == raft.Voter
		}
	}
	return false
}

func bootstrapConfiguration(config Config) raft.Configuration {
	servers := make([]raft.Server, 0, len(config.BootstrapVoters))
	for _, voter := range config.BootstrapVoters {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(voter.NodeID),
			Address:  raft.ServerAddress(voter.Address),
		})
	}
	return raft.Configuration{Servers: servers}
}

type serverAddress string

func (a serverAddress) Network() string { return "tcp" }
func (a serverAddress) String() string  { return string(a) }

type tcpStreamLayer struct {
	net.Listener
	advertise net.Addr
}

func (s *tcpStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return (&net.Dialer{Timeout: timeout}).DialContext(context.Background(), "tcp", string(address))
}

func (s *tcpStreamLayer) Addr() net.Addr { return s.advertise }
