package raftnode

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
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
