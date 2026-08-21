package raftnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

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
