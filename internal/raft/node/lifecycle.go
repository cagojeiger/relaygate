package raftnode

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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
