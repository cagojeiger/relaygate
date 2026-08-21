package authority

import (
	"context"
	"errors"
	"fmt"
	"time"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

func (m *Manager) Confirm(ctx context.Context) (controlmodel.AuthorityRef, error) {
	return m.confirm(ctx, false)
}

func (m *Manager) confirm(ctx context.Context, fenceOnContextError bool) (controlmodel.AuthorityRef, error) {
	status := m.node.Status()
	if status.Role != "Leader" || m.node.ClusterEpoch() != m.config.ClusterEpoch {
		m.fence()
		return controlmodel.AuthorityRef{}, ErrNoAuthority
	}
	if err := m.node.VerifyLeader(ctx); err != nil {
		callerEnded := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		shouldFence := !callerEnded || fenceOnContextError
		if callerEnded && !fenceOnContextError {
			shouldFence = m.node.Status().Role != "Leader" || m.node.ClusterEpoch() != m.config.ClusterEpoch
		}
		if shouldFence {
			m.fence()
		}
		return controlmodel.AuthorityRef{}, fmt.Errorf("%w: %w", ErrNoAuthority, err)
	}
	confirmed := m.node.Status()
	if confirmed.Role != "Leader" || m.node.ClusterEpoch() != m.config.ClusterEpoch {
		m.fence()
		return controlmodel.AuthorityRef{}, ErrNoAuthority
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if confirmed.Term < m.currentTerm {
		return controlmodel.AuthorityRef{}, ErrNoAuthority
	}
	if m.current != nil {
		if confirmed.Term != m.currentTerm {
			m.fenceLocked()
		}
	}
	if m.current == nil {
		// Only a newly established authority needs the full committed C
		// snapshot to seed its revalidation cleanup set. Steady-state
		// confirmations, including Open admission, stay on point lookups.
		committed := m.node.State()
		authorityID, err := newID()
		if err != nil {
			return controlmodel.AuthorityRef{}, err
		}
		m.current = &controlmodel.AuthorityRef{ClusterEpoch: m.config.ClusterEpoch, AuthorityID: authorityID}
		m.currentTerm = confirmed.Term
		deadline := m.now().Add(m.config.GatewayRevalidationTimeout)
		for _, gateway := range committed.Gateways {
			m.cleanup[gateway] = deadline
		}
	}
	return *m.current, nil
}

func (m *Manager) Observe(ctx context.Context) (controlmodel.AuthorityRef, Presence, error) {
	ref, err := m.Confirm(ctx)
	if err != nil {
		return controlmodel.AuthorityRef{}, m.Presence(), err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil || *m.current != ref {
		return controlmodel.AuthorityRef{}, m.presenceLocked(m.node.State()), ErrNoAuthority
	}
	return ref, m.presenceLocked(m.node.State()), nil
}

// OpenSession first commits the gateway process incarnation (C), then creates
// a new leader-local control stream (V=false). A same-instance reconnect keeps
// its committed routes until its replacement snapshot arrives; a new instance
// is an atomic FSM replacement that deletes the old instance's routes.
func (m *Manager) run(ctx context.Context) {
	defer m.finish()
	ticker := time.NewTicker(m.config.ProbeInterval)
	defer ticker.Stop()
	for {
		m.probe(ctx)
		select {
		case <-ctx.Done():
			m.fence()
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) finish() { m.doneOnce.Do(func() { close(m.done) }) }

func (m *Manager) probe(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, m.config.ProbeTimeout)
	_, err := m.confirm(ctx, true)
	cancel()
	if err == nil {
		m.sweep(parent)
	}
}

func (m *Manager) sweep(parent context.Context) {
	now := m.now()
	m.mu.Lock()
	due := make([]controlstate.GatewaySessionRef, 0)
	for gateway, deadline := range m.cleanup {
		if entry := m.sessions[gateway.GatewayID]; entry != nil && !entry.closed && entry.state == SessionRevalidated && gatewayRef(entry.ref) == gateway {
			delete(m.cleanup, gateway)
			continue
		}
		if !deadline.After(now) {
			due = append(due, gateway)
		}
	}
	m.mu.Unlock()

	for _, gateway := range due {
		m.mutationMu.Lock()
		if !m.cleanupStillDue(gateway, now) {
			m.mutationMu.Unlock()
			continue
		}
		current, exists := m.node.LookupGateway(gateway.GatewayID)
		if !exists || current != gateway {
			m.clearCleanup(gateway)
			m.mutationMu.Unlock()
			continue
		}
		command, err := controlstate.EncodeRemoveGateway(controlstate.RemoveGateway{ClusterEpoch: m.config.ClusterEpoch, Gateway: gateway})
		if err != nil {
			m.clearCleanup(gateway)
			m.mutationMu.Unlock()
			continue
		}
		if _, err := m.applyWithParent(parent, command); err != nil {
			m.mutationMu.Unlock()
			continue // retain deadline for a later confirmed leader probe.
		}
		m.finishCleanup(gateway)
		m.mutationMu.Unlock()
	}
}

func (m *Manager) cleanupStillDue(gateway controlstate.GatewaySessionRef, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline, exists := m.cleanup[gateway]
	if !exists || deadline.After(now) {
		return false
	}
	if entry := m.sessions[gateway.GatewayID]; entry != nil && !entry.closed && entry.state == SessionRevalidated && gatewayRef(entry.ref) == gateway {
		delete(m.cleanup, gateway)
		return false
	}
	return true
}

func (m *Manager) clearCleanup(gateway controlstate.GatewaySessionRef) {
	m.mu.Lock()
	delete(m.cleanup, gateway)
	m.mu.Unlock()
}

func (m *Manager) finishCleanup(gateway controlstate.GatewaySessionRef) {
	m.mu.Lock()
	delete(m.cleanup, gateway)
	if entry := m.sessions[gateway.GatewayID]; entry != nil && gatewayRef(entry.ref) == gateway {
		entry.close()
		delete(m.sessions, gateway.GatewayID)
	}
	m.mu.Unlock()
}

func (m *Manager) fence() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fenceLocked()
}

// fenceLocked intentionally never changes the Raft FSM. Losing leadership
// invalidates V and all one-term control/session addresses, while committed C
// survives for the next leader's revalidation grace period.
func (m *Manager) fenceLocked() {
	if m.current == nil && len(m.sessions) == 0 && len(m.cleanup) == 0 {
		return
	}
	for _, session := range m.sessions {
		session.close()
	}
	clear(m.sessions)
	clear(m.cleanup)
	m.current = nil
}
