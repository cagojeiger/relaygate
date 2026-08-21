package localbinding

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	clientauth "github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	clientsession "github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

func (m *Manager) Unbind(session clientsession.Ref, bindingID string) error {
	if bindingID == "" {
		return fmt.Errorf("%w: listener binding ID is required", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[bindingID]
	if e == nil || e.session != session {
		return nil
	}
	m.retireLocked(e, true)
	return nil
}

func (m *Manager) RetireSession(session clientsession.Ref) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retireMatchingLocked(func(e *entry) bool { return e.session == session })
}

func (m *Manager) Retire(change clientauth.ChangeSet) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retireMatchingLocked(func(e *entry) bool { return change.Removes(e.session.ClientID, e.session.APIKeyID) })
}

func (m *Manager) RetireAll() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retireMatchingLocked(func(*entry) bool { return true })
}

func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int(m.active)
}

func (m *Manager) Inspect(bindingID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[bindingID]
	if e == nil {
		return Snapshot{}, false
	}
	return snapshotOf(e), true
}

func (m *Manager) Eligible(bindingID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[bindingID]
	if e == nil || e.state != StateLive {
		return false
	}
	select {
	case <-m.ctx.Done():
		m.retireLocked(e, true)
		return false
	case <-e.aborted:
		return false
	default:
	}
	select {
	case <-e.done:
		m.retireLocked(e, true)
		return false
	default:
	}
	if err := m.sessions.Require(e.session); err != nil {
		m.retireLocked(e, true)
		return false
	}
	return true
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.retireMatchingLocked(func(*entry) bool { return true })
		m.mu.Unlock()
		m.cancel()
		m.wg.Wait()
	})
}

func (m *Manager) install(e *entry, result chan<- bindResult) {
	defer m.wg.Done()
	err := m.committer.Declare(m.ctx, e.binding)
	m.mu.Lock()
	if err != nil {
		m.recordFailedInstallLocked(e)
		m.mu.Unlock()
		result <- bindResult{err: fmt.Errorf("%w: install listener binding: %w", classifyInstallError(err), err)}
		m.notifyAborted(e)
		return
	}
	if e.state != StateRegistering || m.closed {
		m.startRemoveLocked(e)
		m.mu.Unlock()
		result <- bindResult{err: ErrSessionEnded}
		return
	}
	select {
	case <-e.aborted:
		m.startRemoveLocked(e)
		m.mu.Unlock()
		result <- bindResult{err: ErrSessionEnded}
		return
	default:
	}
	if err := m.sessions.Require(e.session); err != nil {
		m.retireLocked(e, true)
		m.startRemoveLocked(e)
		m.mu.Unlock()
		result <- bindResult{err: fmt.Errorf("%w: %w", ErrSessionEnded, err)}
		return
	}
	e.state = StateLive
	m.mu.Unlock()
	result <- bindResult{binding: e.binding}
}

func (m *Manager) recordFailedInstallLocked(e *entry) {
	if e.state == StateRegistering {
		m.retireLocked(e, false)
	}
	m.releaseCapacityLocked(e)
}

func classifyInstallError(err error) error {
	switch {
	case errors.Is(err, routing.ErrCapacity):
		return ErrCapacity
	case errors.Is(err, routing.ErrConflict):
		return ErrConflict
	default:
		return ErrUnavailable
	}
}

func (m *Manager) watchSession(e *entry, done <-chan struct{}) {
	defer m.wg.Done()
	select {
	case <-done:
		m.RetireSession(e.session)
	case <-e.aborted:
	case <-m.ctx.Done():
	}
}

func (m *Manager) retireAttempt(e *entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retireLocked(e, true)
}

func (m *Manager) notifyAborted(e *entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !e.abortedClosed {
		close(e.aborted)
		e.abortedClosed = true
	}
}

func (m *Manager) live(e *entry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return e.state == StateLive && !m.closed
}

func (m *Manager) retireMatchingLocked(match func(*entry) bool) int {
	retired := 0
	for _, e := range m.entries {
		if !match(e) || !e.canRetire() {
			continue
		}
		m.retireLocked(e, true)
		retired++
	}
	return retired
}

func (e *entry) canRetire() bool { return e.state == StateRegistering || e.state == StateLive }

func (m *Manager) retireLocked(e *entry, notify bool) {
	switch e.state {
	case StateRegistering:
		e.state = StateRetired
		m.makeIneligibleLocked(e)
		m.recordRetiredLocked(e)
	case StateLive:
		e.state = StateRetiring
		m.makeIneligibleLocked(e)
		m.startRemoveLocked(e)
	default:
		return
	}
	if notify && !e.abortedClosed {
		close(e.aborted)
		e.abortedClosed = true
	}
}

func (m *Manager) makeIneligibleLocked(e *entry) {
	if bindingID, ok := m.byKey[e.binding.Key]; ok && bindingID == e.binding.Ref.ListenerBindingID {
		delete(m.byKey, e.binding.Key)
	}
	if owned := m.bySession[e.session]; owned != nil {
		delete(owned, e.binding.Ref.ListenerBindingID)
		if len(owned) == 0 {
			delete(m.bySession, e.session)
		}
	}
}

func (m *Manager) releaseCapacityLocked(e *entry) {
	if !e.capacityHeld {
		return
	}
	e.capacityHeld = false
	if m.active > 0 {
		m.active--
	}
}

func (m *Manager) startRemoveLocked(e *entry) {
	m.wg.Add(1)
	go func(binding routing.LiveBinding) {
		defer m.wg.Done()
		_ = m.committer.Withdraw(m.ctx, binding)
		m.mu.Lock()
		if e.state == StateRetiring {
			e.state = StateRetired
			m.recordRetiredLocked(e)
		}
		m.releaseCapacityLocked(e)
		m.mu.Unlock()
	}(e.binding)
}

func (m *Manager) recordRetiredLocked(e *entry) {
	if e.terminalRecorded {
		return
	}
	e.terminalRecorded = true
	m.retiredOrder = append(m.retiredOrder, e.binding.Ref.ListenerBindingID)
	for uint64(len(m.retiredOrder)) > uint64(m.max) {
		oldest := m.retiredOrder[0]
		m.retiredOrder = m.retiredOrder[1:]
		old := m.entries[oldest]
		if old != nil && old.state == StateRetired {
			delete(m.entries, oldest)
		}
	}
}

func snapshotOf(e *entry) Snapshot {
	return Snapshot{Binding: e.binding, Session: e.session, State: e.state}
}

func validateOpenContext(open routing.OpenContext) error {
	for _, identity := range []struct{ field, value string }{
		{"cluster_epoch", open.ClusterEpoch},
		{"authority_id", open.AuthorityID},
		{"attempt_id", open.AttemptID},
		{"owner_control_session_id", open.OwnerControlSessionID},
	} {
		if err := routing.ValidateIdentity(identity.field, identity.value); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalid, err)
		}
	}
	if err := open.Auth.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if err := open.Binding.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func bindingLess(left, right routing.BindingKey) bool {
	if left.ClientID != right.ClientID {
		return left.ClientID < right.ClientID
	}
	if left.EndpointPattern != right.EndpointPattern {
		return left.EndpointPattern < right.EndpointPattern
	}
	return left.TargetID < right.TargetID
}

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
