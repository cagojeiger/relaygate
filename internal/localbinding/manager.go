package localbinding

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
)

var (
	ErrInvalid      = errors.New("invalid listener binding")
	ErrCapacity     = errors.New("listener binding capacity reached")
	ErrConflict     = errors.New("listener binding already exists")
	ErrNotFound     = errors.New("listener binding not found")
	ErrAttemptUsed  = errors.New("open attempt was already reserved")
	ErrUnavailable  = errors.New("listener binding control unavailable")
	ErrSessionEnded = errors.New("client session ended")
)

// Committer is the control-plane boundary used by the local binding runtime.
// Install must return the exact committed slot. Remove is an exact,
// generation-and-ref conditional cleanup.
type Committer interface {
	Install(context.Context, controlstate.BindingKey, controlstate.ListenerBindingRef) (controlstate.BindingSlot, error)
	Remove(context.Context, controlstate.BindingSlot) error
}

type SessionValidator interface {
	Require(clientsession.Ref) error
}

type State string

const (
	StateRegistering State = "RegisteringB"
	StateLive        State = "LiveB"
	StateRetiring    State = "RetiringB"
	StateRetired     State = "RetiredB"
)

// Snapshot is a process-local diagnostic view. It is not a routing or
// control-plane contract.
type Snapshot struct {
	Key     controlstate.BindingKey
	Ref     controlstate.ListenerBindingRef
	Session clientsession.Ref
	Slot    controlstate.BindingSlot
	State   State
}

// Reservation is an immutable exact owner reservation. It intentionally does
// not expose the binding retirement signal: explicit unbind after Reserve must
// not cancel an already admitted attempt. Session lifetime remains observable
// through ListenerDone.
type Reservation struct {
	Context      authority.OpenContext
	Caller       clientsession.Ref
	Binding      controlstate.BindingSlot
	Listener     clientsession.Ref
	Endpoint     ListenerEndpoint
	ListenerDone <-chan struct{}
}

type entry struct {
	key      controlstate.BindingKey
	ref      controlstate.ListenerBindingRef
	session  clientsession.Ref
	done     <-chan struct{}
	endpoint ListenerEndpoint
	slot     controlstate.BindingSlot
	state    State
	aborted  chan struct{}

	abortedClosed    bool
	terminalRecorded bool
	capacityHeld     bool
}

type bindResult struct {
	slot controlstate.BindingSlot
	err  error
}

type Manager struct {
	gatewayID         string
	gatewayInstanceID string
	max               uint32
	committer         Committer
	sessions          SessionValidator

	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	entries      map[string]*entry
	byKey        map[controlstate.BindingKey]string
	bySession    map[clientsession.Ref]map[string]struct{}
	retiredOrder []string
	active       uint32
	closed       bool
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

func New(gatewayID, gatewayInstanceID string, max uint32, committer Committer, sessions SessionValidator) (*Manager, error) {
	if gatewayID == "" {
		return nil, fmt.Errorf("%w: gateway ID is required", ErrInvalid)
	}
	if gatewayInstanceID == "" {
		return nil, fmt.Errorf("%w: gateway instance ID is required", ErrInvalid)
	}
	if max == 0 || max > controlstate.MaxListenerBindingsPerGateway {
		return nil, fmt.Errorf("%w: maximum listener bindings must be between 1 and %d", ErrInvalid, controlstate.MaxListenerBindingsPerGateway)
	}
	if committer == nil {
		return nil, fmt.Errorf("%w: committer is required", ErrInvalid)
	}
	if sessions == nil {
		return nil, fmt.Errorf("%w: session validator is required", ErrInvalid)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		gatewayID:         gatewayID,
		gatewayInstanceID: gatewayInstanceID,
		max:               max,
		committer:         committer,
		sessions:          sessions,
		ctx:               ctx,
		cancel:            cancel,
		entries:           make(map[string]*entry),
		byKey:             make(map[controlstate.BindingKey]string),
		bySession:         make(map[clientsession.Ref]map[string]struct{}),
	}, nil
}

// Bind reserves local capacity before submitting an install and returns only
// after the exact slot is committed. ClientID is always derived from session.
func (m *Manager) Bind(ctx context.Context, session clientsession.Session, endpointPattern, targetID string, endpoint ListenerEndpoint) (controlstate.BindingSlot, error) {
	if ctx == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if endpointPattern == "" {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: endpoint pattern is required", ErrInvalid)
	}
	if targetID == "" {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: target ID is required", ErrInvalid)
	}
	if session.Ref.ClientSessionID == "" || session.Ref.ClientID == "" || session.Done == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: authenticated session is required", ErrInvalid)
	}
	if endpoint == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: listener endpoint is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return controlstate.BindingSlot{}, err
	}

	key := controlstate.BindingKey{
		ClientID:        session.Ref.ClientID,
		EndpointPattern: endpointPattern,
		TargetID:        targetID,
	}
	if err := key.Validate(); err != nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return controlstate.BindingSlot{}, ErrUnavailable
	}
	// This call intentionally remains under m.mu. A successful validation and
	// insertion must be ordered against credential-reload retirement.
	if err := m.sessions.Require(session.Ref); err != nil {
		m.mu.Unlock()
		return controlstate.BindingSlot{}, fmt.Errorf("%w: %w", ErrSessionEnded, err)
	}
	select {
	case <-session.Done:
		m.mu.Unlock()
		return controlstate.BindingSlot{}, ErrSessionEnded
	default:
	}
	if _, exists := m.byKey[key]; exists {
		m.mu.Unlock()
		return controlstate.BindingSlot{}, ErrConflict
	}
	if m.active >= m.max {
		m.mu.Unlock()
		return controlstate.BindingSlot{}, ErrCapacity
	}
	bindingID, err := newID()
	if err != nil {
		m.mu.Unlock()
		return controlstate.BindingSlot{}, fmt.Errorf("%w: generate listener binding ID: %w", ErrUnavailable, err)
	}
	e := &entry{
		key: key,
		ref: controlstate.ListenerBindingRef{
			GatewayID:         m.gatewayID,
			GatewayInstanceID: m.gatewayInstanceID,
			ListenerBindingID: bindingID,
		},
		session:      session.Ref,
		done:         session.Done,
		endpoint:     endpoint,
		state:        StateRegistering,
		aborted:      make(chan struct{}),
		capacityHeld: true,
	}
	m.entries[bindingID] = e
	m.byKey[key] = bindingID
	owned := m.bySession[session.Ref]
	if owned == nil {
		owned = make(map[string]struct{})
		m.bySession[session.Ref] = owned
	}
	owned[bindingID] = struct{}{}
	m.active++

	result := make(chan bindResult, 1)
	m.wg.Add(2)
	go m.install(e, result)
	go m.watchSession(e, session.Done)
	m.mu.Unlock()

	select {
	case outcome := <-result:
		if err := ctx.Err(); err != nil {
			m.retireAttempt(e)
			return controlstate.BindingSlot{}, err
		}
		select {
		case <-session.Done:
			m.retireAttempt(e)
			return controlstate.BindingSlot{}, ErrSessionEnded
		default:
		}
		if outcome.err == nil && !m.live(e) {
			return controlstate.BindingSlot{}, ErrSessionEnded
		}
		return outcome.slot, outcome.err
	case <-ctx.Done():
		m.retireAttempt(e)
		return controlstate.BindingSlot{}, ctx.Err()
	case <-session.Done:
		m.retireAttempt(e)
		return controlstate.BindingSlot{}, ErrSessionEnded
	case <-e.aborted:
		select {
		case outcome := <-result:
			return outcome.slot, outcome.err
		default:
			return controlstate.BindingSlot{}, ErrSessionEnded
		}
	case <-m.ctx.Done():
		m.retireAttempt(e)
		return controlstate.BindingSlot{}, ErrUnavailable
	}
}

// GatewayID returns the stable local owner identity used by route dispatch to
// reject the remote-owner case before attempting a local reservation.
func (m *Manager) GatewayID() string {
	return m.gatewayID
}

// Reserve atomically consumes one exact authority-issued attempt against the
// current LiveB entry. The comparison and both session validations are ordered
// with unbind and credential retirement by m.mu.
func (m *Manager) Reserve(open authority.OpenContext, caller clientsession.Ref) (Reservation, error) {
	if open.ClusterEpoch == "" || len(open.ClusterEpoch) > controlstate.MaxIdentityBytes ||
		open.AuthorityID == "" || len(open.AuthorityID) > controlstate.MaxIdentityBytes ||
		open.AttemptID == "" || len(open.AttemptID) > controlstate.MaxIdentityBytes ||
		open.Binding.Generation == 0 || open.Binding.Ref == nil {
		return Reservation{}, fmt.Errorf("%w: incomplete open context", ErrInvalid)
	}
	if err := open.Auth.Validate(); err != nil {
		return Reservation{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := open.Binding.Ref.Validate(); err != nil {
		return Reservation{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if open.Auth.ClientSessionID != caller.ClientSessionID ||
		open.Auth.ClientID != caller.ClientID ||
		open.Auth.APIKeyID != caller.APIKeyID ||
		open.Auth.AuthRevision != caller.AuthRevision {
		return Reservation{}, fmt.Errorf("%w: caller does not match open context", ErrInvalid)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Reservation{}, ErrUnavailable
	}
	ref := *open.Binding.Ref
	if ref.GatewayID != m.gatewayID || ref.GatewayInstanceID != m.gatewayInstanceID {
		return Reservation{}, ErrNotFound
	}
	e := m.entries[ref.ListenerBindingID]
	if e == nil || e.state != StateLive || !sameExactSlot(open.Binding, e.slot) || e.endpoint == nil {
		return Reservation{}, ErrNotFound
	}
	if err := m.sessions.Require(caller); err != nil {
		return Reservation{}, fmt.Errorf("%w: caller: %w", ErrSessionEnded, err)
	}
	if err := m.sessions.Require(e.session); err != nil {
		m.retireLocked(e, true)
		return Reservation{}, fmt.Errorf("%w: listener: %w", ErrSessionEnded, err)
	}
	select {
	case <-e.done:
		m.retireLocked(e, true)
		return Reservation{}, ErrSessionEnded
	default:
	}
	if e.session == caller {
		return Reservation{}, fmt.Errorf("%w: caller and listener sessions must differ", ErrInvalid)
	}
	if !open.TryConsume() {
		return Reservation{}, ErrAttemptUsed
	}

	exact := cloneOpenContext(open)
	return Reservation{
		Context:      exact,
		Caller:       caller,
		Binding:      cloneSlot(exact.Binding),
		Listener:     e.session,
		Endpoint:     e.endpoint,
		ListenerDone: e.done,
	}, nil
}

// Unbind is deliberately non-disclosing: an unknown binding ID, a duplicate
// request, or an ID owned by another exact session are all successful no-ops.
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
	return m.retireMatchingLocked(func(e *entry) bool {
		return change.Removes(e.session.ClientID, e.session.APIKeyID)
	})
}

func (m *Manager) RetireAll() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retireMatchingLocked(func(*entry) bool { return true })
}

// ActiveCount reports occupied admission slots. RegisteringB, LiveB, and
// cleanup-pending retired entries retain a slot so blocked control work cannot
// create unbounded entries or goroutines.
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
	slot, err := m.committer.Install(m.ctx, e.key, e.ref)

	m.mu.Lock()
	if err != nil {
		m.recordFailedInstallLocked(e)
		m.mu.Unlock()
		result <- bindResult{err: fmt.Errorf("%w: install listener binding: %w", classifyInstallError(err), err)}
		m.notifyAborted(e)
		return
	}
	if !exactInstalledSlot(slot, e.key, e.ref) {
		m.recordFailedInstallLocked(e)
		m.mu.Unlock()
		result <- bindResult{err: fmt.Errorf("%w: committer returned a non-exact slot", ErrUnavailable)}
		m.notifyAborted(e)
		return
	}

	if e.state != StateRegistering || m.closed {
		m.startRemoveLocked(e, slot)
		m.mu.Unlock()
		result <- bindResult{err: ErrSessionEnded}
		return
	}
	select {
	case <-e.aborted:
		m.startRemoveLocked(e, slot)
		m.mu.Unlock()
		result <- bindResult{err: ErrSessionEnded}
		return
	default:
	}
	if err := m.sessions.Require(e.session); err != nil {
		m.retireLocked(e, true)
		m.startRemoveLocked(e, slot)
		m.mu.Unlock()
		result <- bindResult{err: fmt.Errorf("%w: %w", ErrSessionEnded, err)}
		return
	}

	e.slot = cloneSlot(slot)
	e.state = StateLive
	m.mu.Unlock()
	result <- bindResult{slot: cloneSlot(slot)}
}

func (m *Manager) recordFailedInstallLocked(e *entry) {
	if e.state == StateRegistering {
		m.retireLocked(e, false)
	}
	m.releaseCapacityLocked(e)
}

func classifyInstallError(err error) error {
	switch {
	case errors.Is(err, controlstate.ErrBindingCapacity), errors.Is(err, controlstate.ErrKeyLimit):
		return ErrCapacity
	case errors.Is(err, controlstate.ErrCASMismatch):
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

func (e *entry) canRetire() bool {
	return e.state == StateRegistering || e.state == StateLive
}

func (m *Manager) retireLocked(e *entry, notify bool) {
	switch e.state {
	case StateRegistering:
		e.state = StateRetired
		m.makeIneligibleLocked(e)
		m.recordRetiredLocked(e)
	case StateLive:
		e.state = StateRetiring
		m.makeIneligibleLocked(e)
		m.startRemoveLocked(e, e.slot)
	default:
		return
	}
	if notify && !e.abortedClosed {
		close(e.aborted)
		e.abortedClosed = true
	}
}

func (m *Manager) makeIneligibleLocked(e *entry) {
	if bindingID, ok := m.byKey[e.key]; ok && bindingID == e.ref.ListenerBindingID {
		delete(m.byKey, e.key)
	}
	if owned := m.bySession[e.session]; owned != nil {
		delete(owned, e.ref.ListenerBindingID)
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

func (m *Manager) startRemoveLocked(e *entry, slot controlstate.BindingSlot) {
	if !exactInstalledSlot(slot, e.key, e.ref) {
		return
	}
	exact := cloneSlot(slot)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		_ = m.committer.Remove(m.ctx, exact)
		m.mu.Lock()
		if e.state == StateRetiring {
			e.state = StateRetired
			m.recordRetiredLocked(e)
		}
		m.releaseCapacityLocked(e)
		m.mu.Unlock()
	}()
}

func (m *Manager) recordRetiredLocked(e *entry) {
	if e.terminalRecorded {
		return
	}
	e.terminalRecorded = true
	m.retiredOrder = append(m.retiredOrder, e.ref.ListenerBindingID)
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
	return Snapshot{
		Key:     e.key,
		Ref:     e.ref,
		Session: e.session,
		Slot:    cloneSlot(e.slot),
		State:   e.state,
	}
}

func exactInstalledSlot(slot controlstate.BindingSlot, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) bool {
	return slot.Key == key && slot.Generation > 0 && slot.Ref != nil && *slot.Ref == ref
}

func sameExactSlot(left, right controlstate.BindingSlot) bool {
	if left.Key != right.Key || left.Generation != right.Generation || left.Ref == nil || right.Ref == nil {
		return false
	}
	return *left.Ref == *right.Ref
}

func cloneSlot(slot controlstate.BindingSlot) controlstate.BindingSlot {
	copy := slot
	if slot.Ref != nil {
		ref := *slot.Ref
		copy.Ref = &ref
	}
	return copy
}

func cloneOpenContext(open authority.OpenContext) authority.OpenContext {
	return open.Clone()
}

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
