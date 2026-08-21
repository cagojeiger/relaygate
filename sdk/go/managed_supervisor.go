package relaygate

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *ManagedClient) run(ctx context.Context) {
	defer close(m.done)
	delay := managedInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			m.finish(ManagedClosed, nil)
			return
		}
		m.setState(ManagedConnecting, nil)
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, managedConnectTimeout)
		client, err := m.connect(attemptCtx, m.config)
		cancelAttempt()
		if err != nil {
			if isPermanentManagedConnectError(err) {
				m.finish(ManagedFailed, err)
				return
			}
			if !m.waitBackoff(ctx, delay, err) {
				m.finish(ManagedClosed, nil)
				return
			}
			delay = nextManagedBackoff(delay)
			continue
		}

		readyAt, err := m.installAndRebind(ctx, client)
		if err != nil {
			_ = client.Close()
			if isPermanentManagedConnectError(err) {
				m.finish(ManagedFailed, err)
				return
			}
			if !m.waitBackoff(ctx, delay, err) {
				m.finish(ManagedClosed, nil)
				return
			}
			delay = nextManagedBackoff(delay)
			continue
		}

		select {
		case <-client.Done():
			clientErr := client.Err()
			if errors.Is(clientErr, errProtocol) || isPermanentManagedConnectError(clientErr) {
				m.finish(ManagedFailed, clientErr)
				return
			}
			if m.now().Sub(readyAt) >= managedStableWindow {
				delay = managedInitialBackoff
			}
			m.detach(client)
			if !m.waitBackoff(ctx, delay, clientErr) {
				m.finish(ManagedClosed, nil)
				return
			}
			delay = nextManagedBackoff(delay)
		case <-ctx.Done():
			_ = client.Close()
			m.finish(ManagedClosed, nil)
			return
		}
	}
}

func (m *ManagedClient) installAndRebind(ctx context.Context, client *Client) (time.Time, error) {
	m.mu.Lock()
	m.current = client
	m.generation++
	generation := m.generation
	for _, binding := range m.bindings {
		binding.current = nil
	}
	m.state = ManagedRebinding
	m.finalErr = nil
	m.signalLocked()
	m.mu.Unlock()

	for {
		m.mu.Lock()
		pending := make([]*managedBinding, 0, len(m.bindings))
		for _, binding := range m.bindings {
			if binding.active && binding.generation != generation {
				pending = append(pending, binding)
			}
		}
		m.mu.Unlock()
		if len(pending) == 0 {
			m.mu.Lock()
			if m.current != client || m.generation != generation {
				m.mu.Unlock()
				return time.Time{}, ErrManagedNotReady
			}
			m.state = ManagedReady
			m.finalErr = nil
			m.signalLocked()
			m.mu.Unlock()
			return m.now(), nil
		}

		for _, binding := range pending {
			raw, err := client.Bind(ctx, binding.key.endpoint, binding.key.target)
			if err != nil {
				return time.Time{}, err
			}
			m.mu.Lock()
			current := m.current == client && m.generation == generation
			active := binding.active && m.bindings[binding.key] == binding
			if current && active {
				binding.current = raw
				binding.generation = generation
				m.signalLocked()
			}
			m.mu.Unlock()
			if !current || !active {
				_ = raw.Unbind(ctx)
			}
		}
	}
}

func (m *ManagedClient) detach(client *Client) {
	m.mu.Lock()
	if m.current == client {
		m.current = nil
		for _, binding := range m.bindings {
			binding.current = nil
		}
		m.state = ManagedBackoff
		m.finalErr = client.Err()
		m.signalLocked()
	}
	m.mu.Unlock()
}

func (m *ManagedClient) waitBackoff(ctx context.Context, delay time.Duration, cause error) bool {
	m.mu.Lock()
	if m.state != ManagedClosed && m.state != ManagedFailed {
		m.state = ManagedBackoff
		m.finalErr = cause
		m.signalLocked()
	}
	m.mu.Unlock()
	timer := time.NewTimer(m.backoff(delay))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *ManagedClient) setState(state ManagedState, err error) {
	m.mu.Lock()
	m.state = state
	m.finalErr = err
	m.signalLocked()
	m.mu.Unlock()
}

func (m *ManagedClient) finish(state ManagedState, err error) {
	m.mu.Lock()
	m.current = nil
	for _, binding := range m.bindings {
		binding.current = nil
	}
	m.state = state
	m.finalErr = err
	m.signalLocked()
	m.mu.Unlock()
}

func (m *ManagedClient) signalLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

func isPermanentManagedConnectError(err error) bool {
	if errors.Is(err, errProtocol) {
		return true
	}
	var bindErr *BindError
	if errors.As(err, &bindErr) {
		return bindErr.Failure != BindingFailureUnavailable
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

func nextManagedBackoff(delay time.Duration) time.Duration {
	if delay >= managedMaximumBackoff/2 {
		return managedMaximumBackoff
	}
	return delay * 2
}

func jitterBackoff(delay time.Duration) time.Duration {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return delay
	}
	const unitPrecision = uint64(1) << 53
	fraction := float64(binary.LittleEndian.Uint64(random[:])>>(64-53)) / float64(unitPrecision)
	factor := 1 - managedJitterRatio + fraction*(2*managedJitterRatio)
	result := time.Duration(float64(delay) * factor)
	if result < 0 {
		return delay
	}
	return result
}
