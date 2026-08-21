package relaygate

import (
	"context"
	"fmt"
)

func (m *ManagedClient) Bind(ctx context.Context, endpoint, target string) (*ManagedListener, error) {
	if m == nil {
		return nil, ErrManagedClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if !validEndpoint(endpoint) || !validIdentity(target) {
		return nil, fmt.Errorf("relaygate: invalid endpoint or target")
	}
	key := managedBindingKey{endpoint: endpoint, target: target}
	binding := &managedBinding{key: key, active: true}
	m.mu.Lock()
	if _, exists := m.bindings[key]; exists {
		m.mu.Unlock()
		return nil, ErrManagedBindingExists
	}
	if m.state == ManagedFailed {
		err := m.finalErr
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrManagedClosed
	}
	if m.state == ManagedClosed {
		m.mu.Unlock()
		return nil, ErrManagedClosed
	}
	if len(m.bindings) >= maxListeners {
		m.mu.Unlock()
		return nil, errCapacity
	}
	m.bindings[key] = binding
	m.signalLocked()
	m.mu.Unlock()

	listener := &ManagedListener{owner: m, binding: binding}
	if err := m.bindDeclaration(ctx, binding); err != nil {
		m.removeBinding(binding)
		return nil, err
	}
	return listener, nil
}

// Open performs exactly one Open on the current Ready session. It never waits
// in a reconnect queue and never retries on a later session.
func (l *ManagedListener) Endpoint() string { return l.binding.key.endpoint }
func (l *ManagedListener) Target() string   { return l.binding.key.target }

// Next waits across reconnects for the next Offer on the current underlying
// Listener. An Offer already returned to the caller remains session-bound.
func (l *ManagedListener) Next(ctx context.Context) (*Offer, error) {
	if l == nil || l.owner == nil || l.binding == nil {
		return nil, ErrListenerEnded
	}
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	var observed uint64
	for {
		raw, generation, err := l.owner.currentListener(ctx, l.binding, observed)
		if err != nil {
			return nil, err
		}
		offer, err := raw.Next(ctx)
		if err == nil {
			return offer, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		select {
		case <-raw.client.Done():
			observed = generation
			continue
		default:
			return nil, err
		}
	}
}

// Unbind removes the desired declaration before attempting current-session
// cleanup, so a reconnect cannot resurrect it.
func (l *ManagedListener) Unbind(ctx context.Context) error {
	if l == nil || l.owner == nil || l.binding == nil {
		return ErrListenerEnded
	}
	if ctx == nil {
		return fmt.Errorf("relaygate: context is required")
	}
	m := l.owner
	m.mu.Lock()
	if !l.binding.active || m.bindings[l.binding.key] != l.binding {
		m.mu.Unlock()
		return ErrListenerEnded
	}
	l.binding.active = false
	delete(m.bindings, l.binding.key)
	raw := l.binding.current
	l.binding.current = nil
	m.signalLocked()
	m.mu.Unlock()
	if raw == nil {
		return nil
	}
	return raw.Unbind(ctx)
}

func (m *ManagedClient) currentListener(ctx context.Context, binding *managedBinding, observed uint64) (*Listener, uint64, error) {
	for {
		m.mu.Lock()
		active := binding.active && m.bindings[binding.key] == binding
		raw, generation := binding.current, binding.generation
		state, changed, finalErr := m.state, m.changed, m.finalErr
		m.mu.Unlock()
		if !active {
			return nil, 0, ErrListenerEnded
		}
		if raw != nil && generation > observed {
			return raw, generation, nil
		}
		if state == ManagedFailed {
			if finalErr != nil {
				return nil, 0, finalErr
			}
			return nil, 0, ErrManagedClosed
		}
		if state == ManagedClosed {
			return nil, 0, ErrManagedClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-m.done:
			return nil, 0, ErrManagedClosed
		}
	}
}

func (m *ManagedClient) removeBinding(binding *managedBinding) {
	m.mu.Lock()
	if m.bindings[binding.key] == binding {
		binding.active = false
		binding.current = nil
		delete(m.bindings, binding.key)
		m.signalLocked()
	}
	m.mu.Unlock()
}

func (m *ManagedClient) bindDeclaration(ctx context.Context, binding *managedBinding) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		if !binding.active || m.bindings[binding.key] != binding {
			m.mu.Unlock()
			return ErrListenerEnded
		}
		if binding.current != nil && binding.generation == m.generation {
			m.mu.Unlock()
			return nil
		}
		client, generation := m.current, m.generation
		state, changed, finalErr := m.state, m.changed, m.finalErr
		m.mu.Unlock()

		switch state {
		case ManagedFailed:
			if finalErr != nil {
				return finalErr
			}
			return ErrManagedClosed
		case ManagedClosed:
			return ErrManagedClosed
		case ManagedReady:
			if client == nil {
				continue
			}
			raw, err := client.Bind(ctx, binding.key.endpoint, binding.key.target)
			if err != nil {
				select {
				case <-client.Done():
					continue
				default:
					return err
				}
			}
			m.mu.Lock()
			current := m.current == client && m.generation == generation && m.state == ManagedReady
			active := binding.active && m.bindings[binding.key] == binding
			if current && active {
				binding.current = raw
				binding.generation = generation
				m.signalLocked()
				m.mu.Unlock()
				return nil
			}
			m.mu.Unlock()
			if active {
				select {
				case <-client.Done():
				default:
					_ = raw.Unbind(ctx)
				}
			}
			continue
		}

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return ErrManagedClosed
		}
	}
}
