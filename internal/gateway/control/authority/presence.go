package authority

import controlstate "github.com/cagojeiger/relaygate/internal/raft/state"

func (m *Manager) Presence() Presence {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.presenceLocked(m.node.State())
}

func (m *Manager) presenceLocked(committed controlstate.State) Presence {
	presence := Presence{
		CommittedGateways: len(committed.Gateways),
		CommittedRoutes:   len(committed.Routes),
	}
	if m.current == nil {
		presence.State = PresenceNoAuthority
		return presence
	}
	presence.State = PresenceCurrent
	for _, entry := range m.sessions {
		if entry.closed || entry.state != SessionRevalidated || !m.isCurrentGatewayLocked(entry.ref) {
			continue
		}
		presence.RevalidatedGateways++
	}
	for _, route := range committed.Routes {
		entry := m.sessions[route.Owner.GatewayID]
		if entry == nil || entry.closed || entry.state != SessionRevalidated || gatewayRef(entry.ref) != route.Owner {
			continue
		}
		if entry.bindings[routingKey(route.Key)] == routeToBinding(route) {
			presence.EligibleRoutes++
		}
	}
	return presence
}
