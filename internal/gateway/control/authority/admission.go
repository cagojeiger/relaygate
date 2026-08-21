package authority

import (
	"context"
	"fmt"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

func (m *Manager) AdmitOpen(ctx context.Context, ingress controlmodel.SessionRef, auth routing.AuthContext, endpoint, targetID string) (routing.OpenContext, error) {
	key, err := routing.ExactBindingKey(auth, endpoint, targetID)
	if err != nil {
		return routing.OpenContext{}, err
	}
	current, err := m.Confirm(ctx)
	if err != nil {
		return routing.OpenContext{}, err
	}
	return m.resolveOpen(current, ingress, auth, key)
}

// resolveOpen requires an exact committed route plus exact, revalidated local
// ingress and owner sessions from the already confirmed authority. The route
// itself carries no leader-local address or control-session identifier.
func (m *Manager) resolveOpen(current controlmodel.AuthorityRef, ingress controlmodel.SessionRef, auth routing.AuthContext, key routing.BindingKey) (routing.OpenContext, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil || *m.current != current {
		return routing.OpenContext{}, fmt.Errorf("%w: %w", routing.ErrOpenUnavailable, ErrNoAuthority)
	}
	if ingress.ClusterEpoch != current.ClusterEpoch || ingress.AuthorityID != current.AuthorityID {
		return routing.OpenContext{}, fmt.Errorf("%w: ingress control session belongs to a stale authority", routing.ErrOpenUnavailable)
	}
	ingressEntry, err := m.sessionLocked(ingress)
	if err != nil || ingressEntry.state != SessionRevalidated || !m.isCurrentGatewayLocked(ingress) {
		return routing.OpenContext{}, fmt.Errorf("%w: ingress control session", routing.ErrOpenUnavailable)
	}

	stateKey := controlstate.BindingKey{ClientID: key.ClientID, EndpointPattern: key.EndpointPattern, TargetID: key.TargetID}
	route, ok := m.node.LookupRoute(stateKey)
	if !ok {
		return routing.OpenContext{}, routing.ErrRouteNotFound
	}
	if currentGateway, ok := m.node.LookupGateway(route.Owner.GatewayID); !ok || currentGateway != route.Owner {
		return routing.OpenContext{}, routing.ErrRouteNotFound
	}
	binding := routeToBinding(route)
	owner := m.sessions[route.Owner.GatewayID]
	if owner == nil || owner.closed || owner.state != SessionRevalidated || gatewayRef(owner.ref) != route.Owner || owner.bindings[key] != binding {
		return routing.OpenContext{}, routing.ErrRouteNotFound
	}
	attemptID, err := newID()
	if err != nil {
		return routing.OpenContext{}, fmt.Errorf("%w: %w", routing.ErrOpenUnavailable, err)
	}
	return routing.NewForwardedOpenContext(
		current.ClusterEpoch, current.AuthorityID, attemptID, auth, binding,
		routing.ForwardingContext{
			IngressGatewayID:         ingressEntry.ref.GatewayID,
			IngressGatewayInstanceID: ingressEntry.ref.GatewayInstanceID,
			IngressControlSessionID:  ingressEntry.ref.ControlSessionID,
			OwnerControlSessionID:    owner.ref.ControlSessionID,
			OwnerRelayAddress:        owner.relayAddress,
			ExpiresAt:                m.now().Add(m.config.OpenContextTTL),
		},
	)
}

// EndSession only drops V. The committed C record is retained through the
// revalidation grace period so an ordinary control reconnect does not erase a
// healthy gateway's current directory. The sweeper later performs an exact,
// conditional RemoveGateway if it has not returned.
