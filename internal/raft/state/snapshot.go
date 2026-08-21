package controlstate

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/hashicorp/raft"
)

const snapshotVersion = 2

func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stateLocked()
}

func (f *FSM) ClusterEpoch() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.ClusterEpoch
}

func (f *FSM) LookupGateway(gatewayID string) (GatewaySessionRef, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	value, ok := f.gateways[gatewayID]
	return value, ok
}

func (f *FSM) LookupRoute(key BindingKey) (Route, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	value, ok := f.routes[key]
	return value, ok
}

func (f *FSM) stateLocked() State {
	state := State{
		ClusterEpoch:          f.state.ClusterEpoch,
		MaxGatewaySessions:    f.state.MaxGatewaySessions,
		MaxRoutes:             f.state.MaxRoutes,
		MaxBindingsPerGateway: f.state.MaxBindingsPerGateway,
		Gateways:              make([]GatewaySessionRef, 0, len(f.gateways)),
		Routes:                make([]Route, 0, len(f.routes)),
	}
	for _, gateway := range f.gateways {
		state.Gateways = append(state.Gateways, gateway)
	}
	for _, route := range f.routes {
		state.Routes = append(state.Routes, route)
	}
	sortGateways(state.Gateways)
	sortRoutes(state.Routes)
	return state
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &snapshot{state: f.stateLocked()}, nil
}

func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	var envelope snapshotEnvelope
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("decode raft current-state snapshot: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return fmt.Errorf("decode raft current-state snapshot: %w", err)
	}
	if envelope.Version != snapshotVersion {
		return fmt.Errorf("unsupported raft current-state snapshot version %d", envelope.Version)
	}
	gateways, routes, err := validateAndIndexState(envelope.State)
	if err != nil {
		return fmt.Errorf("validate raft current-state snapshot: %w", err)
	}
	f.mu.Lock()
	f.state = copyState(envelope.State)
	f.gateways = gateways
	f.routes = routes
	f.mu.Unlock()
	return nil
}

type snapshotEnvelope struct {
	Version uint8 `json:"version"`
	State   State `json:"state"`
}

type snapshot struct{ state State }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(snapshotEnvelope{Version: snapshotVersion, State: s.state}); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("encode raft current-state snapshot: %w", err)
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("close raft current-state snapshot: %w", err)
	}
	return nil
}

func (s *snapshot) Release() {}

func validateAndIndexState(state State) (map[string]GatewaySessionRef, map[BindingKey]Route, error) {
	if err := validateInitializeCluster(InitializeCluster{
		ClusterEpoch: state.ClusterEpoch, MaxGatewaySessions: state.MaxGatewaySessions,
		MaxRoutes: state.MaxRoutes, MaxBindingsPerGateway: state.MaxBindingsPerGateway,
	}); err != nil {
		return nil, nil, err
	}
	if uint64(len(state.Gateways)) > uint64(state.MaxGatewaySessions) || uint64(len(state.Routes)) > uint64(state.MaxRoutes) {
		return nil, nil, fmt.Errorf("snapshot exceeds configured live capacity")
	}
	gateways := make(map[string]GatewaySessionRef, len(state.Gateways))
	for index, gateway := range state.Gateways {
		if err := validateGateway(gateway); err != nil {
			return nil, nil, err
		}
		if index > 0 && !gatewayLess(state.Gateways[index-1], gateway) {
			return nil, nil, fmt.Errorf("gateways are not strictly sorted")
		}
		if _, exists := gateways[gateway.GatewayID]; exists {
			return nil, nil, fmt.Errorf("duplicate gateway_id %q", gateway.GatewayID)
		}
		gateways[gateway.GatewayID] = gateway
	}
	routes := make(map[BindingKey]Route, len(state.Routes))
	counts := make(map[GatewaySessionRef]uint32, len(state.Gateways))
	for index, route := range state.Routes {
		if err := validateRoute(route); err != nil {
			return nil, nil, err
		}
		if index > 0 && !routeLess(state.Routes[index-1], route) {
			return nil, nil, fmt.Errorf("routes are not strictly sorted")
		}
		owner, exists := gateways[route.Owner.GatewayID]
		if !exists || owner != route.Owner {
			return nil, nil, fmt.Errorf("route owner is not a current gateway")
		}
		if _, exists := routes[route.Key]; exists {
			return nil, nil, fmt.Errorf("duplicate route key")
		}
		counts[route.Owner]++
		if counts[route.Owner] > state.MaxBindingsPerGateway {
			return nil, nil, fmt.Errorf("gateway route count exceeds configured capacity")
		}
		routes[route.Key] = route
	}
	return gateways, routes, nil
}

func validateRoute(route Route) error {
	if err := validateBindingKey(route.Key); err != nil {
		return err
	}
	if err := validateGateway(route.Owner); err != nil {
		return err
	}
	return validateIdentity("listener_binding_id", route.ListenerBindingID)
}

func copyState(state State) State {
	copy := state
	copy.Gateways = append([]GatewaySessionRef(nil), state.Gateways...)
	copy.Routes = append([]Route(nil), state.Routes...)
	return copy
}

func sortGateways(gateways []GatewaySessionRef) {
	sort.Slice(gateways, func(i, j int) bool { return gatewayLess(gateways[i], gateways[j]) })
}

func gatewayLess(left, right GatewaySessionRef) bool {
	if left.GatewayID != right.GatewayID {
		return left.GatewayID < right.GatewayID
	}
	return left.GatewayInstanceID < right.GatewayInstanceID
}

func sortRoutes(routes []Route) {
	sort.Slice(routes, func(i, j int) bool { return routeLess(routes[i], routes[j]) })
}

func routeLess(left, right Route) bool {
	if left.Key.ClientID != right.Key.ClientID {
		return left.Key.ClientID < right.Key.ClientID
	}
	if left.Key.EndpointPattern != right.Key.EndpointPattern {
		return left.Key.EndpointPattern < right.Key.EndpointPattern
	}
	if left.Key.TargetID != right.Key.TargetID {
		return left.Key.TargetID < right.Key.TargetID
	}
	if left.Owner.GatewayID != right.Owner.GatewayID {
		return left.Owner.GatewayID < right.Owner.GatewayID
	}
	if left.Owner.GatewayInstanceID != right.Owner.GatewayInstanceID {
		return left.Owner.GatewayInstanceID < right.Owner.GatewayInstanceID
	}
	return left.ListenerBindingID < right.ListenerBindingID
}
