package controlstate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

const snapshotVersion = 2

// FSM keeps only the current gateway directory. It never records historical
// ownership, control sessions, relay addresses, payloads, or deleted routes.
type FSM struct {
	mu       sync.RWMutex
	state    State
	gateways map[string]GatewaySessionRef
	routes   map[BindingKey]Route
}

func NewFSM() *FSM {
	return &FSM{gateways: make(map[string]GatewaySessionRef), routes: make(map[BindingKey]Route)}
}

func (f *FSM) Apply(log *raft.Log) any {
	var envelope commandEnvelope
	if err := decodeStrict(log.Data, &envelope); err != nil {
		return rejected(fmt.Errorf("%w: decode envelope: %w", ErrInvalidCommand, err))
	}
	if envelope.Version != commandVersion {
		return rejected(fmt.Errorf("%w: unsupported command version %d", ErrInvalidCommand, envelope.Version))
	}

	switch envelope.Kind {
	case commandInitializeCluster:
		var command InitializeCluster
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode initialize_cluster: %w", ErrInvalidCommand, err))
		}
		if err := validateInitializeCluster(command); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		return f.applyInitializeCluster(command)
	case commandRegisterGateway:
		var command RegisterGateway
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode register_gateway: %w", ErrInvalidCommand, err))
		}
		if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		return f.applyRegisterGateway(command)
	case commandReplaceSnapshot:
		var command ReplaceSnapshot
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode replace_snapshot: %w", ErrInvalidCommand, err))
		}
		if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		return f.applyReplaceSnapshot(command)
	case commandDeclareRoute:
		var command DeclareRoute
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode declare_route: %w", ErrInvalidCommand, err))
		}
		if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		if err := validateBinding(command.Binding); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		return f.applyDeclareRoute(command)
	case commandWithdrawRoute:
		var command WithdrawRoute
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode withdraw_route: %w", ErrInvalidCommand, err))
		}
		if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		if err := validateBinding(command.Binding); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		return f.applyWithdrawRoute(command)
	case commandRemoveGateway:
		var command RemoveGateway
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode remove_gateway: %w", ErrInvalidCommand, err))
		}
		if err := validateEpochAndGateway(command.ClusterEpoch, command.Gateway); err != nil {
			return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
		}
		return f.applyRemoveGateway(command)
	default:
		return rejected(fmt.Errorf("%w: unsupported command kind %q", ErrInvalidCommand, envelope.Kind))
	}
}

func (f *FSM) applyInitializeCluster(command InitializeCluster) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state.ClusterEpoch == "" {
		f.state = State{
			ClusterEpoch:          command.ClusterEpoch,
			MaxGatewaySessions:    command.MaxGatewaySessions,
			MaxRoutes:             command.MaxRoutes,
			MaxBindingsPerGateway: command.MaxBindingsPerGateway,
		}
		return ApplyResult{Code: ResultApplied}
	}
	if f.state.ClusterEpoch == command.ClusterEpoch &&
		f.state.MaxGatewaySessions == command.MaxGatewaySessions &&
		f.state.MaxRoutes == command.MaxRoutes &&
		f.state.MaxBindingsPerGateway == command.MaxBindingsPerGateway {
		return ApplyResult{Code: ResultAlreadyApplied}
	}
	return rejected(fmt.Errorf("%w: initialized cluster differs from requested configuration", ErrEpochMismatch))
}

func (f *FSM) applyRegisterGateway(command RegisterGateway) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if result := f.requireEpochLocked(command.ClusterEpoch); result != nil {
		return *result
	}
	current, exists := f.gateways[command.Gateway.GatewayID]
	if exists && current == command.Gateway {
		return ApplyResult{Code: ResultAlreadyApplied}
	}
	if !exists && uint64(len(f.gateways)) >= uint64(f.state.MaxGatewaySessions) {
		return capacity("gateway session capacity reached")
	}
	// Replacing an instance is a single FSM transition: stale routes cannot
	// remain visible between the replacement and its full snapshot.
	if exists {
		f.deleteGatewayRoutesLocked(current)
	}
	f.gateways[command.Gateway.GatewayID] = command.Gateway
	return ApplyResult{Code: ResultApplied}
}

func (f *FSM) applyReplaceSnapshot(command ReplaceSnapshot) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if result := f.requireCurrentGatewayLocked(command.ClusterEpoch, command.Gateway); result != nil {
		return *result
	}
	if uint64(len(command.Bindings)) > uint64(f.state.MaxBindingsPerGateway) {
		return capacity("gateway route capacity reached")
	}
	if err := validateBindings(command.Bindings, MaxListenerBindingsPerGateway); err != nil {
		return rejected(fmt.Errorf("%w: %w", ErrInvalidCommand, err))
	}
	candidate := make(map[BindingKey]Route, len(command.Bindings))
	for _, binding := range command.Bindings {
		candidate[binding.Key] = Route{Key: binding.Key, Owner: command.Gateway, ListenerBindingID: binding.ListenerBindingID}
	}
	owned := f.routeCountForGatewayLocked(command.Gateway)
	if uint64(len(f.routes))-owned+uint64(len(candidate)) > uint64(f.state.MaxRoutes) {
		return capacity("route capacity reached")
	}
	for key, route := range candidate {
		if existing, exists := f.routes[key]; exists && existing.Owner != command.Gateway {
			return conflict("route key is owned by another gateway")
		}
		if route.Owner != command.Gateway { // defensive: candidate construction above guarantees this.
			return rejected(fmt.Errorf("%w: snapshot route owner differs from gateway", ErrInvalidCommand))
		}
	}
	f.deleteGatewayRoutesLocked(command.Gateway)
	for key, route := range candidate {
		f.routes[key] = route
	}
	return ApplyResult{Code: ResultApplied}
}

func (f *FSM) applyDeclareRoute(command DeclareRoute) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if result := f.requireCurrentGatewayLocked(command.ClusterEpoch, command.Gateway); result != nil {
		return *result
	}
	route := Route{Key: command.Binding.Key, Owner: command.Gateway, ListenerBindingID: command.Binding.ListenerBindingID}
	if existing, exists := f.routes[route.Key]; exists {
		if existing == route {
			return ApplyResult{Code: ResultAlreadyApplied}
		}
		return conflict("route key is owned or referenced differently")
	}
	if uint64(len(f.routes)) >= uint64(f.state.MaxRoutes) || f.routeCountForGatewayLocked(command.Gateway) >= uint64(f.state.MaxBindingsPerGateway) {
		return capacity("route capacity reached")
	}
	f.routes[route.Key] = route
	return ApplyResult{Code: ResultApplied}
}

func (f *FSM) applyWithdrawRoute(command WithdrawRoute) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if result := f.requireCurrentGatewayLocked(command.ClusterEpoch, command.Gateway); result != nil {
		return *result
	}
	existing, exists := f.routes[command.Binding.Key]
	if !exists {
		return ApplyResult{Code: ResultAlreadyApplied}
	}
	if existing.Owner != command.Gateway || existing.ListenerBindingID != command.Binding.ListenerBindingID {
		return conflict("route owner or listener binding differs")
	}
	delete(f.routes, command.Binding.Key)
	return ApplyResult{Code: ResultApplied}
}

func (f *FSM) applyRemoveGateway(command RemoveGateway) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if result := f.requireEpochLocked(command.ClusterEpoch); result != nil {
		return *result
	}
	current, exists := f.gateways[command.Gateway.GatewayID]
	if !exists || current != command.Gateway {
		return ApplyResult{Code: ResultAlreadyApplied}
	}
	f.deleteGatewayRoutesLocked(command.Gateway)
	delete(f.gateways, command.Gateway.GatewayID)
	return ApplyResult{Code: ResultApplied}
}

func (f *FSM) requireEpochLocked(epoch string) *ApplyResult {
	if f.state.ClusterEpoch == "" {
		result := rejected(ErrNotInitialized)
		return &result
	}
	if f.state.ClusterEpoch != epoch {
		result := rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.state.ClusterEpoch, epoch))
		return &result
	}
	return nil
}

func (f *FSM) requireCurrentGatewayLocked(epoch string, gateway GatewaySessionRef) *ApplyResult {
	if result := f.requireEpochLocked(epoch); result != nil {
		return result
	}
	if current, exists := f.gateways[gateway.GatewayID]; !exists || current != gateway {
		result := rejected(fmt.Errorf("%w: gateway_id=%q", ErrStaleGateway, gateway.GatewayID))
		return &result
	}
	return nil
}

func (f *FSM) deleteGatewayRoutesLocked(gateway GatewaySessionRef) {
	for key, route := range f.routes {
		if route.Owner == gateway {
			delete(f.routes, key)
		}
	}
}

func (f *FSM) routeCountForGatewayLocked(gateway GatewaySessionRef) uint64 {
	var count uint64
	for _, route := range f.routes {
		if route.Owner == gateway {
			count++
		}
	}
	return count
}

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

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func rejected(err error) ApplyResult      { return ApplyResult{Code: ResultRejected, Error: err.Error()} }
func conflict(message string) ApplyResult { return ApplyResult{Code: ResultConflict, Error: message} }
func capacity(message string) ApplyResult { return ApplyResult{Code: ResultCapacity, Error: message} }
