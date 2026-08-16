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

const snapshotVersion = 1

type FSM struct {
	mu sync.RWMutex

	clusterEpoch                   string
	maxDistinctBindingKeysPerEpoch uint64
	maxDistinctGatewayIDsPerEpoch  uint64
	bindings                       map[BindingKey]BindingSlot
	gateways                       map[string]GatewaySlot
}

func NewFSM() *FSM {
	return &FSM{
		bindings: make(map[BindingKey]BindingSlot),
		gateways: make(map[string]GatewaySlot),
	}
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
	case commandInitializeEpoch:
		var command InitializeEpoch
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode initialize_epoch: %w", ErrInvalidCommand, err))
		}
		if command.ClusterEpoch == "" || command.MaxDistinctBindingKeysPerEpoch == 0 {
			return rejected(fmt.Errorf("%w: invalid epoch initialization", ErrInvalidCommand))
		}
		return f.applyInitializeEpoch(command)

	case commandInstallBinding:
		var command InstallBinding
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode install_binding: %w", ErrInvalidCommand, err))
		}
		if err := validateInstall(command); err != nil {
			return rejected(err)
		}
		return f.applyInstall(command)

	case commandRemoveBinding:
		var command RemoveBinding
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode remove_binding: %w", ErrInvalidCommand, err))
		}
		if err := validateRemove(command); err != nil {
			return rejected(err)
		}
		return f.applyRemove(command)

	case commandRegisterGateway:
		var command RegisterGateway
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode register_gateway: %w", ErrInvalidCommand, err))
		}
		if err := validateRegisterGateway(command); err != nil {
			return rejected(err)
		}
		return f.applyRegisterGateway(command)

	case commandRemoveGateway:
		var command RemoveGateway
		if err := decodeStrict(envelope.Payload, &command); err != nil {
			return rejected(fmt.Errorf("%w: decode remove_gateway: %w", ErrInvalidCommand, err))
		}
		if err := validateRemoveGateway(command); err != nil {
			return rejected(err)
		}
		return f.applyRemoveGateway(command)

	default:
		return rejected(fmt.Errorf("%w: unsupported command kind %q", ErrInvalidCommand, envelope.Kind))
	}
}

func (f *FSM) applyInitializeEpoch(command InitializeEpoch) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if command.MaxDistinctGatewayIDsPerEpoch == 0 {
		command.MaxDistinctGatewayIDsPerEpoch = DefaultMaxDistinctGatewayIDsPerEpoch
	}
	if f.clusterEpoch == "" {
		f.clusterEpoch = command.ClusterEpoch
		f.maxDistinctBindingKeysPerEpoch = command.MaxDistinctBindingKeysPerEpoch
		f.maxDistinctGatewayIDsPerEpoch = command.MaxDistinctGatewayIDsPerEpoch
		return ApplyResult{Code: ResultApplied}
	}
	if f.clusterEpoch == command.ClusterEpoch &&
		f.maxDistinctBindingKeysPerEpoch == command.MaxDistinctBindingKeysPerEpoch &&
		f.maxDistinctGatewayIDsPerEpoch == command.MaxDistinctGatewayIDsPerEpoch {
		return ApplyResult{Code: ResultAlreadyApplied}
	}
	return rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.clusterEpoch, command.ClusterEpoch))
}

func (f *FSM) applyRegisterGateway(command RegisterGateway) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if command.ClusterEpoch != f.clusterEpoch || f.clusterEpoch == "" {
		return rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.clusterEpoch, command.ClusterEpoch))
	}
	current, exists := f.gateways[command.GatewayID]
	if !exists {
		current = GatewaySlot{GatewayID: command.GatewayID}
	}
	targetGeneration := command.ExpectedGeneration + 1
	if current.Generation == targetGeneration && gatewayRefsEqual(current.Ref, &command.NewRef) {
		copy := cloneGatewaySlot(current)
		return ApplyResult{Code: ResultAlreadyApplied, GatewaySlot: &copy}
	}
	if current.Generation != command.ExpectedGeneration || !gatewayRefsEqual(current.Ref, command.ExpectedRef) {
		return rejectedGatewaySlot(fmt.Errorf("%w: generation or gateway ref differs", ErrCASMismatch), current)
	}
	if !exists && uint64(len(f.gateways)) >= f.maxDistinctGatewayIDsPerEpoch {
		return rejected(ErrGatewayLimit)
	}
	ref := command.NewRef
	updated := GatewaySlot{GatewayID: command.GatewayID, Generation: targetGeneration, Ref: &ref}
	f.gateways[command.GatewayID] = updated
	copy := cloneGatewaySlot(updated)
	return ApplyResult{Code: ResultApplied, GatewaySlot: &copy}
}

func (f *FSM) applyRemoveGateway(command RemoveGateway) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if command.ClusterEpoch != f.clusterEpoch || f.clusterEpoch == "" {
		return rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.clusterEpoch, command.ClusterEpoch))
	}
	current, exists := f.gateways[command.GatewayID]
	targetGeneration := command.ExpectedGeneration + 1
	if exists && current.Generation == targetGeneration && current.Ref == nil {
		copy := cloneGatewaySlot(current)
		return ApplyResult{Code: ResultAlreadyApplied, GatewaySlot: &copy}
	}
	if !exists || current.Generation != command.ExpectedGeneration || !gatewayRefsEqual(current.Ref, &command.ExpectedRef) {
		return rejectedGatewaySlot(fmt.Errorf("%w: generation or gateway ref differs", ErrCASMismatch), current)
	}
	updated := GatewaySlot{GatewayID: command.GatewayID, Generation: targetGeneration}
	f.gateways[command.GatewayID] = updated
	copy := cloneGatewaySlot(updated)
	return ApplyResult{Code: ResultApplied, GatewaySlot: &copy}
}

func (f *FSM) applyInstall(command InstallBinding) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if command.ClusterEpoch != f.clusterEpoch || f.clusterEpoch == "" {
		return rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.clusterEpoch, command.ClusterEpoch))
	}

	current, exists := f.bindings[command.Key]
	if !exists {
		current = BindingSlot{Key: command.Key, Generation: 0}
	}
	targetGeneration := command.ExpectedGeneration + 1
	if current.Generation == targetGeneration && refsEqual(current.Ref, &command.NewRef) {
		copy := cloneSlot(current)
		return ApplyResult{Code: ResultAlreadyApplied, Slot: &copy}
	}
	if current.Generation != command.ExpectedGeneration || !refsEqual(current.Ref, command.ExpectedRef) {
		return rejectedBindingSlot(fmt.Errorf("%w: generation or ref differs", ErrCASMismatch), current)
	}
	if !exists && uint64(len(f.bindings)) >= f.maxDistinctBindingKeysPerEpoch {
		return rejected(ErrKeyLimit)
	}
	if !sameBindingOwner(current.Ref, &command.NewRef) && f.liveBindingCount(command.NewRef.GatewayID, command.NewRef.GatewayInstanceID) >= MaxListenerBindingsPerGateway {
		return capacityBindingSlot(current)
	}

	ref := command.NewRef
	updated := BindingSlot{
		Key:        command.Key,
		Generation: targetGeneration,
		Ref:        &ref,
	}
	f.bindings[command.Key] = updated
	copy := cloneSlot(updated)
	return ApplyResult{Code: ResultApplied, Slot: &copy}
}

func (f *FSM) liveBindingCount(gatewayID, gatewayInstanceID string) int {
	count := 0
	for _, slot := range f.bindings {
		if slot.Ref != nil && slot.Ref.GatewayID == gatewayID && slot.Ref.GatewayInstanceID == gatewayInstanceID {
			count++
		}
	}
	return count
}

func (f *FSM) applyRemove(command RemoveBinding) ApplyResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	if command.ClusterEpoch != f.clusterEpoch || f.clusterEpoch == "" {
		return rejected(fmt.Errorf("%w: current=%q requested=%q", ErrEpochMismatch, f.clusterEpoch, command.ClusterEpoch))
	}

	current, exists := f.bindings[command.Key]
	targetGeneration := command.ExpectedGeneration + 1
	if exists && current.Generation == targetGeneration && current.Ref == nil {
		copy := cloneSlot(current)
		return ApplyResult{Code: ResultAlreadyApplied, Slot: &copy}
	}
	if !exists || current.Generation != command.ExpectedGeneration || !refsEqual(current.Ref, &command.ExpectedRef) {
		return rejectedBindingSlot(fmt.Errorf("%w: generation or ref differs", ErrCASMismatch), current)
	}

	updated := BindingSlot{
		Key:        command.Key,
		Generation: targetGeneration,
	}
	f.bindings[command.Key] = updated
	copy := cloneSlot(updated)
	return ApplyResult{Code: ResultApplied, Slot: &copy}
}

func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.stateLocked()
}

func (f *FSM) Lookup(key BindingKey) BindingSlot {
	f.mu.RLock()
	defer f.mu.RUnlock()

	slot, ok := f.bindings[key]
	if !ok {
		return BindingSlot{Key: key}
	}
	return cloneSlot(slot)
}

func (f *FSM) LookupGateway(gatewayID string) GatewaySlot {
	f.mu.RLock()
	defer f.mu.RUnlock()

	slot, ok := f.gateways[gatewayID]
	if !ok {
		return GatewaySlot{GatewayID: gatewayID}
	}
	return cloneGatewaySlot(slot)
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
		return fmt.Errorf("decode control snapshot: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return fmt.Errorf("decode control snapshot: %w", err)
	}
	if envelope.Version != snapshotVersion {
		return fmt.Errorf("unsupported control snapshot version %d", envelope.Version)
	}
	if envelope.State.ClusterEpoch != "" && envelope.State.MaxDistinctGatewayIDsPerEpoch == 0 {
		envelope.State.MaxDistinctGatewayIDsPerEpoch = DefaultMaxDistinctGatewayIDsPerEpoch
	}
	if err := validateState(envelope.State); err != nil {
		return fmt.Errorf("validate control snapshot: %w", err)
	}

	bindings := make(map[BindingKey]BindingSlot, len(envelope.State.Bindings))
	for _, slot := range envelope.State.Bindings {
		bindings[slot.Key] = cloneSlot(slot)
	}
	gateways := make(map[string]GatewaySlot, len(envelope.State.Gateways))
	for _, slot := range envelope.State.Gateways {
		gateways[slot.GatewayID] = cloneGatewaySlot(slot)
	}

	f.mu.Lock()
	f.clusterEpoch = envelope.State.ClusterEpoch
	f.maxDistinctBindingKeysPerEpoch = envelope.State.MaxDistinctBindingKeysPerEpoch
	f.maxDistinctGatewayIDsPerEpoch = envelope.State.MaxDistinctGatewayIDsPerEpoch
	f.bindings = bindings
	f.gateways = gateways
	f.mu.Unlock()
	return nil
}

func (f *FSM) stateLocked() State {
	bindings := make([]BindingSlot, 0, len(f.bindings))
	for _, slot := range f.bindings {
		bindings = append(bindings, cloneSlot(slot))
	}
	sort.Slice(bindings, func(i, j int) bool {
		left := bindings[i].Key
		right := bindings[j].Key
		if left.ClientID != right.ClientID {
			return left.ClientID < right.ClientID
		}
		if left.EndpointPattern != right.EndpointPattern {
			return left.EndpointPattern < right.EndpointPattern
		}
		return left.TargetID < right.TargetID
	})
	gateways := make([]GatewaySlot, 0, len(f.gateways))
	for _, slot := range f.gateways {
		gateways = append(gateways, cloneGatewaySlot(slot))
	}
	sort.Slice(gateways, func(i, j int) bool {
		return gateways[i].GatewayID < gateways[j].GatewayID
	})
	return State{
		ClusterEpoch:                   f.clusterEpoch,
		MaxDistinctBindingKeysPerEpoch: f.maxDistinctBindingKeysPerEpoch,
		MaxDistinctGatewayIDsPerEpoch:  f.maxDistinctGatewayIDsPerEpoch,
		Bindings:                       bindings,
		Gateways:                       gateways,
	}
}

type snapshotEnvelope struct {
	Version uint8 `json:"version"`
	State   State `json:"state"`
}

type snapshot struct {
	state State
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(snapshotEnvelope{Version: snapshotVersion, State: s.state}); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("encode control snapshot: %w", err)
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("close control snapshot: %w", err)
	}
	return nil
}

func (s *snapshot) Release() {}

func validateInstall(command InstallBinding) error {
	if command.ClusterEpoch == "" {
		return fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if err := command.Key.Validate(); err != nil {
		return err
	}
	if command.ExpectedRef != nil {
		if err := command.ExpectedRef.Validate(); err != nil {
			return err
		}
	}
	if err := command.NewRef.Validate(); err != nil {
		return err
	}
	if command.ExpectedGeneration == ^uint64(0) {
		return fmt.Errorf("%w: generation overflow", ErrInvalidCommand)
	}
	return nil
}

func validateRemove(command RemoveBinding) error {
	if command.ClusterEpoch == "" {
		return fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if err := command.Key.Validate(); err != nil {
		return err
	}
	if err := command.ExpectedRef.Validate(); err != nil {
		return err
	}
	if command.ExpectedGeneration == ^uint64(0) {
		return fmt.Errorf("%w: generation overflow", ErrInvalidCommand)
	}
	return nil
}

func validateRegisterGateway(command RegisterGateway) error {
	if command.ClusterEpoch == "" {
		return fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if err := validateIdentity("gateway_id", command.GatewayID); err != nil {
		return err
	}
	if command.ExpectedRef != nil {
		if err := command.ExpectedRef.Validate(); err != nil {
			return err
		}
	}
	if err := command.NewRef.Validate(); err != nil {
		return err
	}
	if command.ExpectedGeneration == ^uint64(0) {
		return fmt.Errorf("%w: generation overflow", ErrInvalidCommand)
	}
	return nil
}

func validateRemoveGateway(command RemoveGateway) error {
	if command.ClusterEpoch == "" {
		return fmt.Errorf("%w: cluster_epoch is required", ErrInvalidCommand)
	}
	if err := validateIdentity("gateway_id", command.GatewayID); err != nil {
		return err
	}
	if err := command.ExpectedRef.Validate(); err != nil {
		return err
	}
	if command.ExpectedGeneration == ^uint64(0) {
		return fmt.Errorf("%w: generation overflow", ErrInvalidCommand)
	}
	return nil
}

func validateState(state State) error {
	if state.ClusterEpoch == "" {
		if state.MaxDistinctBindingKeysPerEpoch != 0 || state.MaxDistinctGatewayIDsPerEpoch != 0 || len(state.Bindings) != 0 || len(state.Gateways) != 0 {
			return fmt.Errorf("empty epoch has control state")
		}
		return nil
	}
	if state.MaxDistinctBindingKeysPerEpoch == 0 {
		return fmt.Errorf("initialized epoch has zero key limit")
	}
	if state.MaxDistinctGatewayIDsPerEpoch == 0 {
		return fmt.Errorf("initialized epoch has zero gateway ID limit")
	}
	if uint64(len(state.Bindings)) > state.MaxDistinctBindingKeysPerEpoch {
		return fmt.Errorf("binding count exceeds key limit")
	}
	seen := make(map[BindingKey]struct{}, len(state.Bindings))
	ownerCounts := make(map[[2]string]int)
	for _, slot := range state.Bindings {
		if err := slot.Key.Validate(); err != nil {
			return err
		}
		if slot.Generation == 0 {
			return fmt.Errorf("persisted binding has implicit generation zero")
		}
		if slot.Ref != nil {
			if err := slot.Ref.Validate(); err != nil {
				return err
			}
			owner := [2]string{slot.Ref.GatewayID, slot.Ref.GatewayInstanceID}
			ownerCounts[owner]++
			if ownerCounts[owner] > MaxListenerBindingsPerGateway {
				return fmt.Errorf("gateway instance binding count exceeds protocol limit")
			}
		}
		if _, ok := seen[slot.Key]; ok {
			return fmt.Errorf("duplicate binding key")
		}
		seen[slot.Key] = struct{}{}
	}
	if uint64(len(state.Gateways)) > state.MaxDistinctGatewayIDsPerEpoch {
		return fmt.Errorf("gateway count exceeds gateway ID limit")
	}
	seenGateways := make(map[string]struct{}, len(state.Gateways))
	for _, slot := range state.Gateways {
		if err := validateIdentity("gateway_id", slot.GatewayID); err != nil {
			return err
		}
		if slot.Generation == 0 {
			return fmt.Errorf("persisted gateway has implicit generation zero")
		}
		if slot.Ref != nil {
			if err := slot.Ref.Validate(); err != nil {
				return err
			}
		}
		if _, ok := seenGateways[slot.GatewayID]; ok {
			return fmt.Errorf("duplicate gateway ID")
		}
		seenGateways[slot.GatewayID] = struct{}{}
	}
	return nil
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

func refsEqual(left, right *ListenerBindingRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameBindingOwner(left, right *ListenerBindingRef) bool {
	return left != nil && right != nil && left.GatewayID == right.GatewayID && left.GatewayInstanceID == right.GatewayInstanceID
}

func cloneSlot(slot BindingSlot) BindingSlot {
	copy := slot
	if slot.Ref != nil {
		ref := *slot.Ref
		copy.Ref = &ref
	}
	return copy
}

func gatewayRefsEqual(left, right *GatewayRegistrationRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneGatewaySlot(slot GatewaySlot) GatewaySlot {
	copy := slot
	if slot.Ref != nil {
		ref := *slot.Ref
		copy.Ref = &ref
	}
	return copy
}

func rejected(err error) ApplyResult {
	return ApplyResult{Code: ResultRejected, Error: err.Error()}
}

func rejectedBindingSlot(err error, slot BindingSlot) ApplyResult {
	copy := cloneSlot(slot)
	return ApplyResult{Code: ResultRejected, Slot: &copy, Error: err.Error()}
}

func capacityBindingSlot(slot BindingSlot) ApplyResult {
	copy := cloneSlot(slot)
	return ApplyResult{Code: ResultCapacityReached, Slot: &copy, Error: ErrBindingCapacity.Error()}
}

func rejectedGatewaySlot(err error, slot GatewaySlot) ApplyResult {
	copy := cloneGatewaySlot(slot)
	return ApplyResult{Code: ResultRejected, GatewaySlot: &copy, Error: err.Error()}
}
