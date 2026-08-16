package controlgrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControlState interface {
	Apply(context.Context, []byte) (controlstate.ApplyResult, error)
	State() controlstate.State
	LookupGateway(string) controlstate.GatewaySlot
}

type Service struct {
	controlv1.UnimplementedGatewayControlServer

	clusterEpoch string
	node         ControlState
	authority    *authority.Manager
	// controlMu makes Gateway registration/session fencing and binding CAS
	// operations a single ordered control lane on the current authority.
	controlMu sync.Mutex
}

func NewService(clusterEpoch string, node ControlState, manager *authority.Manager) (*Service, error) {
	if clusterEpoch == "" {
		return nil, fmt.Errorf("cluster epoch is required")
	}
	if node == nil {
		return nil, fmt.Errorf("control state is required")
	}
	if manager == nil {
		return nil, fmt.Errorf("authority manager is required")
	}
	return &Service{clusterEpoch: clusterEpoch, node: node, authority: manager}, nil
}

func (s *Service) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "hello is required")
		}
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "hello must be the first control message")
	}
	if err := s.validateHello(hello); err != nil {
		return err
	}
	gatewaySlot, session, ownedBindings, err := s.openControlSession(stream.Context(), hello)
	if err != nil {
		return err
	}
	defer s.authority.EndSession(session.Ref)

	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SessionOpened{
		SessionOpened: &controlv1.SessionOpened{
			Session:           sessionRefToProto(session.Ref),
			GatewayGeneration: gatewaySlot.Generation,
			OwnedBindings:     bindingSlotsToProto(ownedBindings),
		},
	}}); err != nil {
		return err
	}

	received := receiveRequests(stream)
	snapshotAccepted := false
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-session.Done:
			return status.Error(codes.Unavailable, "control session was fenced")
		case item, ok := <-received:
			if !ok {
				return nil
			}
			if item.err != nil {
				if errors.Is(item.err, io.EOF) {
					return nil
				}
				return item.err
			}
			request := item.request
			if !snapshotAccepted {
				snapshot := request.GetFullSnapshot()
				if snapshot == nil {
					return status.Error(codes.FailedPrecondition, "full snapshot must follow session_opened")
				}
				if err := s.acceptSnapshot(stream.Context(), session.Ref, snapshot); err != nil {
					return err
				}
				snapshotAccepted = true
				if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SnapshotAccepted{
					SnapshotAccepted: &controlv1.SnapshotAccepted{Presence: presenceToProto(s.authority.Presence().State)},
				}}); err != nil {
					return err
				}
				continue
			}

			mutation := request.GetBindingMutation()
			if mutation == nil {
				return status.Error(codes.FailedPrecondition, "only binding mutations are allowed after snapshot acceptance")
			}
			result, err := s.applyMutation(stream.Context(), session.Ref, mutation)
			if err != nil {
				return err
			}
			if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{
				MutationResult: result,
			}}); err != nil {
				return err
			}
		}
	}
}

// AdmitOpen issues a context for one exact target. It is deliberately separate
// from the ordered mutation stream: the RPC does not mutate control state,
// reserve an owner attempt, forward to another Gateway, or replay on response
// loss.
func (s *Service) AdmitOpen(ctx context.Context, request *controlv1.AdmitOpenRequest) (*controlv1.AdmitOpenResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "Open admission request is required")
	}
	wireSession := request.GetSession()
	if err := validateAdmissionSession(wireSession); err != nil {
		return nil, err
	}
	auth, err := authContextFromProto(request.GetAuth())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid auth context: %v", err)
	}
	if _, err := authority.ExactBindingKey(auth, request.GetEndpoint(), request.GetTargetId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	current, err := s.authority.Confirm(ctx)
	if err != nil {
		return nil, unavailable("confirm Open authority", err)
	}
	if wireSession.GetClusterEpoch() != current.ClusterEpoch || wireSession.GetAuthorityId() != current.AuthorityID {
		return nil, status.Error(codes.Unavailable, "Open ingress session belongs to a stale authority")
	}
	gatewaySlot := s.node.LookupGateway(wireSession.GetGatewayId())
	if gatewaySlot.Generation == 0 || gatewaySlot.Ref == nil || gatewaySlot.Ref.GatewayInstanceID != wireSession.GetGatewayInstanceId() {
		return nil, status.Error(codes.Unavailable, "Open ingress gateway registration is stale")
	}
	ingress := authority.SessionRef{
		ClusterEpoch:      wireSession.GetClusterEpoch(),
		AuthorityID:       wireSession.GetAuthorityId(),
		ControlSessionID:  wireSession.GetControlSessionId(),
		GatewayID:         wireSession.GetGatewayId(),
		GatewayInstanceID: wireSession.GetGatewayInstanceId(),
		GatewayGeneration: gatewaySlot.Generation,
	}
	openContext, err := s.authority.ResolveOpen(ingress, auth, request.GetEndpoint(), request.GetTargetId())
	if err != nil {
		return nil, mapOpenAdmissionError(err)
	}
	return &controlv1.AdmitOpenResponse{Context: openContextToProto(openContext)}, nil
}

func (s *Service) openControlSession(ctx context.Context, hello *controlv1.Hello) (controlstate.GatewaySlot, authority.Session, []controlstate.BindingSlot, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	if _, err := s.authority.Confirm(ctx); err != nil {
		return controlstate.GatewaySlot{}, authority.Session{}, nil, unavailable("confirm authority", err)
	}
	gatewaySlot, err := s.registerGateway(ctx, hello.GetGatewayId(), hello.GetGatewayInstanceId())
	if err != nil {
		return controlstate.GatewaySlot{}, authority.Session{}, nil, err
	}
	if _, err := s.authority.Confirm(ctx); err != nil {
		return controlstate.GatewaySlot{}, authority.Session{}, nil, unavailable("confirm authority after gateway registration", err)
	}
	ownedBindings := ownedBindingSlots(s.node.State(), hello.GetGatewayId(), hello.GetGatewayInstanceId())
	session, err := s.authority.OpenSession(gatewaySlot, hello.GetRelayAddress())
	if err != nil {
		return controlstate.GatewaySlot{}, authority.Session{}, nil, unavailable("open control session", err)
	}
	return gatewaySlot, session, ownedBindings, nil
}

func (s *Service) validateHello(hello *controlv1.Hello) error {
	if hello.GetClusterEpoch() == "" || len(hello.GetClusterEpoch()) > controlstate.MaxIdentityBytes {
		return status.Errorf(codes.InvalidArgument, "cluster_epoch must be 1..%d bytes", controlstate.MaxIdentityBytes)
	}
	if hello.GetClusterEpoch() != s.clusterEpoch {
		return status.Error(codes.FailedPrecondition, "cluster epoch is not current")
	}
	if hello.GetGatewayId() == "" || len(hello.GetGatewayId()) > controlstate.MaxIdentityBytes ||
		hello.GetGatewayInstanceId() == "" || len(hello.GetGatewayInstanceId()) > controlstate.MaxIdentityBytes {
		return status.Errorf(codes.InvalidArgument, "gateway_id and gateway_instance_id must be 1..%d bytes", controlstate.MaxIdentityBytes)
	}
	if err := authority.ValidateRelayAddress(hello.GetRelayAddress()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid relay_address: %v", err)
	}
	return nil
}

func (s *Service) registerGateway(ctx context.Context, gatewayID, gatewayInstanceID string) (controlstate.GatewaySlot, error) {
	current := s.node.LookupGateway(gatewayID)
	if current.Ref != nil && current.Ref.GatewayInstanceID == gatewayInstanceID {
		return current, nil
	}
	command, err := controlstate.EncodeRegisterGateway(controlstate.RegisterGateway{
		ClusterEpoch:       s.clusterEpoch,
		GatewayID:          gatewayID,
		ExpectedGeneration: current.Generation,
		ExpectedRef:        current.Ref,
		NewRef:             controlstate.GatewayRegistrationRef{GatewayInstanceID: gatewayInstanceID},
	})
	if err != nil {
		return controlstate.GatewaySlot{}, status.Errorf(codes.InvalidArgument, "encode gateway registration: %v", err)
	}
	result, err := s.node.Apply(ctx, command)
	if err != nil {
		return controlstate.GatewaySlot{}, unavailable("commit gateway registration", err)
	}
	if !result.Applied() {
		return controlstate.GatewaySlot{}, status.Errorf(codes.Aborted, "gateway registration lost compare-and-set: %s", result.Error)
	}
	if result.GatewaySlot == nil {
		return controlstate.GatewaySlot{}, status.Error(codes.Internal, "gateway registration committed without a slot")
	}
	return *result.GatewaySlot, nil
}

func (s *Service) acceptSnapshot(ctx context.Context, session authority.SessionRef, snapshot *controlv1.FullSnapshot) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	if err := requireSession(snapshot.GetSession(), session); err != nil {
		return err
	}
	if err := s.confirmSession(ctx, session); err != nil {
		return err
	}
	state := s.node.State()
	if !gatewaySlotMatches(state, session) {
		return status.Error(codes.Unavailable, "control session gateway registration is stale")
	}
	expected := make(map[controlstate.BindingKey]controlstate.BindingSlot)
	for _, slot := range state.Bindings {
		if slot.Ref != nil && slot.Ref.GatewayID == session.GatewayID && slot.Ref.GatewayInstanceID == session.GatewayInstanceID {
			expected[slot.Key] = slot
		}
	}

	bindings := make([]controlstate.BindingSlot, 0, len(snapshot.GetBindings()))
	seen := make(map[controlstate.BindingKey]struct{}, len(snapshot.GetBindings()))
	for index, wireSlot := range snapshot.GetBindings() {
		slot, err := bindingSlotFromProto(wireSlot, session.GatewayID)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "bindings[%d]: %v", index, err)
		}
		if slot.Ref.GatewayInstanceID != session.GatewayInstanceID {
			return status.Errorf(codes.PermissionDenied, "bindings[%d] belongs to another gateway instance", index)
		}
		if _, duplicate := seen[slot.Key]; duplicate {
			return status.Errorf(codes.InvalidArgument, "bindings[%d] duplicates a binding key", index)
		}
		seen[slot.Key] = struct{}{}
		if durable, ok := expected[slot.Key]; !ok || !bindingSlotsEqual(durable, slot) {
			return status.Errorf(codes.FailedPrecondition, "bindings[%d] does not match the committed slot", index)
		}
		bindings = append(bindings, slot)
	}
	if len(seen) != len(expected) {
		for _, slot := range state.Bindings {
			if slot.Ref == nil || slot.Ref.GatewayID != session.GatewayID || slot.Ref.GatewayInstanceID != session.GatewayInstanceID {
				continue
			}
			if _, ok := seen[slot.Key]; !ok {
				return status.Errorf(codes.FailedPrecondition, "full snapshot omits committed binding %q %q %q", slot.Key.ClientID, slot.Key.EndpointPattern, slot.Key.TargetID)
			}
		}
		return status.Error(codes.FailedPrecondition, "full snapshot does not match committed bindings")
	}
	if err := s.authority.Revalidate(session, bindings); err != nil {
		return unavailable("accept full snapshot", err)
	}
	return nil
}

func (s *Service) applyMutation(ctx context.Context, session authority.SessionRef, mutation *controlv1.BindingMutation) (*controlv1.MutationResult, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	if err := requireSession(mutation.GetSession(), session); err != nil {
		return nil, err
	}
	if err := s.confirmSession(ctx, session); err != nil {
		return nil, err
	}
	if err := s.authority.RequireRevalidated(session); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "control session is not revalidated: %v", err)
	}

	command, err := mutationCommand(s.clusterEpoch, session, mutation)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid binding mutation: %v", err)
	}
	applyResult, err := s.node.Apply(ctx, command)
	if err != nil {
		return nil, unavailable("commit binding mutation", err)
	}
	result := &controlv1.MutationResult{
		Code:             mutationCodeToProto(applyResult.Code),
		Error:            applyResult.Error,
		SameGatewayOwner: applyResult.Slot != nil && applyResult.Slot.Ref != nil && applyResult.Slot.Ref.GatewayID == session.GatewayID,
	}
	if applyResult.Slot != nil {
		result.Slot = bindingSlotToProto(*applyResult.Slot)
	}
	if applyResult.Applied() {
		if applyResult.Slot == nil {
			return nil, status.Error(codes.Internal, "binding mutation committed without a slot")
		}
		if err := s.authority.UpdateBinding(session, *applyResult.Slot); err != nil {
			return nil, unavailable("publish committed binding", err)
		}
	}
	return result, nil
}

func (s *Service) confirmSession(ctx context.Context, session authority.SessionRef) error {
	current, err := s.authority.Confirm(ctx)
	if err != nil {
		return unavailable("confirm authority", err)
	}
	if current.ClusterEpoch != session.ClusterEpoch || current.AuthorityID != session.AuthorityID {
		return status.Error(codes.Unavailable, "control session belongs to a stale authority")
	}
	return nil
}

type receivedRequest struct {
	request *controlv1.ControlRequest
	err     error
}

func receiveRequests(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) <-chan receivedRequest {
	received := make(chan receivedRequest, 1)
	go func() {
		defer close(received)
		for {
			request, err := stream.Recv()
			select {
			case received <- receivedRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func requireSession(wire *controlv1.SessionRef, expected authority.SessionRef) error {
	if wire == nil {
		return status.Error(codes.InvalidArgument, "session is required")
	}
	if wire.GetClusterEpoch() != expected.ClusterEpoch ||
		wire.GetAuthorityId() != expected.AuthorityID ||
		wire.GetControlSessionId() != expected.ControlSessionID ||
		wire.GetGatewayId() != expected.GatewayID ||
		wire.GetGatewayInstanceId() != expected.GatewayInstanceID {
		return status.Error(codes.PermissionDenied, "session reference does not match this stream")
	}
	return nil
}

func mutationCommand(clusterEpoch string, session authority.SessionRef, mutation *controlv1.BindingMutation) ([]byte, error) {
	switch value := mutation.GetMutation().(type) {
	case *controlv1.BindingMutation_Install:
		key, err := bindingKeyFromProto(value.Install.GetKey())
		if err != nil {
			return nil, err
		}
		expectedRef, err := optionalBindingRefFromProto(value.Install.GetExpectedRef(), session.GatewayID)
		if err != nil {
			return nil, err
		}
		newRef, err := bindingRefFromProto(value.Install.GetNewRef(), session.GatewayID)
		if err != nil {
			return nil, err
		}
		if newRef.GatewayInstanceID != session.GatewayInstanceID {
			return nil, fmt.Errorf("new binding belongs to another gateway instance")
		}
		return controlstate.EncodeInstallBinding(controlstate.InstallBinding{
			ClusterEpoch:       clusterEpoch,
			Key:                key,
			ExpectedGeneration: value.Install.GetExpectedGeneration(),
			ExpectedRef:        expectedRef,
			NewRef:             newRef,
		})

	case *controlv1.BindingMutation_Remove:
		key, err := bindingKeyFromProto(value.Remove.GetKey())
		if err != nil {
			return nil, err
		}
		expectedRef, err := bindingRefFromProto(value.Remove.GetExpectedRef(), session.GatewayID)
		if err != nil {
			return nil, err
		}
		return controlstate.EncodeRemoveBinding(controlstate.RemoveBinding{
			ClusterEpoch:       clusterEpoch,
			Key:                key,
			ExpectedGeneration: value.Remove.GetExpectedGeneration(),
			ExpectedRef:        expectedRef,
		})

	default:
		return nil, fmt.Errorf("install or remove is required")
	}
}

func gatewaySlotMatches(state controlstate.State, session authority.SessionRef) bool {
	for _, slot := range state.Gateways {
		if slot.GatewayID != session.GatewayID {
			continue
		}
		return slot.Generation == session.GatewayGeneration && slot.Ref != nil && slot.Ref.GatewayInstanceID == session.GatewayInstanceID
	}
	return false
}

func bindingSlotFromProto(wire *controlv1.BindingSlot, gatewayID string) (controlstate.BindingSlot, error) {
	if wire == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("slot is required")
	}
	key, err := bindingKeyFromProto(wire.GetKey())
	if err != nil {
		return controlstate.BindingSlot{}, err
	}
	ref, err := bindingRefFromProto(wire.GetRef(), gatewayID)
	if err != nil {
		return controlstate.BindingSlot{}, err
	}
	if wire.GetGeneration() == 0 {
		return controlstate.BindingSlot{}, fmt.Errorf("generation must be positive")
	}
	return controlstate.BindingSlot{Key: key, Generation: wire.GetGeneration(), Ref: &ref}, nil
}

func bindingKeyFromProto(wire *controlv1.BindingKey) (controlstate.BindingKey, error) {
	if wire == nil {
		return controlstate.BindingKey{}, fmt.Errorf("binding key is required")
	}
	key := controlstate.BindingKey{
		ClientID:        wire.GetClientId(),
		EndpointPattern: wire.GetEndpointPattern(),
		TargetID:        wire.GetTargetId(),
	}
	if err := key.Validate(); err != nil {
		return controlstate.BindingKey{}, err
	}
	return key, nil
}

func bindingRefFromProto(wire *controlv1.ListenerBindingRef, gatewayID string) (controlstate.ListenerBindingRef, error) {
	if wire == nil {
		return controlstate.ListenerBindingRef{}, fmt.Errorf("binding ref is required")
	}
	ref := controlstate.ListenerBindingRef{
		GatewayID:         gatewayID,
		GatewayInstanceID: wire.GetGatewayInstanceId(),
		ListenerBindingID: wire.GetListenerBindingId(),
	}
	if err := ref.Validate(); err != nil {
		return controlstate.ListenerBindingRef{}, err
	}
	return ref, nil
}

func validateAdmissionSession(wire *controlv1.SessionRef) error {
	if wire == nil {
		return status.Error(codes.InvalidArgument, "session is required")
	}
	for _, identity := range []struct {
		field string
		value string
	}{
		{field: "cluster_epoch", value: wire.GetClusterEpoch()},
		{field: "authority_id", value: wire.GetAuthorityId()},
		{field: "control_session_id", value: wire.GetControlSessionId()},
		{field: "gateway_id", value: wire.GetGatewayId()},
		{field: "gateway_instance_id", value: wire.GetGatewayInstanceId()},
	} {
		if identity.value == "" || len(identity.value) > controlstate.MaxIdentityBytes {
			return status.Errorf(codes.InvalidArgument, "%s must be 1..%d bytes", identity.field, controlstate.MaxIdentityBytes)
		}
	}
	return nil
}

func authContextFromProto(wire *controlv1.AuthContext) (authority.AuthContext, error) {
	if wire == nil {
		return authority.AuthContext{}, fmt.Errorf("auth context is required")
	}
	auth := authority.AuthContext{
		ClientSessionID: wire.GetClientSessionId(),
		ClientID:        wire.GetClientId(),
		APIKeyID:        wire.GetApiKeyId(),
		AuthRevision:    wire.GetAuthRevision(),
	}
	if err := auth.Validate(); err != nil {
		return authority.AuthContext{}, err
	}
	return auth, nil
}

func authContextToProto(auth authority.AuthContext) *controlv1.AuthContext {
	return &controlv1.AuthContext{
		ClientSessionId: auth.ClientSessionID,
		ClientId:        auth.ClientID,
		ApiKeyId:        auth.APIKeyID,
		AuthRevision:    auth.AuthRevision,
	}
}

func openContextToProto(openContext authority.OpenContext) *controlv1.OpenContext {
	binding := bindingSlotToProto(openContext.Binding)
	if binding.Ref != nil && openContext.Binding.Ref != nil {
		binding.Ref.GatewayId = openContext.Binding.Ref.GatewayID
	}
	return &controlv1.OpenContext{
		ClusterEpoch:             openContext.ClusterEpoch,
		AuthorityId:              openContext.AuthorityID,
		AttemptId:                openContext.AttemptID,
		Auth:                     authContextToProto(openContext.Auth),
		Binding:                  binding,
		IngressGatewayId:         openContext.IngressGatewayID,
		IngressGatewayInstanceId: openContext.IngressGatewayInstanceID,
		IngressControlSessionId:  openContext.IngressControlSessionID,
		OwnerRelayAddress:        openContext.OwnerRelayAddress,
		ExpiresAtUnixMillis:      openContext.ExpiresAt.UnixMilli(),
	}
}

func mapOpenAdmissionError(err error) error {
	switch {
	case errors.Is(err, authority.ErrInvalidOpen):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, authority.ErrRouteNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, authority.ErrOpenUnavailable),
		errors.Is(err, authority.ErrNoAuthority),
		errors.Is(err, authority.ErrStaleSession),
		errors.Is(err, authority.ErrSnapshotFirst):
		return status.Errorf(codes.Unavailable, "%v", err)
	default:
		return status.Errorf(codes.Internal, "issue Open context: %v", err)
	}
}

func optionalBindingRefFromProto(wire *controlv1.ListenerBindingRef, gatewayID string) (*controlstate.ListenerBindingRef, error) {
	if wire == nil {
		return nil, nil
	}
	ref, err := bindingRefFromProto(wire, gatewayID)
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

func sessionRefToProto(ref authority.SessionRef) *controlv1.SessionRef {
	return &controlv1.SessionRef{
		ClusterEpoch:      ref.ClusterEpoch,
		AuthorityId:       ref.AuthorityID,
		ControlSessionId:  ref.ControlSessionID,
		GatewayId:         ref.GatewayID,
		GatewayInstanceId: ref.GatewayInstanceID,
	}
}

func bindingSlotToProto(slot controlstate.BindingSlot) *controlv1.BindingSlot {
	wire := &controlv1.BindingSlot{
		Key: &controlv1.BindingKey{
			ClientId:        slot.Key.ClientID,
			EndpointPattern: slot.Key.EndpointPattern,
			TargetId:        slot.Key.TargetID,
		},
		Generation: slot.Generation,
	}
	if slot.Ref != nil {
		wire.Ref = &controlv1.ListenerBindingRef{
			GatewayInstanceId: slot.Ref.GatewayInstanceID,
			ListenerBindingId: slot.Ref.ListenerBindingID,
		}
	}
	return wire
}

func bindingSlotsToProto(slots []controlstate.BindingSlot) []*controlv1.BindingSlot {
	wire := make([]*controlv1.BindingSlot, 0, len(slots))
	for _, slot := range slots {
		wire = append(wire, bindingSlotToProto(slot))
	}
	return wire
}

func ownedBindingSlots(state controlstate.State, gatewayID, gatewayInstanceID string) []controlstate.BindingSlot {
	owned := make([]controlstate.BindingSlot, 0)
	for _, slot := range state.Bindings {
		if slot.Ref != nil && slot.Ref.GatewayID == gatewayID && slot.Ref.GatewayInstanceID == gatewayInstanceID {
			owned = append(owned, slot)
		}
	}
	sort.Slice(owned, func(i, j int) bool {
		left := owned[i].Key
		right := owned[j].Key
		if left.ClientID != right.ClientID {
			return left.ClientID < right.ClientID
		}
		if left.EndpointPattern != right.EndpointPattern {
			return left.EndpointPattern < right.EndpointPattern
		}
		return left.TargetID < right.TargetID
	})
	return owned
}

func bindingSlotsEqual(left, right controlstate.BindingSlot) bool {
	if left.Key != right.Key || left.Generation != right.Generation {
		return false
	}
	if left.Ref == nil || right.Ref == nil {
		return left.Ref == nil && right.Ref == nil
	}
	return *left.Ref == *right.Ref
}

func presenceToProto(state authority.PresenceState) controlv1.PresenceState {
	switch state {
	case authority.PresenceComplete:
		return controlv1.PresenceState_PRESENCE_STATE_COMPLETE
	case authority.PresenceRebuilding:
		return controlv1.PresenceState_PRESENCE_STATE_REBUILDING
	default:
		return controlv1.PresenceState_PRESENCE_STATE_UNSPECIFIED
	}
}

func mutationCodeToProto(code controlstate.ResultCode) controlv1.MutationCode {
	switch code {
	case controlstate.ResultApplied:
		return controlv1.MutationCode_MUTATION_CODE_APPLIED
	case controlstate.ResultAlreadyApplied:
		return controlv1.MutationCode_MUTATION_CODE_ALREADY_APPLIED
	case controlstate.ResultCapacityReached:
		return controlv1.MutationCode_MUTATION_CODE_CAPACITY_REACHED
	default:
		return controlv1.MutationCode_MUTATION_CODE_REJECTED
	}
}

func unavailable(operation string, err error) error {
	return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
}
