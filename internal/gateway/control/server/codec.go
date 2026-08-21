package controlgrpc

import (
	"errors"

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) validateHello(hello *controlv1.Hello) error {
	if hello.GetClusterEpoch() != s.clusterEpoch {
		return status.Error(codes.FailedPrecondition, "cluster epoch is not current")
	}
	if err := routing.ValidateIdentity("gateway_id", hello.GetGatewayId()); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := routing.ValidateIdentity("gateway_instance_id", hello.GetGatewayInstanceId()); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := routing.ValidateRelayAddress(hello.GetRelayAddress()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid relay_address: %v", err)
	}
	return nil
}

func snapshotBindings(snapshot *controlv1.FullSnapshot, ref controlmodel.SessionRef) ([]routing.LiveBinding, error) {
	if err := requireSession(snapshot.GetSession(), ref); err != nil {
		return nil, err
	}
	if len(snapshot.GetBindings()) > routing.MaxListenerBindingsPerGateway {
		return nil, status.Error(codes.ResourceExhausted, "snapshot binding capacity reached")
	}
	bindings := make([]routing.LiveBinding, 0, len(snapshot.GetBindings()))
	seen := make(map[routing.BindingKey]struct{}, len(snapshot.GetBindings()))
	for index, item := range snapshot.GetBindings() {
		binding, err := liveBindingFromProto(item, ref.GatewayID, ref.GatewayInstanceID, false)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot binding %d: %v", index, err)
		}
		if _, duplicate := seen[binding.Key]; duplicate {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot binding %d duplicates a key", index)
		}
		seen[binding.Key] = struct{}{}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func requireSession(wire *controlv1.SessionRef, ref controlmodel.SessionRef) error {
	if wire == nil || wire.GetClusterEpoch() != ref.ClusterEpoch || wire.GetAuthorityId() != ref.AuthorityID || wire.GetControlSessionId() != ref.ControlSessionID || wire.GetGatewayId() != ref.GatewayID || wire.GetGatewayInstanceId() != ref.GatewayInstanceID {
		return status.Error(codes.Unavailable, "control session is stale")
	}
	return nil
}

func validateAdmissionSession(wire *controlv1.SessionRef) error {
	if wire == nil {
		return status.Error(codes.InvalidArgument, "control session is required")
	}
	for _, field := range []struct{ name, value string }{{"cluster_epoch", wire.GetClusterEpoch()}, {"authority_id", wire.GetAuthorityId()}, {"control_session_id", wire.GetControlSessionId()}, {"gateway_id", wire.GetGatewayId()}, {"gateway_instance_id", wire.GetGatewayInstanceId()}} {
		if err := routing.ValidateIdentity(field.name, field.value); err != nil {
			return status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}
	return nil
}

func sessionRefFromProto(wire *controlv1.SessionRef) controlmodel.SessionRef {
	return controlmodel.SessionRef{ClusterEpoch: wire.GetClusterEpoch(), AuthorityID: wire.GetAuthorityId(), ControlSessionID: wire.GetControlSessionId(), GatewayID: wire.GetGatewayId(), GatewayInstanceID: wire.GetGatewayInstanceId()}
}

func sessionRefToProto(ref controlmodel.SessionRef) *controlv1.SessionRef {
	return &controlv1.SessionRef{ClusterEpoch: ref.ClusterEpoch, AuthorityId: ref.AuthorityID, ControlSessionId: ref.ControlSessionID, GatewayId: ref.GatewayID, GatewayInstanceId: ref.GatewayInstanceID}
}

func liveBindingToProto(binding routing.LiveBinding, includeGatewayID bool) *controlv1.LiveBinding {
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: binding.Ref.GatewayInstanceID, ListenerBindingId: binding.Ref.ListenerBindingID}
	if includeGatewayID {
		ref.GatewayId = binding.Ref.GatewayID
	}
	return &controlv1.LiveBinding{Key: &controlv1.BindingKey{ClientId: binding.Key.ClientID, EndpointPattern: binding.Key.EndpointPattern, TargetId: binding.Key.TargetID}, Ref: ref}
}

func liveBindingFromProto(wire *controlv1.LiveBinding, sessionGatewayID, sessionGatewayInstanceID string, requireGatewayID bool) (routing.LiveBinding, error) {
	if wire == nil || wire.GetKey() == nil || wire.GetRef() == nil {
		return routing.LiveBinding{}, errors.New("live binding is required")
	}
	gatewayID := wire.GetRef().GetGatewayId()
	if gatewayID == "" {
		gatewayID = sessionGatewayID
	}
	if requireGatewayID && gatewayID == "" {
		return routing.LiveBinding{}, errors.New("live binding gateway_id is required")
	}
	binding := routing.LiveBinding{Key: routing.BindingKey{ClientID: wire.GetKey().GetClientId(), EndpointPattern: wire.GetKey().GetEndpointPattern(), TargetID: wire.GetKey().GetTargetId()}, Ref: routing.ListenerBindingRef{GatewayID: gatewayID, GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(), ListenerBindingID: wire.GetRef().GetListenerBindingId()}}
	if err := binding.Validate(); err != nil {
		return routing.LiveBinding{}, err
	}
	if sessionGatewayID != "" && (binding.Ref.GatewayID != sessionGatewayID || binding.Ref.GatewayInstanceID != sessionGatewayInstanceID) {
		return routing.LiveBinding{}, errors.New("live binding does not belong to current control session")
	}
	return binding, nil
}

func authContextFromProto(wire *controlv1.AuthContext) (routing.AuthContext, error) {
	if wire == nil {
		return routing.AuthContext{}, errors.New("auth context is required")
	}
	auth := routing.AuthContext{ClientSessionID: wire.GetClientSessionId(), ClientID: wire.GetClientId(), APIKeyID: wire.GetApiKeyId(), AuthRevision: wire.GetAuthRevision()}
	return auth, auth.Validate()
}

func openContextToProto(open routing.OpenContext) *controlv1.OpenContext {
	return &controlv1.OpenContext{ClusterEpoch: open.ClusterEpoch, AuthorityId: open.AuthorityID, AttemptId: open.AttemptID, Auth: &controlv1.AuthContext{ClientSessionId: open.Auth.ClientSessionID, ClientId: open.Auth.ClientID, ApiKeyId: open.Auth.APIKeyID, AuthRevision: open.Auth.AuthRevision}, Binding: liveBindingToProto(open.Binding, true), IngressGatewayId: open.IngressGatewayID, IngressGatewayInstanceId: open.IngressGatewayInstanceID, IngressControlSessionId: open.IngressControlSessionID, OwnerRelayAddress: open.OwnerRelayAddress, ExpiresAtUnixMillis: open.ExpiresAt.UnixMilli(), OwnerControlSessionId: open.OwnerControlSessionID}
}

func unavailable(operation string, err error) error {
	return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
}

func mapAuthorityError(operation string, err error) error {
	switch {
	case errors.Is(err, authority.ErrNoAuthority), errors.Is(err, authority.ErrStaleSession):
		return unavailable(operation, err)
	case errors.Is(err, authority.ErrSnapshotFirst):
		return status.Errorf(codes.FailedPrecondition, "%s: %v", operation, err)
	case errors.Is(err, routing.ErrInvalid):
		return status.Errorf(codes.InvalidArgument, "%s: %v", operation, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", operation, err)
	}
}

func mapOpenAdmissionError(err error) error {
	switch {
	case errors.Is(err, routing.ErrRouteNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, routing.ErrInvalidOpen), errors.Is(err, routing.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, authority.ErrNoAuthority), errors.Is(err, authority.ErrStaleSession), errors.Is(err, authority.ErrSnapshotFirst), errors.Is(err, routing.ErrOpenUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
