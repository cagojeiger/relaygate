package gatewayrelay

import (
	"fmt"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
)

func openContextToProto(open authority.OpenContext) *controlv1.OpenContext {
	return &controlv1.OpenContext{
		ClusterEpoch:             open.ClusterEpoch,
		AuthorityId:              open.AuthorityID,
		AttemptId:                open.AttemptID,
		Auth:                     authContextToProto(open.Auth),
		Binding:                  liveBindingToProto(open.Binding),
		IngressGatewayId:         open.IngressGatewayID,
		IngressGatewayInstanceId: open.IngressGatewayInstanceID,
		IngressControlSessionId:  open.IngressControlSessionID,
		OwnerRelayAddress:        open.OwnerRelayAddress,
		ExpiresAtUnixMillis:      open.ExpiresAt.UnixMilli(),
		OwnerControlSessionId:    open.OwnerControlSessionID,
	}
}

func openContextFromProto(wire *controlv1.OpenContext) (authority.OpenContext, error) {
	if wire == nil {
		return authority.OpenContext{}, fmt.Errorf("%w: forwarded Open context is required", ErrInvalid)
	}
	auth := authContextFromProto(wire.GetAuth())
	binding, err := liveBindingFromProto(wire.GetBinding())
	if err != nil {
		return authority.OpenContext{}, err
	}
	open, err := authority.NewForwardedOpenContext(
		wire.GetClusterEpoch(),
		wire.GetAuthorityId(),
		wire.GetAttemptId(),
		auth,
		binding,
		authority.ForwardingContext{
			IngressGatewayID:         wire.GetIngressGatewayId(),
			IngressGatewayInstanceID: wire.GetIngressGatewayInstanceId(),
			IngressControlSessionID:  wire.GetIngressControlSessionId(),
			OwnerControlSessionID:    wire.GetOwnerControlSessionId(),
			OwnerRelayAddress:        wire.GetOwnerRelayAddress(),
			ExpiresAt:                time.UnixMilli(wire.GetExpiresAtUnixMillis()),
		},
	)
	if err != nil {
		return authority.OpenContext{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return open, nil
}

func authContextToProto(auth authority.AuthContext) *controlv1.AuthContext {
	return &controlv1.AuthContext{
		ClientSessionId: auth.ClientSessionID,
		ClientId:        auth.ClientID,
		ApiKeyId:        auth.APIKeyID,
		AuthRevision:    auth.AuthRevision,
	}
}

func authContextFromProto(wire *controlv1.AuthContext) authority.AuthContext {
	if wire == nil {
		return authority.AuthContext{}
	}
	return authority.AuthContext{
		ClientSessionID: wire.GetClientSessionId(),
		ClientID:        wire.GetClientId(),
		APIKeyID:        wire.GetApiKeyId(),
		AuthRevision:    wire.GetAuthRevision(),
	}
}

func liveBindingToProto(binding routing.LiveBinding) *controlv1.LiveBinding {
	return &controlv1.LiveBinding{
		Key: &controlv1.BindingKey{
			ClientId:        binding.Key.ClientID,
			EndpointPattern: binding.Key.EndpointPattern,
			TargetId:        binding.Key.TargetID,
		},
		Ref: &controlv1.ListenerBindingRef{
			GatewayInstanceId: binding.Ref.GatewayInstanceID,
			ListenerBindingId: binding.Ref.ListenerBindingID,
			GatewayId:         binding.Ref.GatewayID,
		},
	}
}

func liveBindingFromProto(wire *controlv1.LiveBinding) (routing.LiveBinding, error) {
	if wire == nil || wire.GetKey() == nil || wire.GetRef() == nil {
		return routing.LiveBinding{}, fmt.Errorf("%w: exact live binding is required", ErrInvalid)
	}
	binding := routing.LiveBinding{
		Key: routing.BindingKey{
			ClientID:        wire.GetKey().GetClientId(),
			EndpointPattern: wire.GetKey().GetEndpointPattern(),
			TargetID:        wire.GetKey().GetTargetId(),
		},
		Ref: routing.ListenerBindingRef{
			GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(),
			ListenerBindingID: wire.GetRef().GetListenerBindingId(),
			GatewayID:         wire.GetRef().GetGatewayId(),
		},
	}
	if err := binding.Validate(); err != nil {
		return routing.LiveBinding{}, fmt.Errorf("%w: invalid live binding: %w", ErrInvalid, err)
	}
	return binding, nil
}

func cloneBinding(binding routing.LiveBinding) routing.LiveBinding {
	return binding
}

func sameBinding(left, right routing.LiveBinding) bool {
	return left == right
}
