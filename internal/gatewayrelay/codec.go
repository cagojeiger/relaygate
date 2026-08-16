package gatewayrelay

import (
	"fmt"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
)

func openContextToProto(open authority.OpenContext) *controlv1.OpenContext {
	return &controlv1.OpenContext{
		ClusterEpoch:             open.ClusterEpoch,
		AuthorityId:              open.AuthorityID,
		AttemptId:                open.AttemptID,
		Auth:                     authContextToProto(open.Auth),
		Binding:                  bindingSlotToProto(open.Binding),
		IngressGatewayId:         open.IngressGatewayID,
		IngressGatewayInstanceId: open.IngressGatewayInstanceID,
		IngressControlSessionId:  open.IngressControlSessionID,
		OwnerRelayAddress:        open.OwnerRelayAddress,
		ExpiresAtUnixMillis:      open.ExpiresAt.UnixMilli(),
	}
}

func openContextFromProto(wire *controlv1.OpenContext) (authority.OpenContext, error) {
	if wire == nil {
		return authority.OpenContext{}, fmt.Errorf("%w: forwarded Open context is required", ErrInvalid)
	}
	auth := authContextFromProto(wire.GetAuth())
	binding, err := bindingSlotFromProto(wire.GetBinding())
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
			GatewayId:         slot.Ref.GatewayID,
		}
	}
	return wire
}

func bindingSlotFromProto(wire *controlv1.BindingSlot) (controlstate.BindingSlot, error) {
	if wire == nil || wire.GetKey() == nil || wire.GetRef() == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: exact live binding is required", ErrInvalid)
	}
	slot := controlstate.BindingSlot{
		Key: controlstate.BindingKey{
			ClientID:        wire.GetKey().GetClientId(),
			EndpointPattern: wire.GetKey().GetEndpointPattern(),
			TargetID:        wire.GetKey().GetTargetId(),
		},
		Generation: wire.GetGeneration(),
		Ref: &controlstate.ListenerBindingRef{
			GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(),
			ListenerBindingID: wire.GetRef().GetListenerBindingId(),
			GatewayID:         wire.GetRef().GetGatewayId(),
		},
	}
	if err := slot.Key.Validate(); err != nil || slot.Generation == 0 {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: invalid binding slot", ErrInvalid)
	}
	if err := slot.Ref.Validate(); err != nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: invalid binding ref: %w", ErrInvalid, err)
	}
	return slot, nil
}

func cloneBinding(slot controlstate.BindingSlot) controlstate.BindingSlot {
	clone := slot
	if slot.Ref != nil {
		ref := *slot.Ref
		clone.Ref = &ref
	}
	return clone
}

func sameBinding(left, right controlstate.BindingSlot) bool {
	if left.Key != right.Key || left.Generation != right.Generation {
		return false
	}
	if left.Ref == nil || right.Ref == nil {
		return left.Ref == nil && right.Ref == nil
	}
	return *left.Ref == *right.Ref
}
