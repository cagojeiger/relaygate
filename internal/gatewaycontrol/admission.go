package gatewaycontrol

import (
	"context"
	"errors"
	"fmt"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidOpen     = authority.ErrInvalidOpen
	ErrRouteNotFound   = authority.ErrRouteNotFound
	ErrOpenUnavailable = authority.ErrOpenUnavailable
)

// AdmitOpen asks the exact currently revalidated control endpoint for one
// exact-target authority context. It performs a single unary RPC and never
// replays the request or reserves anything on the owning Gateway.
func (c *Client) AdmitOpen(ctx context.Context, session clientsession.Ref, endpoint, targetID string) (authority.OpenContext, error) {
	if ctx == nil {
		return authority.OpenContext{}, fmt.Errorf("%w: context is required", ErrInvalidOpen)
	}
	auth := authority.AuthContext{
		ClientSessionID: session.ClientSessionID,
		ClientID:        session.ClientID,
		APIKeyID:        session.APIKeyID,
		AuthRevision:    session.AuthRevision,
	}
	key, err := authority.ExactBindingKey(auth, endpoint, targetID)
	if err != nil {
		return authority.OpenContext{}, err
	}
	if err := ctx.Err(); err != nil {
		return authority.OpenContext{}, fmt.Errorf("%w: %w", ErrOpenUnavailable, err)
	}

	c.mu.Lock()
	current := c.status
	stopped := c.stopped
	controlClient := c.admissionClient
	controlSession := cloneSessionRef(c.admissionSession)
	c.mu.Unlock()
	if stopped || current.State != StateRevalidated || current.Endpoint == "" ||
		current.AuthorityID == "" || current.ControlSessionID == "" ||
		controlClient == nil || controlSession == nil {
		return authority.OpenContext{}, ErrOpenUnavailable
	}

	request := &controlv1.AdmitOpenRequest{
		Session: controlSession,
		Auth: &controlv1.AuthContext{
			ClientSessionId: auth.ClientSessionID,
			ClientId:        auth.ClientID,
			ApiKeyId:        auth.APIKeyID,
			AuthRevision:    auth.AuthRevision,
		},
		Endpoint: endpoint,
		TargetId: targetID,
	}
	response, err := controlClient.AdmitOpen(ctx, request)
	if err != nil {
		return authority.OpenContext{}, mapAdmitOpenRPCError(err)
	}
	return openContextFromProto(response.GetContext(), c.config.ClusterEpoch, current, auth, key)
}

func openContextFromProto(
	wire *controlv1.OpenContext,
	clusterEpoch string,
	control Status,
	auth authority.AuthContext,
	key controlstate.BindingKey,
) (authority.OpenContext, error) {
	if wire == nil {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned no Open context", ErrOpenUnavailable)
	}
	if wire.GetClusterEpoch() != clusterEpoch || wire.GetAuthorityId() != control.AuthorityID || wire.GetAttemptId() == "" ||
		len(wire.GetClusterEpoch()) > controlstate.MaxIdentityBytes ||
		len(wire.GetAuthorityId()) > controlstate.MaxIdentityBytes ||
		len(wire.GetAttemptId()) > controlstate.MaxIdentityBytes {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned a mismatched Open identity", ErrOpenUnavailable)
	}
	wireAuth := wire.GetAuth()
	if wireAuth == nil || wireAuth.GetClientSessionId() != auth.ClientSessionID ||
		wireAuth.GetClientId() != auth.ClientID || wireAuth.GetApiKeyId() != auth.APIKeyID ||
		wireAuth.GetAuthRevision() != auth.AuthRevision {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned a mismatched auth context", ErrOpenUnavailable)
	}
	wireBinding := wire.GetBinding()
	if wireBinding == nil || wireBinding.GetGeneration() == 0 || wireBinding.GetRef() == nil {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned no exact live binding", ErrOpenUnavailable)
	}
	returnedKey, err := bindingKeyFromProto(wireBinding.GetKey())
	if err != nil || returnedKey != key {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned a mismatched binding key", ErrOpenUnavailable)
	}
	ref := controlstate.ListenerBindingRef{
		GatewayID:         wireBinding.GetRef().GetGatewayId(),
		GatewayInstanceID: wireBinding.GetRef().GetGatewayInstanceId(),
		ListenerBindingID: wireBinding.GetRef().GetListenerBindingId(),
	}
	if err := ref.Validate(); err != nil {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned an invalid full binding ref: %w", ErrOpenUnavailable, err)
	}
	openContext, err := authority.NewOpenContext(
		wire.GetClusterEpoch(),
		wire.GetAuthorityId(),
		wire.GetAttemptId(),
		auth,
		controlstate.BindingSlot{
			Key:        returnedKey,
			Generation: wireBinding.GetGeneration(),
			Ref:        &ref,
		},
	)
	if err != nil {
		return authority.OpenContext{}, fmt.Errorf("%w: control returned an invalid Open context: %w", ErrOpenUnavailable, err)
	}
	return openContext, nil
}

func mapAdmitOpenRPCError(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %w", ErrInvalidOpen, err)
	case codes.NotFound:
		return fmt.Errorf("%w: %w", ErrRouteNotFound, err)
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return fmt.Errorf("%w: %w", ErrOpenUnavailable, err)
	default:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", ErrOpenUnavailable, err)
		}
		return fmt.Errorf("%w: admission RPC failed: %w", ErrOpenUnavailable, err)
	}
}
