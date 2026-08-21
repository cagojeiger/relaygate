package gatewaycontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidOpen     = routing.ErrInvalidOpen
	ErrRouteNotFound   = routing.ErrRouteNotFound
	ErrOpenUnavailable = routing.ErrOpenUnavailable
)

// AdmitOpen asks the exact currently revalidated control endpoint for one
// exact-target authority context. It performs one unary RPC and never replays
// the request across a control-session change.
func (c *Client) AdmitOpen(ctx context.Context, clientSession clientsession.Ref, endpoint, targetID string) (routing.OpenContext, error) {
	if ctx == nil {
		return routing.OpenContext{}, fmt.Errorf("%w: context is required", ErrInvalidOpen)
	}
	auth := routing.AuthContext{
		ClientSessionID: clientSession.ClientSessionID,
		ClientID:        clientSession.ClientID,
		APIKeyID:        clientSession.APIKeyID,
		AuthRevision:    clientSession.AuthRevision,
	}
	key, err := routing.ExactBindingKey(auth, endpoint, targetID)
	if err != nil {
		return routing.OpenContext{}, err
	}
	if err := ctx.Err(); err != nil {
		return routing.OpenContext{}, fmt.Errorf("%w: %w", ErrOpenUnavailable, err)
	}

	c.mu.Lock()
	current := c.status
	stopped := c.stopped
	controlClient := c.admissionClient
	controlSession := cloneSessionRef(c.admissionSession)
	c.mu.Unlock()
	if stopped || current.State != StateRevalidated || current.Endpoint == "" || current.AuthorityID == "" || current.ControlSessionID == "" || controlClient == nil || controlSession == nil {
		return routing.OpenContext{}, ErrOpenUnavailable
	}

	response, err := controlClient.AdmitOpen(ctx, &controlv1.AdmitOpenRequest{
		Session: controlSession,
		Auth: &controlv1.AuthContext{
			ClientSessionId: auth.ClientSessionID,
			ClientId:        auth.ClientID,
			ApiKeyId:        auth.APIKeyID,
			AuthRevision:    auth.AuthRevision,
		},
		Endpoint: endpoint,
		TargetId: targetID,
	})
	if err != nil {
		return routing.OpenContext{}, mapAdmitOpenRPCError(err)
	}
	return openContextFromProto(response.GetContext(), c.config.ClusterEpoch, current, auth, key)
}

func openContextFromProto(wire *controlv1.OpenContext, clusterEpoch string, control Status, auth routing.AuthContext, key routing.BindingKey) (routing.OpenContext, error) {
	if wire == nil {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned no Open context", ErrOpenUnavailable)
	}
	if wire.GetClusterEpoch() != clusterEpoch || wire.GetAuthorityId() != control.AuthorityID || wire.GetAttemptId() == "" {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned a mismatched Open identity", ErrOpenUnavailable)
	}
	if wire.GetIngressGatewayId() != control.GatewayID || wire.GetIngressGatewayInstanceId() != control.GatewayInstanceID || wire.GetIngressControlSessionId() != control.ControlSessionID {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned a mismatched ingress identity", ErrOpenUnavailable)
	}
	if wire.GetOwnerControlSessionId() == "" {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned no owner control session", ErrOpenUnavailable)
	}
	wireAuth := wire.GetAuth()
	if wireAuth == nil || wireAuth.GetClientSessionId() != auth.ClientSessionID || wireAuth.GetClientId() != auth.ClientID || wireAuth.GetApiKeyId() != auth.APIKeyID || wireAuth.GetAuthRevision() != auth.AuthRevision {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned a mismatched auth context", ErrOpenUnavailable)
	}
	binding, err := liveBindingFromProto(wire.GetBinding(), "", "", true)
	if err != nil {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned an invalid live binding", ErrOpenUnavailable)
	}
	if binding.Key != key {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned a mismatched binding key", ErrOpenUnavailable)
	}
	openContext, err := routing.NewForwardedOpenContext(
		wire.GetClusterEpoch(), wire.GetAuthorityId(), wire.GetAttemptId(), auth, binding,
		routing.ForwardingContext{
			IngressGatewayID:         wire.GetIngressGatewayId(),
			IngressGatewayInstanceID: wire.GetIngressGatewayInstanceId(),
			IngressControlSessionID:  wire.GetIngressControlSessionId(),
			OwnerControlSessionID:    wire.GetOwnerControlSessionId(),
			OwnerRelayAddress:        wire.GetOwnerRelayAddress(),
			ExpiresAt:                time.UnixMilli(wire.GetExpiresAtUnixMillis()),
		},
	)
	if err != nil {
		return routing.OpenContext{}, fmt.Errorf("%w: control returned an invalid Open context: %w", ErrOpenUnavailable, err)
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
