package authority

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

const MaxRelayAddressBytes = 1024

var (
	ErrInvalidOpen     = errors.New("invalid open admission request")
	ErrRouteNotFound   = errors.New("exact open route not found")
	ErrOpenUnavailable = errors.New("open admission unavailable")
)

// AuthContext is the authenticated caller identity captured by one Open
// admission decision. It never selects a ClientID independently of the caller
// session.
type AuthContext struct {
	ClientSessionID string
	ClientID        string
	APIKeyID        string
	AuthRevision    string
}

// OpenContext is one current-directory admission decision. It is not durable:
// OwnerControlSessionID binds the decision to the owner session which declared
// Binding, and the owner rejects it once that session is no longer current.
// AttemptID remains a process-local single-use capability.
type OpenContext struct {
	ClusterEpoch             string
	AuthorityID              string
	AttemptID                string
	Auth                     AuthContext
	Binding                  routing.LiveBinding
	OwnerControlSessionID    string
	IngressGatewayID         string
	IngressGatewayInstanceID string
	IngressControlSessionID  string
	OwnerRelayAddress        string
	ExpiresAt                time.Time
	attempt                  *attemptToken
}

type ForwardingContext struct {
	IngressGatewayID         string
	IngressGatewayInstanceID string
	IngressControlSessionID  string
	OwnerControlSessionID    string
	OwnerRelayAddress        string
	ExpiresAt                time.Time
}

type attemptToken struct {
	consumed atomic.Bool
}

// NewOpenContext constructs a locally consumable context. The single-use token
// is process-local and intentionally is not serialized; any trusted wire
// decoder must call this constructor to initialize a fresh token.
func NewOpenContext(
	clusterEpoch, authorityID, attemptID string,
	auth AuthContext,
	binding routing.LiveBinding,
	ownerControlSessionID string,
) (OpenContext, error) {
	for _, identity := range []struct {
		field string
		value string
	}{
		{field: "cluster_epoch", value: clusterEpoch},
		{field: "authority_id", value: authorityID},
		{field: "attempt_id", value: attemptID},
		{field: "owner_control_session_id", value: ownerControlSessionID},
	} {
		if err := routing.ValidateIdentity(identity.field, identity.value); err != nil {
			return OpenContext{}, fmt.Errorf("%w: %v", ErrInvalidOpen, err)
		}
	}
	if err := auth.Validate(); err != nil {
		return OpenContext{}, err
	}
	if err := binding.Validate(); err != nil || binding.Key.ClientID != auth.ClientID {
		return OpenContext{}, fmt.Errorf("%w: exact current live binding is required", ErrInvalidOpen)
	}
	return OpenContext{
		ClusterEpoch:          clusterEpoch,
		AuthorityID:           authorityID,
		AttemptID:             attemptID,
		Auth:                  auth,
		Binding:               binding,
		OwnerControlSessionID: ownerControlSessionID,
		attempt:               &attemptToken{},
	}, nil
}

// NewForwardedOpenContext constructs a single-use context carrying the exact
// ingress and owner control identities plus the live owner's advertised relay
// address. Evaluating expiry remains an owner-side responsibility.
func NewForwardedOpenContext(
	clusterEpoch, authorityID, attemptID string,
	auth AuthContext,
	binding routing.LiveBinding,
	forwarding ForwardingContext,
) (OpenContext, error) {
	open, err := NewOpenContext(clusterEpoch, authorityID, attemptID, auth, binding, forwarding.OwnerControlSessionID)
	if err != nil {
		return OpenContext{}, err
	}
	for _, identity := range []struct {
		field string
		value string
	}{
		{field: "ingress_gateway_id", value: forwarding.IngressGatewayID},
		{field: "ingress_gateway_instance_id", value: forwarding.IngressGatewayInstanceID},
		{field: "ingress_control_session_id", value: forwarding.IngressControlSessionID},
	} {
		if err := routing.ValidateIdentity(identity.field, identity.value); err != nil {
			return OpenContext{}, fmt.Errorf("%w: %v", ErrInvalidOpen, err)
		}
	}
	if err := ValidateRelayAddress(forwarding.OwnerRelayAddress); err != nil {
		return OpenContext{}, fmt.Errorf("%w: owner_relay_address: %w", ErrInvalidOpen, err)
	}
	if forwarding.ExpiresAt.UnixMilli() <= 0 {
		return OpenContext{}, fmt.Errorf("%w: expires_at must be a positive Unix time", ErrInvalidOpen)
	}
	open.IngressGatewayID = forwarding.IngressGatewayID
	open.IngressGatewayInstanceID = forwarding.IngressGatewayInstanceID
	open.IngressControlSessionID = forwarding.IngressControlSessionID
	open.OwnerRelayAddress = forwarding.OwnerRelayAddress
	open.ExpiresAt = forwarding.ExpiresAt
	return open, nil
}

func ValidateRelayAddress(address string) error {
	if address == "" || len(address) > MaxRelayAddressBytes {
		return fmt.Errorf("relay address must be 1..%d bytes", MaxRelayAddressBytes)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid relay address: %w", err)
	}
	if host == "" || port == "" {
		return fmt.Errorf("relay address must include a host and port")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("relay address cannot use an unspecified host")
	}
	return nil
}

// Clone returns an immutable value copy that shares the same single-use token.
// Exactly one TryConsume call can succeed across the original and all clones.
func (c OpenContext) Clone() OpenContext { return c }

// TryConsume atomically consumes this attempt once across all value copies.
// A zero-value or wire-decoded context that bypassed NewOpenContext fails
// closed.
func (c OpenContext) TryConsume() bool {
	return c.attempt != nil && c.attempt.consumed.CompareAndSwap(false, true)
}

func (a AuthContext) Validate() error {
	for _, identity := range []struct {
		field string
		value string
	}{
		{field: "client_session_id", value: a.ClientSessionID},
		{field: "client_id", value: a.ClientID},
		{field: "api_key_id", value: a.APIKeyID},
		{field: "auth_revision", value: a.AuthRevision},
	} {
		if err := routing.ValidateIdentity(identity.field, identity.value); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidOpen, err)
		}
	}
	return nil
}

// ExactBindingKey derives the only BindingKey eligible for this request. It
// performs no wildcard selection or cross-client fallback.
func ExactBindingKey(auth AuthContext, endpoint, targetID string) (routing.BindingKey, error) {
	if err := auth.Validate(); err != nil {
		return routing.BindingKey{}, err
	}
	key := routing.BindingKey{ClientID: auth.ClientID, EndpointPattern: endpoint, TargetID: targetID}
	if err := key.Validate(); err != nil {
		return routing.BindingKey{}, fmt.Errorf("%w: %w", ErrInvalidOpen, err)
	}
	return key, nil
}
