package authority

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/cagojeiger/relaygate/internal/controlstate"
)

var (
	ErrInvalidOpen     = errors.New("invalid Open admission request")
	ErrRouteNotFound   = errors.New("exact Open route not found")
	ErrOpenUnavailable = errors.New("Open admission unavailable")
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

// OpenContext is a quorum-confirmed, side-effect-free decision for one exact
// target. AttemptID is fresh for every successful decision; owner-side
// reservation and consumption are outside this authority lane.
type OpenContext struct {
	ClusterEpoch string
	AuthorityID  string
	AttemptID    string
	Auth         AuthContext
	Binding      controlstate.BindingSlot
	attempt      *attemptToken
}

type attemptToken struct {
	consumed atomic.Bool
}

// NewOpenContext constructs a locally consumable context. The single-use
// token is process-local and intentionally is not serialized; any trusted wire
// decoder must call this constructor to initialize a fresh token.
func NewOpenContext(
	clusterEpoch, authorityID, attemptID string,
	auth AuthContext,
	binding controlstate.BindingSlot,
) (OpenContext, error) {
	for _, identity := range []struct {
		field string
		value string
	}{
		{field: "cluster_epoch", value: clusterEpoch},
		{field: "authority_id", value: authorityID},
		{field: "attempt_id", value: attemptID},
	} {
		if identity.value == "" || len(identity.value) > controlstate.MaxIdentityBytes {
			return OpenContext{}, fmt.Errorf("%w: %s must be 1..%d bytes", ErrInvalidOpen, identity.field, controlstate.MaxIdentityBytes)
		}
	}
	if err := auth.Validate(); err != nil {
		return OpenContext{}, err
	}
	if err := binding.Key.Validate(); err != nil || binding.Key.ClientID != auth.ClientID ||
		binding.Generation == 0 || binding.Ref == nil {
		return OpenContext{}, fmt.Errorf("%w: exact committed live binding is required", ErrInvalidOpen)
	}
	if err := binding.Ref.Validate(); err != nil {
		return OpenContext{}, fmt.Errorf("%w: invalid binding ref: %v", ErrInvalidOpen, err)
	}
	ref := *binding.Ref
	binding.Ref = &ref
	return OpenContext{
		ClusterEpoch: clusterEpoch,
		AuthorityID:  authorityID,
		AttemptID:    attemptID,
		Auth:         auth,
		Binding:      binding,
		attempt:      &attemptToken{},
	}, nil
}

// Clone returns an immutable value copy that shares the same single-use token.
// Exactly one TryConsume call can succeed across the original and all clones.
func (c OpenContext) Clone() OpenContext {
	clone := c
	if c.Binding.Ref != nil {
		ref := *c.Binding.Ref
		clone.Binding.Ref = &ref
	}
	return clone
}

// TryConsume atomically consumes this attempt once across all value copies.
// A zero-value or wire-decoded context that bypassed NewOpenContext fails
// closed.
func (c OpenContext) TryConsume() bool {
	return c.attempt != nil && c.attempt.consumed.CompareAndSwap(false, true)
}

// Validate checks the bounded wire identity contract for Open admission.
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
		if identity.value == "" || len(identity.value) > controlstate.MaxIdentityBytes {
			return fmt.Errorf("%w: %s must be 1..%d bytes", ErrInvalidOpen, identity.field, controlstate.MaxIdentityBytes)
		}
	}
	return nil
}

// ExactBindingKey derives the only BindingKey eligible for this request. It
// performs no wildcard selection or cross-client fallback.
func ExactBindingKey(auth AuthContext, endpoint, targetID string) (controlstate.BindingKey, error) {
	if err := auth.Validate(); err != nil {
		return controlstate.BindingKey{}, err
	}
	key := controlstate.BindingKey{
		ClientID:        auth.ClientID,
		EndpointPattern: endpoint,
		TargetID:        targetID,
	}
	if err := key.Validate(); err != nil {
		return controlstate.BindingKey{}, fmt.Errorf("%w: %v", ErrInvalidOpen, err)
	}
	return key, nil
}
