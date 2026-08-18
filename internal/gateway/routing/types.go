package routing

import (
	"errors"
	"fmt"
)

const (
	MaxIdentityBytes              = 128
	MaxEndpointPatternBytes       = 1024
	MaxListenerBindingsPerGateway = 512
)

var (
	ErrInvalid  = errors.New("invalid live route")
	ErrConflict = errors.New("live route conflict")
	ErrCapacity = errors.New("live route capacity reached")
)

// BindingKey is the exact, client-scoped route lookup key. It is a volatile
// routing identity and is never part of Raft state.
type BindingKey struct {
	ClientID        string
	EndpointPattern string
	TargetID        string
}

func (k BindingKey) Validate() error {
	if err := ValidateIdentity("client_id", k.ClientID); err != nil {
		return err
	}
	if k.EndpointPattern == "" || len(k.EndpointPattern) > MaxEndpointPatternBytes {
		return fmt.Errorf("%w: endpoint_pattern must be 1..%d bytes", ErrInvalid, MaxEndpointPatternBytes)
	}
	if err := ValidateIdentity("target_id", k.TargetID); err != nil {
		return err
	}
	return nil
}

// ListenerBindingRef identifies one current listener owned by one Gateway
// process instance. A rebind or process restart always creates a new identity.
type ListenerBindingRef struct {
	GatewayID         string
	GatewayInstanceID string
	ListenerBindingID string
}

func (r ListenerBindingRef) Validate() error {
	if err := ValidateIdentity("gateway_id", r.GatewayID); err != nil {
		return err
	}
	if err := ValidateIdentity("gateway_instance_id", r.GatewayInstanceID); err != nil {
		return err
	}
	return ValidateIdentity("listener_binding_id", r.ListenerBindingID)
}

// LiveBinding is a current, live-only declaration. Absence is represented by
// no map entry; there is no generation, tombstone, or historical slot.
type LiveBinding struct {
	Key BindingKey
	Ref ListenerBindingRef
}

func (b LiveBinding) Validate() error {
	if err := b.Key.Validate(); err != nil {
		return err
	}
	if err := b.Ref.Validate(); err != nil {
		return err
	}
	return nil
}

func ValidateIdentity(field, value string) error {
	if value == "" || len(value) > MaxIdentityBytes {
		return fmt.Errorf("%w: %s must be 1..%d bytes", ErrInvalid, field, MaxIdentityBytes)
	}
	return nil
}
