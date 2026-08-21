package relaygate

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
)

// Config describes one authenticated Relay connection. Plaintext transport is
// an explicit local-development choice and is accepted only for loopback
// addresses.
type Config struct {
	Address   string
	ClientID  string
	APIKeyID  string
	TLSConfig *tls.Config
	Insecure  bool
	apiKey    string
}

// NewConfig creates a Config without exposing the bearer secret as a public
// struct field. With no transport override, Connect uses TLS system roots.
func NewConfig(address, clientID, apiKeyID, apiKey string) Config {
	return Config{Address: address, ClientID: clientID, APIKeyID: apiKeyID, apiKey: apiKey}
}

// WithTLSConfig returns a copy that owns a clone of tlsConfig.
func (c Config) WithTLSConfig(tlsConfig *tls.Config) Config {
	if tlsConfig == nil {
		c.TLSConfig = nil
	} else {
		c.TLSConfig = tlsConfig.Clone()
	}
	c.Insecure = false
	return c
}

// WithInsecureLocal enables plaintext transport for a loopback-only local
// development connection.
func (c Config) WithInsecureLocal() Config {
	c.TLSConfig = nil
	c.Insecure = true
	return c
}

func (c Config) String() string {
	return fmt.Sprintf("Config{Address:%q ClientID:%q APIKeyID:%q APIKey:<redacted> TLSConfig:%s Insecure:%t}",
		c.Address, c.ClientID, c.APIKeyID, tlsConfigState(c.TLSConfig), c.Insecure)
}

func (c Config) GoString() string { return "relaygate." + c.String() }

func tlsConfigState(config *tls.Config) string {
	if config == nil {
		return "<system-roots>"
	}
	return "<custom>"
}

// Session is the authenticated identity fixed to a Client connection.
type Session struct {
	ID           string
	ClientID     string
	APIKeyID     string
	AuthRevision string
}

// BindingFailure is a stable operation-local Bind or Unbind failure. It does
// not imply that the authenticated Client session ended.
type BindingFailure uint8

const (
	BindingFailureInvalidRequest BindingFailure = iota + 1
	BindingFailureCapacityReached
	BindingFailureConflict
	BindingFailureUnavailable
)

var (
	ErrBindFailed   = errors.New("relaygate: Bind failed")
	ErrUnbindFailed = errors.New("relaygate: Unbind failed")
)

type BindError struct {
	Failure  BindingFailure
	Endpoint string
	Target   string
}

func (e *BindError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("relaygate: Bind %q target %q failed (%s)", e.Endpoint, e.Target, e.Failure)
}

func (e *BindError) Is(target error) bool { return e != nil && target == ErrBindFailed }

type UnbindError struct {
	Failure    BindingFailure
	ListenerID string
}

func (e *UnbindError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("relaygate: Unbind listener %q failed (%s)", e.ListenerID, e.Failure)
}

func (e *UnbindError) Is(target error) bool { return e != nil && target == ErrUnbindFailed }

func (f BindingFailure) String() string {
	switch f {
	case BindingFailureInvalidRequest:
		return "invalid request"
	case BindingFailureCapacityReached:
		return "capacity reached"
	case BindingFailureConflict:
		return "conflict"
	case BindingFailureUnavailable:
		return "unavailable"
	default:
		return "unspecified"
	}
}

type OpenOutcome uint8

const (
	OpenOutcomeFailed OpenOutcome = iota + 1
	OpenOutcomeCancelled
	OpenOutcomeUnknown
	OpenOutcomeRejected
)

type OpenFailure uint8

const (
	OpenFailureInvalidRequest OpenFailure = iota + 1
	OpenFailureRouteNotFound
	OpenFailureUnavailable
	OpenFailureCapacityReached
	OpenFailureListenerRejected
	OpenFailureDeadlineExceeded
	OpenFailureCancelled
)

var (
	ErrOpenFailed            = errors.New("relaygate: Open failed")
	ErrOpenCancelled         = errors.New("relaygate: Open cancelled")
	ErrOpenUnknown           = errors.New("relaygate: Open outcome unknown")
	ErrOpenDuplicateInFlight = errors.New("relaygate: Open request is already in flight")
	ErrClientClosed          = errors.New("relaygate: client closed")
	ErrListenerEnded         = errors.New("relaygate: listener ended")
	ErrPipeClosed            = errors.New("relaygate: pipe closed")
)

type pipeNotOwnedError struct{}

func (pipeNotOwnedError) Error() string { return "relaygate: pipe is not owned by this session" }
func (pipeNotOwnedError) Unwrap() error { return ErrPipeClosed }

// ErrPipeNotOwned identifies a Close rejected because the current session no
// longer owns the Pipe. It unwraps to ErrPipeClosed for existing callers.
var ErrPipeNotOwned error = pipeNotOwnedError{}

// OpenError reports the caller-visible terminal outcome of an Open. Unknown
// means the same logical Pipe cannot safely be recovered or resumed.
type OpenError struct {
	Outcome  OpenOutcome
	Failure  OpenFailure
	Endpoint string
	Target   string
	Cause    error
}

func (e *OpenError) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch e.Outcome {
	case OpenOutcomeCancelled:
		return fmt.Sprintf("relaygate: Open %q target %q cancelled", e.Endpoint, e.Target)
	case OpenOutcomeUnknown:
		return fmt.Sprintf("relaygate: Open %q target %q has unknown outcome", e.Endpoint, e.Target)
	case OpenOutcomeRejected:
		return fmt.Sprintf("relaygate: Open %q target %q rejected because the request is already in flight", e.Endpoint, e.Target)
	default:
		return fmt.Sprintf("relaygate: Open %q target %q failed (%s)", e.Endpoint, e.Target, e.Failure)
	}
}

func (e *OpenError) Unwrap() error { return e.Cause }

func (e *OpenError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrOpenFailed:
		return e.Outcome == OpenOutcomeFailed
	case ErrOpenCancelled, context.Canceled:
		return e.Outcome == OpenOutcomeCancelled
	case ErrOpenUnknown:
		return e.Outcome == OpenOutcomeUnknown
	case ErrOpenDuplicateInFlight:
		return e.Outcome == OpenOutcomeRejected
	case context.DeadlineExceeded:
		return e.Outcome == OpenOutcomeFailed && e.Failure == OpenFailureDeadlineExceeded
	default:
		return false
	}
}

func (f OpenFailure) String() string {
	switch f {
	case OpenFailureInvalidRequest:
		return "invalid request"
	case OpenFailureRouteNotFound:
		return "route not found"
	case OpenFailureUnavailable:
		return "unavailable"
	case OpenFailureCapacityReached:
		return "capacity reached"
	case OpenFailureListenerRejected:
		return "listener rejected"
	case OpenFailureDeadlineExceeded:
		return "deadline exceeded"
	case OpenFailureCancelled:
		return "cancelled"
	default:
		return "unspecified"
	}
}

type PipePayloadFailure uint8

const (
	PipePayloadInvalidRequest PipePayloadFailure = iota + 1
	PipePayloadNotOwned
	PipePayloadBackpressure
	PipePayloadUnavailable
)

// PipeError is the first participant-local terminal error for a Pipe.
type PipeError struct {
	Failure PipePayloadFailure
}

func (e *PipeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("relaygate: Pipe payload rejected (%d)", e.Failure)
}

// DeliveryOutcome is the sender-observed terminal state of one exact payload.
type DeliveryOutcome uint8

const (
	DeliveryReceived DeliveryOutcome = iota + 1
	DeliveryNotSent
	DeliveryRejected
	DeliveryUnknown
)

var (
	ErrDeliveryNotSent  = errors.New("relaygate: payload was not sent")
	ErrDeliveryRejected = errors.New("relaygate: payload was rejected")
	ErrDeliveryUnknown  = errors.New("relaygate: payload delivery is unknown")
)

// DeliveryError preserves the retry-relevant outcome for one Send.
type DeliveryError struct {
	PayloadID string
	Outcome   DeliveryOutcome
	Cause     error
}

func (e *DeliveryError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("relaygate: payload %s ended with delivery outcome %d: %v", e.PayloadID, e.Outcome, e.Cause)
}

func (e *DeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *DeliveryError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrDeliveryNotSent:
		return e.Outcome == DeliveryNotSent
	case ErrDeliveryRejected:
		return e.Outcome == DeliveryRejected
	case ErrDeliveryUnknown:
		return e.Outcome == DeliveryUnknown
	default:
		return errors.Is(e.Cause, target)
	}
}
