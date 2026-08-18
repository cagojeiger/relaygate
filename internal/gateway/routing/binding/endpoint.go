package localbinding

import (
	"context"
	"errors"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

var (
	// ErrOfferRejected is an explicit listener application rejection.
	ErrOfferRejected = errors.New("listener rejected offer")
	// ErrEndpointUnavailable means the listener transport or stream ended
	// without proving that the listener session itself was retired.
	ErrEndpointUnavailable = errors.New("listener endpoint unavailable")
	// ErrPayloadBackpressure means a payload could not be delivered within the
	// endpoint's bounded flow-control capacity. The owning Pipe must become
	// terminal rather than silently dropping the payload.
	ErrPayloadBackpressure = errors.New("payload backpressure exhausted")
)

const MaxPayloadBytes = 60 << 10

// PipePayload is one opaque, volatile payload for an exact live Pipe.
type PipePayload struct {
	PipeID string
	Data   []byte
}

// PayloadEndpoint is the protocol-neutral payload delivery boundary.
type PayloadEndpoint interface {
	DeliverPayload(context.Context, PipePayload) error
}

// CallerEndpoint is the owner-to-caller boundary for payload delivery and
// best-effort accepted-Pipe terminal propagation.
type CallerEndpoint interface {
	PayloadEndpoint
	TerminatePipe(context.Context, string) error
}

// Offer asks the listener application to provisionally accept one exact
// attempt. A nil error means provisional acceptance only; no listener handle
// may be exposed until Confirmation is acknowledged.
type Offer struct {
	AttemptID string
	Caller    clientsession.Ref
	Binding   routing.LiveBinding
}

// Confirmation tells a provisionally accepting listener that the owner has
// crossed the Open linearization point and assigned the exact PipeID.
type Confirmation struct {
	AttemptID string
	PipeID    string
}

// Termination is a best-effort, idempotent terminal signal for an attempt or
// accepted pipe. PipeID is empty when the owner never accepted the attempt.
type Termination struct {
	AttemptID string
	PipeID    string
}

// ListenerEndpoint is the protocol-neutral owner-to-listener boundary. Wire
// implementations must make all methods responsive to ctx cancellation.
type ListenerEndpoint interface {
	PayloadEndpoint
	Offer(context.Context, Offer) error
	Confirm(context.Context, Confirmation) error
	Terminate(context.Context, Termination) error
}
