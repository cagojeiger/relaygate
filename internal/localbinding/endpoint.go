package localbinding

import (
	"context"
	"errors"

	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
)

var (
	// ErrOfferRejected is an explicit listener application rejection.
	ErrOfferRejected = errors.New("listener rejected offer")
	// ErrEndpointUnavailable means the listener transport or stream ended
	// without proving that the listener session itself was retired.
	ErrEndpointUnavailable = errors.New("listener endpoint unavailable")
)

// Offer asks the listener application to provisionally accept one exact
// attempt. A nil error means provisional acceptance only; no listener handle
// may be exposed until Confirmation is acknowledged.
type Offer struct {
	AttemptID string
	Caller    clientsession.Ref
	Binding   controlstate.BindingSlot
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
	Offer(context.Context, Offer) error
	Confirm(context.Context, Confirmation) error
	Terminate(context.Context, Termination) error
}
