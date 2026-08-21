package opening

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

func snapshotOf(e *entry) Snapshot {
	return Snapshot{
		AttemptID: e.attemptID,
		PipeID:    e.pipeID,
		Caller:    e.caller,
		Listener:  e.listener,
		Binding:   cloneSlot(e.binding),
		State:     e.state,
		Err:       e.terminalErr,
	}
}

func classifyAdmissionError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, routing.ErrInvalidOpen):
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	case errors.Is(err, routing.ErrRouteNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	default:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

func classifyReservationError(err error) error {
	switch {
	case errors.Is(err, ErrContextExpired), errors.Is(err, ErrAttemptReplay), errors.Is(err, ErrCapacity):
		return err
	case errors.Is(err, ErrInvalid):
		return err
	case errors.Is(err, localbinding.ErrInvalid):
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	case errors.Is(err, localbinding.ErrCapacity):
		return fmt.Errorf("%w: %w", ErrCapacity, err)
	case errors.Is(err, localbinding.ErrAttemptUsed):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case errors.Is(err, localbinding.ErrNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	case errors.Is(err, localbinding.ErrSessionEnded):
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	default:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

func classifyRemoteError(ctx context.Context, err error) error {
	if errors.Is(err, ErrUnknown) {
		return err
	}
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, ErrInvalid), errors.Is(err, ErrCapacity), errors.Is(err, ErrNotFound),
		errors.Is(err, ErrUnavailable), errors.Is(err, ErrListenerRejected), errors.Is(err, ErrDeadline),
		errors.Is(err, ErrSessionEnded), errors.Is(err, ErrContextExpired), errors.Is(err, ErrAttemptReplay):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, context.Canceled):
		return context.Canceled
	default:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

func classifyOfferError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, ErrSessionEnded), errors.Is(err, localbinding.ErrSessionEnded):
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	case errors.Is(err, ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, localbinding.ErrEndpointUnavailable):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case errors.Is(err, localbinding.ErrOfferRejected):
		return fmt.Errorf("%w: %w", ErrListenerRejected, err)
	default:
		return fmt.Errorf("%w: %w", ErrListenerRejected, err)
	}
}

func classifyConfirmationError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return mapContextError(cause)
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrDeadline):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, localbinding.ErrSessionEnded), errors.Is(err, ErrSessionEnded):
		return fmt.Errorf("%w: %w", ErrSessionEnded, err)
	case errors.Is(err, localbinding.ErrEndpointUnavailable):
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

func classifyPayloadError(err error) error {
	if errors.Is(err, localbinding.ErrPayloadBackpressure) {
		return ErrPayloadBackpressure
	}
	return ErrUnavailable
}

func mapContextError(err error) error {
	switch {
	case errors.Is(err, ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrDeadline, err)
	case errors.Is(err, ErrSessionEnded):
		return ErrSessionEnded
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case err == nil:
		return ErrUnavailable
	default:
		return err
	}
}

func unknown(cause error) error {
	if cause == nil {
		return ErrUnknown
	}
	if errors.Is(cause, ErrUnknown) {
		return cause
	}
	return fmt.Errorf("%w: %w", ErrUnknown, cause)
}

func cloneReservation(reservation localbinding.Reservation) localbinding.Reservation {
	copy := reservation
	copy.Context = cloneOpenContext(reservation.Context)
	copy.Binding = cloneSlot(reservation.Binding)
	return copy
}

func cloneOpenContext(open routing.OpenContext) routing.OpenContext {
	return open.Clone()
}

func cloneSlot(slot routing.LiveBinding) routing.LiveBinding {
	return slot
}

func sameExactSlot(left, right routing.LiveBinding) bool {
	return left == right
}

func newPipeID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
