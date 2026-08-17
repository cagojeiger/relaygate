package gatewayrelay

import (
	"context"
	"errors"
	"sync"
)

type sendCommand[T any] struct {
	ctx    context.Context //nolint:containedctx // The command owns its caller deadline until the serialized Send completes.
	value  T
	result chan error
}

// sendActor is the only goroutine allowed to call Send on one Gateway relay
// stream. A command timeout fails the whole volatile Pipe: gRPC cannot cancel
// one blocked Send without canceling its stream.
type sendActor[T any] struct {
	ctx      context.Context //nolint:containedctx // The actor owns one stream-lifetime context.
	sendWire func(T) error
	onFail   func(error)
	queue    chan sendCommand[T]

	failOnce sync.Once
	failed   chan struct{}
	joined   chan struct{}
	errMu    sync.Mutex
	err      error
}

func newSendActor[T any](ctx context.Context, sendWire func(T) error, onFail func(error)) *sendActor[T] {
	a := &sendActor[T]{
		ctx:      ctx,
		sendWire: sendWire,
		onFail:   onFail,
		queue:    make(chan sendCommand[T], 1),
		failed:   make(chan struct{}),
		joined:   make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *sendActor[T]) run() {
	defer close(a.joined)
	for {
		select {
		case <-a.ctx.Done():
			a.fail(a.ctx.Err())
			return
		case <-a.failed:
			return
		case command := <-a.queue:
			if err := command.ctx.Err(); err != nil {
				command.result <- err
				a.fail(err)
				return
			}
			err := a.sendWire(command.value)
			command.result <- err
			if err != nil {
				a.fail(err)
				return
			}
		}
	}
}

func (a *sendActor[T]) send(ctx context.Context, value T) error {
	if ctx == nil {
		return ErrInvalid
	}
	result := make(chan error, 1)
	command := sendCommand[T]{ctx: ctx, value: value, result: result}
	select {
	case a.queue <- command:
	case <-ctx.Done():
		a.fail(ctx.Err())
		return ctx.Err()
	case <-a.failed:
		return a.failure()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		a.fail(ctx.Err())
		return ctx.Err()
	case <-a.failed:
		return a.failure()
	}
}

func (a *sendActor[T]) stop(cause error) {
	if cause == nil {
		cause = context.Canceled
	}
	a.fail(cause)
}

func (a *sendActor[T]) fail(cause error) {
	if cause == nil {
		cause = ErrClosed
	}
	a.failOnce.Do(func() {
		a.errMu.Lock()
		a.err = cause
		a.errMu.Unlock()
		close(a.failed)
		if a.onFail != nil {
			a.onFail(cause)
		}
	})
}

func (a *sendActor[T]) failure() error {
	a.errMu.Lock()
	defer a.errMu.Unlock()
	if a.err == nil {
		return ErrClosed
	}
	return a.err
}

func isSendContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
