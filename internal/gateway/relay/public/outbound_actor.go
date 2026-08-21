package relaygrpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
)

const (
	outboundQueueCapacity        = 32
	outboundPayloadQueueCapacity = 32
)

type outboundMessage struct {
	ctx      context.Context //nolint:containedctx // The outbound actor must preserve each Send caller's cancellation contract until dequeued.
	response *relayv1.ConnectResponse
	result   chan error
}

type outboundPayloadState uint32

const (
	outboundPayloadQueued outboundPayloadState = iota
	outboundPayloadSending
	outboundPayloadAborting
	outboundPayloadCompleted
	outboundPayloadCanceled
)

type outboundPayload struct {
	ctx      context.Context //nolint:containedctx // Pipe lifetime cancellation decides whether a queued volatile frame is still deliverable.
	response *relayv1.ConnectResponse
	result   chan error
	state    atomic.Uint32
}

// outboundActor is the only code allowed to call the gRPC stream's Send
// method. Control and payload use separate bounded lanes so terminal/control
// messages can bypass payload pressure. Every queued or in-flight payload also
// holds one Service-owned process-wide slot.
type outboundActor struct {
	stream         grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]
	ctx            context.Context //nolint:containedctx // Actor owns a stream-root context for queue shutdown.
	cancel         context.CancelFunc
	queue          chan outboundMessage
	payloadQueue   chan *outboundPayload
	payloadSlots   chan struct{}
	payloadTimeout time.Duration
	failed         chan error
	done           chan struct{}

	payloadEnqueueGate chan struct{}
	mu                 sync.Mutex
	failure            error
	failOnce           sync.Once
}

func newOutboundActor(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse], payloadSlots chan struct{}, payloadTimeout time.Duration) *outboundActor {
	ctx, cancel := context.WithCancel(stream.Context())
	a := &outboundActor{
		stream:             stream,
		ctx:                ctx,
		cancel:             cancel,
		queue:              make(chan outboundMessage, outboundQueueCapacity),
		payloadQueue:       make(chan *outboundPayload, outboundPayloadQueueCapacity),
		payloadSlots:       payloadSlots,
		payloadTimeout:     payloadTimeout,
		payloadEnqueueGate: make(chan struct{}, 1),
		failed:             make(chan error, 1),
		done:               make(chan struct{}),
	}
	a.payloadEnqueueGate <- struct{}{}
	go a.run()
	return a
}

func (a *outboundActor) run() {
	for {
		// A non-blocking control read establishes control preference whenever
		// both lanes already contain work.
		select {
		case <-a.ctx.Done():
			a.fail(a.ctx.Err())
			return
		case message := <-a.queue:
			if !a.sendControl(message) {
				return
			}
			continue
		default:
		}

		select {
		case <-a.ctx.Done():
			a.fail(a.ctx.Err())
			return
		case message := <-a.queue:
			if !a.sendControl(message) {
				return
			}
		case payload := <-a.payloadQueue:
			// A payload chosen concurrently with newly-ready control work waits
			// until the current control backlog has drained.
			for {
				select {
				case <-a.ctx.Done():
					completeOutboundPayload(payload, a.ctx.Err())
					a.releasePayloadSlot()
					a.fail(a.ctx.Err())
					return
				case message := <-a.queue:
					if !a.sendControl(message) {
						completeOutboundPayload(payload, a.err())
						a.releasePayloadSlot()
						return
					}
					continue
				default:
				}
				break
			}
			if err := a.sendQueuedPayload(payload); err != nil {
				a.fail(err)
				return
			}
		}
	}
}

func (a *outboundActor) sendControl(message outboundMessage) bool {
	if err := message.ctx.Err(); err != nil {
		completeOutbound(message, err)
		return true
	}
	err := a.stream.Send(message.response)
	completeOutbound(message, err)
	if err != nil {
		a.fail(err)
		return false
	}
	return true
}

func (a *outboundActor) sendQueuedPayload(payload *outboundPayload) error {
	defer a.releasePayloadSlot()
	if !payload.state.CompareAndSwap(uint32(outboundPayloadQueued), uint32(outboundPayloadSending)) {
		completeOutboundPayload(payload, payload.ctx.Err())
		return nil
	}
	if err := payload.ctx.Err(); err != nil {
		completeOutboundPayload(payload, err)
		return nil
	}
	select {
	case <-a.done:
		err := a.err()
		completeOutboundPayload(payload, err)
		return err
	default:
	}
	err := a.stream.Send(payload.response)
	completeOutboundPayload(payload, err)
	return err
}

func completeOutboundPayload(payload *outboundPayload, err error) {
	payload.state.Store(uint32(outboundPayloadCompleted))
	if payload.result == nil {
		return
	}
	payload.result <- err
}

func completeOutbound(message outboundMessage, err error) {
	if message.result == nil {
		return
	}
	message.result <- err
}

func (a *outboundActor) send(ctx context.Context, response *relayv1.ConnectResponse) error {
	if ctx == nil || response == nil {
		return errInvalidListenerDecision
	}
	result := make(chan error, 1)
	message := outboundMessage{ctx: ctx, response: response, result: result}
	select {
	case a.queue <- message:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.done:
		return a.err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-a.done:
		return a.err()
	}
}

// sendPayload returns after the local gRPC write completes. That keeps the
// opening layer's delivery context alive through queueing and actor dispatch;
// it still does not imply peer application observation or durable delivery.
func (a *outboundActor) sendPayload(ctx context.Context, response *relayv1.ConnectResponse) error {
	if ctx == nil || response == nil {
		return fmt.Errorf("%w: invalid outbound payload", localbinding.ErrEndpointUnavailable)
	}
	payloadCtx, cancelPayload := context.WithCancel(ctx)
	defer cancelPayload()
	timer := time.NewTimer(a.payloadTimeout)
	defer timer.Stop()

	select {
	case a.payloadSlots <- struct{}{}:
	case <-timer.C:
		return localbinding.ErrPayloadBackpressure
	case <-payloadCtx.Done():
		return payloadCtx.Err()
	case <-a.ctx.Done():
		return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, a.ctx.Err())
	case <-a.done:
		return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, a.err())
	}

	select {
	case <-a.payloadEnqueueGate:
	case <-timer.C:
		a.releasePayloadSlot()
		return localbinding.ErrPayloadBackpressure
	case <-payloadCtx.Done():
		a.releasePayloadSlot()
		return payloadCtx.Err()
	case <-a.ctx.Done():
		a.releasePayloadSlot()
		return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, a.ctx.Err())
	}
	releaseGate := func() { a.payloadEnqueueGate <- struct{}{} }

	result := make(chan error, 1)
	payload := &outboundPayload{ctx: payloadCtx, response: response, result: result}
	select {
	case <-a.done:
		releaseGate()
		a.releasePayloadSlot()
		return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, a.err())
	default:
	}
	select {
	case a.payloadQueue <- payload:
		releaseGate()
	case <-timer.C:
		releaseGate()
		a.releasePayloadSlot()
		return localbinding.ErrPayloadBackpressure
	case <-payloadCtx.Done():
		releaseGate()
		a.releasePayloadSlot()
		return payloadCtx.Err()
	case <-a.ctx.Done():
		releaseGate()
		a.releasePayloadSlot()
		return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, a.ctx.Err())
	}

	select {
	case err := <-result:
		return err
	case <-timer.C:
		cancelPayload()
		return a.abortPayload(payload, localbinding.ErrPayloadBackpressure)
	case <-payloadCtx.Done():
		return a.abortPayload(payload, payloadCtx.Err())
	case <-a.done:
		cancelPayload()
		return a.abortPayload(payload, fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, a.err()))
	}
}

// abortPayload makes timeout/cancellation race atomically with local Send
// ownership. A queued frame is canceled without delivery. Once Send owns the
// frame, the stream is failed and its exact Send result is joined before this
// call returns, so a reported failure cannot be followed by a late local write.
func (a *outboundActor) abortPayload(payload *outboundPayload, cause error) error {
	for {
		switch outboundPayloadState(payload.state.Load()) {
		case outboundPayloadQueued:
			if payload.state.CompareAndSwap(uint32(outboundPayloadQueued), uint32(outboundPayloadCanceled)) {
				return cause
			}
		case outboundPayloadSending:
			if payload.state.CompareAndSwap(uint32(outboundPayloadSending), uint32(outboundPayloadAborting)) {
				a.fail(cause)
				return <-payload.result
			}
		case outboundPayloadAborting, outboundPayloadCompleted:
			return <-payload.result
		case outboundPayloadCanceled:
			return cause
		}
	}
}

func (a *outboundActor) failures() <-chan error {
	return a.failed
}

func (a *outboundActor) fail(err error) {
	if err == nil {
		err = context.Canceled
	}
	a.cancel()
	<-a.payloadEnqueueGate
	a.failOnce.Do(func() {
		a.mu.Lock()
		a.failure = err
		a.mu.Unlock()
		a.failed <- err
		close(a.done)
		for {
			select {
			case payload := <-a.payloadQueue:
				completeOutboundPayload(payload, err)
				a.releasePayloadSlot()
			default:
				return
			}
		}
	})
	a.payloadEnqueueGate <- struct{}{}
}

func (a *outboundActor) err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failure == nil {
		return context.Canceled
	}
	return a.failure
}

func (a *outboundActor) close() {
	a.fail(context.Canceled)
}

func (a *outboundActor) releasePayloadSlot() {
	<-a.payloadSlots
}

// streamPipeEndpoint is the one generic caller endpoint owned by an
// authenticated Connect stream. Listener endpoints on the same stream reuse
// its payload delivery path while retaining their listener-specific terminal
// message.
