package gatewayrelay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const maxGatewayMessageBytes = 64 << 10

var (
	_ opening.RemoteOpener   = (*Client)(nil)
	_ opening.RemoteEndpoint = (*remoteEndpoint)(nil)
)

type Client struct {
	connectTimeout time.Duration
	openTimeout    time.Duration
	slots          chan struct{}
	ctx            context.Context //nolint:containedctx // The client owns one process-lifetime cancellation root.
	cancel         context.CancelCauseFunc

	mu         sync.Mutex
	closed     bool
	active     sync.WaitGroup
	closedDone chan struct{}
}

func NewClient(connectTimeout, openTimeout time.Duration, maxPipes uint32) (*Client, error) {
	if connectTimeout <= 0 || openTimeout <= 0 {
		return nil, fmt.Errorf("%w: connect and Open timeouts must be positive", ErrInvalid)
	}
	if maxPipes == 0 {
		return nil, fmt.Errorf("%w: maximum Pipes must be positive", ErrInvalid)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Client{
		connectTimeout: connectTimeout,
		openTimeout:    openTimeout,
		slots:          make(chan struct{}, maxPipes),
		ctx:            ctx,
		cancel:         cancel,
		closedDone:     make(chan struct{}),
	}, nil
}

func (c *Client) Open(ctx context.Context, open routing.OpenContext, callerEndpoint localbinding.CallerEndpoint) (opening.RemoteResult, error) {
	if ctx == nil || callerEndpoint == nil {
		return opening.RemoteResult{}, fmt.Errorf("%w: context and caller endpoint are required", ErrInvalid)
	}
	if err := validateClientOpen(open, time.Now()); err != nil {
		return opening.RemoteResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return opening.RemoteResult{}, err
	}
	if err := c.acquire(); err != nil {
		return opening.RemoteResult{}, err
	}
	leaseOwned := true
	defer func() {
		if leaseOwned {
			c.release()
		}
	}()

	attemptCtx, cancelAttempt := mergeContext(ctx, c.ctx)
	attemptCtx, cancelTimeout := context.WithTimeout(attemptCtx, c.openTimeout)
	defer cancelAttempt()
	defer cancelTimeout()

	dialCtx, cancelDial := context.WithTimeout(attemptCtx, c.connectTimeout)
	connection, err := grpc.NewClient(
		"passthrough:///"+open.OwnerRelayAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGatewayMessageBytes),
			grpc.MaxCallSendMsgSize(maxGatewayMessageBytes),
			grpc.WaitForReady(false),
		),
	)
	if err != nil {
		cancelDial()
		return opening.RemoteResult{}, fmt.Errorf("%w: dial owner gateway: %w", opening.ErrUnavailable, err)
	}
	if err := waitForReady(dialCtx, connection); err != nil {
		cancelDial()
		_ = connection.Close()
		return opening.RemoteResult{}, fmt.Errorf("%w: connect to owner gateway: %w", opening.ErrUnavailable, err)
	}
	cancelDial()

	pipeCtx, cancelPipe := detachedPipeContext(attemptCtx, c.ctx)
	stream, err := gatewayv1.NewGatewayRelayClient(connection).Forward(pipeCtx)
	if err != nil {
		cancelPipe(err)
		_ = connection.Close()
		return opening.RemoteResult{}, fmt.Errorf("%w: open owner gateway stream: %w", opening.ErrUnavailable, err)
	}
	sender := newSendActor(pipeCtx, stream.Send, func(cause error) { cancelPipe(cause) })
	cleanupFailed := func(cause error) {
		cancelPipe(cause)
		sender.stop(cause)
		_ = connection.Close()
		<-sender.joined
	}

	request := &gatewayv1.ForwardRequest{Message: &gatewayv1.ForwardRequest_ForwardOpen{
		ForwardOpen: &gatewayv1.ForwardOpen{Context: openContextToProto(open)},
	}}
	if err := sender.send(attemptCtx, request); err != nil {
		cleanupFailed(err)
		return opening.RemoteResult{}, fmt.Errorf("%w: ForwardOpen send failed: %w", opening.ErrUnknown, err)
	}

	first, err := receiveFirstResponse(attemptCtx, stream, cancelPipe)
	if err != nil {
		cleanupFailed(err)
		return opening.RemoteResult{}, fmt.Errorf("%w: ForwardOpen outcome unavailable: %w", opening.ErrUnknown, err)
	}
	if failed := first.GetFailed(); failed != nil {
		cleanupFailed(mapForwardFailure(failed.GetFailure()))
		if failed.GetAttemptId() != open.AttemptID {
			return opening.RemoteResult{}, fmt.Errorf("%w: mismatched stable failure", opening.ErrUnknown)
		}
		return opening.RemoteResult{}, mapForwardFailure(failed.GetFailure())
	}
	if unknown := first.GetUnknown(); unknown != nil {
		cleanupFailed(opening.ErrUnknown)
		return opening.RemoteResult{}, opening.ErrUnknown
	}
	accepted := first.GetAccepted()
	if accepted == nil {
		cleanupFailed(opening.ErrUnknown)
		return opening.RemoteResult{}, fmt.Errorf("%w: owner returned an invalid Open outcome", opening.ErrUnknown)
	}
	binding, err := liveBindingFromProto(accepted.GetBinding())
	if err != nil || accepted.GetAttemptId() != open.AttemptID || accepted.GetPipeId() == "" || len(accepted.GetPipeId()) > routing.MaxIdentityBytes || !sameBinding(binding, open.Binding) {
		cleanupFailed(opening.ErrUnknown)
		return opening.RemoteResult{}, fmt.Errorf("%w: owner returned a non-exact Open result", opening.ErrUnknown)
	}

	endpoint := newRemoteEndpoint(c, cancelPipe, connection, stream, sender, callerEndpoint, accepted.GetAttemptId(), accepted.GetPipeId(), binding)
	leaseOwned = false
	endpoint.start(pipeCtx)
	return opening.RemoteResult{
		AttemptID: accepted.GetAttemptId(),
		PipeID:    accepted.GetPipeId(),
		Binding:   cloneBinding(binding),
		Endpoint:  endpoint,
	}, nil
}

func waitForReady(ctx context.Context, connection *grpc.ClientConn) error {
	connection.Connect()
	for {
		state := connection.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return fmt.Errorf("connection shut down before becoming ready")
		}
		if !connection.WaitForStateChange(ctx, state) {
			return context.Cause(ctx)
		}
		connection.Connect()
	}
}

func (c *Client) acquire() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return opening.ErrUnavailable
	}
	select {
	case c.slots <- struct{}{}:
		c.active.Add(1)
		return nil
	default:
		return opening.ErrCapacity
	}
}

func (c *Client) release() {
	<-c.slots
	c.active.Done()
}

func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.closedDone
		return
	}
	c.closed = true
	c.cancel(ErrClosed)
	c.mu.Unlock()
	c.active.Wait()
	close(c.closedDone)
}

func validateClientOpen(open routing.OpenContext, now time.Time) error {
	if open.ExpiresAt.IsZero() || !now.Before(open.ExpiresAt) {
		return opening.ErrContextExpired
	}
	if _, err := routing.NewForwardedOpenContext(
		open.ClusterEpoch,
		open.AuthorityID,
		open.AttemptID,
		open.Auth,
		cloneBinding(open.Binding),
		routing.ForwardingContext{
			IngressGatewayID:         open.IngressGatewayID,
			IngressGatewayInstanceID: open.IngressGatewayInstanceID,
			IngressControlSessionID:  open.IngressControlSessionID,
			OwnerControlSessionID:    open.OwnerControlSessionID,
			OwnerRelayAddress:        open.OwnerRelayAddress,
			ExpiresAt:                open.ExpiresAt,
		},
	); err != nil {
		return fmt.Errorf("%w: invalid exact forwarding context: %w", opening.ErrInvalid, err)
	}
	return nil
}

func receiveFirstResponse(ctx context.Context, stream grpc.BidiStreamingClient[gatewayv1.ForwardRequest, gatewayv1.ForwardResponse], cancel context.CancelCauseFunc) (*gatewayv1.ForwardResponse, error) {
	type outcome struct {
		response *gatewayv1.ForwardResponse
		err      error
	}
	result := make(chan outcome, 1)
	go func() {
		response, err := stream.Recv()
		result <- outcome{response: response, err: err}
	}()
	select {
	case item := <-result:
		return item.response, item.err
	case <-ctx.Done():
		cancel(ctx.Err())
		<-result
		return nil, ctx.Err()
	}
}

func mapForwardFailure(failure gatewayv1.ForwardFailure) error {
	switch failure {
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_INVALID_REQUEST:
		return opening.ErrInvalid
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_CAPACITY_REACHED:
		return opening.ErrCapacity
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_ROUTE_NOT_FOUND:
		return opening.ErrNotFound
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_LISTENER_REJECTED:
		return opening.ErrListenerRejected
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_DEADLINE_EXCEEDED:
		return opening.ErrDeadline
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_SESSION_ENDED:
		return opening.ErrSessionEnded
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_CONTEXT_EXPIRED:
		return opening.ErrContextExpired
	case gatewayv1.ForwardFailure_FORWARD_FAILURE_ATTEMPT_REPLAYED:
		return opening.ErrAttemptReplay
	default:
		return opening.ErrUnavailable
	}
}

func mergeContext(parent, root context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(root, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// detachedPipeContext preserves request values but intentionally detaches the
// accepted Pipe lifetime from the Open attempt deadline. The client root still
// owns shutdown, and the returned cancel function removes its callback.
func detachedPipeContext(attempt, root context.Context) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(attempt))
	stop := context.AfterFunc(root, func() { cancel(context.Cause(root)) })
	return ctx, func(cause error) {
		stop()
		cancel(cause)
	}
}
