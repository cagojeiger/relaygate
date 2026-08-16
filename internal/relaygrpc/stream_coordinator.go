package relaygrpc

import (
	"context"
	"sync"

	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
)

type openWorker struct {
	cancel            context.CancelFunc
	cancelRequested   bool
	responseCommitted bool
}

// streamCoordinator keeps blocking Open work off the Connect receive loop.
// Its worker table is the session-local request_id namespace; the Service-owned
// slot channel is the process-wide goroutine bound. Successful Pipes leave this
// table after their response and remain owned by the opening manager.
type streamCoordinator struct {
	ctx        context.Context //nolint:containedctx // Coordinator owns the stream-root context for worker cancellation.
	sendCtx    context.Context //nolint:containedctx // Terminal Open responses use a coordinator-owned send context after Open cancellation.
	cancelSend context.CancelFunc
	session    clientsession.Session
	opener     Opener
	outbound   *outboundActor
	openSlots  chan struct{}

	mu      sync.Mutex
	workers map[string]*openWorker
	closed  bool
	wait    sync.WaitGroup
}

func newStreamCoordinator(ctx context.Context, session clientsession.Session, opener Opener, outbound *outboundActor, openSlots chan struct{}) *streamCoordinator {
	sendCtx, cancelSend := context.WithCancel(ctx)
	return &streamCoordinator{
		ctx:        ctx,
		sendCtx:    sendCtx,
		cancelSend: cancelSend,
		session:    session,
		opener:     opener,
		outbound:   outbound,
		openSlots:  openSlots,
		workers:    make(map[string]*openWorker),
	}
}

func (c *streamCoordinator) startOpen(ctx context.Context, service *Service, request *relayv1.Open) *relayv1.ConnectResponse {
	requestID := safeWireValue(request.GetRequestId(), controlstate.MaxIdentityBytes)
	endpoint := safeWireValue(request.GetEndpoint(), controlstate.MaxEndpointPatternBytes)
	targetID := safeWireValue(request.GetTargetId(), controlstate.MaxIdentityBytes)
	if requestID == "" || endpoint == "" || targetID == "" {
		return pipeOpenFailed(requestID, endpoint, targetID, relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return pipeOpenFailed(requestID, endpoint, targetID, relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE)
	}
	if _, exists := c.workers[requestID]; exists {
		c.mu.Unlock()
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenRequestRejected{
			OpenRequestRejected: &relayv1.OpenRequestRejected{
				RequestId: requestID,
				Failure:   relayv1.OpenRequestFailure_OPEN_REQUEST_FAILURE_DUPLICATE_IN_FLIGHT,
			},
		}}
	}
	select {
	case c.openSlots <- struct{}{}:
	default:
		c.mu.Unlock()
		return pipeOpenFailed(requestID, endpoint, targetID, relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED)
	}
	openContext, cancel := context.WithCancel(ctx)
	worker := &openWorker{cancel: cancel}
	c.workers[requestID] = worker
	c.wait.Add(1)
	c.mu.Unlock()

	normalized := &relayv1.Open{RequestId: requestID, Endpoint: endpoint, TargetId: targetID}
	go c.runOpen(service, openContext, normalized, worker)
	return nil
}

func (c *streamCoordinator) runOpen(service *Service, ctx context.Context, request *relayv1.Open, worker *openWorker) {
	defer c.wait.Done()
	defer func() { <-c.openSlots }()

	response := service.open(ctx, c.session, request)
	opened := response.GetPipeOpened()
	pipeID := ""
	if opened != nil {
		pipeID = opened.GetPipeId()
	}

	c.mu.Lock()
	cancelled := worker.cancelRequested
	closed := c.closed
	worker.responseCommitted = true
	c.mu.Unlock()

	if pipeID != "" && (cancelled || closed) {
		c.opener.ClosePipe(c.session.Ref, pipeID)
		response = &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenUnknown{
			PipeOpenUnknown: &relayv1.PipeOpenUnknown{
				RequestId: request.GetRequestId(),
				Endpoint:  request.GetEndpoint(),
				TargetId:  request.GetTargetId(),
			},
		}}
	} else if cancelled && response.GetPipeOpenUnknown() == nil {
		// Cancel won the coordinator response LP. A stable failure computed just
		// before that lock cannot overtake the accepted cancellation ACK.
		response = pipeOpenFailed(
			request.GetRequestId(),
			request.GetEndpoint(),
			request.GetTargetId(),
			relayv1.OpenFailure_OPEN_FAILURE_CANCELLED,
		)
	}

	var sendErr error
	if !closed {
		sendErr = c.outbound.send(c.sendCtx, response) //nolint:contextcheck // Open cancellation must not suppress the terminal Open response.
	}
	if sendErr != nil && pipeID != "" {
		c.opener.ClosePipe(c.session.Ref, pipeID)
	}

	c.mu.Lock()
	if c.workers[request.GetRequestId()] == worker {
		delete(c.workers, request.GetRequestId())
	}
	c.mu.Unlock()

	// opening.Open detaches the attempt context after AcceptedO. The Pipe then
	// lives under exact ClosePipe/session retirement, so every worker context can
	// be released as soon as its terminal response has been handled.
	worker.cancel()
}

func (c *streamCoordinator) cancelOpen(request *relayv1.CancelOpen) *relayv1.ConnectResponse {
	requestID := safeWireValue(request.GetRequestId(), controlstate.MaxIdentityBytes)
	wasPending := false
	var cancel context.CancelFunc
	if requestID != "" {
		c.mu.Lock()
		worker := c.workers[requestID]
		if worker != nil && !worker.responseCommitted {
			worker.cancelRequested = true
			cancel = worker.cancel
			wasPending = true
		}
		c.mu.Unlock()
	}
	if cancel != nil {
		cancel()
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenCancelAcknowledged{
		OpenCancelAcknowledged: &relayv1.OpenCancelAcknowledged{RequestId: requestID, WasPending: wasPending},
	}}
}

func (c *streamCoordinator) closePipe(request *relayv1.ClosePipe) *relayv1.ConnectResponse {
	pipeID := safeWireValue(request.GetPipeId(), controlstate.MaxIdentityBytes)
	owned := false
	if pipeID != "" {
		owned = c.opener.ClosePipe(c.session.Ref, pipeID)
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{
		PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: pipeID, Owned: owned},
	}}
}

func (c *streamCoordinator) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wait.Wait()
		return
	}
	c.closed = true
	cancels := make([]context.CancelFunc, 0, len(c.workers))
	for _, worker := range c.workers {
		cancels = append(cancels, worker.cancel)
	}
	c.mu.Unlock()

	c.cancelSend()
	for _, cancel := range cancels {
		cancel()
	}
	c.wait.Wait()
}
