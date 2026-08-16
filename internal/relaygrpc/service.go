package relaygrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"github.com/cagojeiger/relaygate/internal/localbinding"
	"github.com/cagojeiger/relaygate/internal/opening"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionManager interface {
	Authenticate(clientID, apiKeyID, presentedKey string) (clientsession.Session, error)
	End(clientsession.Ref)
}

type BindingManager interface {
	Bind(context.Context, clientsession.Session, string, string, localbinding.ListenerEndpoint) (controlstate.BindingSlot, error)
	Unbind(clientsession.Ref, string) error
	RetireSession(clientsession.Ref) int
}

type Opener interface {
	Open(context.Context, clientsession.Session, string, string) (opening.Result, error)
	ClosePipe(clientsession.Ref, string) bool
	RetireSession(clientsession.Ref) int
}

type Service struct {
	relayv1.UnimplementedRelayServer
	sessions              SessionManager
	bindings              BindingManager
	opener                Opener
	authenticationTimeout time.Duration
	terminalSendTimeout   time.Duration
	openSlots             chan struct{}
}

func NewService(sessions SessionManager, bindings BindingManager, opener Opener, authenticationTimeout, terminalSendTimeout time.Duration, maxInFlightOpens uint32) (*Service, error) {
	if sessions == nil {
		return nil, fmt.Errorf("client session manager is required")
	}
	if bindings == nil {
		return nil, fmt.Errorf("listener binding manager is required")
	}
	if opener == nil {
		return nil, fmt.Errorf("pipe opener is required")
	}
	if authenticationTimeout <= 0 {
		return nil, fmt.Errorf("authentication timeout must be positive")
	}
	if maxInFlightOpens == 0 {
		return nil, fmt.Errorf("maximum in-flight Opens must be positive")
	}
	if terminalSendTimeout <= 0 {
		return nil, fmt.Errorf("terminal send timeout must be positive")
	}
	return &Service{
		sessions:              sessions,
		bindings:              bindings,
		opener:                opener,
		authenticationTimeout: authenticationTimeout,
		terminalSendTimeout:   terminalSendTimeout,
		openSlots:             make(chan struct{}, maxInFlightOpens),
	}, nil
}

func (s *Service) Connect(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
	outbound := newOutboundActor(stream)
	defer outbound.close()
	listener := newStreamListenerEndpoint(stream.Context(), outbound, s.terminalSendTimeout)
	defer listener.close()

	received := receiveRequests(stream)
	authenticationTimer := time.NewTimer(s.authenticationTimeout)
	defer authenticationTimer.Stop()
	var first *relayv1.ConnectRequest
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-authenticationTimer.C:
		return status.Error(codes.DeadlineExceeded, "client authentication timed out")
	case item, ok := <-received:
		if !ok || errors.Is(item.err, io.EOF) {
			return status.Error(codes.Unauthenticated, "client authentication is required")
		}
		if item.err != nil {
			return item.err
		}
		first = item.request
	}
	authenticate := first.GetAuthenticate()
	if authenticate == nil {
		return status.Error(codes.Unauthenticated, "client authentication failed")
	}
	session, err := s.sessions.Authenticate(authenticate.GetClientId(), authenticate.GetApiKeyId(), authenticate.GetApiKey())
	authenticate.ApiKey = ""
	if err != nil {
		if errors.Is(err, clientsession.ErrCapacity) {
			return status.Error(codes.ResourceExhausted, "client session capacity reached")
		}
		return status.Error(codes.Unauthenticated, "client authentication failed")
	}
	defer s.sessions.End(session.Ref)
	defer s.bindings.RetireSession(session.Ref)
	defer s.opener.RetireSession(session.Ref)
	coordinator := newStreamCoordinator(stream.Context(), session, s.opener, outbound, s.openSlots)
	defer coordinator.close()

	if err := outbound.send(stream.Context(), &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
		ClientSessionOpened: &relayv1.ClientSessionOpened{Session: sessionRefToProto(session.Ref)},
	}}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-session.Done:
			return status.Error(codes.Unauthenticated, "client session ended")
		case err := <-outbound.failures():
			return err
		case item, ok := <-received:
			if !ok || errors.Is(item.err, io.EOF) {
				return nil
			}
			if item.err != nil {
				return item.err
			}
			orderBeforeOffers := item.request.GetBindListener() != nil
			if orderBeforeOffers {
				listener.requestOrder.Lock()
			}
			response, err := s.handleRequest(stream.Context(), session, listener, coordinator, item.request)
			if err != nil {
				if orderBeforeOffers {
					listener.requestOrder.Unlock()
				}
				return err
			}
			if response == nil {
				if orderBeforeOffers {
					listener.requestOrder.Unlock()
				}
				continue
			}
			if err := outbound.send(stream.Context(), response); err != nil {
				if orderBeforeOffers {
					listener.requestOrder.Unlock()
				}
				return err
			}
			if orderBeforeOffers {
				listener.requestOrder.Unlock()
			}
		}
	}
}

func (s *Service) handleRequest(ctx context.Context, session clientsession.Session, listener *streamListenerEndpoint, coordinator *streamCoordinator, request *relayv1.ConnectRequest) (*relayv1.ConnectResponse, error) {
	if bind := request.GetBindListener(); bind != nil {
		slot, err := s.bindings.Bind(ctx, session, bind.GetEndpointPattern(), bind.GetTargetId(), listener)
		if err != nil {
			return nil, bindingStatus(err)
		}
		if slot.Ref == nil || slot.Ref.ListenerBindingID == "" {
			return nil, status.Error(codes.Internal, "binding committed without a listener reference")
		}
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerBound{
			ListenerBound: &relayv1.ListenerBound{Binding: &relayv1.ListenerBinding{
				ListenerBindingId: slot.Ref.ListenerBindingID,
				EndpointPattern:   slot.Key.EndpointPattern,
				TargetId:          slot.Key.TargetID,
			}},
		}}, nil
	}

	if unbind := request.GetUnbindListener(); unbind != nil {
		if err := s.bindings.Unbind(session.Ref, unbind.GetListenerBindingId()); err != nil {
			return nil, bindingStatus(err)
		}
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
			ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: unbind.GetListenerBindingId()},
		}}, nil
	}

	if open := request.GetOpen(); open != nil {
		return coordinator.startOpen(ctx, s, open), nil
	}

	if cancelOpen := request.GetCancelOpen(); cancelOpen != nil {
		return coordinator.cancelOpen(cancelOpen), nil
	}

	if closePipe := request.GetClosePipe(); closePipe != nil {
		return coordinator.closePipe(closePipe), nil
	}

	if accept := request.GetListenerAccept(); accept != nil {
		if err := listener.accept(accept.GetAttemptId()); err != nil {
			return listenerDecisionRejected(accept.GetAttemptId(), err), nil
		}
		return nil, nil
	}

	if reject := request.GetListenerReject(); reject != nil {
		if err := listener.reject(reject.GetAttemptId()); err != nil {
			return listenerDecisionRejected(reject.GetAttemptId(), err), nil
		}
		return nil, nil
	}

	if confirmed := request.GetListenerConfirmed(); confirmed != nil {
		if err := listener.confirmed(confirmed.GetAttemptId(), confirmed.GetPipeId()); err != nil {
			return listenerDecisionRejected(confirmed.GetAttemptId(), err), nil
		}
		return nil, nil
	}

	return nil, status.Error(codes.FailedPrecondition, "authenticate must be followed by a relay operation")
}

func bindingStatus(err error) error {
	switch {
	case errors.Is(err, localbinding.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid listener binding request")
	case errors.Is(err, localbinding.ErrCapacity):
		return status.Error(codes.ResourceExhausted, "listener binding capacity reached")
	case errors.Is(err, localbinding.ErrConflict):
		return status.Error(codes.AlreadyExists, "listener binding conflicts with an existing binding")
	case errors.Is(err, localbinding.ErrUnavailable):
		return status.Error(codes.Unavailable, "gateway control is unavailable")
	case errors.Is(err, localbinding.ErrSessionEnded),
		errors.Is(err, clientsession.ErrCredentialRevoked),
		errors.Is(err, clientsession.ErrStaleSession),
		errors.Is(err, clientsession.ErrClosed):
		return status.Error(codes.Unauthenticated, "client session ended")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Internal, "listener binding operation failed")
	}
}

func (s *Service) open(ctx context.Context, session clientsession.Session, request *relayv1.Open) *relayv1.ConnectResponse {
	requestID := safeWireValue(request.GetRequestId(), controlstate.MaxIdentityBytes)
	endpoint := safeWireValue(request.GetEndpoint(), controlstate.MaxEndpointPatternBytes)
	targetID := safeWireValue(request.GetTargetId(), controlstate.MaxIdentityBytes)
	if requestID == "" || endpoint == "" || targetID == "" {
		return pipeOpenFailed(requestID, endpoint, targetID, relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST)
	}

	result, err := s.opener.Open(ctx, session, endpoint, targetID)
	if err != nil {
		if errors.Is(err, opening.ErrUnknown) {
			return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenUnknown{
				PipeOpenUnknown: &relayv1.PipeOpenUnknown{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
			}}
		}
		return pipeOpenFailed(requestID, endpoint, targetID, openFailure(err))
	}
	if result.AttemptID == "" || result.PipeID == "" ||
		len(result.AttemptID) > controlstate.MaxIdentityBytes || len(result.PipeID) > controlstate.MaxIdentityBytes {
		return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenUnknown{
			PipeOpenUnknown: &relayv1.PipeOpenUnknown{RequestId: requestID, Endpoint: endpoint, TargetId: targetID},
		}}
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpened{
		PipeOpened: &relayv1.PipeOpened{
			RequestId: requestID,
			AttemptId: result.AttemptID,
			PipeId:    result.PipeID,
			Endpoint:  endpoint,
			TargetId:  targetID,
		},
	}}
}

func openFailure(err error) relayv1.OpenFailure {
	switch {
	case errors.Is(err, opening.ErrInvalid):
		return relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST
	case errors.Is(err, opening.ErrNotFound):
		return relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND
	case errors.Is(err, opening.ErrCapacity):
		return relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED
	case errors.Is(err, opening.ErrListenerRejected):
		return relayv1.OpenFailure_OPEN_FAILURE_LISTENER_REJECTED
	case errors.Is(err, opening.ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		return relayv1.OpenFailure_OPEN_FAILURE_DEADLINE_EXCEEDED
	case errors.Is(err, context.Canceled):
		return relayv1.OpenFailure_OPEN_FAILURE_CANCELLED
	default:
		return relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE
	}
}

func pipeOpenFailed(requestID, endpoint, targetID string, failure relayv1.OpenFailure) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenFailed{
		PipeOpenFailed: &relayv1.PipeOpenFailed{
			RequestId: requestID,
			Endpoint:  endpoint,
			TargetId:  targetID,
			Failure:   failure,
		},
	}}
}

func safeWireValue(value string, maxBytes int) string {
	if value == "" || len(value) > maxBytes {
		return ""
	}
	return value
}

var (
	errInvalidListenerDecision = errors.New("invalid listener decision")
	errAttemptNotPending       = errors.New("listener attempt is not pending")
	errWrongListenerPhase      = errors.New("listener attempt is in the wrong phase")
)

func listenerDecisionRejected(attemptID string, err error) *relayv1.ConnectResponse {
	failure := relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE
	switch {
	case errors.Is(err, errInvalidListenerDecision):
		failure = relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST
	case errors.Is(err, errAttemptNotPending):
		failure = relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING
	}
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerDecisionRejected{
		ListenerDecisionRejected: &relayv1.ListenerDecisionRejected{
			AttemptId: safeWireValue(attemptID, controlstate.MaxIdentityBytes),
			Failure:   failure,
		},
	}}
}

const outboundQueueCapacity = 32

type outboundMessage struct {
	ctx      context.Context //nolint:containedctx // The outbound actor must preserve each Send caller's cancellation contract until dequeued.
	response *relayv1.ConnectResponse
	result   chan error
}

// outboundActor is the only code allowed to call the gRPC stream's Send
// method. The bounded queue prevents async listener offers from creating an
// unbounded per-stream backlog.
type outboundActor struct {
	stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]
	ctx    context.Context //nolint:containedctx // Actor owns a stream-root context for queue shutdown.
	cancel context.CancelFunc
	queue  chan outboundMessage
	failed chan error
	done   chan struct{}

	mu       sync.Mutex
	failure  error
	failOnce sync.Once
}

func newOutboundActor(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) *outboundActor {
	ctx, cancel := context.WithCancel(stream.Context())
	a := &outboundActor{
		stream: stream,
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan outboundMessage, outboundQueueCapacity),
		failed: make(chan error, 1),
		done:   make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *outboundActor) run() {
	for {
		select {
		case <-a.ctx.Done():
			a.fail(a.ctx.Err())
			return
		case message := <-a.queue:
			if err := message.ctx.Err(); err != nil {
				completeOutbound(message, err)
				continue
			}
			err := a.stream.Send(message.response)
			completeOutbound(message, err)
			if err != nil {
				a.fail(err)
				return
			}
		}
	}
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

func (a *outboundActor) failures() <-chan error {
	return a.failed
}

func (a *outboundActor) fail(err error) {
	if err == nil {
		err = context.Canceled
	}
	a.failOnce.Do(func() {
		a.mu.Lock()
		a.failure = err
		a.mu.Unlock()
		a.failed <- err
		close(a.done)
	})
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
	a.cancel()
}

type listenerAttemptPhase uint8

const (
	listenerOffered listenerAttemptPhase = iota + 1
	listenerProvisional
	listenerConfirming
	listenerOpen
)

type listenerAttempt struct {
	phase        listenerAttemptPhase
	pipeID       string
	decision     chan bool
	confirmation chan struct{}
	terminal     chan struct{}
}

// streamListenerEndpoint adapts the protocol-neutral local listener boundary
// to one authenticated Connect stream. It owns only volatile attempt state.
type streamListenerEndpoint struct {
	ctx                 context.Context //nolint:containedctx // Endpoint owns the stream-root context for listener attempt retirement.
	outbound            *outboundActor
	terminalSendTimeout time.Duration

	requestOrder sync.RWMutex
	mu           sync.Mutex
	attempts     map[string]*listenerAttempt
	closed       bool
}

func newStreamListenerEndpoint(ctx context.Context, outbound *outboundActor, terminalSendTimeout time.Duration) *streamListenerEndpoint {
	return &streamListenerEndpoint{
		ctx:                 ctx,
		outbound:            outbound,
		terminalSendTimeout: terminalSendTimeout,
		attempts:            make(map[string]*listenerAttempt),
	}
}

func (e *streamListenerEndpoint) Offer(ctx context.Context, offer localbinding.Offer) error {
	if err := validateOffer(ctx, offer); err != nil {
		return err
	}
	attempt := &listenerAttempt{
		phase:        listenerOffered,
		decision:     make(chan bool, 1),
		confirmation: make(chan struct{}, 1),
		terminal:     make(chan struct{}),
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return localbinding.ErrEndpointUnavailable
	}
	if _, exists := e.attempts[offer.AttemptID]; exists {
		e.mu.Unlock()
		return fmt.Errorf("%w: duplicate listener attempt", localbinding.ErrEndpointUnavailable)
	}
	e.attempts[offer.AttemptID] = attempt
	e.mu.Unlock()

	ref := offer.Binding.Ref
	response := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{
		ListenerOffer: &relayv1.ListenerOffer{
			AttemptId:         offer.AttemptID,
			ListenerBindingId: ref.ListenerBindingID,
			Endpoint:          offer.Binding.Key.EndpointPattern,
			TargetId:          offer.Binding.Key.TargetID,
			CallerSessionId:   offer.Caller.ClientSessionID,
		},
	}}
	e.requestOrder.RLock()
	err := e.outbound.send(ctx, response)
	e.requestOrder.RUnlock()
	if err != nil {
		e.remove(offer.AttemptID, attempt)
		return endpointSendError(ctx, err)
	}

	select {
	case accepted := <-attempt.decision:
		if accepted {
			return nil
		}
		return localbinding.ErrOfferRejected
	case <-attempt.terminal:
		return localbinding.ErrEndpointUnavailable
	case <-e.ctx.Done():
		e.cancelAttempt(ctx, offer.AttemptID, attempt, "")
		return localbinding.ErrEndpointUnavailable
	case <-ctx.Done():
		e.cancelAttempt(ctx, offer.AttemptID, attempt, "")
		return ctx.Err()
	}
}

func validateOffer(ctx context.Context, offer localbinding.Offer) error {
	if ctx == nil || offer.AttemptID == "" || len(offer.AttemptID) > controlstate.MaxIdentityBytes ||
		offer.Caller.ClientSessionID == "" || offer.Binding.Generation == 0 || offer.Binding.Ref == nil {
		return fmt.Errorf("%w: invalid listener offer", localbinding.ErrEndpointUnavailable)
	}
	if err := offer.Binding.Key.Validate(); err != nil {
		return fmt.Errorf("%w: invalid listener offer", localbinding.ErrEndpointUnavailable)
	}
	if err := offer.Binding.Ref.Validate(); err != nil {
		return fmt.Errorf("%w: invalid listener offer", localbinding.ErrEndpointUnavailable)
	}
	return nil
}

func (e *streamListenerEndpoint) Confirm(ctx context.Context, confirmation localbinding.Confirmation) error {
	if ctx == nil || confirmation.AttemptID == "" || confirmation.PipeID == "" ||
		len(confirmation.AttemptID) > controlstate.MaxIdentityBytes || len(confirmation.PipeID) > controlstate.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	attempt := e.attempts[confirmation.AttemptID]
	if attempt == nil {
		e.mu.Unlock()
		return errAttemptNotPending
	}
	if attempt.phase != listenerProvisional {
		e.mu.Unlock()
		return errWrongListenerPhase
	}
	attempt.phase = listenerConfirming
	attempt.pipeID = confirmation.PipeID
	e.mu.Unlock()

	response := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerEstablished{
		ListenerEstablished: &relayv1.ListenerEstablished{AttemptId: confirmation.AttemptID, PipeId: confirmation.PipeID},
	}}
	err := e.outbound.send(ctx, response)
	if err != nil {
		e.cancelAttempt(ctx, confirmation.AttemptID, attempt, confirmation.PipeID)
		return endpointSendError(ctx, err)
	}

	select {
	case <-attempt.confirmation:
		return nil
	case <-attempt.terminal:
		return localbinding.ErrEndpointUnavailable
	case <-e.ctx.Done():
		e.cancelAttempt(ctx, confirmation.AttemptID, attempt, confirmation.PipeID)
		return localbinding.ErrEndpointUnavailable
	case <-ctx.Done():
		e.cancelAttempt(ctx, confirmation.AttemptID, attempt, confirmation.PipeID)
		return ctx.Err()
	}
}

func endpointSendError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w", localbinding.ErrEndpointUnavailable, err)
}

func (e *streamListenerEndpoint) Terminate(ctx context.Context, termination localbinding.Termination) error {
	if ctx == nil || termination.AttemptID == "" || len(termination.AttemptID) > controlstate.MaxIdentityBytes ||
		len(termination.PipeID) > controlstate.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	attempt := e.attempts[termination.AttemptID]
	if attempt == nil {
		e.mu.Unlock()
		return nil
	}
	delete(e.attempts, termination.AttemptID)
	close(attempt.terminal)
	e.mu.Unlock()

	err := e.outbound.send(ctx, &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: termination.AttemptID, PipeId: termination.PipeID},
	}})
	if err != nil {
		// The opening manager treats endpoint termination as best-effort. Turn a
		// bounded delivery failure into stream failure so session retirement, not
		// a silent terminal drop, is the fallback.
		e.outbound.fail(err)
	}
	return err
}

func (e *streamListenerEndpoint) accept(attemptID string) error {
	if attemptID == "" || len(attemptID) > controlstate.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	attempt := e.attempts[attemptID]
	if attempt == nil {
		return errAttemptNotPending
	}
	if attempt.phase != listenerOffered {
		return errWrongListenerPhase
	}
	attempt.phase = listenerProvisional
	attempt.decision <- true
	return nil
}

func (e *streamListenerEndpoint) reject(attemptID string) error {
	if attemptID == "" || len(attemptID) > controlstate.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	attempt := e.attempts[attemptID]
	if attempt == nil {
		return errAttemptNotPending
	}
	if attempt.phase != listenerOffered {
		return errWrongListenerPhase
	}
	delete(e.attempts, attemptID)
	attempt.decision <- false
	return nil
}

func (e *streamListenerEndpoint) confirmed(attemptID, pipeID string) error {
	if attemptID == "" || pipeID == "" || len(attemptID) > controlstate.MaxIdentityBytes || len(pipeID) > controlstate.MaxIdentityBytes {
		return errInvalidListenerDecision
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	attempt := e.attempts[attemptID]
	if attempt == nil {
		return errAttemptNotPending
	}
	if attempt.phase != listenerConfirming || attempt.pipeID != pipeID {
		return errWrongListenerPhase
	}
	attempt.phase = listenerOpen
	attempt.confirmation <- struct{}{}
	return nil
}

func (e *streamListenerEndpoint) cancelAttempt(parent context.Context, attemptID string, attempt *listenerAttempt, pipeID string) {
	if !e.remove(attemptID, attempt) {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), e.terminalSendTimeout)
	err := e.outbound.send(ctx, &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{
		ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: attemptID, PipeId: pipeID},
	}})
	cancel()
	if err != nil {
		// Terminal signals may not be silently dropped behind listener traffic.
		// Failing the stream makes session retirement the bounded fallback.
		e.outbound.fail(err)
	}
}

func (e *streamListenerEndpoint) remove(attemptID string, attempt *listenerAttempt) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.attempts[attemptID] != attempt {
		return false
	}
	delete(e.attempts, attemptID)
	close(attempt.terminal)
	return true
}

func (e *streamListenerEndpoint) close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	for attemptID, attempt := range e.attempts {
		delete(e.attempts, attemptID)
		close(attempt.terminal)
	}
	e.mu.Unlock()
}

type receivedRequest struct {
	request *relayv1.ConnectRequest
	err     error
}

func receiveRequests(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) <-chan receivedRequest {
	received := make(chan receivedRequest, 1)
	go func() {
		defer close(received)
		for {
			request, err := stream.Recv()
			select {
			case received <- receivedRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func sessionRefToProto(ref clientsession.Ref) *relayv1.ClientSessionRef {
	return &relayv1.ClientSessionRef{
		ClientSessionId: ref.ClientSessionID,
		ClientId:        ref.ClientID,
		ApiKeyId:        ref.APIKeyID,
		AuthRevision:    ref.AuthRevision,
	}
}
