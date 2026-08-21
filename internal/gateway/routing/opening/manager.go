package opening

import (
	"context"
	"fmt"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

func New(config Config, admitter Admitter, bindings ReservationStore, remote ...RemoteOpener) (*Manager, error) {
	if config.ClusterEpoch == "" || len(config.ClusterEpoch) > routing.MaxIdentityBytes {
		return nil, fmt.Errorf("%w: cluster epoch must be 1..%d bytes", ErrInvalid, routing.MaxIdentityBytes)
	}
	if config.MaxPipes == 0 {
		return nil, fmt.Errorf("%w: maximum pipes must be positive", ErrInvalid)
	}
	if config.OpenTimeout <= 0 {
		return nil, fmt.Errorf("%w: Open timeout must be positive", ErrInvalid)
	}
	if admitter == nil {
		return nil, fmt.Errorf("%w: admitter is required", ErrInvalid)
	}
	if bindings == nil {
		return nil, fmt.Errorf("%w: local reservation store is required", ErrInvalid)
	}
	if len(remote) > 1 {
		return nil, fmt.Errorf("%w: at most one remote opener is allowed", ErrInvalid)
	}
	var remoteOpener RemoteOpener
	if len(remote) == 1 {
		remoteOpener = remote[0]
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:            config,
		admitter:          admitter,
		bindings:          bindings,
		remote:            remoteOpener,
		now:               time.Now,
		ctx:               ctx,
		cancel:            cancel,
		entries:           make(map[*entry]struct{}),
		byAttempt:         make(map[string]*entry),
		byPipe:            make(map[string]*entry),
		forwardedAttempts: make(map[string]time.Time),
		terminations:      make(chan termination, config.MaxPipes),
	}
	m.terminationWorker.Add(1)
	go m.runTerminations()
	return m, nil
}

// Open executes one exact-target attempt through either the local owner or the
// configured remote relay. A successful return means AcceptedO and listener
// confirmation have occurred; caller-facing apply remains the transport's job.
func (m *Manager) Open(ctx context.Context, caller clientsession.Session, endpoint, targetID string) (Result, error) {
	return m.open(ctx, caller, nil, endpoint, targetID, nil, false)
}

// OpenPipe opens one exact target with both payload directions and terminal
// propagation wired before the Open linearization point. Payload remains gated
// until ActivatePipe records that caller-facing PipeOpened was written.
func (m *Manager) OpenPipe(ctx context.Context, caller clientsession.Session, callerEndpoint localbinding.CallerEndpoint, endpoint, targetID string) (Result, error) {
	if callerEndpoint == nil {
		return Result{}, fmt.Errorf("%w: caller endpoint is required", ErrInvalid)
	}
	return m.open(ctx, caller, callerEndpoint, endpoint, targetID, nil, false)
}

// OpenForwarded executes the owner segment for a context admitted on another
// Gateway. The hop context is the remote caller lifetime; once it ends, an
// accepted owner Pipe becomes terminal and is never resumed.
func (m *Manager) OpenForwarded(ctx context.Context, openContext routing.OpenContext, callerEndpoint localbinding.CallerEndpoint) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if callerEndpoint == nil {
		return Result{}, fmt.Errorf("%w: caller endpoint is required", ErrInvalid)
	}
	caller := clientsession.Session{
		Ref: clientsession.Ref{
			ClientSessionID: openContext.Auth.ClientSessionID,
			ClientID:        openContext.Auth.ClientID,
			APIKeyID:        openContext.Auth.APIKeyID,
			AuthRevision:    openContext.Auth.AuthRevision,
		},
		Done: ctx.Done(),
	}
	endpoint := openContext.Binding.Key.EndpointPattern
	targetID := openContext.Binding.Key.TargetID
	copy := openContext.Clone()
	return m.open(ctx, caller, callerEndpoint, endpoint, targetID, &copy, true)
}

func (m *Manager) open(
	ctx context.Context,
	caller clientsession.Session,
	callerEndpoint localbinding.CallerEndpoint,
	endpoint, targetID string,
	provided *routing.OpenContext,
	forwardedOwner bool,
) (Result, error) {
	if err := validateOpenInput(ctx, caller, endpoint, targetID); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, mapContextError(err)
	}
	select {
	case <-caller.Done:
		return Result{}, ErrSessionEnded
	default:
	}

	pipeCtx, cancelPipe := context.WithCancelCause(context.WithoutCancel(ctx))
	e := &entry{
		caller:         caller.Ref,
		callerDone:     caller.Done,
		callerEndpoint: callerEndpoint,
		state:          StateOpening,
		done:           make(chan struct{}),
		attemptDone:    make(chan struct{}),
		activation:     make(chan struct{}),
		pipeCtx:        pipeCtx,
		cancelPipe:     cancelPipe,
	}
	if err := m.insert(ctx, e); err != nil {
		cancelPipe(err)
		return Result{}, err
	}
	defer m.opens.Done()

	phaseBase, cancelPhase := context.WithCancelCause(ctx)
	attemptCtx, cancelTimeout := context.WithTimeoutCause(phaseBase, m.config.OpenTimeout, ErrDeadline)
	m.setPhaseCancel(e, cancelPhase)
	defer cancelTimeout()

	var openContext routing.OpenContext
	if provided == nil {
		var err error
		openContext, err = m.admitter.AdmitOpen(attemptCtx, caller.Ref, endpoint, targetID)
		if err != nil {
			return Result{}, m.fail(e, classifyAdmissionError(attemptCtx, err))
		}
	} else {
		openContext = provided.Clone()
	}
	if err := validateOpenContext(m.config.ClusterEpoch, caller.Ref, endpoint, targetID, openContext); err != nil {
		return Result{}, m.fail(e, err)
	}
	ownerLocal := openContext.Binding.Ref.GatewayID == m.bindings.GatewayID()
	if forwardedOwner {
		if err := validateForwardingContext(openContext); err != nil {
			return Result{}, m.fail(e, err)
		}
	}
	if forwardedOwner && !ownerLocal {
		return Result{}, m.fail(e, ErrNotFound)
	}
	if !forwardedOwner && !ownerLocal {
		if m.remote == nil {
			return Result{}, m.fail(e, ErrRemoteRelayUnavailable)
		}
		if err := validateForwardingContext(openContext); err != nil {
			return Result{}, m.fail(e, err)
		}
		return m.openRemote(e, attemptCtx, cancelTimeout, cancelPhase, openContext)
	}
	if cause := context.Cause(attemptCtx); cause != nil {
		return Result{}, m.fail(e, mapContextError(cause))
	}

	reservation, err := m.reserve(e, openContext, forwardedOwner)
	if err != nil {
		return Result{}, m.fail(e, classifyReservationError(err))
	}
	if err := m.beginOffer(e, attemptCtx, caller.Done, reservation.ListenerDone); err != nil {
		return Result{}, err
	}
	offer := localbinding.Offer{
		AttemptID: reservation.Context.AttemptID,
		Caller:    caller.Ref,
		Binding:   cloneSlot(reservation.Binding),
	}
	offerErr := reservation.Endpoint.Offer(attemptCtx, offer)
	work, terminalErr := m.finishOffer(e, offerErr == nil)
	m.launchTermination(work)
	if terminalErr != nil {
		return Result{}, terminalErr
	}
	if offerErr != nil {
		return Result{}, m.fail(e, classifyOfferError(attemptCtx, offerErr))
	}
	if cause := context.Cause(attemptCtx); cause != nil {
		return Result{}, m.fail(e, mapContextError(cause))
	}

	pipeID, err := newPipeID()
	if err != nil {
		return Result{}, m.fail(e, fmt.Errorf("%w: generate PipeID: %w", ErrUnavailable, err))
	}

	confirmBase, cancelConfirm := context.WithCancelCause(ctx)
	confirmCtx, cancelConfirmTimeout := context.WithTimeoutCause(confirmBase, m.config.OpenTimeout, ErrDeadline)
	accepted, work, terminalErr := m.accept(e, pipeID, cancelConfirm, attemptCtx)
	cancelTimeout()
	cancelPhase(nil)
	if !accepted {
		cancelConfirmTimeout()
		cancelConfirm(nil)
		if work != nil {
			m.launchTermination(work)
		}
		return Result{}, terminalErr
	}

	confirmation := localbinding.Confirmation{AttemptID: reservation.Context.AttemptID, PipeID: pipeID}
	if err := reservation.Endpoint.Confirm(confirmCtx, confirmation); err != nil {
		classified := classifyConfirmationError(confirmCtx, err)
		cancelConfirmTimeout()
		return Result{}, m.fail(e, classified)
	}
	if cause := context.Cause(confirmCtx); cause != nil {
		cancelConfirmTimeout()
		return Result{}, m.fail(e, mapContextError(cause))
	}
	cancelConfirmTimeout()

	result, err := m.result(e)
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func (m *Manager) openRemote(
	e *entry,
	attemptCtx context.Context,
	cancelTimeout context.CancelFunc,
	cancelPhase context.CancelCauseFunc,
	openContext routing.OpenContext,
) (Result, error) {
	if !m.now().Before(openContext.ExpiresAt) {
		return Result{}, m.fail(e, ErrContextExpired)
	}
	result, err := m.remote.Open(attemptCtx, openContext.Clone(), e.callerEndpoint)
	if err != nil {
		return Result{}, m.fail(e, classifyRemoteError(attemptCtx, err))
	}
	accepted, terminalErr := m.acceptRemote(e, openContext, result)
	cancelTimeout()
	cancelPhase(nil)
	if !accepted {
		if result.Endpoint != nil {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(attemptCtx), m.config.OpenTimeout)
			_ = result.Endpoint.Close(closeCtx)
			cancel()
		}
		return Result{}, m.failRemoteResult(e, terminalErr)
	}
	return m.result(e)
}

func validateOpenInput(ctx context.Context, caller clientsession.Session, endpoint, targetID string) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if caller.Done == nil || caller.Ref.ClientSessionID == "" || caller.Ref.ClientID == "" || caller.Ref.APIKeyID == "" || caller.Ref.AuthRevision == "" {
		return fmt.Errorf("%w: authenticated caller session is required", ErrInvalid)
	}
	auth := routing.AuthContext{
		ClientSessionID: caller.Ref.ClientSessionID,
		ClientID:        caller.Ref.ClientID,
		APIKeyID:        caller.Ref.APIKeyID,
		AuthRevision:    caller.Ref.AuthRevision,
	}
	if _, err := routing.ExactBindingKey(auth, endpoint, targetID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func validateOpenContext(clusterEpoch string, caller clientsession.Ref, endpoint, targetID string, open routing.OpenContext) error {
	if open.ClusterEpoch != clusterEpoch || open.AuthorityID == "" || open.AttemptID == "" ||
		len(open.AuthorityID) > routing.MaxIdentityBytes || len(open.AttemptID) > routing.MaxIdentityBytes {
		return fmt.Errorf("%w: mismatched Open context identity", ErrUnavailable)
	}
	if open.Auth.ClientSessionID != caller.ClientSessionID || open.Auth.ClientID != caller.ClientID ||
		open.Auth.APIKeyID != caller.APIKeyID || open.Auth.AuthRevision != caller.AuthRevision {
		return fmt.Errorf("%w: mismatched Open auth context", ErrUnavailable)
	}
	if err := open.Binding.Validate(); err != nil {
		return fmt.Errorf("%w: Open context has no exact live binding", ErrUnavailable)
	}
	expected, err := routing.ExactBindingKey(open.Auth, endpoint, targetID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if open.Binding.Key != expected {
		return fmt.Errorf("%w: Open context has a mismatched binding", ErrUnavailable)
	}
	return nil
}

func validateForwardingContext(open routing.OpenContext) error {
	if open.IngressGatewayID == "" || open.IngressGatewayInstanceID == "" || open.IngressControlSessionID == "" ||
		len(open.IngressGatewayID) > routing.MaxIdentityBytes ||
		len(open.IngressGatewayInstanceID) > routing.MaxIdentityBytes ||
		len(open.IngressControlSessionID) > routing.MaxIdentityBytes {
		return fmt.Errorf("%w: invalid forwarded ingress identity", ErrUnavailable)
	}
	if err := routing.ValidateRelayAddress(open.OwnerRelayAddress); err != nil {
		return fmt.Errorf("%w: invalid owner relay address: %w", ErrUnavailable, err)
	}
	if open.ExpiresAt.UnixMilli() <= 0 {
		return fmt.Errorf("%w: forwarded context has no absolute expiry", ErrUnavailable)
	}
	return nil
}
