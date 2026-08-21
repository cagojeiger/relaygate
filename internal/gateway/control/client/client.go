package gatewaycontrol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/control/transport"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	ErrControlUnavailable      = errors.New("gateway control is not revalidated")
	ErrBindingConflict         = fmt.Errorf("live binding conflict: %w", routing.ErrConflict)
	ErrBindingCapacity         = fmt.Errorf("gateway listener binding capacity reached: %w", routing.ErrCapacity)
	ErrClientClosed            = errors.New("gateway control client is closed")
	ErrInvalidMutation         = errors.New("invalid binding mutation")
	ErrSnapshotProviderMissing = errors.New("gateway control snapshot provider is required before Run")
)

// SnapshotProvider returns this Gateway's current local bindings. It is read
// for every new control session: a reconnect never replays an older control
// operation or asks the authority to reconcile history.
type SnapshotProvider interface {
	LiveBindings() []routing.LiveBinding
}

type Config struct {
	ClusterEpoch     string
	GatewayID        string
	RelayAddress     string
	ControlEndpoints []string
	ConnectTimeout   time.Duration
	RetryInterval    time.Duration
}

type State string

const (
	StateDisconnected State = "Disconnected"
	StateConnecting   State = "Connecting"
	StateSyncing      State = "Syncing"
	StateRevalidated  State = "Revalidated"
)

type Status struct {
	GatewayID         string `json:"gateway_id"`
	GatewayInstanceID string `json:"gateway_instance_id"`
	State             State  `json:"state"`
	Endpoint          string `json:"endpoint,omitempty"`
	AuthorityID       string `json:"authority_id,omitempty"`
	ControlSessionID  string `json:"control_session_id,omitempty"`
}

func (s Status) Ready() bool { return s.State == StateRevalidated }

type mutationKind uint8

const (
	mutationDeclare mutationKind = iota + 1
	mutationWithdraw
)

type pendingMutation struct {
	kind    mutationKind
	ctx     context.Context //nolint:containedctx // The exact current-session mutation owns the caller deadline.
	done    chan error
	binding routing.LiveBinding
}

const maxPendingMutations = 2 * routing.MaxListenerBindingsPerGateway

type Client struct {
	config     Config
	logger     *slog.Logger
	instanceID string
	keepalive  keepalive.ClientParameters

	mu       sync.Mutex
	status   Status
	provider SnapshotProvider
	running  bool
	// admissionClient and admissionSession are published atomically with a
	// Revalidated status and cleared whenever that current session ends.
	admissionClient  controlv1.GatewayControlClient
	admissionSession *controlv1.SessionRef
	queue            []*pendingMutation
	active           *pendingMutation
	wake             chan struct{}
	stopped          bool
}

func New(config Config, logger *slog.Logger) (*Client, error) {
	instanceID, err := newInstanceID()
	if err != nil {
		return nil, err
	}
	return newClient(config, logger, instanceID)
}

func newClient(config Config, logger *slog.Logger, instanceID string) (*Client, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := routing.ValidateIdentity("gateway_instance_id", instanceID); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		config:     config,
		logger:     logger.With("component", "gateway_control", "gateway_id", config.GatewayID, "gateway_instance_id", instanceID),
		instanceID: instanceID,
		keepalive: keepalive.ClientParameters{
			Time:    controltransport.KeepaliveTime,
			Timeout: controltransport.KeepaliveTimeout,
		},
		status: Status{GatewayID: config.GatewayID, GatewayInstanceID: instanceID, State: StateDisconnected},
		wake:   make(chan struct{}, 1),
	}, nil
}

func (c Config) validate() error {
	if err := routing.ValidateIdentity("cluster_epoch", c.ClusterEpoch); err != nil {
		return err
	}
	if err := routing.ValidateIdentity("gateway_id", c.GatewayID); err != nil {
		return err
	}
	if err := routing.ValidateRelayAddress(c.RelayAddress); err != nil {
		return fmt.Errorf("relay address: %w", err)
	}
	if len(c.ControlEndpoints) == 0 {
		return fmt.Errorf("at least one control endpoint is required")
	}
	for index, endpoint := range c.ControlEndpoints {
		if endpoint == "" {
			return fmt.Errorf("control endpoint %d is empty", index)
		}
	}
	if c.ConnectTimeout <= 0 || c.RetryInterval <= 0 {
		return fmt.Errorf("connect timeout and retry interval must be positive")
	}
	return nil
}

// AttachSnapshotProvider must happen before Run. The provider is deliberately
// not copied: every reconnect takes a fresh declaration of local truth.
func (c *Client) AttachSnapshotProvider(provider SnapshotProvider) error {
	if provider == nil {
		return ErrSnapshotProviderMissing
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.stopped {
		return fmt.Errorf("%w: snapshot provider cannot change after Run", ErrClientClosed)
	}
	c.provider = provider
	return nil
}

func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// CurrentSession returns only a current, revalidated control session. It
// never returns a last-known session across reconnect/failover.
func (c *Client) CurrentSession() (controlmodel.SessionRef, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status.State != StateRevalidated || c.admissionSession == nil {
		return controlmodel.SessionRef{}, false
	}
	return controlmodel.SessionRef{
		ClusterEpoch:      c.admissionSession.GetClusterEpoch(),
		AuthorityID:       c.admissionSession.GetAuthorityId(),
		ControlSessionID:  c.admissionSession.GetControlSessionId(),
		GatewayID:         c.admissionSession.GetGatewayId(),
		GatewayInstanceID: c.admissionSession.GetGatewayInstanceId(),
	}, true
}

// Declare sends one exact live binding on the current control session. It is
// intentionally fail-fast while disconnected; reconnect is represented by a
// fresh full snapshot from SnapshotProvider, never mutation replay.
func (c *Client) Declare(ctx context.Context, binding routing.LiveBinding) error {
	return c.enqueue(ctx, mutationDeclare, binding)
}

// Withdraw best-effort removes one exact live binding from the current
// authority directory. Local retirement must not wait for it; stream loss
// fails this operation instead of carrying it into the next session.
func (c *Client) Withdraw(ctx context.Context, binding routing.LiveBinding) error {
	return c.enqueue(ctx, mutationWithdraw, binding)
}

func (c *Client) enqueue(ctx context.Context, kind mutationKind, binding routing.LiveBinding) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidMutation)
	}
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMutation, err)
	}
	if binding.Ref.GatewayID != c.config.GatewayID || binding.Ref.GatewayInstanceID != c.instanceID {
		return fmt.Errorf("%w: binding does not belong to this gateway instance", ErrInvalidMutation)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	mutation := &pendingMutation{kind: kind, ctx: ctx, done: make(chan error, 1), binding: binding}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return ErrClientClosed
	}
	// Syncing accepts bounded current-session mutations so a local change that
	// races the FullSnapshot is serialized immediately after snapshot
	// acceptance. Disconnected sessions still fail fast and nothing is replayed
	// into a later session.
	if c.status.State != StateSyncing && c.status.State != StateRevalidated {
		c.mu.Unlock()
		return ErrControlUnavailable
	}
	pending := len(c.queue)
	if c.active != nil {
		pending++
	}
	if pending >= maxPendingMutations {
		c.mu.Unlock()
		return ErrBindingCapacity
	}
	c.queue = append(c.queue, mutation)
	c.signalLocked()
	c.mu.Unlock()

	select {
	case err := <-mutation.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) Run(ctx context.Context) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	if c.provider == nil {
		c.mu.Unlock()
		c.stop(ErrSnapshotProviderMissing)
		return
	}
	c.running = true
	c.mu.Unlock()
	defer c.stop(ctx.Err())

	endpointIndex := 0
	for ctx.Err() == nil {
		endpoint := c.config.ControlEndpoints[endpointIndex]
		c.setConnecting(endpoint)
		revalidated, err := c.runEndpoint(ctx, endpoint)
		c.endCurrentSession(err)
		if ctx.Err() != nil {
			return
		}
		if revalidated {
			c.logger.Warn("gateway control session ended", "endpoint", endpoint, "error", err)
		} else {
			c.logger.Debug("gateway control endpoint unavailable", "endpoint", endpoint, "error", err)
		}
		endpointIndex = (endpointIndex + 1) % len(c.config.ControlEndpoints)
		if !wait(ctx, c.config.RetryInterval) {
			return
		}
	}
}

func (c *Client) runEndpoint(ctx context.Context, endpoint string) (bool, error) {
	connection, err := grpc.NewClient(
		"passthrough:///"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(c.keepalive),
		grpc.WithDisableRetry(),
	)
	if err != nil {
		return false, fmt.Errorf("create control connection: %w", err)
	}
	defer connection.Close()
	if err := waitForReady(ctx, connection, c.config.ConnectTimeout); err != nil {
		return false, fmt.Errorf("connect to control endpoint: %w", err)
	}

	streamContext, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	controlClient := controlv1.NewGatewayControlClient(connection)
	stream, err := controlClient.Connect(streamContext)
	if err != nil {
		return false, fmt.Errorf("open control stream: %w", err)
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{Hello: &controlv1.Hello{
		ClusterEpoch: c.config.ClusterEpoch, GatewayId: c.config.GatewayID, GatewayInstanceId: c.instanceID, RelayAddress: c.config.RelayAddress,
	}}}); err != nil {
		return false, fmt.Errorf("send gateway hello: %w", err)
	}
	response, err := receiveWithTimeout(streamContext, stream, c.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Errorf("receive control session: %w", err)
	}
	opened := response.GetSessionOpened()
	if err := c.validateSession(opened); err != nil {
		return false, err
	}
	session := opened.GetSession()
	c.setSyncing(endpoint, session)
	snapshot, err := c.currentSnapshot()
	if err != nil {
		return false, err
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: cloneSessionRef(session), Bindings: snapshot},
	}}); err != nil {
		return false, fmt.Errorf("send gateway snapshot: %w", err)
	}
	response, err = receiveWithTimeout(streamContext, stream, c.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Errorf("receive snapshot acceptance: %w", err)
	}
	accepted := response.GetSnapshotAccepted()
	bindingCount := uint32(len(snapshot)) //nolint:gosec // currentSnapshot caps this slice at 512 entries.
	if accepted == nil || accepted.GetBindingCount() != bindingCount {
		return false, fmt.Errorf("control endpoint returned an invalid snapshot response")
	}
	c.setRevalidated(endpoint, session, controlClient)
	c.logger.Info("gateway control session revalidated", "endpoint", endpoint, "authority_id", session.GetAuthorityId(), "control_session_id", session.GetControlSessionId(), "binding_count", accepted.GetBindingCount())
	return true, c.serveMutations(streamContext, stream, session)
}

func (c *Client) currentSnapshot() ([]*controlv1.LiveBinding, error) {
	c.mu.Lock()
	provider := c.provider
	c.mu.Unlock()
	if provider == nil {
		return nil, ErrSnapshotProviderMissing
	}
	bindings := provider.LiveBindings()
	if len(bindings) > routing.MaxListenerBindingsPerGateway {
		return nil, fmt.Errorf("%w: snapshot has %d live bindings", ErrBindingCapacity, len(bindings))
	}
	seen := make(map[routing.BindingKey]struct{}, len(bindings))
	for index, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("snapshot binding %d: %w", index, err)
		}
		if binding.Ref.GatewayID != c.config.GatewayID || binding.Ref.GatewayInstanceID != c.instanceID {
			return nil, fmt.Errorf("snapshot binding %d does not belong to this gateway instance", index)
		}
		if _, exists := seen[binding.Key]; exists {
			return nil, fmt.Errorf("snapshot binding %d duplicates a key", index)
		}
		seen[binding.Key] = struct{}{}
	}
	sort.Slice(bindings, func(i, j int) bool { return bindingKeyLess(bindings[i].Key, bindings[j].Key) })
	wire := make([]*controlv1.LiveBinding, 0, len(bindings))
	for _, binding := range bindings {
		wire = append(wire, liveBindingToProto(binding, false))
	}
	return wire, nil
}

type receivedResponse struct {
	response *controlv1.ControlResponse
	err      error
}

func receiveResponses(ctx context.Context, stream controlv1.GatewayControl_ConnectClient) <-chan receivedResponse {
	received := make(chan receivedResponse, 1)
	go func() {
		defer close(received)
		for {
			response, err := stream.Recv()
			select {
			case received <- receivedResponse{response: response, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func nextControlResponse(received receivedResponse, ok bool) (*controlv1.ControlResponse, error) {
	if !ok {
		return nil, fmt.Errorf("control response stream ended")
	}
	if received.err != nil {
		return nil, received.err
	}
	return received.response, nil
}

func (c *Client) serveMutations(ctx context.Context, stream controlv1.GatewayControl_ConnectClient, session *controlv1.SessionRef) error {
	responses := receiveResponses(ctx, stream)
	for {
		mutation := c.nextMutation()
		if mutation == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.wake:
				continue
			case received, ok := <-responses:
				response, err := nextControlResponse(received, ok)
				if err != nil {
					return err
				}
				return fmt.Errorf("unexpected control response while idle: %T", response.GetMessage())
			}
		}
		if err := mutation.ctx.Err(); err != nil {
			c.finishMutation(mutation, err)
			continue
		}
		if err := stream.Send(c.mutationRequest(session, mutation)); err != nil {
			return fmt.Errorf("send binding mutation: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case received, ok := <-responses:
			response, err := nextControlResponse(received, ok)
			if err != nil {
				return err
			}
			result := response.GetMutationResult()
			if result == nil {
				return fmt.Errorf("unexpected control response to mutation: %T", response.GetMessage())
			}
			c.finishMutation(mutation, c.mutationResult(mutation, result))
		}
	}
}

func (c *Client) nextMutation() *pendingMutation {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != nil {
		return c.active
	}
	if len(c.queue) == 0 {
		return nil
	}
	c.active = c.queue[0]
	c.queue = c.queue[1:]
	return c.active
}

func (c *Client) mutationRequest(session *controlv1.SessionRef, mutation *pendingMutation) *controlv1.ControlRequest {
	wire := &controlv1.BindingMutation{Session: cloneSessionRef(session)}
	switch mutation.kind {
	case mutationDeclare:
		wire.Mutation = &controlv1.BindingMutation_Declare{Declare: liveBindingToProto(mutation.binding, false)}
	case mutationWithdraw:
		wire.Mutation = &controlv1.BindingMutation_Withdraw{Withdraw: liveBindingToProto(mutation.binding, false)}
	}
	return &controlv1.ControlRequest{Message: &controlv1.ControlRequest_BindingMutation{BindingMutation: wire}}
}

func (c *Client) mutationResult(mutation *pendingMutation, result *controlv1.MutationResult) error {
	switch result.GetCode() {
	case controlv1.MutationCode_MUTATION_CODE_APPLIED, controlv1.MutationCode_MUTATION_CODE_ALREADY_APPLIED:
		binding, err := liveBindingFromProto(result.GetBinding(), c.config.GatewayID, c.instanceID, false)
		if err != nil || binding != mutation.binding {
			return fmt.Errorf("control returned a mismatched mutation result")
		}
		return nil
	case controlv1.MutationCode_MUTATION_CODE_CONFLICT:
		return bindingConflict(result.GetError())
	case controlv1.MutationCode_MUTATION_CODE_CAPACITY_REACHED:
		return ErrBindingCapacity
	default:
		return fmt.Errorf("control returned an unspecified mutation result")
	}
}

func (c *Client) finishMutation(mutation *pendingMutation, err error) {
	c.mu.Lock()
	if c.active != mutation {
		c.mu.Unlock()
		return
	}
	c.active = nil
	c.signalLocked()
	c.mu.Unlock()
	mutation.done <- err
}

func (c *Client) validateSession(opened *controlv1.SessionOpened) error {
	if opened == nil || opened.GetSession() == nil {
		return fmt.Errorf("control endpoint did not open a session")
	}
	session := opened.GetSession()
	if session.GetClusterEpoch() != c.config.ClusterEpoch || session.GetGatewayId() != c.config.GatewayID || session.GetGatewayInstanceId() != c.instanceID ||
		session.GetAuthorityId() == "" || session.GetControlSessionId() == "" ||
		len(session.GetAuthorityId()) > routing.MaxIdentityBytes || len(session.GetControlSessionId()) > routing.MaxIdentityBytes {
		return fmt.Errorf("control endpoint returned a mismatched session")
	}
	return nil
}

func bindingKeyLess(left, right routing.BindingKey) bool {
	if left.ClientID != right.ClientID {
		return left.ClientID < right.ClientID
	}
	if left.EndpointPattern != right.EndpointPattern {
		return left.EndpointPattern < right.EndpointPattern
	}
	return left.TargetID < right.TargetID
}

func liveBindingToProto(binding routing.LiveBinding, includeGatewayID bool) *controlv1.LiveBinding {
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: binding.Ref.GatewayInstanceID, ListenerBindingId: binding.Ref.ListenerBindingID}
	if includeGatewayID {
		ref.GatewayId = binding.Ref.GatewayID
	}
	return &controlv1.LiveBinding{Key: &controlv1.BindingKey{ClientId: binding.Key.ClientID, EndpointPattern: binding.Key.EndpointPattern, TargetId: binding.Key.TargetID}, Ref: ref}
}

func liveBindingFromProto(wire *controlv1.LiveBinding, sessionGatewayID, sessionGatewayInstanceID string, requireGatewayID bool) (routing.LiveBinding, error) {
	if wire == nil || wire.GetKey() == nil || wire.GetRef() == nil {
		return routing.LiveBinding{}, fmt.Errorf("live binding is required")
	}
	key := routing.BindingKey{ClientID: wire.GetKey().GetClientId(), EndpointPattern: wire.GetKey().GetEndpointPattern(), TargetID: wire.GetKey().GetTargetId()}
	gatewayID := wire.GetRef().GetGatewayId()
	if gatewayID == "" {
		gatewayID = sessionGatewayID
	}
	if requireGatewayID && wire.GetRef().GetGatewayId() == "" {
		return routing.LiveBinding{}, fmt.Errorf("live binding gateway_id is required")
	}
	binding := routing.LiveBinding{Key: key, Ref: routing.ListenerBindingRef{GatewayID: gatewayID, GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(), ListenerBindingID: wire.GetRef().GetListenerBindingId()}}
	if err := binding.Validate(); err != nil {
		return routing.LiveBinding{}, err
	}
	if sessionGatewayID != "" && (binding.Ref.GatewayID != sessionGatewayID || binding.Ref.GatewayInstanceID != sessionGatewayInstanceID) {
		return routing.LiveBinding{}, fmt.Errorf("live binding does not belong to current control session")
	}
	return binding, nil
}

func bindingConflict(detail string) error {
	if detail == "" {
		return ErrBindingConflict
	}
	return fmt.Errorf("%w: %s", ErrBindingConflict, detail)
}

func (c *Client) setConnecting(endpoint string) {
	c.replaceStatus(Status{GatewayID: c.config.GatewayID, GatewayInstanceID: c.instanceID, State: StateConnecting, Endpoint: endpoint})
}
func (c *Client) setSyncing(endpoint string, session *controlv1.SessionRef) {
	c.replaceSession(StateSyncing, endpoint, session, nil)
}
func (c *Client) setRevalidated(endpoint string, session *controlv1.SessionRef, client controlv1.GatewayControlClient) {
	c.replaceSession(StateRevalidated, endpoint, session, client)
}

func (c *Client) replaceSession(state State, endpoint string, session *controlv1.SessionRef, client controlv1.GatewayControlClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = Status{GatewayID: c.config.GatewayID, GatewayInstanceID: c.instanceID, State: state, Endpoint: endpoint, AuthorityID: session.GetAuthorityId(), ControlSessionID: session.GetControlSessionId()}
	c.admissionClient, c.admissionSession = client, cloneSessionRef(session)
}

func (c *Client) replaceStatus(status Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = status
	c.admissionClient = nil
	c.admissionSession = nil
}

func (c *Client) endCurrentSession(cause error) {
	if cause == nil {
		cause = ErrControlUnavailable
	} else {
		cause = fmt.Errorf("%w: %w", ErrControlUnavailable, cause)
	}
	c.mu.Lock()
	c.status = Status{GatewayID: c.config.GatewayID, GatewayInstanceID: c.instanceID, State: StateDisconnected}
	c.admissionClient, c.admissionSession = nil, nil
	pending := c.queue
	c.queue = nil
	if c.active != nil {
		pending = append([]*pendingMutation{c.active}, pending...)
		c.active = nil
	}
	c.mu.Unlock()
	for _, mutation := range pending {
		mutation.done <- cause
	}
}

func (c *Client) signalLocked() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) stop(cause error) {
	if cause == nil {
		cause = ErrClientClosed
	} else {
		cause = fmt.Errorf("%w: %w", ErrClientClosed, cause)
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped, c.running = true, false
	c.status = Status{GatewayID: c.config.GatewayID, GatewayInstanceID: c.instanceID, State: StateDisconnected}
	c.admissionClient, c.admissionSession = nil, nil
	pending := c.queue
	c.queue = nil
	if c.active != nil {
		pending = append([]*pendingMutation{c.active}, pending...)
		c.active = nil
	}
	c.mu.Unlock()
	for _, mutation := range pending {
		mutation.done <- cause
	}
}

func cloneSessionRef(session *controlv1.SessionRef) *controlv1.SessionRef {
	if session == nil {
		return nil
	}
	return &controlv1.SessionRef{ClusterEpoch: session.GetClusterEpoch(), AuthorityId: session.GetAuthorityId(), ControlSessionId: session.GetControlSessionId(), GatewayId: session.GetGatewayId(), GatewayInstanceId: session.GetGatewayInstanceId()}
}

func newInstanceID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate gateway instance ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func waitForReady(ctx context.Context, connection *grpc.ClientConn, timeout time.Duration) error {
	connectContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connection.Connect()
	for {
		state := connection.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("connection shut down")
		}
		if !connection.WaitForStateChange(connectContext, state) {
			if err := connectContext.Err(); err != nil {
				return err
			}
			return fmt.Errorf("connection state did not change")
		}
	}
}

type responseReceiver interface {
	Recv() (*controlv1.ControlResponse, error)
}

func receiveWithTimeout(ctx context.Context, receiver responseReceiver, timeout time.Duration) (*controlv1.ControlResponse, error) {
	type result struct {
		response *controlv1.ControlResponse
		err      error
	}
	completed := make(chan result, 1)
	go func() { response, err := receiver.Recv(); completed <- result{response, err} }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("control handshake timed out")
	case outcome := <-completed:
		return outcome.response, outcome.err
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
