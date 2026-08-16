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

	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	controlKeepaliveTime    = 10 * time.Second
	controlKeepaliveTimeout = 5 * time.Second
)

var (
	ErrControlUnavailable = errors.New("gateway control is not revalidated")
	ErrBindingConflict    = fmt.Errorf("binding compare-and-set conflict: %w", controlstate.ErrCASMismatch)
	ErrBindingCapacity    = fmt.Errorf("gateway listener binding capacity reached: %w", controlstate.ErrBindingCapacity)
	ErrClientClosed       = errors.New("gateway control client is closed")
	ErrInvalidMutation    = errors.New("invalid binding mutation")
)

type Config struct {
	ClusterEpoch     string
	GatewayID        string
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
	GatewayGeneration uint64 `json:"gateway_generation,omitempty"`
}

func (s Status) Ready() bool {
	return s.State == StateRevalidated
}

type mutationKind uint8

const (
	mutationInstall mutationKind = iota + 1
	mutationRemove
)

type pendingMutation struct {
	kind mutationKind
	ctx  context.Context //nolint:containedctx // Pending mutations own the caller deadline until Raft commit or retry completes.
	done chan mutationOutcome

	key      controlstate.BindingKey
	newRef   controlstate.ListenerBindingRef
	remove   controlstate.BindingSlot
	expected controlstate.BindingSlot
	prepared bool
	retried  bool
	sent     bool
}

type mutationOutcome struct {
	slot controlstate.BindingSlot
	err  error
}

type Client struct {
	config     Config
	logger     *slog.Logger
	instanceID string
	keepalive  keepalive.ClientParameters

	mu     sync.Mutex
	status Status
	// admissionClient and admissionSession are published atomically with a
	// Revalidated status and cleared on every other control state.
	admissionClient  controlv1.GatewayControlClient
	admissionSession *controlv1.SessionRef
	owned            map[controlstate.BindingKey]controlstate.BindingSlot
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
	if instanceID == "" || len(instanceID) > controlstate.MaxIdentityBytes {
		return nil, fmt.Errorf("gateway instance ID must be 1..%d bytes", controlstate.MaxIdentityBytes)
	}
	if logger == nil {
		logger = slog.Default()
	}
	client := &Client{
		config:     config,
		logger:     logger.With("component", "gateway_control", "gateway_id", config.GatewayID, "gateway_instance_id", instanceID),
		instanceID: instanceID,
		keepalive: keepalive.ClientParameters{
			Time:    controlKeepaliveTime,
			Timeout: controlKeepaliveTimeout,
		},
		status: Status{
			GatewayID:         config.GatewayID,
			GatewayInstanceID: instanceID,
			State:             StateDisconnected,
		},
		owned: make(map[controlstate.BindingKey]controlstate.BindingSlot),
		wake:  make(chan struct{}, 1),
	}
	return client, nil
}

func (c Config) validate() error {
	if c.ClusterEpoch == "" || len(c.ClusterEpoch) > controlstate.MaxIdentityBytes {
		return fmt.Errorf("cluster epoch must be 1..%d bytes", controlstate.MaxIdentityBytes)
	}
	if c.GatewayID == "" || len(c.GatewayID) > controlstate.MaxIdentityBytes {
		return fmt.Errorf("gateway ID must be 1..%d bytes", controlstate.MaxIdentityBytes)
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

func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// Install commits a binding for this process instance. Admission is fail-fast:
// once accepted while Revalidated, the exact CAS remains queued across control
// reconnects until the authority gives a definitive result.
func (c *Client) Install(ctx context.Context, key controlstate.BindingKey, ref controlstate.ListenerBindingRef) (controlstate.BindingSlot, error) {
	if ctx == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: context is required", ErrInvalidMutation)
	}
	if err := key.Validate(); err != nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: %w", ErrInvalidMutation, err)
	}
	if err := ref.Validate(); err != nil {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: %w", ErrInvalidMutation, err)
	}
	if ref.GatewayID != c.config.GatewayID || ref.GatewayInstanceID != c.instanceID {
		return controlstate.BindingSlot{}, fmt.Errorf("%w: install ref does not belong to this gateway instance", ErrInvalidMutation)
	}
	if err := ctx.Err(); err != nil {
		return controlstate.BindingSlot{}, err
	}

	mutation := &pendingMutation{
		kind:   mutationInstall,
		ctx:    ctx,
		done:   make(chan mutationOutcome, 1),
		key:    key,
		newRef: ref,
	}
	if err := c.enqueueMutation(mutation, true); err != nil {
		return controlstate.BindingSlot{}, err
	}

	result := waitMutation(ctx, mutation.done)
	return result.slot, result.err
}

// Remove conditionally removes an exact committed slot. Cleanup is accepted
// while disconnected and waits for the next revalidated control stream.
func (c *Client) Remove(ctx context.Context, slot controlstate.BindingSlot) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidMutation)
	}
	if err := slot.Key.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMutation, err)
	}
	if slot.Generation == 0 || slot.Generation == ^uint64(0) || slot.Ref == nil {
		return fmt.Errorf("%w: remove requires a committed live slot", ErrInvalidMutation)
	}
	if err := slot.Ref.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMutation, err)
	}
	if slot.Ref.GatewayID != c.config.GatewayID {
		return fmt.Errorf("%w: remove ref belongs to another gateway", ErrInvalidMutation)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	mutation := &pendingMutation{
		kind:   mutationRemove,
		ctx:    ctx,
		done:   make(chan mutationOutcome, 1),
		key:    slot.Key,
		remove: cloneBindingSlot(slot),
	}
	if err := c.enqueueMutation(mutation, false); err != nil {
		return err
	}

	return waitMutation(ctx, mutation.done).err
}

func (c *Client) enqueueMutation(mutation *pendingMutation, requireRevalidated bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return ErrClientClosed
	}
	if requireRevalidated && c.status.State != StateRevalidated {
		return ErrControlUnavailable
	}
	c.queue = append(c.queue, mutation)
	c.signalLocked()
	return nil
}

func waitMutation(ctx context.Context, completed <-chan mutationOutcome) mutationOutcome {
	select {
	case result := <-completed:
		return result
	case <-ctx.Done():
		select {
		case result := <-completed:
			return result
		default:
			return mutationOutcome{err: ctx.Err()}
		}
	}
}

func (c *Client) Run(ctx context.Context) {
	defer func() { c.stop(ctx.Err()) }()
	endpointIndex := 0
	for ctx.Err() == nil {
		endpoint := c.config.ControlEndpoints[endpointIndex]
		c.setConnecting(endpoint)
		revalidated, err := c.runEndpoint(ctx, endpoint)
		c.setDisconnected()
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
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{
		Hello: &controlv1.Hello{
			ClusterEpoch:      c.config.ClusterEpoch,
			GatewayId:         c.config.GatewayID,
			GatewayInstanceId: c.instanceID,
		},
	}}); err != nil {
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
	owned, snapshot, err := c.canonicalOwnedBindings(opened.GetOwnedBindings())
	if err != nil {
		return false, fmt.Errorf("validate authoritative bindings: %w", err)
	}
	session := opened.GetSession()
	c.setSyncing(endpoint, session, opened.GetGatewayGeneration(), owned)
	reconciled := c.reconcileActive(owned)

	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: session, Bindings: snapshot},
	}}); err != nil {
		return false, fmt.Errorf("send gateway snapshot: %w", err)
	}
	response, err = receiveWithTimeout(streamContext, stream, c.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Errorf("receive snapshot acceptance: %w", err)
	}
	accepted := response.GetSnapshotAccepted()
	if accepted == nil || accepted.GetPresence() == controlv1.PresenceState_PRESENCE_STATE_UNSPECIFIED {
		return false, fmt.Errorf("control endpoint returned an invalid snapshot response")
	}
	c.setRevalidated(endpoint, session, opened.GetGatewayGeneration(), controlClient)
	if reconciled != nil {
		c.finishMutation(reconciled.mutation, reconciled.outcome)
	}
	c.logger.Info("gateway control session revalidated",
		"endpoint", endpoint,
		"authority_id", session.GetAuthorityId(),
		"control_session_id", session.GetControlSessionId(),
		"gateway_generation", opened.GetGatewayGeneration(),
		"presence", accepted.GetPresence().String(),
	)

	return true, c.serveMutations(streamContext, stream, session)
}

type reconciledMutation struct {
	mutation *pendingMutation
	outcome  mutationOutcome
}

func (c *Client) reconcileActive(owned map[controlstate.BindingKey]controlstate.BindingSlot) *reconciledMutation {
	c.mu.Lock()
	defer c.mu.Unlock()
	mutation := c.active
	if mutation == nil || !mutation.prepared {
		return nil
	}
	current, exists := owned[mutation.key]
	switch mutation.kind {
	case mutationInstall:
		target := mutation.installTarget()
		if exists && bindingSlotsEqual(current, target) {
			return &reconciledMutation{mutation: mutation, outcome: mutationOutcome{slot: cloneBindingSlot(current)}}
		}
	case mutationRemove:
		if !exists || !bindingSlotsEqual(current, mutation.remove) {
			return &reconciledMutation{mutation: mutation}
		}
	}
	return nil
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

		if err := mutation.ctx.Err(); err != nil && !mutation.sent {
			c.finishMutation(mutation, mutationOutcome{err: err})
			continue
		}
		request, err := c.mutationRequest(session, mutation)
		if err != nil {
			c.finishMutation(mutation, mutationOutcome{err: err})
			continue
		}
		c.markSent(mutation)
		if err := stream.Send(request); err != nil {
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
			retry, outcome, err := c.handleMutationResult(mutation, result)
			if err != nil {
				return err
			}
			if retry {
				continue
			}
			c.finishMutation(mutation, outcome)
		}
	}
}

func (c *Client) nextMutation() *pendingMutation {
	for {
		c.mu.Lock()
		if c.active != nil {
			mutation := c.active
			c.mu.Unlock()
			return mutation
		}
		if len(c.queue) == 0 {
			c.mu.Unlock()
			return nil
		}
		mutation := c.queue[0]
		c.queue = c.queue[1:]
		if mutation.kind == mutationInstall {
			expected, exists := c.owned[mutation.key]
			if !exists {
				expected = controlstate.BindingSlot{Key: mutation.key}
			}
			if expected.Generation == ^uint64(0) {
				c.mu.Unlock()
				mutation.done <- mutationOutcome{err: fmt.Errorf("%w: binding generation overflow", ErrInvalidMutation)}
				continue
			}
			mutation.expected = cloneBindingSlot(expected)
			mutation.prepared = true
		}
		c.active = mutation
		c.mu.Unlock()
		return mutation
	}
}

func (c *Client) mutationRequest(session *controlv1.SessionRef, mutation *pendingMutation) (*controlv1.ControlRequest, error) {
	wire := &controlv1.BindingMutation{Session: session}
	switch mutation.kind {
	case mutationInstall:
		if !mutation.prepared {
			return nil, fmt.Errorf("%w: install was not prepared", ErrInvalidMutation)
		}
		wire.Mutation = &controlv1.BindingMutation_Install{Install: &controlv1.InstallBinding{
			Key:                bindingKeyToProto(mutation.key),
			ExpectedGeneration: mutation.expected.Generation,
			ExpectedRef:        bindingRefToProto(mutation.expected.Ref),
			NewRef:             bindingRefToProto(&mutation.newRef),
		}}
	case mutationRemove:
		wire.Mutation = &controlv1.BindingMutation_Remove{Remove: &controlv1.RemoveBinding{
			Key:                bindingKeyToProto(mutation.remove.Key),
			ExpectedGeneration: mutation.remove.Generation,
			ExpectedRef:        bindingRefToProto(mutation.remove.Ref),
		}}
	default:
		return nil, fmt.Errorf("%w: unknown mutation kind", ErrInvalidMutation)
	}
	return &controlv1.ControlRequest{Message: &controlv1.ControlRequest_BindingMutation{BindingMutation: wire}}, nil
}

func (c *Client) markSent(mutation *pendingMutation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == mutation {
		mutation.sent = true
	}
}

func (c *Client) handleMutationResult(mutation *pendingMutation, result *controlv1.MutationResult) (bool, mutationOutcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != mutation {
		return false, mutationOutcome{}, fmt.Errorf("mutation result does not match the active request")
	}

	switch result.GetCode() {
	case controlv1.MutationCode_MUTATION_CODE_APPLIED, controlv1.MutationCode_MUTATION_CODE_ALREADY_APPLIED:
		if mutation.kind == mutationInstall {
			slot, err := bindingSlotFromProto(result.GetSlot(), c.config.GatewayID, true)
			if err != nil {
				return false, mutationOutcome{}, fmt.Errorf("control returned an invalid install result: %w", err)
			}
			if !bindingSlotsEqual(slot, mutation.installTarget()) {
				return false, mutationOutcome{}, fmt.Errorf("control returned a mismatched install result")
			}
			c.owned[slot.Key] = cloneBindingSlot(slot)
			return false, mutationOutcome{slot: cloneBindingSlot(slot)}, nil
		}
		slot, err := bindingSlotFromProto(result.GetSlot(), c.config.GatewayID, false)
		if err != nil {
			return false, mutationOutcome{}, fmt.Errorf("control returned an invalid remove result: %w", err)
		}
		if slot.Key != mutation.remove.Key || slot.Ref != nil || slot.Generation != mutation.remove.Generation+1 {
			return false, mutationOutcome{}, fmt.Errorf("control returned a mismatched remove result")
		}
		c.owned[slot.Key] = cloneBindingSlot(slot)
		return false, mutationOutcome{}, nil

	case controlv1.MutationCode_MUTATION_CODE_CAPACITY_REACHED:
		return false, mutationOutcome{err: ErrBindingCapacity}, nil

	case controlv1.MutationCode_MUTATION_CODE_REJECTED:
		if mutation.kind == mutationRemove {
			c.recordRejectedRemoveLocked(mutation, result.GetSlot(), result.GetSameGatewayOwner())
			return false, mutationOutcome{}, nil
		}
		current, knownOwn, err := c.rejectedInstallSlotLocked(mutation, result.GetSlot(), result.GetSameGatewayOwner())
		if err != nil {
			return false, mutationOutcome{}, err
		}
		if result.GetSlot().GetRef() != nil && !knownOwn {
			return false, mutationOutcome{err: bindingConflict(result.GetError())}, nil
		}
		if mutation.retried || bindingSlotsEqual(current, mutation.expected) || current.Generation == ^uint64(0) {
			return false, mutationOutcome{err: bindingConflict(result.GetError())}, nil
		}
		c.owned[current.Key] = cloneBindingSlot(current)
		mutation.expected = cloneBindingSlot(current)
		mutation.retried = true
		return true, mutationOutcome{}, nil

	default:
		return false, mutationOutcome{}, fmt.Errorf("control returned an unspecified mutation result")
	}
}

func (c *Client) rejectedInstallSlotLocked(mutation *pendingMutation, wire *controlv1.BindingSlot, sameGatewayOwner bool) (controlstate.BindingSlot, bool, error) {
	if wire == nil {
		return controlstate.BindingSlot{}, false, fmt.Errorf("control rejected install without a current slot")
	}
	key, err := bindingKeyFromProto(wire.GetKey())
	if err != nil {
		return controlstate.BindingSlot{}, false, fmt.Errorf("control rejected install with an invalid current key: %w", err)
	}
	if key != mutation.key {
		return controlstate.BindingSlot{}, false, fmt.Errorf("control rejected install with a mismatched current key")
	}
	current := controlstate.BindingSlot{Key: key, Generation: wire.GetGeneration()}
	if wire.GetRef() == nil {
		return current, true, nil
	}
	if !sameGatewayOwner {
		return current, false, nil
	}
	ref := controlstate.ListenerBindingRef{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(),
		ListenerBindingID: wire.GetRef().GetListenerBindingId(),
	}
	if err := ref.Validate(); err != nil {
		return controlstate.BindingSlot{}, false, fmt.Errorf("control rejected install with an invalid current ref: %w", err)
	}
	current.Ref = &ref
	return current, true, nil
}

func (c *Client) knownOwnRefLocked(key controlstate.BindingKey, wire *controlv1.ListenerBindingRef, mutation *pendingMutation) *controlstate.ListenerBindingRef {
	if slot, exists := c.owned[key]; exists {
		if ref := c.matchKnownRef(slot.Ref, wire); ref != nil {
			return ref
		}
	}
	for _, candidate := range []*controlstate.ListenerBindingRef{mutation.expected.Ref, mutation.remove.Ref, &mutation.newRef} {
		if ref := c.matchKnownRef(candidate, wire); ref != nil {
			return ref
		}
	}
	return nil
}

func (c *Client) matchKnownRef(candidate *controlstate.ListenerBindingRef, wire *controlv1.ListenerBindingRef) *controlstate.ListenerBindingRef {
	if candidate == nil ||
		candidate.GatewayID != c.config.GatewayID ||
		candidate.GatewayInstanceID != wire.GetGatewayInstanceId() ||
		candidate.ListenerBindingID != wire.GetListenerBindingId() {
		return nil
	}
	copy := *candidate
	return &copy
}

func (c *Client) recordRejectedRemoveLocked(mutation *pendingMutation, wire *controlv1.BindingSlot, sameGatewayOwner bool) {
	if wire == nil {
		delete(c.owned, mutation.key)
		return
	}
	key, err := bindingKeyFromProto(wire.GetKey())
	if err != nil || key != mutation.key {
		delete(c.owned, mutation.key)
		return
	}
	slot := controlstate.BindingSlot{Key: key, Generation: wire.GetGeneration()}
	if wire.GetRef() != nil {
		if !sameGatewayOwner {
			delete(c.owned, key)
			return
		}
		known := c.knownOwnRefLocked(key, wire.GetRef(), mutation)
		if known == nil {
			delete(c.owned, key)
			return
		}
		slot.Ref = known
	}
	c.owned[key] = slot
}

func bindingConflict(detail string) error {
	if detail == "" {
		return ErrBindingConflict
	}
	return fmt.Errorf("%w: %s", ErrBindingConflict, detail)
}

func (m *pendingMutation) installTarget() controlstate.BindingSlot {
	ref := m.newRef
	return controlstate.BindingSlot{Key: m.key, Generation: m.expected.Generation + 1, Ref: &ref}
}

func (c *Client) finishMutation(mutation *pendingMutation, outcome mutationOutcome) {
	c.mu.Lock()
	if c.active != mutation {
		c.mu.Unlock()
		return
	}
	c.active = nil
	c.signalLocked()
	c.mu.Unlock()
	mutation.done <- outcome
}

func (c *Client) validateSession(opened *controlv1.SessionOpened) error {
	if opened == nil || opened.GetSession() == nil {
		return fmt.Errorf("control endpoint did not open a session")
	}
	session := opened.GetSession()
	if session.GetClusterEpoch() != c.config.ClusterEpoch ||
		session.GetGatewayId() != c.config.GatewayID ||
		session.GetGatewayInstanceId() != c.instanceID ||
		session.GetAuthorityId() == "" ||
		len(session.GetAuthorityId()) > controlstate.MaxIdentityBytes ||
		session.GetControlSessionId() == "" ||
		len(session.GetControlSessionId()) > controlstate.MaxIdentityBytes ||
		opened.GetGatewayGeneration() == 0 {
		return fmt.Errorf("control endpoint returned a mismatched session")
	}
	return nil
}

func (c *Client) canonicalOwnedBindings(wire []*controlv1.BindingSlot) (map[controlstate.BindingKey]controlstate.BindingSlot, []*controlv1.BindingSlot, error) {
	owned := make(map[controlstate.BindingKey]controlstate.BindingSlot, len(wire))
	currentInstance := make([]controlstate.BindingSlot, 0, len(wire))
	for index, item := range wire {
		slot, err := bindingSlotFromProto(item, c.config.GatewayID, true)
		if err != nil {
			return nil, nil, fmt.Errorf("owned binding %d: %w", index, err)
		}
		if _, duplicate := owned[slot.Key]; duplicate {
			return nil, nil, fmt.Errorf("owned binding %d duplicates a key", index)
		}
		owned[slot.Key] = cloneBindingSlot(slot)
		if slot.Ref.GatewayInstanceID != c.instanceID {
			return nil, nil, fmt.Errorf("owned binding %d belongs to another gateway instance", index)
		}
		currentInstance = append(currentInstance, cloneBindingSlot(slot))
	}
	sort.Slice(currentInstance, func(left, right int) bool {
		return bindingKeyLess(currentInstance[left].Key, currentInstance[right].Key)
	})
	snapshot := make([]*controlv1.BindingSlot, 0, len(currentInstance))
	for _, slot := range currentInstance {
		snapshot = append(snapshot, bindingSlotToProto(slot))
	}
	return owned, snapshot, nil
}

func bindingKeyLess(left, right controlstate.BindingKey) bool {
	if left.ClientID != right.ClientID {
		return left.ClientID < right.ClientID
	}
	if left.EndpointPattern != right.EndpointPattern {
		return left.EndpointPattern < right.EndpointPattern
	}
	return left.TargetID < right.TargetID
}

func bindingKeyToProto(key controlstate.BindingKey) *controlv1.BindingKey {
	return &controlv1.BindingKey{ClientId: key.ClientID, EndpointPattern: key.EndpointPattern, TargetId: key.TargetID}
}

func bindingRefToProto(ref *controlstate.ListenerBindingRef) *controlv1.ListenerBindingRef {
	if ref == nil {
		return nil
	}
	return &controlv1.ListenerBindingRef{GatewayInstanceId: ref.GatewayInstanceID, ListenerBindingId: ref.ListenerBindingID}
}

func bindingSlotToProto(slot controlstate.BindingSlot) *controlv1.BindingSlot {
	return &controlv1.BindingSlot{Key: bindingKeyToProto(slot.Key), Generation: slot.Generation, Ref: bindingRefToProto(slot.Ref)}
}

func bindingKeyFromProto(wire *controlv1.BindingKey) (controlstate.BindingKey, error) {
	if wire == nil {
		return controlstate.BindingKey{}, fmt.Errorf("binding key is required")
	}
	key := controlstate.BindingKey{ClientID: wire.GetClientId(), EndpointPattern: wire.GetEndpointPattern(), TargetID: wire.GetTargetId()}
	if err := key.Validate(); err != nil {
		return controlstate.BindingKey{}, err
	}
	return key, nil
}

func bindingSlotFromProto(wire *controlv1.BindingSlot, gatewayID string, requireLive bool) (controlstate.BindingSlot, error) {
	if wire == nil {
		return controlstate.BindingSlot{}, fmt.Errorf("binding slot is required")
	}
	key, err := bindingKeyFromProto(wire.GetKey())
	if err != nil {
		return controlstate.BindingSlot{}, err
	}
	if requireLive && (wire.GetGeneration() == 0 || wire.GetRef() == nil) {
		return controlstate.BindingSlot{}, fmt.Errorf("owned binding must be a committed live slot")
	}
	slot := controlstate.BindingSlot{Key: key, Generation: wire.GetGeneration()}
	if wire.GetRef() != nil {
		ref := controlstate.ListenerBindingRef{
			GatewayID:         gatewayID,
			GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(),
			ListenerBindingID: wire.GetRef().GetListenerBindingId(),
		}
		if err := ref.Validate(); err != nil {
			return controlstate.BindingSlot{}, err
		}
		if slot.Generation == 0 {
			return controlstate.BindingSlot{}, fmt.Errorf("live binding generation must be positive")
		}
		slot.Ref = &ref
	}
	return slot, nil
}

func bindingSlotsEqual(left, right controlstate.BindingSlot) bool {
	if left.Key != right.Key || left.Generation != right.Generation {
		return false
	}
	if left.Ref == nil || right.Ref == nil {
		return left.Ref == nil && right.Ref == nil
	}
	return *left.Ref == *right.Ref
}

func cloneBindingSlot(slot controlstate.BindingSlot) controlstate.BindingSlot {
	copy := slot
	if slot.Ref != nil {
		ref := *slot.Ref
		copy.Ref = &ref
	}
	return copy
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
	go func() {
		response, err := receiver.Recv()
		completed <- result{response: response, err: err}
	}()
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

func (c *Client) setConnecting(endpoint string) {
	c.replaceStatus(Status{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: c.instanceID,
		State:             StateConnecting,
		Endpoint:          endpoint,
	})
}

func (c *Client) setSyncing(endpoint string, session *controlv1.SessionRef, generation uint64, owned map[controlstate.BindingKey]controlstate.BindingSlot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = c.sessionStatus(StateSyncing, endpoint, session, generation)
	c.admissionClient = nil
	c.admissionSession = nil
	c.owned = owned
}

func (c *Client) setRevalidated(
	endpoint string,
	session *controlv1.SessionRef,
	generation uint64,
	controlClient controlv1.GatewayControlClient,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = c.sessionStatus(StateRevalidated, endpoint, session, generation)
	c.admissionClient = controlClient
	c.admissionSession = cloneSessionRef(session)
}

func (c *Client) sessionStatus(state State, endpoint string, session *controlv1.SessionRef, generation uint64) Status {
	return Status{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: c.instanceID,
		State:             state,
		Endpoint:          endpoint,
		AuthorityID:       session.GetAuthorityId(),
		ControlSessionID:  session.GetControlSessionId(),
		GatewayGeneration: generation,
	}
}

func (c *Client) setDisconnected() {
	c.replaceStatus(Status{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: c.instanceID,
		State:             StateDisconnected,
	})
}

func (c *Client) replaceStatus(status Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = status
	c.admissionClient = nil
	c.admissionSession = nil
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
	c.status = Status{GatewayID: c.config.GatewayID, GatewayInstanceID: c.instanceID, State: StateDisconnected}
	c.admissionClient = nil
	c.admissionSession = nil
	c.stopped = true
	pending := c.queue
	c.queue = nil
	if c.active != nil {
		pending = append([]*pendingMutation{c.active}, pending...)
		c.active = nil
	}
	c.mu.Unlock()
	for _, mutation := range pending {
		mutation.done <- mutationOutcome{err: cause}
	}
}

func cloneSessionRef(session *controlv1.SessionRef) *controlv1.SessionRef {
	if session == nil {
		return nil
	}
	return &controlv1.SessionRef{
		ClusterEpoch:      session.GetClusterEpoch(),
		AuthorityId:       session.GetAuthorityId(),
		ControlSessionId:  session.GetControlSessionId(),
		GatewayId:         session.GetGatewayId(),
		GatewayInstanceId: session.GetGatewayInstanceId(),
	}
}

func newInstanceID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate gateway instance ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
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
