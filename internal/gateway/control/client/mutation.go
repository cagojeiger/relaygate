package gatewaycontrol

import (
	"context"
	"fmt"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
)

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
