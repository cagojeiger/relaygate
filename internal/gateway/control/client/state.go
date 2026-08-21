package gatewaycontrol

import (
	"fmt"

	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
)

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
