package relaygate

import relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"

func (c *Client) dispatchListenerBindFailed(failed *relayv1.ListenerBindFailed) error {
	failure, ok := bindingFailureFromProto(failed.GetFailure())
	if !ok || !validEndpoint(failed.GetEndpointPattern()) || !validIdentity(failed.GetTargetId()) {
		return protocolError("invalid ListenerBindFailed")
	}
	c.mu.Lock()
	call := c.pendingBinding
	if call == nil || call.kind != bindingBind || call.endpoint != failed.GetEndpointPattern() || call.target != failed.GetTargetId() {
		c.mu.Unlock()
		return protocolError("foreign ListenerBindFailed")
	}
	c.pendingBinding = nil
	c.mu.Unlock()
	call.result <- bindingResult{err: &BindError{Failure: failure, Endpoint: call.endpoint, Target: call.target}}
	return nil
}

func (c *Client) dispatchListenerUnbindFailed(failed *relayv1.ListenerUnbindFailed) error {
	failure, ok := bindingFailureFromProto(failed.GetFailure())
	if !ok || !validIdentity(failed.GetListenerBindingId()) {
		return protocolError("invalid ListenerUnbindFailed")
	}
	c.mu.Lock()
	call := c.pendingBinding
	listener := c.listeners[failed.GetListenerBindingId()]
	if call == nil || call.kind != bindingUnbind || call.id != failed.GetListenerBindingId() || listener == nil {
		c.mu.Unlock()
		return protocolError("foreign ListenerUnbindFailed")
	}
	c.pendingBinding = nil
	c.mu.Unlock()
	call.result <- bindingResult{err: &UnbindError{Failure: failure, ListenerID: call.id}}
	return nil
}

func (c *Client) dispatchSession(opened *relayv1.ClientSessionOpened) error {
	ref := opened.GetSession()
	if ref == nil || !validIdentity(ref.GetClientSessionId()) || !validIdentity(ref.GetClientId()) ||
		!validIdentity(ref.GetApiKeyId()) || ref.GetAuthRevision() == "" {
		return protocolError("invalid authenticated session")
	}
	c.mu.Lock()
	if c.authenticated || ref.GetClientId() != c.expectedClientID || ref.GetApiKeyId() != c.expectedAPIKeyID {
		c.mu.Unlock()
		return protocolError("unexpected authenticated session")
	}
	c.session = Session{ID: ref.GetClientSessionId(), ClientID: ref.GetClientId(), APIKeyID: ref.GetApiKeyId(), AuthRevision: ref.GetAuthRevision()}
	c.authenticated = true
	session := c.session
	c.mu.Unlock()
	c.auth <- authResult{session: session}
	return nil
}

func (c *Client) dispatchListenerBound(bound *relayv1.ListenerBound) error {
	binding := bound.GetBinding()
	if binding == nil || !validIdentity(binding.GetListenerBindingId()) || !validEndpoint(binding.GetEndpointPattern()) || !validIdentity(binding.GetTargetId()) {
		return protocolError("invalid ListenerBound")
	}
	c.mu.Lock()
	call := c.pendingBinding
	if record, known := c.bindingRecords[binding.GetListenerBindingId()]; known {
		if record.endpoint != binding.GetEndpointPattern() || record.target != binding.GetTargetId() {
			c.mu.Unlock()
			return protocolError("ListenerBound reused a retired identity with different metadata")
		}
		if call != nil && call.kind == bindingBind && call.endpoint == binding.GetEndpointPattern() && call.target == binding.GetTargetId() {
			c.mu.Unlock()
			return protocolError("ListenerBound reused an ambiguous retired identity")
		}
		c.mu.Unlock()
		return nil
	}
	if call == nil || call.kind != bindingBind || call.endpoint != binding.GetEndpointPattern() || call.target != binding.GetTargetId() {
		c.mu.Unlock()
		return protocolError("foreign ListenerBound")
	}
	if len(c.listeners) >= maxListeners {
		c.mu.Unlock()
		return protocolError("listener table capacity exceeded")
	}
	if _, exists := c.listeners[binding.GetListenerBindingId()]; exists {
		c.mu.Unlock()
		return protocolError("duplicate listener binding")
	}
	listener := newListener(c, binding.GetListenerBindingId(), binding.GetEndpointPattern(), binding.GetTargetId())
	if !c.addBindingRecordLocked(bindingRecord{id: listener.id, endpoint: listener.endpoint, target: listener.target}) {
		c.mu.Unlock()
		return protocolError("binding retired-history capacity exceeded")
	}
	c.listeners[listener.id] = listener
	c.pendingBinding = nil
	c.mu.Unlock()
	call.result <- bindingResult{listener: listener}
	return nil
}

func (c *Client) dispatchListenerUnbound(unbound *relayv1.ListenerUnbound) error {
	id := unbound.GetListenerBindingId()
	if !validIdentity(id) {
		return protocolError("invalid ListenerUnbound")
	}
	c.mu.Lock()
	call := c.pendingBinding
	listener := c.listeners[id]
	if record, known := c.bindingRecords[id]; known && record.unbound {
		c.mu.Unlock()
		return nil
	}
	if call == nil || call.kind != bindingUnbind || call.id != id || listener == nil {
		c.mu.Unlock()
		return protocolError("foreign ListenerUnbound")
	}
	delete(c.listeners, id)
	record := c.bindingRecords[id]
	record.unbound = true
	c.bindingRecords[id] = record
	c.pendingBinding = nil
	c.mu.Unlock()
	listener.end(ErrListenerEnded)
	call.result <- bindingResult{}
	return nil
}
