package relaygate

import (
	"fmt"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

func (c *Client) dispatchListenerOffer(message *relayv1.ListenerOffer) error {
	if !validIdentity(message.GetAttemptId()) || !validIdentity(message.GetListenerBindingId()) ||
		!validEndpoint(message.GetEndpoint()) || !validIdentity(message.GetTargetId()) || !validIdentity(message.GetCallerSessionId()) {
		return protocolError("invalid ListenerOffer")
	}
	c.mu.Lock()
	listener := c.listeners[message.GetListenerBindingId()]
	if listener == nil || listener.endpoint != message.GetEndpoint() || listener.target != message.GetTargetId() {
		c.mu.Unlock()
		return protocolError("foreign ListenerOffer")
	}
	if _, exists := c.offerTombstones[message.GetAttemptId()]; exists {
		c.mu.Unlock()
		return protocolError("replayed ListenerOffer")
	}
	if len(c.offers) >= maxPendingOffers {
		c.mu.Unlock()
		return protocolError("offer table capacity exceeded")
	}
	if _, exists := c.offers[message.GetAttemptId()]; exists {
		c.mu.Unlock()
		return protocolError("duplicate ListenerOffer")
	}
	offer := newOffer(listener, message.GetAttemptId(), message.GetCallerSessionId())
	c.offers[offer.attemptID] = offer
	c.mu.Unlock()
	if !listener.enqueue(offer) {
		return protocolError("listener offer queue capacity exceeded")
	}
	return nil
}

func (c *Client) dispatchListenerEstablished(message *relayv1.ListenerEstablished) error {
	if !validIdentity(message.GetAttemptId()) || !validIdentity(message.GetPipeId()) {
		return protocolError("invalid ListenerEstablished")
	}
	c.mu.Lock()
	offer := c.offers[message.GetAttemptId()]
	if offer == nil || !offer.isAccepting() {
		c.mu.Unlock()
		return protocolError("foreign ListenerEstablished")
	}
	if _, exists := c.pipes[message.GetPipeId()]; exists {
		c.mu.Unlock()
		return protocolError("duplicate Pipe identity")
	}
	if !offer.transferReservation() {
		c.mu.Unlock()
		return protocolError("ListenerEstablished without a Pipe reservation")
	}
	pipe := newPipe(c, message.GetPipeId(), message.GetAttemptId(), offer.endpoint, offer.target)
	c.pipes[pipe.id] = pipe
	if !offer.establish(pipe) {
		c.mu.Unlock()
		return protocolError("ListenerEstablished in wrong phase")
	}
	c.mu.Unlock()
	return nil
}

func (c *Client) dispatchListenerConfirmationAcknowledged(message *relayv1.ListenerConfirmationAcknowledged) error {
	if !validIdentity(message.GetAttemptId()) || !validIdentity(message.GetPipeId()) {
		return protocolError("invalid ListenerConfirmationAcknowledged")
	}
	c.mu.Lock()
	if retired, exists := c.offerTombstones[message.GetAttemptId()]; exists {
		exact := retired.pipeID == message.GetPipeId() &&
			retired.decisionFailure == relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_UNSPECIFIED
		c.mu.Unlock()
		if exact {
			return nil
		}
		return protocolError("ListenerConfirmationAcknowledged conflicted with terminal history")
	}
	offer := c.offers[message.GetAttemptId()]
	pipe := c.pipes[message.GetPipeId()]
	if offer == nil || pipe == nil || pipe.attemptID != message.GetAttemptId() {
		c.mu.Unlock()
		return protocolError("foreign ListenerConfirmationAcknowledged")
	}
	delete(c.offers, message.GetAttemptId())
	c.addOfferTombstoneLocked(message.GetAttemptId(), offerTombstone{pipeID: message.GetPipeId()})
	c.mu.Unlock()
	if !offer.acknowledge(pipe) {
		return protocolError("confirmation acknowledgement in wrong phase")
	}
	return nil
}

func (c *Client) dispatchListenerTerminated(message *relayv1.ListenerTerminated) error {
	if !validIdentity(message.GetAttemptId()) || (message.GetPipeId() != "" && !validIdentity(message.GetPipeId())) {
		return protocolError("invalid ListenerTerminated")
	}
	c.mu.Lock()
	offer := c.offers[message.GetAttemptId()]
	var pipe *Pipe
	if message.GetPipeId() != "" {
		pipe = c.pipes[message.GetPipeId()]
		if pipe == nil {
			if call := c.closeCalls[message.GetPipeId()]; call != nil && call.pipe.attemptID == message.GetAttemptId() {
				call.terminalSeen = true
				pipe = call.pipe
				c.addPipeTombstoneLocked(pipe)
			}
		}
		if pipe == nil {
			if tombstone := c.pipeTombstones[message.GetPipeId()]; tombstone != nil && tombstone.attemptID == message.GetAttemptId() &&
				c.matchesOfferTombstoneLocked(message.GetAttemptId(), message.GetPipeId()) {
				c.mu.Unlock()
				return nil
			}
		}
		if pipe == nil || pipe.attemptID != message.GetAttemptId() {
			c.mu.Unlock()
			return protocolError("foreign ListenerTerminated")
		}
		delete(c.pipes, pipe.id)
		c.addPipeTombstoneLocked(pipe)
		c.addOfferTombstoneLocked(message.GetAttemptId(), offerTombstone{pipeID: message.GetPipeId()})
		if call := c.closeCalls[pipe.id]; call != nil {
			call.terminalSeen = true
		}
	}
	if offer == nil && pipe == nil {
		if c.matchesOfferTombstoneLocked(message.GetAttemptId(), message.GetPipeId()) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		return protocolError("unknown ListenerTerminated")
	}
	delete(c.offers, message.GetAttemptId())
	c.addOfferTombstoneLocked(message.GetAttemptId(), offerTombstone{pipeID: message.GetPipeId()})
	c.mu.Unlock()
	if offer != nil {
		offer.terminate(ErrPipeClosed)
	}
	if pipe != nil {
		pipe.terminate(ErrPipeClosed)
	}
	return nil
}

func (c *Client) dispatchListenerDecisionRejected(message *relayv1.ListenerDecisionRejected) error {
	if !validIdentity(message.GetAttemptId()) || !validListenerDecisionFailure(message.GetFailure()) {
		return protocolError("invalid ListenerDecisionRejected")
	}
	rejection := fmt.Errorf("relaygate: listener decision rejected (%s)", message.GetFailure())
	c.mu.Lock()
	offer := c.offers[message.GetAttemptId()]
	if offer == nil {
		retired, exists := c.offerTombstones[message.GetAttemptId()]
		c.mu.Unlock()
		if exists && retired.decisionFailure == message.GetFailure() {
			return nil
		}
		if exists && retired.decisionFailure != relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_UNSPECIFIED {
			return protocolError("conflicting duplicate ListenerDecisionRejected")
		}
		return protocolError("foreign ListenerDecisionRejected")
	}
	pipe, ok := offer.markDecisionRejected()
	if !ok {
		c.mu.Unlock()
		return protocolError("foreign ListenerDecisionRejected")
	}
	delete(c.offers, message.GetAttemptId())
	pipeID := ""
	if pipe != nil {
		pipeID = pipe.id
		if c.pipes[pipe.id] == pipe {
			delete(c.pipes, pipe.id)
		}
		c.addPipeTombstoneLocked(pipe)
	}
	c.addOfferTombstoneLocked(message.GetAttemptId(), offerTombstone{
		pipeID:          pipeID,
		decisionFailure: message.GetFailure(),
	})
	c.mu.Unlock()
	if pipe != nil {
		pipe.terminate(rejection)
	}
	offer.finishDecisionRejection(rejection)
	return nil
}
