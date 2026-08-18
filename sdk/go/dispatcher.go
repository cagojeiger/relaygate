package relaygate

import (
	"fmt"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

func (c *Client) dispatch(response *relayv1.ConnectResponse) error {
	if response == nil || response.GetMessage() == nil {
		return protocolError("empty response")
	}
	if opened := response.GetClientSessionOpened(); opened != nil {
		return c.dispatchSession(opened)
	}
	c.mu.Lock()
	authenticated := c.authenticated
	c.mu.Unlock()
	if !authenticated {
		return protocolError("relay operation before authentication acknowledgement")
	}

	switch {
	case response.GetListenerBound() != nil:
		return c.dispatchListenerBound(response.GetListenerBound())
	case response.GetListenerUnbound() != nil:
		return c.dispatchListenerUnbound(response.GetListenerUnbound())
	case response.GetListenerOffer() != nil:
		return c.dispatchListenerOffer(response.GetListenerOffer())
	case response.GetListenerEstablished() != nil:
		return c.dispatchListenerEstablished(response.GetListenerEstablished())
	case response.GetListenerConfirmationAcknowledged() != nil:
		return c.dispatchListenerConfirmationAcknowledged(response.GetListenerConfirmationAcknowledged())
	case response.GetListenerTerminated() != nil:
		return c.dispatchListenerTerminated(response.GetListenerTerminated())
	case response.GetListenerDecisionRejected() != nil:
		return c.dispatchListenerDecisionRejected(response.GetListenerDecisionRejected())
	case response.GetPipeOpened() != nil:
		return c.dispatchPipeOpened(response.GetPipeOpened())
	case response.GetPipeOpenFailed() != nil:
		return c.dispatchPipeOpenFailed(response.GetPipeOpenFailed())
	case response.GetPipeOpenUnknown() != nil:
		return c.dispatchPipeOpenUnknown(response.GetPipeOpenUnknown())
	case response.GetOpenRequestRejected() != nil:
		return c.dispatchOpenRequestRejected(response.GetOpenRequestRejected())
	case response.GetOpenCancelAcknowledged() != nil:
		return c.dispatchOpenCancelAcknowledged(response.GetOpenCancelAcknowledged())
	case response.GetPipePayload() != nil:
		return c.dispatchPipePayload(response.GetPipePayload())
	case response.GetPipeTerminated() != nil:
		return c.dispatchPipeTerminated(response.GetPipeTerminated())
	case response.GetPipePayloadRejected() != nil:
		return c.dispatchPipePayloadRejected(response.GetPipePayloadRejected())
	case response.GetPipeCloseAcknowledged() != nil:
		return c.dispatchPipeCloseAcknowledged(response.GetPipeCloseAcknowledged())
	default:
		return protocolError("unknown response")
	}
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
	c.mu.Unlock()
	if !offer.establish(pipe) {
		return protocolError("ListenerEstablished in wrong phase")
	}
	return nil
}

func (c *Client) dispatchListenerConfirmationAcknowledged(message *relayv1.ListenerConfirmationAcknowledged) error {
	if !validIdentity(message.GetAttemptId()) || !validIdentity(message.GetPipeId()) {
		return protocolError("invalid ListenerConfirmationAcknowledged")
	}
	c.mu.Lock()
	offer := c.offers[message.GetAttemptId()]
	pipe := c.pipes[message.GetPipeId()]
	if offer == nil || pipe == nil || pipe.attemptID != message.GetAttemptId() {
		c.mu.Unlock()
		return protocolError("foreign ListenerConfirmationAcknowledged")
	}
	delete(c.offers, message.GetAttemptId())
	c.addOfferTombstoneLocked(message.GetAttemptId(), message.GetPipeId())
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
			if tombstone := c.pipeTombstones[message.GetPipeId()]; tombstone != nil && tombstone.attemptID == message.GetAttemptId() {
				c.matchesPipeTombstoneLocked(message.GetPipeId())
				c.matchesOfferTombstoneLocked(message.GetAttemptId(), message.GetPipeId())
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
		c.addOfferTombstoneLocked(message.GetAttemptId(), message.GetPipeId())
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
	c.addOfferTombstoneLocked(message.GetAttemptId(), message.GetPipeId())
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
	if !validIdentity(message.GetAttemptId()) {
		return protocolError("invalid ListenerDecisionRejected")
	}
	c.mu.Lock()
	offer := c.offers[message.GetAttemptId()]
	c.mu.Unlock()
	if offer == nil || !offer.rejectDecision(fmt.Errorf("relaygate: listener decision rejected (%s)", message.GetFailure())) {
		return protocolError("foreign ListenerDecisionRejected")
	}
	return nil
}

func (c *Client) dispatchPipeOpened(message *relayv1.PipeOpened) error {
	if !validIdentity(message.GetRequestId()) || !validIdentity(message.GetAttemptId()) || !validIdentity(message.GetPipeId()) ||
		!validEndpoint(message.GetEndpoint()) || !validIdentity(message.GetTargetId()) {
		return protocolError("invalid PipeOpened")
	}
	outcome := openTombstone{endpoint: message.GetEndpoint(), target: message.GetTargetId(), kind: openOutcomeOpened, attemptID: message.GetAttemptId(), pipeID: message.GetPipeId()}
	c.mu.Lock()
	if retired, exists := c.openTombstones[message.GetRequestId()]; exists {
		same := sameOpenOutcome(retired, outcome)
		c.mu.Unlock()
		if same {
			return nil
		}
		return protocolError("conflicting duplicate PipeOpened")
	}
	call := c.opens[message.GetRequestId()]
	if call == nil || call.endpoint != message.GetEndpoint() || call.target != message.GetTargetId() || !call.reserved {
		if call != nil && call.outcomeReceived && sameOpenOutcome(call.outcome, outcome) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		return protocolError("foreign PipeOpened")
	}
	if _, exists := c.pipes[message.GetPipeId()]; exists {
		c.mu.Unlock()
		return protocolError("duplicate PipeOpened")
	}
	call.reserved = false
	pipe := newPipe(c, message.GetPipeId(), message.GetAttemptId(), message.GetEndpoint(), message.GetTargetId())
	c.pipes[pipe.id] = pipe
	c.recordOpenOutcomeLocked(call, outcome)
	c.mu.Unlock()
	call.complete(openResult{pipe: pipe})
	return nil
}

func (c *Client) dispatchPipeOpenFailed(message *relayv1.PipeOpenFailed) error {
	failure := openFailureFromProto(message.GetFailure())
	outcomeRecord := openTombstone{endpoint: message.GetEndpoint(), target: message.GetTargetId(), kind: openOutcomeFailed, failure: failure}
	call, duplicate, err := c.takeOpenOutcome(message.GetRequestId(), outcomeRecord)
	if err != nil {
		return err
	}
	if duplicate {
		return nil
	}
	outcome := OpenOutcomeFailed
	if failure == OpenFailureCancelled {
		outcome = OpenOutcomeCancelled
	}
	c.releaseOpenReservation(call)
	call.complete(openResult{err: &OpenError{Outcome: outcome, Failure: failure, Endpoint: call.endpoint, Target: call.target}})
	return nil
}

func (c *Client) dispatchPipeOpenUnknown(message *relayv1.PipeOpenUnknown) error {
	outcomeRecord := openTombstone{endpoint: message.GetEndpoint(), target: message.GetTargetId(), kind: openOutcomeUnknown}
	call, duplicate, err := c.takeOpenOutcome(message.GetRequestId(), outcomeRecord)
	if err != nil {
		return err
	}
	if duplicate {
		return nil
	}
	c.releaseOpenReservation(call)
	call.complete(openResult{err: &OpenError{Outcome: OpenOutcomeUnknown, Endpoint: call.endpoint, Target: call.target}})
	return nil
}

func (c *Client) dispatchOpenRequestRejected(message *relayv1.OpenRequestRejected) error {
	if !validIdentity(message.GetRequestId()) {
		return protocolError("invalid OpenRequestRejected")
	}
	c.mu.Lock()
	if retired, exists := c.openTombstones[message.GetRequestId()]; exists {
		same := retired.kind == openOutcomeRejected
		c.mu.Unlock()
		if same {
			return nil
		}
		return protocolError("conflicting duplicate OpenRequestRejected")
	}
	call := c.opens[message.GetRequestId()]
	switch {
	case call != nil && !call.outcomeReceived:
		c.recordOpenOutcomeLocked(call, openTombstone{endpoint: call.endpoint, target: call.target, kind: openOutcomeRejected, failure: OpenFailureInvalidRequest})
	case call != nil && call.outcome.kind == openOutcomeRejected:
		c.mu.Unlock()
		return nil
	default:
		call = nil
	}
	c.mu.Unlock()
	if call == nil {
		return protocolError("foreign OpenRequestRejected")
	}
	c.releaseOpenReservation(call)
	call.complete(openResult{err: &OpenError{Outcome: OpenOutcomeFailed, Failure: OpenFailureInvalidRequest, Endpoint: call.endpoint, Target: call.target}})
	return nil
}

func (c *Client) dispatchOpenCancelAcknowledged(message *relayv1.OpenCancelAcknowledged) error {
	if !validIdentity(message.GetRequestId()) {
		return protocolError("invalid OpenCancelAcknowledged")
	}
	c.mu.Lock()
	if tombstone, retired := c.openTombstones[message.GetRequestId()]; retired {
		if !tombstone.cancelAck || tombstone.wasPending != message.GetWasPending() {
			c.mu.Unlock()
			return protocolError("conflicting duplicate OpenCancelAcknowledged")
		}
		c.mu.Unlock()
		return nil
	}
	call := c.opens[message.GetRequestId()]
	if call != nil && call.cancelRequested {
		if call.cancelAcknowledged {
			if call.cancelWasPending != message.GetWasPending() {
				c.mu.Unlock()
				return protocolError("conflicting duplicate OpenCancelAcknowledged")
			}
			c.mu.Unlock()
			return nil
		}
		call.cancelAcknowledged = true
		call.cancelWasPending = message.GetWasPending()
		if call.outcomeReceived {
			c.retireOpenLocked(call)
		}
	} else {
		call = nil
	}
	c.mu.Unlock()
	if call == nil {
		return protocolError("foreign OpenCancelAcknowledged")
	}
	return nil
}

func (c *Client) dispatchPipePayload(message *relayv1.PipePayload) error {
	if !validIdentity(message.GetPipeId()) || len(message.GetPayload()) == 0 || len(message.GetPayload()) > maxPayloadBytes {
		return protocolError("invalid PipePayload")
	}
	c.mu.Lock()
	pipe := c.pipes[message.GetPipeId()]
	c.mu.Unlock()
	if pipe == nil {
		return protocolError("foreign PipePayload")
	}
	if !pipe.deliver(append([]byte(nil), message.GetPayload()...)) {
		c.removePipe(pipe)
		pipe.terminate(&PipeError{Failure: PipePayloadBackpressure})
		go pipe.closeAfterTerminal()
	}
	return nil
}

func (c *Client) dispatchPipeTerminated(message *relayv1.PipeTerminated) error {
	if !validIdentity(message.GetPipeId()) {
		return protocolError("invalid PipeTerminated")
	}
	c.mu.Lock()
	pipe := c.pipes[message.GetPipeId()]
	if pipe != nil {
		delete(c.pipes, pipe.id)
		c.addPipeTombstoneLocked(pipe)
		if call := c.closeCalls[pipe.id]; call != nil {
			call.terminalSeen = true
		}
	} else if call := c.closeCalls[message.GetPipeId()]; call != nil {
		call.terminalSeen = true
		pipe = call.pipe
		c.addPipeTombstoneLocked(pipe)
	} else if c.matchesPipeTombstoneLocked(message.GetPipeId()) {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if pipe == nil {
		return protocolError("foreign PipeTerminated")
	}
	pipe.terminate(ErrPipeClosed)
	return nil
}

func (c *Client) dispatchPipePayloadRejected(message *relayv1.PipePayloadRejected) error {
	if !validIdentity(message.GetPipeId()) {
		return protocolError("invalid PipePayloadRejected")
	}
	c.mu.Lock()
	pipe := c.pipes[message.GetPipeId()]
	if pipe != nil {
		delete(c.pipes, pipe.id)
		c.addPipeTombstoneLocked(pipe)
	}
	c.mu.Unlock()
	if pipe == nil {
		return protocolError("foreign PipePayloadRejected")
	}
	pipe.terminate(&PipeError{Failure: payloadFailureFromProto(message.GetFailure())})
	return nil
}

func (c *Client) dispatchPipeCloseAcknowledged(message *relayv1.PipeCloseAcknowledged) error {
	if !validIdentity(message.GetPipeId()) {
		return protocolError("invalid PipeCloseAcknowledged")
	}
	c.mu.Lock()
	if owned, retired := c.closeTombstones[message.GetPipeId()]; retired {
		if owned != message.GetOwned() {
			c.mu.Unlock()
			return protocolError("conflicting duplicate PipeCloseAcknowledged")
		}
		c.mu.Unlock()
		return nil
	}
	call := c.closeCalls[message.GetPipeId()]
	if call != nil {
		delete(c.closeCalls, message.GetPipeId())
		delete(c.pipes, message.GetPipeId())
		if !call.terminalSeen {
			c.addPipeTombstoneLocked(call.pipe)
		}
		c.addCloseTombstoneLocked(message.GetPipeId(), message.GetOwned())
	}
	c.mu.Unlock()
	if call == nil {
		return protocolError("foreign PipeCloseAcknowledged")
	}
	var err error
	if !message.GetOwned() {
		err = ErrPipeClosed
	}
	call.pipe.terminate(err)
	call.result <- err
	return nil
}

func (c *Client) takeOpenOutcome(requestID string, outcome openTombstone) (*openCall, bool, error) {
	if !validIdentity(requestID) || !validEndpoint(outcome.endpoint) || !validIdentity(outcome.target) {
		return nil, false, protocolError("invalid Open outcome")
	}
	c.mu.Lock()
	if retired, exists := c.openTombstones[requestID]; exists {
		same := sameOpenOutcome(retired, outcome)
		c.mu.Unlock()
		if same {
			return nil, true, nil
		}
		return nil, false, protocolError("conflicting duplicate Open outcome")
	}
	call := c.opens[requestID]
	if call == nil || call.endpoint != outcome.endpoint || call.target != outcome.target {
		c.mu.Unlock()
		return nil, false, protocolError("foreign Open outcome")
	}
	if call.outcomeReceived {
		same := sameOpenOutcome(call.outcome, outcome)
		c.mu.Unlock()
		if same {
			return nil, true, nil
		}
		return nil, false, protocolError("conflicting duplicate Open outcome")
	}
	c.recordOpenOutcomeLocked(call, outcome)
	c.mu.Unlock()
	return call, false, nil
}

func (c *Client) recordOpenOutcomeLocked(call *openCall, outcome openTombstone) {
	call.outcomeReceived = true
	call.outcome = outcome
	if call.cancelRequested && call.cancelAcknowledged {
		c.retireOpenLocked(call)
	} else if !call.cancelRequested {
		c.retireOpenLocked(call)
	}
}

func (c *Client) retireOpenLocked(call *openCall) {
	delete(c.opens, call.requestID)
	if _, exists := c.openTombstones[call.requestID]; !exists {
		for len(c.openTombstones) >= maxPendingOpens && len(c.openHistory) > 0 {
			oldest := c.openHistory[0]
			c.openHistory = c.openHistory[1:]
			delete(c.openTombstones, oldest)
		}
		c.openHistory = append(c.openHistory, call.requestID)
	}
	outcome := call.outcome
	outcome.cancelAck = call.cancelAcknowledged
	outcome.wasPending = call.cancelWasPending
	c.openTombstones[call.requestID] = outcome
	if call.retired != nil {
		call.retireOnce.Do(func() { close(call.retired) })
	}
}

func sameOpenOutcome(left, right openTombstone) bool {
	return left.endpoint == right.endpoint && left.target == right.target && left.kind == right.kind &&
		left.attemptID == right.attemptID && left.pipeID == right.pipeID && left.failure == right.failure
}

func (c *Client) removePipe(pipe *Pipe) {
	c.mu.Lock()
	if c.pipes[pipe.id] == pipe {
		delete(c.pipes, pipe.id)
	}
	c.mu.Unlock()
}

func (c *Client) addOfferTombstoneLocked(attemptID, pipeID string) {
	if _, exists := c.offerTombstones[attemptID]; !exists {
		for len(c.offerTombstones) >= maxPendingOffers && len(c.offerHistory) > 0 {
			oldest := c.offerHistory[0]
			c.offerHistory = c.offerHistory[1:]
			delete(c.offerTombstones, oldest)
		}
		c.offerHistory = append(c.offerHistory, attemptID)
	}
	c.offerTombstones[attemptID] = pipeID
}

func (c *Client) addBindingRecordLocked(record bindingRecord) bool {
	if _, exists := c.bindingRecords[record.id]; !exists {
		if len(c.bindingRecords) >= maxPendingOffers {
			for index, candidateID := range c.bindingHistory {
				candidate, exists := c.bindingRecords[candidateID]
				if exists && candidate.unbound {
					delete(c.bindingRecords, candidateID)
					c.bindingHistory = append(c.bindingHistory[:index], c.bindingHistory[index+1:]...)
					break
				}
			}
		}
		if len(c.bindingRecords) >= maxPendingOffers {
			return false
		}
		c.bindingHistory = append(c.bindingHistory, record.id)
	}
	c.bindingRecords[record.id] = record
	return true
}

func (c *Client) matchesOfferTombstoneLocked(attemptID, pipeID string) bool {
	expected, exists := c.offerTombstones[attemptID]
	if !exists || expected != pipeID {
		return false
	}
	return true
}

func (c *Client) addPipeTombstoneLocked(pipe *Pipe) {
	if _, exists := c.pipeTombstones[pipe.id]; !exists {
		for len(c.pipeTombstones) >= maxPipes && len(c.pipeHistory) > 0 {
			oldest := c.pipeHistory[0]
			c.pipeHistory = c.pipeHistory[1:]
			delete(c.pipeTombstones, oldest)
		}
		c.pipeHistory = append(c.pipeHistory, pipe.id)
	}
	c.pipeTombstones[pipe.id] = pipe
}

func (c *Client) addCloseTombstoneLocked(pipeID string, owned bool) {
	if _, exists := c.closeTombstones[pipeID]; !exists {
		for len(c.closeTombstones) >= maxPipes && len(c.closeHistory) > 0 {
			oldest := c.closeHistory[0]
			c.closeHistory = c.closeHistory[1:]
			delete(c.closeTombstones, oldest)
		}
		c.closeHistory = append(c.closeHistory, pipeID)
	}
	c.closeTombstones[pipeID] = owned
}

func (c *Client) matchesPipeTombstoneLocked(pipeID string) bool {
	if _, exists := c.pipeTombstones[pipeID]; !exists {
		return false
	}
	return true
}

func protocolError(detail string) error { return fmt.Errorf("%w: %s", errProtocol, detail) }

func validIdentity(value string) bool { return value != "" && len(value) <= maxIdentityBytes }
func validEndpoint(value string) bool { return value != "" && len(value) <= maxEndpointBytes }

func openFailureFromProto(failure relayv1.OpenFailure) OpenFailure {
	switch failure {
	case relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST:
		return OpenFailureInvalidRequest
	case relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND:
		return OpenFailureRouteNotFound
	case relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED:
		return OpenFailureCapacityReached
	case relayv1.OpenFailure_OPEN_FAILURE_LISTENER_REJECTED:
		return OpenFailureListenerRejected
	case relayv1.OpenFailure_OPEN_FAILURE_DEADLINE_EXCEEDED:
		return OpenFailureDeadlineExceeded
	case relayv1.OpenFailure_OPEN_FAILURE_CANCELLED:
		return OpenFailureCancelled
	default:
		return OpenFailureUnavailable
	}
}

func payloadFailureFromProto(failure relayv1.PipePayloadFailure) PipePayloadFailure {
	switch failure {
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST:
		return PipePayloadInvalidRequest
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED:
		return PipePayloadNotOwned
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE:
		return PipePayloadBackpressure
	default:
		return PipePayloadUnavailable
	}
}
