package relaygate

import relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"

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
	failure, ok := openFailureFromProto(message.GetFailure())
	if !ok {
		return protocolError("invalid PipeOpenFailed failure")
	}
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
	if !validIdentity(message.GetRequestId()) || message.GetFailure() != relayv1.OpenRequestFailure_OPEN_REQUEST_FAILURE_DUPLICATE_IN_FLIGHT {
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
		c.recordOpenOutcomeLocked(call, openTombstone{endpoint: call.endpoint, target: call.target, kind: openOutcomeRejected})
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
	call.complete(openResult{err: &OpenError{Outcome: OpenOutcomeRejected, Endpoint: call.endpoint, Target: call.target}})
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
