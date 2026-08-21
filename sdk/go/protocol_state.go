package relaygate

import (
	"fmt"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

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

func (c *Client) addOfferTombstoneLocked(attemptID string, tombstone offerTombstone) {
	if _, exists := c.offerTombstones[attemptID]; !exists {
		for len(c.offerTombstones) >= maxPendingOffers && len(c.offerHistory) > 0 {
			oldest := c.offerHistory[0]
			c.offerHistory = c.offerHistory[1:]
			delete(c.offerTombstones, oldest)
		}
		c.offerHistory = append(c.offerHistory, attemptID)
	}
	c.offerTombstones[attemptID] = tombstone
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
	if !exists || expected.pipeID != pipeID || expected.decisionFailure != relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_UNSPECIFIED {
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

func openFailureFromProto(failure relayv1.OpenFailure) (OpenFailure, bool) {
	switch failure {
	case relayv1.OpenFailure_OPEN_FAILURE_INVALID_REQUEST:
		return OpenFailureInvalidRequest, true
	case relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND:
		return OpenFailureRouteNotFound, true
	case relayv1.OpenFailure_OPEN_FAILURE_UNAVAILABLE:
		return OpenFailureUnavailable, true
	case relayv1.OpenFailure_OPEN_FAILURE_CAPACITY_REACHED:
		return OpenFailureCapacityReached, true
	case relayv1.OpenFailure_OPEN_FAILURE_LISTENER_REJECTED:
		return OpenFailureListenerRejected, true
	case relayv1.OpenFailure_OPEN_FAILURE_DEADLINE_EXCEEDED:
		return OpenFailureDeadlineExceeded, true
	case relayv1.OpenFailure_OPEN_FAILURE_CANCELLED:
		return OpenFailureCancelled, true
	default:
		return 0, false
	}
}

func validListenerDecisionFailure(failure relayv1.ListenerDecisionFailure) bool {
	switch failure {
	case relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_INVALID_REQUEST,
		relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_ATTEMPT_NOT_PENDING,
		relayv1.ListenerDecisionFailure_LISTENER_DECISION_FAILURE_WRONG_PHASE:
		return true
	default:
		return false
	}
}

func bindingFailureFromProto(failure relayv1.ListenerBindingFailure) (BindingFailure, bool) {
	switch failure {
	case relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_INVALID_REQUEST:
		return BindingFailureInvalidRequest, true
	case relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CAPACITY_REACHED:
		return BindingFailureCapacityReached, true
	case relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CONFLICT:
		return BindingFailureConflict, true
	case relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_UNAVAILABLE:
		return BindingFailureUnavailable, true
	default:
		return 0, false
	}
}

func payloadFailureFromProto(failure relayv1.PipePayloadFailure) (PipePayloadFailure, bool) {
	switch failure {
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST:
		return PipePayloadInvalidRequest, true
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED:
		return PipePayloadNotOwned, true
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE:
		return PipePayloadBackpressure, true
	case relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE:
		return PipePayloadUnavailable, true
	default:
		return 0, false
	}
}
