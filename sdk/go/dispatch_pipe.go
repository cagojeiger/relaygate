package relaygate

import relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"

func (c *Client) dispatchPipePayload(message *relayv1.PipePayload) error {
	if !validIdentity(message.GetPipeId()) || !validIdentity(message.GetPayloadId()) || len(message.GetPayload()) == 0 || len(message.GetPayload()) > maxPayloadBytes {
		return protocolError("invalid PipePayload")
	}
	c.mu.Lock()
	pipe := c.pipes[message.GetPipeId()]
	c.mu.Unlock()
	if pipe == nil {
		return protocolError("foreign PipePayload")
	}
	accepted, duplicate, conflict := pipe.deliver(message.GetPayloadId(), append([]byte(nil), message.GetPayload()...))
	if conflict {
		return protocolError("conflicting duplicate PipePayload")
	}
	if accepted != nil || duplicate {
		if err := c.send(c.ctx, &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayloadReceived{
			PipePayloadReceived: &relayv1.PipePayloadReceived{PipeId: pipe.id, PayloadId: message.GetPayloadId()},
		}}); err != nil {
			return err
		}
		if accepted != nil {
			close(accepted.acknowledged)
		}
		return nil
	}
	if err := c.send(c.ctx, &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_PipePayloadRejected{
		PipePayloadRejected: &relayv1.PipePayloadRejected{
			PipeId: pipe.id, PayloadId: message.GetPayloadId(), Failure: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE,
		},
	}}); err != nil {
		return err
	}
	{
		c.removePipe(pipe)
		pipe.terminate(&PipeError{Failure: PipePayloadBackpressure})
		go pipe.closeAfterTerminal()
	}
	return nil
}

func (c *Client) dispatchPipePayloadReceived(message *relayv1.PipePayloadReceived) error {
	if !validIdentity(message.GetPipeId()) || !validIdentity(message.GetPayloadId()) {
		return protocolError("invalid PipePayloadReceived")
	}
	c.mu.Lock()
	pipe := c.pipes[message.GetPipeId()]
	if pipe == nil {
		pipe = c.pipeTombstones[message.GetPipeId()]
	}
	c.mu.Unlock()
	if pipe == nil {
		return protocolError("foreign PipePayloadReceived")
	}
	matched, _ := pipe.finishDelivery(message.GetPayloadId(), DeliveryReceived, PipePayloadFailure(0))
	if !matched {
		return protocolError("conflicting PipePayloadReceived")
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
	if !validIdentity(message.GetPipeId()) || !validIdentity(message.GetPayloadId()) {
		return protocolError("invalid PipePayloadRejected")
	}
	failure, ok := payloadFailureFromProto(message.GetFailure())
	if !ok {
		return protocolError("invalid PipePayloadRejected failure")
	}
	c.mu.Lock()
	pipe := c.pipes[message.GetPipeId()]
	var retired *Pipe
	if pipe != nil {
		delete(c.pipes, pipe.id)
		c.addPipeTombstoneLocked(pipe)
	} else {
		retired = c.pipeTombstones[message.GetPipeId()]
	}
	c.mu.Unlock()
	if pipe == nil {
		if retired != nil {
			matched, _ := retired.finishDelivery(message.GetPayloadId(), DeliveryRejected, failure)
			if matched {
				return nil
			}
			return protocolError("conflicting retired PipePayloadRejected")
		}
		return protocolError("foreign PipePayloadRejected")
	}
	matched, _ := pipe.finishDelivery(message.GetPayloadId(), DeliveryRejected, failure)
	if !matched {
		return protocolError("conflicting PipePayloadRejected")
	}
	pipe.terminate(&DeliveryError{PayloadID: message.GetPayloadId(), Outcome: DeliveryRejected, Cause: &PipeError{Failure: failure}})
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
		err = ErrPipeNotOwned
	}
	call.pipe.terminate(err)
	call.result <- err
	return nil
}
