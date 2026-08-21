package relaygate

import relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"

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
	case response.GetListenerBindFailed() != nil:
		return c.dispatchListenerBindFailed(response.GetListenerBindFailed())
	case response.GetListenerUnbound() != nil:
		return c.dispatchListenerUnbound(response.GetListenerUnbound())
	case response.GetListenerUnbindFailed() != nil:
		return c.dispatchListenerUnbindFailed(response.GetListenerUnbindFailed())
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
	case response.GetPipePayloadReceived() != nil:
		return c.dispatchPipePayloadReceived(response.GetPipePayloadReceived())
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
