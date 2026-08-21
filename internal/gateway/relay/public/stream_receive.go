package relaygrpc

import (
	clientsession "github.com/cagojeiger/relaygate/internal/gateway/access/session"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
)

type receivedRequest struct {
	request *relayv1.ConnectRequest
	err     error
}

func receiveRequests(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) <-chan receivedRequest {
	received := make(chan receivedRequest, 1)
	go func() {
		defer close(received)
		for {
			request, err := stream.Recv()
			select {
			case received <- receivedRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func sessionRefToProto(ref clientsession.Ref) *relayv1.ClientSessionRef {
	return &relayv1.ClientSessionRef{
		ClientSessionId: ref.ClientSessionID,
		ClientId:        ref.ClientID,
		ApiKeyId:        ref.APIKeyID,
		AuthRevision:    ref.AuthRevision,
	}
}
