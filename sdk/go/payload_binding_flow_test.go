package relaygate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPayloadQueuePressureClosesExactPipe(t *testing.T) {
	closeObserved := make(chan struct{})
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		open := request.GetOpen()
		if open == nil {
			return status.Error(codes.FailedPrecondition, "Open required")
		}
		if err := stream.Send(pipeOpened(open, "attempt-pressure", "pipe-pressure")); err != nil {
			return err
		}
		for index := 0; index <= pipePayloadQueueCapacity; index++ {
			if err := stream.Send(pipePayload("pipe-pressure", fmt.Sprintf("payload-pressure-%d", index), []byte{byte(index + 1)})); err != nil {
				return err
			}
		}
		var rejected, closed bool
		for index := 0; index < pipePayloadQueueCapacity+2; index++ {
			request, err = recvRequest(stream)
			if err != nil {
				return err
			}
			if receipt := request.GetPipePayloadReceived(); receipt != nil {
				continue
			}
			if rejection := request.GetPipePayloadRejected(); rejection != nil {
				rejected = rejection.GetPayloadId() == fmt.Sprintf("payload-pressure-%d", pipePayloadQueueCapacity)
				continue
			}
			if request.GetClosePipe().GetPipeId() == "pipe-pressure" {
				closed = true
				break
			}
		}
		if !rejected || !closed {
			return status.Error(codes.FailedPrecondition, "exact rejection and ClosePipe required after pressure")
		}
		close(closeObserved)
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-pressure", Owned: true}}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	pipe, err := client.Open(context.Background(), "/pressure", "worker")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-pipe.Done()
	var pipeErr *PipeError
	if !errors.As(pipe.Err(), &pipeErr) || pipeErr.Failure != PipePayloadBackpressure {
		t.Fatalf("Pipe Err = %v", pipe.Err())
	}
	<-closeObserved
	select {
	case <-client.Done():
		t.Fatalf("queue pressure failed the Client: %v", client.Err())
	default:
	}
}

func TestPayloadRejectionTerminatesOnlyTheExactPipe(t *testing.T) {
	tests := []struct {
		name    string
		wire    relayv1.PipePayloadFailure
		failure PipePayloadFailure
	}{
		{name: "invalid request", wire: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST, failure: PipePayloadInvalidRequest},
		{name: "not owned", wire: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_NOT_OWNED, failure: PipePayloadNotOwned},
		{name: "backpressure", wire: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_BACKPRESSURE, failure: PipePayloadBackpressure},
		{name: "unavailable", wire: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNAVAILABLE, failure: PipePayloadUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payloadSeen := make(chan struct{})
			address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
				if _, err := authenticateScript(stream); err != nil {
					return err
				}
				request, err := recvRequest(stream)
				if err != nil {
					return err
				}
				open := request.GetOpen()
				if open == nil {
					return status.Error(codes.FailedPrecondition, "Open required")
				}
				if err := stream.Send(pipeOpened(open, "attempt-rejected", "pipe-rejected")); err != nil {
					return err
				}
				request, err = recvRequest(stream)
				if err != nil || request.GetPipePayload() == nil {
					return status.Error(codes.FailedPrecondition, "PipePayload required")
				}
				payload := request.GetPipePayload()
				close(payloadSeen)
				if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadRejected{
					PipePayloadRejected: &relayv1.PipePayloadRejected{PipeId: "pipe-rejected", PayloadId: payload.GetPayloadId(), Failure: test.wire},
				}}); err != nil {
					return err
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			})
			client := connectTestClient(t, address)
			pipe, err := client.Open(context.Background(), "/rejected", "worker")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			sendResult := make(chan error, 1)
			go func() { sendResult <- pipe.Send(context.Background(), []byte("rejected")) }()
			<-payloadSeen
			var deliveryErr *DeliveryError
			if err := <-sendResult; !errors.As(err, &deliveryErr) || deliveryErr.Outcome != DeliveryRejected {
				t.Fatalf("Send = %v, want DeliveryRejected", err)
			}
			<-pipe.Done()
			var pipeErr *PipeError
			if !errors.As(pipe.Err(), &pipeErr) || pipeErr.Failure != test.failure {
				t.Fatalf("Pipe Err = %v, want failure %v", pipe.Err(), test.failure)
			}
			select {
			case <-client.Done():
				t.Fatalf("payload rejection failed the Client: %v", client.Err())
			default:
			}
		})
	}
}

func TestPayloadRejectionAfterExactTerminalIsAnIdempotentNoop(t *testing.T) {
	payloadSeen := make(chan struct{})
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		open := request.GetOpen()
		if open == nil {
			return status.Error(codes.FailedPrecondition, "Open required")
		}
		if err := stream.Send(pipeOpened(open, "attempt-terminal-first", "pipe-terminal-first")); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetPipePayload() == nil {
			return status.Error(codes.FailedPrecondition, "PipePayload required")
		}
		payloadID := request.GetPipePayload().GetPayloadId()
		close(payloadSeen)
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeTerminated{
			PipeTerminated: &relayv1.PipeTerminated{PipeId: "pipe-terminal-first"},
		}}); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadRejected{
			PipePayloadRejected: &relayv1.PipePayloadRejected{
				PipeId: "pipe-terminal-first", PayloadId: payloadID, Failure: relayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_INVALID_REQUEST,
			},
		}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil {
			return err
		}
		bind := request.GetBindListener()
		if bind == nil {
			return status.Error(codes.FailedPrecondition, "Bind required after duplicate terminal")
		}
		if err := stream.Send(listenerBound("binding-after-terminal", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	pipe, err := client.Open(context.Background(), "/terminal-first", "worker")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	sendResult := make(chan error, 1)
	go func() { sendResult <- pipe.Send(context.Background(), []byte("terminal-race")) }()
	<-payloadSeen
	if err := <-sendResult; !errors.Is(err, ErrDeliveryUnknown) {
		t.Fatalf("Send = %v, want ErrDeliveryUnknown", err)
	}
	<-pipe.Done()
	if _, err := client.Bind(context.Background(), "/after-terminal", "worker"); err != nil {
		t.Fatalf("Bind after duplicate exact terminal: %v", err)
	}
}

func TestBindUnbindAbsorbsExactStaleAcknowledgements(t *testing.T) {
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		first := request.GetBindListener()
		if first == nil {
			return status.Error(codes.FailedPrecondition, "first Bind required")
		}
		firstBound := listenerBound("binding-1", first.GetEndpointPattern(), first.GetTargetId())
		if err := stream.Send(firstBound); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetUnbindListener().GetListenerBindingId() != "binding-1" {
			return status.Error(codes.FailedPrecondition, "Unbind required")
		}
		if err := stream.Send(firstBound); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "binding-1"}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil {
			return err
		}
		second := request.GetBindListener()
		if second == nil {
			return status.Error(codes.FailedPrecondition, "second Bind required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "binding-1"}}}); err != nil {
			return err
		}
		if err := stream.Send(listenerBound("binding-2", second.GetEndpointPattern(), second.GetTargetId())); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	first, err := client.Bind(context.Background(), "/first", "worker")
	if err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := first.Unbind(context.Background()); err != nil {
		t.Fatalf("Unbind with stale ListenerBound reordered before ACK: %v", err)
	}
	second, err := client.Bind(context.Background(), "/second", "worker")
	if err != nil || second.ID() != "binding-2" {
		t.Fatalf("second Bind with stale ListenerUnbound = %#v, %v", second, err)
	}
}

func TestBindUnbindOperationFailuresKeepSessionUsable(t *testing.T) {
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		request, err := recvRequest(stream)
		if err != nil {
			return err
		}
		failedBind := request.GetBindListener()
		if failedBind == nil {
			return status.Error(codes.FailedPrecondition, "first Bind required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerBindFailed{
			ListenerBindFailed: &relayv1.ListenerBindFailed{
				EndpointPattern: failedBind.GetEndpointPattern(), TargetId: failedBind.GetTargetId(),
				Failure: relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_CONFLICT,
			},
		}}); err != nil {
			return err
		}

		request, err = recvRequest(stream)
		if err != nil {
			return err
		}
		successfulBind := request.GetBindListener()
		if successfulBind == nil {
			return status.Error(codes.FailedPrecondition, "second Bind required")
		}
		if err := stream.Send(listenerBound("binding-1", successfulBind.GetEndpointPattern(), successfulBind.GetTargetId())); err != nil {
			return err
		}

		request, err = recvRequest(stream)
		if err != nil || request.GetUnbindListener().GetListenerBindingId() != "binding-1" {
			return status.Error(codes.FailedPrecondition, "first Unbind required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbindFailed{
			ListenerUnbindFailed: &relayv1.ListenerUnbindFailed{
				ListenerBindingId: "binding-1",
				Failure:           relayv1.ListenerBindingFailure_LISTENER_BINDING_FAILURE_UNAVAILABLE,
			},
		}}); err != nil {
			return err
		}

		request, err = recvRequest(stream)
		if err != nil || request.GetUnbindListener().GetListenerBindingId() != "binding-1" {
			return status.Error(codes.FailedPrecondition, "second Unbind required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerUnbound{
			ListenerUnbound: &relayv1.ListenerUnbound{ListenerBindingId: "binding-1"},
		}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	client := connectTestClient(t, address)
	_, err := client.Bind(context.Background(), "/conflict", "worker")
	var bindErr *BindError
	if !errors.As(err, &bindErr) || !errors.Is(err, ErrBindFailed) || bindErr.Failure != BindingFailureConflict {
		t.Fatalf("failed Bind error = %#v, %v", bindErr, err)
	}
	select {
	case <-client.Done():
		t.Fatalf("operation-local Bind failure ended Client: %v", client.Err())
	default:
	}

	listener, err := client.Bind(context.Background(), "/ok", "worker")
	if err != nil {
		t.Fatalf("Bind after operation-local failure: %v", err)
	}
	err = listener.Unbind(context.Background())
	var unbindErr *UnbindError
	if !errors.As(err, &unbindErr) || !errors.Is(err, ErrUnbindFailed) || unbindErr.Failure != BindingFailureUnavailable {
		t.Fatalf("failed Unbind error = %#v, %v", unbindErr, err)
	}
	select {
	case <-listener.done:
		t.Fatalf("operation-local Unbind failure ended Listener: %v", listener.terminalError())
	default:
	}
	if err := listener.Unbind(context.Background()); err != nil {
		t.Fatalf("Unbind retry on same session: %v", err)
	}
}
