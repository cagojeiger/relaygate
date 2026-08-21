package relaygate

import (
	"context"
	"errors"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnsolicitedOpenCancelAcknowledgementForCompletedOpenIsFatal(t *testing.T) {
	client := &Client{
		authenticated: true,
		openTombstones: map[string]openTombstone{
			"request-complete": {endpoint: "/complete", target: "worker", kind: openOutcomeFailed, failure: OpenFailureRouteNotFound},
		},
	}
	err := client.dispatch(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenCancelAcknowledged{
		OpenCancelAcknowledged: &relayv1.OpenCancelAcknowledged{RequestId: "request-complete", WasPending: false},
	}})
	if !errors.Is(err, errProtocol) {
		t.Fatalf("unsolicited OpenCancelAcknowledged = %v, want protocol failure", err)
	}
}

func TestRetiredTerminalHistoriesRemainIdempotent(t *testing.T) {
	t.Run("rejected offer absorbs repeated empty ListenerTerminated", func(t *testing.T) {
		address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
			if _, err := authenticateScript(stream); err != nil {
				return err
			}
			request, err := recvRequest(stream)
			if err != nil {
				return err
			}
			bind := request.GetBindListener()
			if bind == nil {
				return status.Error(codes.FailedPrecondition, "Bind required")
			}
			if err := stream.Send(listenerBound("binding-reject", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
				return err
			}
			if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{ListenerOffer: &relayv1.ListenerOffer{
				AttemptId: "attempt-reject", ListenerBindingId: "binding-reject", Endpoint: bind.GetEndpointPattern(), TargetId: bind.GetTargetId(), CallerSessionId: "caller-reject",
			}}}); err != nil {
				return err
			}
			request, err = recvRequest(stream)
			if err != nil || request.GetListenerReject().GetAttemptId() != "attempt-reject" {
				return status.Error(codes.FailedPrecondition, "ListenerReject required")
			}
			terminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: "attempt-reject"}}}
			for range 3 {
				if err := stream.Send(terminal); err != nil {
					return err
				}
			}
			request, err = recvRequest(stream)
			if err != nil {
				return err
			}
			second := request.GetBindListener()
			if second == nil {
				return status.Error(codes.FailedPrecondition, "Bind after repeated ListenerTerminated required")
			}
			if err := stream.Send(listenerBound("binding-after-reject", second.GetEndpointPattern(), second.GetTargetId())); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		client := connectTestClient(t, address)
		listener, err := client.Bind(context.Background(), "/reject", "worker")
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		offer, err := listener.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if err := offer.Reject(context.Background()); err != nil {
			t.Fatalf("Reject: %v", err)
		}
		if _, err := client.Bind(context.Background(), "/after-reject", "worker"); err != nil {
			t.Fatalf("Bind after repeated ListenerTerminated: %v", err)
		}
	})

	t.Run("caller pipe absorbs repeated PipeTerminated", func(t *testing.T) {
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
			if err := stream.Send(pipeOpened(open, "attempt-terminal", "pipe-terminal")); err != nil {
				return err
			}
			terminal := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeTerminated{PipeTerminated: &relayv1.PipeTerminated{PipeId: "pipe-terminal"}}}
			for range 3 {
				if err := stream.Send(terminal); err != nil {
					return err
				}
			}
			request, err = recvRequest(stream)
			if err != nil {
				return err
			}
			bind := request.GetBindListener()
			if bind == nil {
				return status.Error(codes.FailedPrecondition, "Bind after repeated PipeTerminated required")
			}
			if err := stream.Send(listenerBound("binding-after-terminal", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		client := connectTestClient(t, address)
		pipe, err := client.Open(context.Background(), "/terminal", "worker")
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		<-pipe.Done()
		if !errors.Is(pipe.Err(), ErrPipeClosed) {
			t.Fatalf("Pipe Err = %v", pipe.Err())
		}
		if _, err := client.Bind(context.Background(), "/after-terminal", "worker"); err != nil {
			t.Fatalf("Bind after repeated PipeTerminated: %v", err)
		}
	})
}

func TestPipeSendRejectsAfterCloseLinearization(t *testing.T) {
	closeReceived := make(chan struct{})
	sendChecked := make(chan struct{})
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
		if err := stream.Send(pipeOpened(open, "attempt-closing", "pipe-closing")); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetClosePipe().GetPipeId() != "pipe-closing" {
			return status.Error(codes.FailedPrecondition, "ClosePipe required")
		}
		nextRequest := make(chan *relayv1.ConnectRequest, 1)
		nextError := make(chan error, 1)
		go func() {
			next, receiveErr := recvRequest(stream)
			if receiveErr != nil {
				nextError <- receiveErr
				return
			}
			nextRequest <- next
		}()
		close(closeReceived)
		<-sendChecked
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{
			PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-closing", Owned: true},
		}}); err != nil {
			return err
		}
		select {
		case request = <-nextRequest:
		case err = <-nextError:
			return err
		}
		if request.GetPipePayload() != nil {
			return status.Error(codes.FailedPrecondition, "payload reached the wire after Close linearized")
		}
		bind := request.GetBindListener()
		if bind == nil {
			return status.Error(codes.FailedPrecondition, "Bind after Close required")
		}
		if err := stream.Send(listenerBound("binding-after-closing", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	pipe, err := client.Open(context.Background(), "/closing", "worker")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- pipe.Close(context.Background()) }()
	<-closeReceived
	if err := pipe.Send(context.Background(), []byte("must-not-reach-wire")); !errors.Is(err, ErrPipeClosed) {
		t.Fatalf("Send after Close linearized = %v, want ErrPipeClosed", err)
	}
	close(sendChecked)
	if err := <-closeResult; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := client.Bind(context.Background(), "/after-closing", "worker"); err != nil {
		t.Fatalf("Bind after rejected payload: %v", err)
	}
}

func TestOpenCancellationDrainIsBoundedAndCleansLatePipe(t *testing.T) {
	t.Run("blackholed cancellation closes the session and returns Unknown", func(t *testing.T) {
		openReceived := make(chan struct{})
		cancelReceived := make(chan struct{})
		address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
			if _, err := authenticateScript(stream); err != nil {
				return err
			}
			request, err := recvRequest(stream)
			if err != nil || request.GetOpen() == nil {
				return status.Error(codes.FailedPrecondition, "Open required")
			}
			close(openReceived)
			request, err = recvRequest(stream)
			if err != nil || request.GetCancelOpen().GetRequestId() == "" {
				return status.Error(codes.FailedPrecondition, "CancelOpen required")
			}
			close(cancelReceived)
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		client := connectTestClient(t, address)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := client.Open(ctx, "/blackhole", "worker")
			result <- err
		}()
		<-openReceived
		cancel()
		<-cancelReceived
		select {
		case err := <-result:
			if !errors.Is(err, ErrOpenUnknown) {
				t.Fatalf("blackholed cancelled Open = %v, want ErrOpenUnknown", err)
			}
		case <-time.After(3 * openCancelDrainTimeout):
			t.Fatal("blackholed cancelled Open exceeded its bounded drain")
		}
		select {
		case <-client.Done():
		default:
			t.Fatal("blackholed cancelled Open returned before closing its session")
		}
	})

	t.Run("late PipeOpened is exactly closed and releases its shared slot", func(t *testing.T) {
		firstOpenReceived := make(chan struct{})
		address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
			if _, err := authenticateScript(stream); err != nil {
				return err
			}
			request, err := recvRequest(stream)
			if err != nil {
				return err
			}
			first := request.GetOpen()
			if first == nil {
				return status.Error(codes.FailedPrecondition, "first Open required")
			}
			close(firstOpenReceived)
			request, err = recvRequest(stream)
			if err != nil || request.GetCancelOpen().GetRequestId() != first.GetRequestId() {
				return status.Error(codes.FailedPrecondition, "exact CancelOpen required")
			}
			if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenCancelAcknowledged{
				OpenCancelAcknowledged: &relayv1.OpenCancelAcknowledged{RequestId: first.GetRequestId(), WasPending: false},
			}}); err != nil {
				return err
			}
			if err := stream.Send(pipeOpened(first, "attempt-late-open", "pipe-late-open")); err != nil {
				return err
			}
			request, err = recvRequest(stream)
			if err != nil || request.GetClosePipe().GetPipeId() != "pipe-late-open" {
				return status.Error(codes.FailedPrecondition, "late PipeOpened requires exact ClosePipe")
			}
			if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{
				PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-late-open", Owned: true},
			}}); err != nil {
				return err
			}
			request, err = recvRequest(stream)
			if err != nil {
				return err
			}
			second := request.GetOpen()
			if second == nil {
				return status.Error(codes.FailedPrecondition, "second Open required")
			}
			if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenFailed{PipeOpenFailed: &relayv1.PipeOpenFailed{
				RequestId: second.GetRequestId(), Endpoint: second.GetEndpoint(), TargetId: second.GetTargetId(), Failure: relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND,
			}}}); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		client := connectTestClient(t, address)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := client.Open(ctx, "/late-open", "worker")
			result <- err
		}()
		<-firstOpenReceived
		cancel()
		if err := <-result; !errors.Is(err, ErrOpenUnknown) {
			t.Fatalf("late opened cancelled Open = %v, want ErrOpenUnknown", err)
		}
		if slots := len(client.pipeSlots); slots != 0 {
			t.Fatalf("late opened cleanup retained %d shared Pipe slots", slots)
		}
		_, err := client.Open(context.Background(), "/after-late-open", "worker")
		if !errors.Is(err, ErrOpenFailed) {
			t.Fatalf("second Open after late cleanup = %v, want stable failure", err)
		}
		if slots := len(client.pipeSlots); slots != 0 {
			t.Fatalf("stable failed Open retained %d shared Pipe slots", slots)
		}
	})
}
