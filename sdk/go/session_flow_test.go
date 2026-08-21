package relaygate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnectAuthenticationRedactionAndSetupContextLifetime(t *testing.T) {
	t.Run("authentication failure does not disclose the API key", func(t *testing.T) {
		const secret = "do-not-print-this-secret"
		address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
			request, err := stream.Recv()
			if err != nil {
				return err
			}
			if request.GetAuthenticate().GetApiKey() != secret {
				return status.Error(codes.InvalidArgument, "wrong test credential")
			}
			return status.Error(codes.Unauthenticated, "authentication failed")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := Connect(ctx, NewConfig(address, "client-1", "key-1", secret).WithInsecureLocal())
		if err == nil || !strings.Contains(err.Error(), "authentication failed") {
			t.Fatalf("Connect error = %v", err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("Connect error disclosed API key: %v", err)
		}
	})

	t.Run("setup cancellation does not own the returned Client", func(t *testing.T) {
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
				return status.Error(codes.FailedPrecondition, "BindListener required")
			}
			if err := stream.Send(listenerBound("binding-1", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		setup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		client, err := Connect(setup, NewConfig(address, "client-1", "key-1", "secret").WithInsecureLocal())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		cancel()
		listener, err := client.Bind(context.Background(), "/still-live", "worker")
		if err != nil || listener.ID() != "binding-1" {
			t.Fatalf("Bind after setup cancel = %#v, %v", listener, err)
		}
		if err := client.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case <-client.Done():
		default:
			t.Fatal("Close returned before Done closed")
		}
		if client.Err() != nil {
			t.Fatalf("Err after explicit Close = %v", client.Err())
		}
	})
}

func TestListenerAcceptBarrierPayloadAndCloseOrdering(t *testing.T) {
	confirmed := make(chan struct{})
	allowAcknowledgement := make(chan struct{})
	terminalSent := make(chan struct{})
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
			return status.Error(codes.FailedPrecondition, "BindListener required")
		}
		if err := stream.Send(listenerBound("binding-1", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{ListenerOffer: &relayv1.ListenerOffer{
			AttemptId: "attempt-1", ListenerBindingId: "binding-1", Endpoint: bind.GetEndpointPattern(), TargetId: bind.GetTargetId(), CallerSessionId: "caller-1",
		}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetListenerAccept().GetAttemptId() != "attempt-1" {
			return status.Error(codes.FailedPrecondition, "ListenerAccept required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerEstablished{ListenerEstablished: &relayv1.ListenerEstablished{AttemptId: "attempt-1", PipeId: "pipe-1"}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetListenerConfirmed().GetPipeId() != "pipe-1" {
			return status.Error(codes.FailedPrecondition, "ListenerConfirmed required")
		}
		close(confirmed)
		if err := stream.Send(pipePayload("pipe-1", "payload-before-ack", []byte("before-ack"))); err != nil {
			return err
		}
		<-allowAcknowledgement
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerConfirmationAcknowledged{
			ListenerConfirmationAcknowledged: &relayv1.ListenerConfirmationAcknowledged{AttemptId: "attempt-1", PipeId: "pipe-1"},
		}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetPipePayloadReceived().GetPayloadId() != "payload-before-ack" {
			return status.Error(codes.FailedPrecondition, "pre-ack payload receipt required")
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetPipePayload() == nil {
			return status.Error(codes.FailedPrecondition, "PipePayload required")
		}
		outbound := request.GetPipePayload()
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayloadReceived{
			PipePayloadReceived: &relayv1.PipePayloadReceived{PipeId: "pipe-1", PayloadId: outbound.GetPayloadId()},
		}}); err != nil {
			return err
		}
		if err := stream.Send(pipePayload("pipe-1", "payload-echo", outbound.GetPayload())); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetPipePayloadReceived().GetPayloadId() != "payload-echo" {
			return status.Error(codes.FailedPrecondition, "echo payload receipt required")
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetClosePipe().GetPipeId() != "pipe-1" {
			return status.Error(codes.FailedPrecondition, "ClosePipe required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-1", Owned: true}}}); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: "attempt-1", PipeId: "pipe-1"}}}); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-1", Owned: true}}}); err != nil {
			return err
		}
		close(terminalSent)
		request, err = recvRequest(stream)
		if err != nil {
			return err
		}
		second := request.GetBindListener()
		if second == nil {
			return status.Error(codes.FailedPrecondition, "second BindListener required")
		}
		if err := stream.Send(listenerBound("binding-2", second.GetEndpointPattern(), second.GetTargetId())); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	client := connectTestClient(t, address)
	listener, err := client.Bind(context.Background(), "/service", "worker")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	offer, err := listener.Next(context.Background())
	if err != nil || offer.AttemptID() != "attempt-1" || offer.CallerSessionID() != "caller-1" {
		t.Fatalf("Next = %#v, %v", offer, err)
	}
	accepted := make(chan openResult, 1)
	go func() {
		pipe, acceptErr := offer.Accept(context.Background())
		accepted <- openResult{pipe: pipe, err: acceptErr}
	}()
	<-confirmed
	select {
	case result := <-accepted:
		t.Fatalf("Accept crossed confirmation barrier early: %#v", result)
	default:
	}
	close(allowAcknowledgement)
	result := <-accepted
	if result.err != nil || result.pipe == nil {
		t.Fatalf("Accept = %#v", result)
	}
	pipe := result.pipe
	payload, err := pipe.Recv(context.Background())
	if err != nil || !bytes.Equal(payload, []byte("before-ack")) {
		t.Fatalf("Recv(pre-ack payload) = %q, %v", payload, err)
	}
	if err := pipe.Send(context.Background(), []byte("round-trip")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	payload, err = pipe.Recv(context.Background())
	if err != nil || !bytes.Equal(payload, []byte("round-trip")) {
		t.Fatalf("Recv(echo) = %q, %v", payload, err)
	}
	if err := pipe.Close(context.Background()); err != nil {
		t.Fatalf("Pipe.Close: %v", err)
	}
	<-terminalSent
	if _, err := client.Bind(context.Background(), "/after-close", "worker"); err != nil {
		t.Fatalf("Bind after ACK-before-terminal ordering: %v", err)
	}
}

func TestAcceptCancellationConfirmsThenClosesLatePipe(t *testing.T) {
	confirmed := make(chan struct{})
	allowAcknowledgement := make(chan struct{})
	closeObserved := make(chan struct{})
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
		if err := stream.Send(listenerBound("binding-cancel", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerOffer{ListenerOffer: &relayv1.ListenerOffer{
			AttemptId: "attempt-cancel", ListenerBindingId: "binding-cancel", Endpoint: bind.GetEndpointPattern(), TargetId: bind.GetTargetId(), CallerSessionId: "caller-cancel",
		}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetListenerAccept().GetAttemptId() != "attempt-cancel" {
			return status.Error(codes.FailedPrecondition, "ListenerAccept required")
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerEstablished{ListenerEstablished: &relayv1.ListenerEstablished{AttemptId: "attempt-cancel", PipeId: "pipe-cancel"}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetListenerConfirmed().GetPipeId() != "pipe-cancel" {
			return status.Error(codes.FailedPrecondition, "ListenerConfirmed required")
		}
		close(confirmed)
		<-allowAcknowledgement
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerConfirmationAcknowledged{ListenerConfirmationAcknowledged: &relayv1.ListenerConfirmationAcknowledged{AttemptId: "attempt-cancel", PipeId: "pipe-cancel"}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetClosePipe().GetPipeId() != "pipe-cancel" {
			return status.Error(codes.FailedPrecondition, "late accepted Pipe was not closed")
		}
		close(closeObserved)
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeCloseAcknowledged{PipeCloseAcknowledged: &relayv1.PipeCloseAcknowledged{PipeId: "pipe-cancel", Owned: true}}}); err != nil {
			return err
		}
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerTerminated{ListenerTerminated: &relayv1.ListenerTerminated{AttemptId: "attempt-cancel", PipeId: "pipe-cancel"}}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil {
			return err
		}
		second := request.GetBindListener()
		if second == nil {
			return status.Error(codes.FailedPrecondition, "Bind after cleanup required")
		}
		if err := stream.Send(listenerBound("binding-after-cancel", second.GetEndpointPattern(), second.GetTargetId())); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	listener, err := client.Bind(context.Background(), "/accept-cancel", "worker")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	offer, err := listener.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := offer.Accept(ctx)
		result <- err
	}()
	<-confirmed
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Accept cancellation = %v", err)
	}
	close(allowAcknowledgement)
	<-closeObserved
	if _, err := client.Bind(context.Background(), "/after-accept-cancel", "worker"); err != nil {
		t.Fatalf("Bind after late accepted cleanup: %v", err)
	}
}

func TestOpenConcurrentAndTypedOutcomes(t *testing.T) {
	t.Run("concurrent exact Opens correlate out of order", func(t *testing.T) {
		address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
			if _, err := authenticateScript(stream); err != nil {
				return err
			}
			first, err := recvRequest(stream)
			if err != nil {
				return err
			}
			second, err := recvRequest(stream)
			if err != nil {
				return err
			}
			opens := []*relayv1.Open{first.GetOpen(), second.GetOpen()}
			if opens[0] == nil || opens[1] == nil || opens[0].GetRequestId() == opens[1].GetRequestId() {
				return status.Error(codes.FailedPrecondition, "two unique Opens required")
			}
			for index := len(opens) - 1; index >= 0; index-- {
				open := opens[index]
				outcome := pipeOpened(open, fmt.Sprintf("attempt-%d", index), fmt.Sprintf("pipe-%d", index))
				for range 2 {
					if err := stream.Send(outcome); err != nil {
						return err
					}
				}
			}
			request, err := recvRequest(stream)
			if err != nil {
				return err
			}
			bind := request.GetBindListener()
			if bind == nil {
				return status.Error(codes.FailedPrecondition, "Bind after duplicate outcomes required")
			}
			if err := stream.Send(listenerBound("binding-after-duplicates", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		client := connectTestClient(t, address)
		var wait sync.WaitGroup
		wait.Add(2)
		results := make(chan openResult, 2)
		for _, endpoint := range []string{"/one", "/two"} {
			endpoint := endpoint
			go func() {
				defer wait.Done()
				pipe, err := client.Open(context.Background(), endpoint, "worker")
				results <- openResult{pipe: pipe, err: err}
			}()
		}
		wait.Wait()
		close(results)
		seen := map[string]bool{}
		for result := range results {
			if result.err != nil {
				t.Fatalf("Open: %v", result.err)
			}
			seen[result.pipe.Endpoint()] = true
		}
		if !seen["/one"] || !seen["/two"] {
			t.Fatalf("Open endpoints = %#v", seen)
		}
		if _, err := client.Bind(context.Background(), "/after-duplicates", "worker"); err != nil {
			t.Fatalf("Bind after duplicate PipeOpened outcomes: %v", err)
		}
	})

	for _, test := range []struct {
		name    string
		outcome *relayv1.ConnectResponse
		want    error
	}{
		{name: "cancelled outcome before acknowledgement", want: ErrOpenCancelled},
		{name: "unknown outcome before acknowledgement", want: ErrOpenUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
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
				close(started)
				request, err = recvRequest(stream)
				if err != nil || request.GetCancelOpen().GetRequestId() != open.GetRequestId() {
					return status.Error(codes.FailedPrecondition, "CancelOpen required")
				}
				var outcome *relayv1.ConnectResponse
				if errors.Is(test.want, ErrOpenCancelled) {
					outcome = &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenFailed{PipeOpenFailed: &relayv1.PipeOpenFailed{
						RequestId: open.GetRequestId(), Endpoint: open.GetEndpoint(), TargetId: open.GetTargetId(), Failure: relayv1.OpenFailure_OPEN_FAILURE_CANCELLED,
					}}}
				} else {
					outcome = &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenUnknown{PipeOpenUnknown: &relayv1.PipeOpenUnknown{
						RequestId: open.GetRequestId(), Endpoint: open.GetEndpoint(), TargetId: open.GetTargetId(),
					}}}
				}
				for range 2 {
					if err := stream.Send(outcome); err != nil {
						return err
					}
				}
				if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenCancelAcknowledged{OpenCancelAcknowledged: &relayv1.OpenCancelAcknowledged{RequestId: open.GetRequestId(), WasPending: true}}}); err != nil {
					return err
				}
				if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_OpenCancelAcknowledged{OpenCancelAcknowledged: &relayv1.OpenCancelAcknowledged{RequestId: open.GetRequestId(), WasPending: true}}}); err != nil {
					return err
				}
				request, err = recvRequest(stream)
				if err != nil {
					return err
				}
				bind := request.GetBindListener()
				if bind == nil {
					return status.Error(codes.FailedPrecondition, "Bind after cancel required")
				}
				if err := stream.Send(listenerBound("binding-after-cancel", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
					return err
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			})
			client := connectTestClient(t, address)
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				_, err := client.Open(ctx, "/cancel", "worker")
				result <- err
			}()
			<-started
			cancel()
			err := <-result
			if !errors.Is(err, test.want) {
				t.Fatalf("Open error = %v, want %v", err, test.want)
			}
			if _, err := client.Bind(context.Background(), "/after-cancel", "worker"); err != nil {
				t.Fatalf("Bind after outcome-before-ACK = %v", err)
			}
		})
	}

	t.Run("stable failure remains typed across duplicate outcomes", func(t *testing.T) {
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
			outcome := &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpenFailed{PipeOpenFailed: &relayv1.PipeOpenFailed{
				RequestId: open.GetRequestId(), Endpoint: open.GetEndpoint(), TargetId: open.GetTargetId(), Failure: relayv1.OpenFailure_OPEN_FAILURE_ROUTE_NOT_FOUND,
			}}}
			for range 2 {
				if err := stream.Send(outcome); err != nil {
					return err
				}
			}
			request, err = recvRequest(stream)
			if err != nil {
				return err
			}
			bind := request.GetBindListener()
			if bind == nil {
				return status.Error(codes.FailedPrecondition, "Bind after duplicate stable failure required")
			}
			if err := stream.Send(listenerBound("binding-after-stable-failure", bind.GetEndpointPattern(), bind.GetTargetId())); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		client := connectTestClient(t, address)
		_, err := client.Open(context.Background(), "/missing", "worker")
		var openErr *OpenError
		if !errors.Is(err, ErrOpenFailed) || !errors.As(err, &openErr) || openErr.Outcome != OpenOutcomeFailed || openErr.Failure != OpenFailureRouteNotFound {
			t.Fatalf("Open stable failure = %#v, %v", openErr, err)
		}
		if _, err := client.Bind(context.Background(), "/after-stable-failure", "worker"); err != nil {
			t.Fatalf("Bind after duplicate stable failure: %v", err)
		}
	})
}
