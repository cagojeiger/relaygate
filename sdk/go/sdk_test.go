package relaygate

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type scriptedRelay struct {
	relayv1.UnimplementedRelayServer
	run func(grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error
}

type blockingRelayClientStream struct {
	ctx      context.Context
	started  chan struct{}
	requests chan *relayv1.ConnectRequest
	once     sync.Once
}

func (s *blockingRelayClientStream) Send(request *relayv1.ConnectRequest) error {
	s.once.Do(func() { close(s.started) })
	if s.requests != nil {
		s.requests <- request
	}
	<-s.ctx.Done()
	return s.ctx.Err()
}

func (s *blockingRelayClientStream) Recv() (*relayv1.ConnectResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *blockingRelayClientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *blockingRelayClientStream) Trailer() metadata.MD         { return nil }
func (s *blockingRelayClientStream) CloseSend() error             { return nil }
func (s *blockingRelayClientStream) Context() context.Context     { return s.ctx }
func (s *blockingRelayClientStream) SendMsg(message any) error {
	request, ok := message.(*relayv1.ConnectRequest)
	if !ok {
		return fmt.Errorf("unexpected SendMsg %T", message)
	}
	return s.Send(request)
}
func (s *blockingRelayClientStream) RecvMsg(any) error {
	<-s.ctx.Done()
	return s.ctx.Err()
}

func (s *scriptedRelay) Connect(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
	return s.run(stream)
}

func startScriptedRelay(t *testing.T, run func(grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	relayv1.RegisterRelayServer(server, &scriptedRelay{run: run})
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})
	return listener.Addr().String()
}

func authenticateScript(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) (*relayv1.Authenticate, error) {
	request, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	authenticate := request.GetAuthenticate()
	if authenticate == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	err = stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ClientSessionOpened{
		ClientSessionOpened: &relayv1.ClientSessionOpened{Session: &relayv1.ClientSessionRef{
			ClientSessionId: "session-1", ClientId: authenticate.GetClientId(), ApiKeyId: authenticate.GetApiKeyId(), AuthRevision: "revision-1",
		}},
	}})
	return authenticate, err
}

func connectTestClient(t *testing.T, address string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := Connect(ctx, NewConfig(address, "client-1", "key-1", "secret-value").WithInsecureLocal())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func recvRequest(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) (*relayv1.ConnectRequest, error) {
	request, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if request == nil || request.GetMessage() == nil {
		return nil, status.Error(codes.FailedPrecondition, "empty request")
	}
	return request, nil
}

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
		if err := stream.Send(pipePayload("pipe-1", []byte("before-ack"))); err != nil {
			return err
		}
		<-allowAcknowledgement
		if err := stream.Send(&relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerConfirmationAcknowledged{
			ListenerConfirmationAcknowledged: &relayv1.ListenerConfirmationAcknowledged{AttemptId: "attempt-1", PipeId: "pipe-1"},
		}}); err != nil {
			return err
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetPipePayload() == nil {
			return status.Error(codes.FailedPrecondition, "PipePayload required")
		}
		if err := stream.Send(pipePayload("pipe-1", request.GetPipePayload().GetPayload())); err != nil {
			return err
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
			if err := stream.Send(pipePayload("pipe-pressure", []byte{byte(index + 1)})); err != nil {
				return err
			}
		}
		request, err = recvRequest(stream)
		if err != nil || request.GetClosePipe().GetPipeId() != "pipe-pressure" {
			return status.Error(codes.FailedPrecondition, "exact ClosePipe required after pressure")
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

func TestBindDeadlineClosesSessionBeforeLateAcknowledgement(t *testing.T) {
	bindReceived := make(chan struct{})
	lateSend := make(chan error, 1)
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
		close(bindReceived)
		<-stream.Context().Done()
		lateSend <- stream.Send(listenerBound("late-binding", bind.GetEndpointPattern(), bind.GetTargetId()))
		return stream.Context().Err()
	})
	client := connectTestClient(t, address)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Bind(ctx, "/late", "worker")
		result <- err
	}()
	<-bindReceived
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Bind after cancellation = %v", err)
	}
	<-client.Done()
	if err := <-lateSend; err == nil {
		t.Fatal("late ListenerBound was written after cancelled Bind returned")
	}
}

func TestBlockedStreamSendCancellationFailsSessionBeforeReturn(t *testing.T) {
	clientCtx, stop := context.WithCancelCause(context.Background())
	stream := &blockingRelayClientStream{ctx: clientCtx, started: make(chan struct{})}
	client := &Client{
		ctx: clientCtx, cancel: stop, stream: stream,
		sendQueue: make(chan sendCommand, sendQueueCapacity), pipeSlots: make(chan struct{}, maxPipes), done: make(chan struct{}),
		listeners: make(map[string]*Listener), offers: make(map[string]*Offer), opens: make(map[string]*openCall), pipes: make(map[string]*Pipe),
	}
	client.tasks.Add(1)
	go client.runSender()
	go client.supervise()
	callCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.send(callCtx, &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_BindListener{BindListener: &relayv1.BindListener{EndpointPattern: "/blocked", TargetId: "worker"}}})
	}()
	<-stream.started
	cancel()
	err := <-result
	var uncertain *sendUncertainError
	if !errors.As(err, &uncertain) || !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked send error = %v", err)
	}
	select {
	case <-client.Done():
	default:
		t.Fatal("blocked Send returned before the Client session closed")
	}
}

func TestPipeCloseDeadlineDoesNotWaitForBlockedPayloadSend(t *testing.T) {
	clientCtx, stop := context.WithCancelCause(context.Background())
	stream := &blockingRelayClientStream{
		ctx: clientCtx, started: make(chan struct{}), requests: make(chan *relayv1.ConnectRequest, 1),
	}
	client := &Client{
		ctx: clientCtx, cancel: stop, stream: stream,
		sendQueue: make(chan sendCommand, sendQueueCapacity), pipeSlots: make(chan struct{}, maxPipes), done: make(chan struct{}),
		listeners: make(map[string]*Listener), offers: make(map[string]*Offer), opens: make(map[string]*openCall),
		pipes: make(map[string]*Pipe), closeCalls: make(map[string]*closeCall),
	}
	if !client.reservePipeSlot() {
		t.Fatal("reserve Pipe slot")
	}
	pipe := newPipe(client, "pipe-blocked", "attempt-blocked", "/blocked", "worker")
	client.pipes[pipe.id] = pipe
	client.tasks.Add(1)
	go client.runSender()
	go client.supervise()
	t.Cleanup(func() {
		client.stop(errExplicitClose)
		<-client.Done()
	})

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- pipe.Send(context.Background(), []byte("first-payload"))
	}()
	<-stream.started
	first := <-stream.requests
	if string(first.GetPipePayload().GetPayload()) != "first-payload" {
		t.Fatalf("first wire request = %T, want first PipePayload", first.GetMessage())
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelClose()
	closeResult := make(chan error, 1)
	go func() { closeResult <- pipe.Close(closeCtx) }()
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		client.stop(errors.New("test: Close remained blocked behind payload Send"))
		<-client.Done()
		t.Fatal("Close did not honor its deadline while payload Send was blocked")
	}

	select {
	case queued := <-client.sendQueue:
		if queued.request.GetClosePipe().GetPipeId() != pipe.id {
			t.Fatalf("queued request after payload = %T, want ClosePipe", queued.request.GetMessage())
		}
	default:
		t.Fatal("ClosePipe was not queued behind the admitted payload")
	}
	if err := pipe.Send(context.Background(), []byte("after-close")); !errors.Is(err, ErrPipeClosed) {
		t.Fatalf("Send after Close admission = %v, want ErrPipeClosed", err)
	}
	select {
	case queued := <-client.sendQueue:
		t.Fatalf("request reached outbound queue after Close admission: %T", queued.request.GetMessage())
	default:
	}
	if err := <-sendResult; err == nil {
		t.Fatal("blocked payload Send succeeded after the Client session closed")
	}
}

func TestForeignMessageFailsClosedAndBoundsAreFixed(t *testing.T) {
	address := startScriptedRelay(t, func(stream grpc.BidiStreamingServer[relayv1.ConnectRequest, relayv1.ConnectResponse]) error {
		if _, err := authenticateScript(stream); err != nil {
			return err
		}
		return stream.Send(pipePayload("foreign-pipe", []byte("payload")))
	})
	client := connectTestClient(t, address)
	<-client.Done()
	if !errors.Is(client.Err(), errProtocol) {
		t.Fatalf("Client Err = %v, want protocol failure", client.Err())
	}

	bounded := &Client{pipeSlots: make(chan struct{}, maxPipes)}
	for index := 0; index < maxPipes; index++ {
		if !bounded.reservePipeSlot() {
			t.Fatalf("reservation %d rejected before bound", index)
		}
	}
	if bounded.reservePipeSlot() {
		t.Fatal("reservation beyond combined Pipe bound succeeded")
	}
	bounded.releasePipeSlot()
	if !bounded.reservePipeSlot() {
		t.Fatal("released Pipe reservation was not reusable")
	}

	history := &Client{bindingRecords: make(map[string]bindingRecord)}
	for index := 0; index < maxPendingOffers; index++ {
		record := bindingRecord{id: fmt.Sprintf("binding-%d", index), endpoint: "/bounded", target: "worker", unbound: index >= maxListeners}
		if !history.addBindingRecordLocked(record) {
			t.Fatalf("initial binding history rejected at %d", index)
		}
	}
	for index := 0; index < maxPendingOffers*2; index++ {
		record := bindingRecord{id: fmt.Sprintf("retired-%d", index), endpoint: "/bounded", target: "worker", unbound: true}
		if !history.addBindingRecordLocked(record) {
			t.Fatalf("binding history churn rejected at %d", index)
		}
		if len(history.bindingRecords) > maxPendingOffers || len(history.bindingHistory) > maxPendingOffers {
			t.Fatalf("binding history exceeded bound: records=%d order=%d", len(history.bindingRecords), len(history.bindingHistory))
		}
	}
	for index := 0; index < maxListeners; index++ {
		if _, exists := history.bindingRecords[fmt.Sprintf("binding-%d", index)]; !exists {
			t.Fatalf("live binding %d was evicted during retired-history churn", index)
		}
	}
}

func TestInsecureRequiresLoopback(t *testing.T) {
	_, err := Connect(context.Background(), NewConfig("example.com:443", "client", "key", "secret").WithInsecureLocal())
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Connect(non-loopback Insecure) = %v", err)
	}
}

func TestConfigRedactsSecretAndClonesTLSConfig(t *testing.T) {
	const secret = "never-format-this-api-key"
	baseTLS := &tls.Config{ServerName: "relay.example", MinVersion: tls.VersionTLS13}
	config := NewConfig("relay.example:443", "client", "key", secret).WithTLSConfig(baseTLS)
	baseTLS.ServerName = "mutated.example"
	if config.TLSConfig == baseTLS || config.TLSConfig.ServerName != "relay.example" {
		t.Fatalf("WithTLSConfig did not own a clone: %#v", config.TLSConfig)
	}
	for _, formatted := range []string{
		fmt.Sprintf("%v", config),
		fmt.Sprintf("%+v", config),
		fmt.Sprintf("%#v", config),
		fmt.Sprintf("%v", &config),
		fmt.Sprintf("%#v", &config),
	} {
		if strings.Contains(formatted, secret) || !strings.Contains(formatted, "redacted") {
			t.Fatalf("formatted Config was not redacted: %s", formatted)
		}
	}
	if err := validateConfig(NewConfig("relay.example:443", "client", "key", secret)); err != nil {
		t.Fatalf("default system-roots TLS config rejected: %v", err)
	}
}

func listenerBound(id, endpoint, target string) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_ListenerBound{ListenerBound: &relayv1.ListenerBound{Binding: &relayv1.ListenerBinding{
		ListenerBindingId: id, EndpointPattern: endpoint, TargetId: target,
	}}}}
}

func pipePayload(id string, payload []byte) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipePayload{PipePayload: &relayv1.PipePayload{PipeId: id, Payload: payload}}}
}

func pipeOpened(open *relayv1.Open, attemptID, pipeID string) *relayv1.ConnectResponse {
	return &relayv1.ConnectResponse{Message: &relayv1.ConnectResponse_PipeOpened{PipeOpened: &relayv1.PipeOpened{
		RequestId: open.GetRequestId(), AttemptId: attemptID, PipeId: pipeID, Endpoint: open.GetEndpoint(), TargetId: open.GetTargetId(),
	}}}
}
