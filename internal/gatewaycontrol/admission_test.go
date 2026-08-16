package gatewaycontrol

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientsession"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAdmitOpenUsesPublishedSessionAndDecodesConsumableExactContext(t *testing.T) {
	requests := make(chan *controlv1.AdmitOpenRequest, 1)
	server := &admissionTestServer{admit: func(request *controlv1.AdmitOpenRequest) (*controlv1.AdmitOpenResponse, error) {
		requests <- proto.Clone(request).(*controlv1.AdmitOpenRequest)
		return exactAdmissionResponse(request), nil
	}}
	address := startControlServer(t, server)
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	controlStatus := waitForClientState(t, client, StateRevalidated)
	session := testClientSession()

	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second)
	defer ctxCancel()
	openContext, err := client.AdmitOpen(ctx, session, "/jobs/one", "worker")
	if err != nil {
		t.Fatalf("AdmitOpen(): %v", err)
	}
	request := receiveTestValue(t, requests)
	if request.GetSession().GetClusterEpoch() != "epoch-1" ||
		request.GetSession().GetAuthorityId() != controlStatus.AuthorityID ||
		request.GetSession().GetControlSessionId() != controlStatus.ControlSessionID ||
		request.GetSession().GetGatewayId() != "gateway-1" ||
		request.GetSession().GetGatewayInstanceId() != "instance-1" ||
		request.GetAuth().GetClientSessionId() != session.ClientSessionID ||
		request.GetAuth().GetClientId() != session.ClientID ||
		request.GetAuth().GetApiKeyId() != session.APIKeyID ||
		request.GetAuth().GetAuthRevision() != session.AuthRevision ||
		request.GetEndpoint() != "/jobs/one" || request.GetTargetId() != "worker" {
		t.Fatalf("admission request = %#v", request)
	}
	if openContext.ClusterEpoch != "epoch-1" || openContext.AuthorityID != controlStatus.AuthorityID ||
		openContext.AttemptID != "attempt-1" || openContext.Auth.ClientSessionID != session.ClientSessionID ||
		openContext.Binding.Key.ClientID != "client-a" || openContext.Binding.Key.EndpointPattern != "/jobs/one" ||
		openContext.Binding.Key.TargetID != "worker" || openContext.Binding.Generation != 7 ||
		openContext.Binding.Ref == nil || openContext.Binding.Ref.GatewayID != "gateway-owner" ||
		openContext.Binding.Ref.GatewayInstanceID != "instance-owner" ||
		openContext.Binding.Ref.ListenerBindingID != "listener-one" {
		t.Fatalf("Open context = %#v", openContext)
	}
	copy := openContext
	if !openContext.TryConsume() || copy.TryConsume() {
		t.Fatal("decoded Open context did not share a strict single-use token")
	}
}

func TestAdmitOpenMapsStableErrorsAndDoesNotReplay(t *testing.T) {
	var calls atomic.Int32
	server := &admissionTestServer{admit: func(request *controlv1.AdmitOpenRequest) (*controlv1.AdmitOpenResponse, error) {
		calls.Add(1)
		switch request.GetTargetId() {
		case "invalid":
			return nil, status.Error(codes.InvalidArgument, "invalid")
		case "missing":
			return nil, status.Error(codes.NotFound, "missing")
		default:
			return nil, status.Error(codes.Unavailable, "response lost")
		}
	}}
	address := startControlServer(t, server)
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	for _, test := range []struct {
		target string
		want   error
	}{
		{target: "invalid", want: ErrInvalidOpen},
		{target: "missing", want: ErrRouteNotFound},
		{target: "unavailable", want: ErrOpenUnavailable},
	} {
		before := calls.Load()
		_, err := client.AdmitOpen(context.Background(), testClientSession(), "/jobs/one", test.target)
		if !errors.Is(err, test.want) {
			t.Fatalf("AdmitOpen(%q) error = %v, want %v", test.target, err, test.want)
		}
		if got := calls.Load() - before; got != 1 {
			t.Fatalf("AdmitOpen(%q) RPC calls = %d, want exactly one", test.target, got)
		}
	}
}

func TestAdmitOpenRejectsInvalidInputAndNonRevalidatedClientLocally(t *testing.T) {
	client := newTestClient(t, "127.0.0.1:1")
	if _, err := client.AdmitOpen(context.Background(), testClientSession(), "/jobs/one", "worker"); !errors.Is(err, ErrOpenUnavailable) {
		t.Fatalf("AdmitOpen(disconnected) error = %v", err)
	}
	if _, err := client.AdmitOpen(context.Background(), testClientSession(), "/jobs/one", ""); !errors.Is(err, ErrInvalidOpen) {
		t.Fatalf("AdmitOpen(empty target) error = %v", err)
	}
}

type admissionTestServer struct {
	controlv1.UnimplementedGatewayControlServer

	admit func(*controlv1.AdmitOpenRequest) (*controlv1.AdmitOpenResponse, error)
}

func (s *admissionTestServer) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	if _, _, err := openTestSession(stream, 1, nil); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *admissionTestServer) AdmitOpen(_ context.Context, request *controlv1.AdmitOpenRequest) (*controlv1.AdmitOpenResponse, error) {
	return s.admit(request)
}

func exactAdmissionResponse(request *controlv1.AdmitOpenRequest) *controlv1.AdmitOpenResponse {
	return &controlv1.AdmitOpenResponse{Context: &controlv1.OpenContext{
		ClusterEpoch: request.GetSession().GetClusterEpoch(),
		AuthorityId:  request.GetSession().GetAuthorityId(),
		AttemptId:    "attempt-1",
		Auth:         proto.Clone(request.GetAuth()).(*controlv1.AuthContext),
		Binding: &controlv1.BindingSlot{
			Key: &controlv1.BindingKey{
				ClientId:        request.GetAuth().GetClientId(),
				EndpointPattern: request.GetEndpoint(),
				TargetId:        request.GetTargetId(),
			},
			Generation: 7,
			Ref: &controlv1.ListenerBindingRef{
				GatewayId:         "gateway-owner",
				GatewayInstanceId: "instance-owner",
				ListenerBindingId: "listener-one",
			},
		},
	}}
}

func testClientSession() clientsession.Ref {
	return clientsession.Ref{
		ClientSessionID: "client-session-1",
		ClientID:        "client-a",
		APIKeyID:        "key-1",
		AuthRevision:    "revision-1",
	}
}
