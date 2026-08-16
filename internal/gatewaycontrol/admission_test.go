package gatewaycontrol

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testOwnerRelayAddress    = "relay-owner.internal:7300"
	testOpenExpiryUnixMillis = int64(1_900_000_000_000)
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
		openContext.Binding.Ref.ListenerBindingID != "listener-one" ||
		openContext.IngressGatewayID != controlStatus.GatewayID ||
		openContext.IngressGatewayInstanceID != controlStatus.GatewayInstanceID ||
		openContext.IngressControlSessionID != controlStatus.ControlSessionID ||
		openContext.OwnerRelayAddress != testOwnerRelayAddress ||
		openContext.ExpiresAt.UnixMilli() != testOpenExpiryUnixMillis {
		t.Fatalf("Open context = %#v", openContext)
	}
	copy := openContext
	if !openContext.TryConsume() || copy.TryConsume() {
		t.Fatal("decoded Open context did not share a strict single-use token")
	}
}

func TestOpenContextFromProtoRejectsTamperedForwardingFields(t *testing.T) {
	session := testClientSession()
	auth := authority.AuthContext{
		ClientSessionID: session.ClientSessionID,
		ClientID:        session.ClientID,
		APIKeyID:        session.APIKeyID,
		AuthRevision:    session.AuthRevision,
	}
	controlStatus := Status{
		GatewayID:         "gateway-1",
		GatewayInstanceID: "instance-1",
		AuthorityID:       "authority-1",
		ControlSessionID:  "session-1",
	}
	request := &controlv1.AdmitOpenRequest{
		Session: &controlv1.SessionRef{
			ClusterEpoch:      "epoch-1",
			AuthorityId:       controlStatus.AuthorityID,
			ControlSessionId:  controlStatus.ControlSessionID,
			GatewayId:         controlStatus.GatewayID,
			GatewayInstanceId: controlStatus.GatewayInstanceID,
		},
		Auth: &controlv1.AuthContext{
			ClientSessionId: auth.ClientSessionID,
			ClientId:        auth.ClientID,
			ApiKeyId:        auth.APIKeyID,
			AuthRevision:    auth.AuthRevision,
		},
		Endpoint: "/jobs/one",
		TargetId: "worker",
	}
	key, err := authority.ExactBindingKey(auth, request.GetEndpoint(), request.GetTargetId())
	if err != nil {
		t.Fatalf("ExactBindingKey(): %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*controlv1.OpenContext)
	}{
		{name: "ingress gateway", mutate: func(wire *controlv1.OpenContext) { wire.IngressGatewayId = "gateway-other" }},
		{name: "ingress instance", mutate: func(wire *controlv1.OpenContext) { wire.IngressGatewayInstanceId = "instance-other" }},
		{name: "ingress control session", mutate: func(wire *controlv1.OpenContext) { wire.IngressControlSessionId = "session-other" }},
		{name: "missing owner relay", mutate: func(wire *controlv1.OpenContext) { wire.OwnerRelayAddress = "" }},
		{name: "unspecified owner relay", mutate: func(wire *controlv1.OpenContext) { wire.OwnerRelayAddress = "0.0.0.0:7300" }},
		{name: "oversized owner relay", mutate: func(wire *controlv1.OpenContext) {
			wire.OwnerRelayAddress = strings.Repeat("a", authority.MaxRelayAddressBytes+1)
		}},
		{name: "zero expiry", mutate: func(wire *controlv1.OpenContext) { wire.ExpiresAtUnixMillis = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := proto.Clone(exactAdmissionResponse(request).GetContext()).(*controlv1.OpenContext)
			test.mutate(wire)
			openContext, err := openContextFromProto(wire, "epoch-1", controlStatus, auth, key)
			if !errors.Is(err, ErrOpenUnavailable) {
				t.Fatalf("openContextFromProto() error = %v, want %v", err, ErrOpenUnavailable)
			}
			if openContext.TryConsume() {
				t.Fatal("rejected Open context remained consumable")
			}
		})
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
		ClusterEpoch:             request.GetSession().GetClusterEpoch(),
		AuthorityId:              request.GetSession().GetAuthorityId(),
		AttemptId:                "attempt-1",
		Auth:                     proto.Clone(request.GetAuth()).(*controlv1.AuthContext),
		IngressGatewayId:         request.GetSession().GetGatewayId(),
		IngressGatewayInstanceId: request.GetSession().GetGatewayInstanceId(),
		IngressControlSessionId:  request.GetSession().GetControlSessionId(),
		OwnerRelayAddress:        testOwnerRelayAddress,
		ExpiresAtUnixMillis:      testOpenExpiryUnixMillis,
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
