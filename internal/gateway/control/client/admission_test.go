package gatewaycontrol

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/protobuf/proto"
)

func TestOpenContextFromProtoRejectsEveryMismatchedProvenanceField(t *testing.T) {
	control := Status{
		GatewayID:         "ingress-gateway",
		GatewayInstanceID: "ingress-instance",
		State:             StateRevalidated,
		AuthorityID:       "authority-a",
		ControlSessionID:  "ingress-control",
	}
	auth := routing.AuthContext{ClientSessionID: "caller-session", ClientID: "client-a", APIKeyID: "key-a", AuthRevision: "revision-a"}
	key := routing.BindingKey{ClientID: auth.ClientID, EndpointPattern: "/jobs", TargetID: "worker"}
	binding := routing.LiveBinding{
		Key: key,
		Ref: routing.ListenerBindingRef{GatewayID: "owner-gateway", GatewayInstanceID: "owner-instance", ListenerBindingID: "listener-a"},
	}
	valid := &controlv1.OpenContext{
		ClusterEpoch:             "epoch-a",
		AuthorityId:              control.AuthorityID,
		AttemptId:                "attempt-a",
		Auth:                     &controlv1.AuthContext{ClientSessionId: auth.ClientSessionID, ClientId: auth.ClientID, ApiKeyId: auth.APIKeyID, AuthRevision: auth.AuthRevision},
		Binding:                  liveBindingToProto(binding, true),
		IngressGatewayId:         control.GatewayID,
		IngressGatewayInstanceId: control.GatewayInstanceID,
		IngressControlSessionId:  control.ControlSessionID,
		OwnerControlSessionId:    "owner-control",
		OwnerRelayAddress:        "127.0.0.1:27430",
		ExpiresAtUnixMillis:      time.Now().Add(time.Minute).UnixMilli(),
	}
	if _, err := openContextFromProto(valid, "epoch-a", control, auth, key); err != nil {
		t.Fatalf("valid OpenContext: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*controlv1.OpenContext)
	}{
		{name: "cluster epoch", mutate: func(w *controlv1.OpenContext) { w.ClusterEpoch = "epoch-b" }},
		{name: "authority", mutate: func(w *controlv1.OpenContext) { w.AuthorityId = "authority-b" }},
		{name: "attempt", mutate: func(w *controlv1.OpenContext) { w.AttemptId = "" }},
		{name: "ingress gateway", mutate: func(w *controlv1.OpenContext) { w.IngressGatewayId = "other-gateway" }},
		{name: "ingress instance", mutate: func(w *controlv1.OpenContext) { w.IngressGatewayInstanceId = "other-instance" }},
		{name: "ingress control session", mutate: func(w *controlv1.OpenContext) { w.IngressControlSessionId = "other-control" }},
		{name: "owner control session", mutate: func(w *controlv1.OpenContext) { w.OwnerControlSessionId = "" }},
		{name: "client session", mutate: func(w *controlv1.OpenContext) { w.Auth.ClientSessionId = "other-session" }},
		{name: "client id", mutate: func(w *controlv1.OpenContext) { w.Auth.ClientId = "other-client" }},
		{name: "api key id", mutate: func(w *controlv1.OpenContext) { w.Auth.ApiKeyId = "other-key" }},
		{name: "auth revision", mutate: func(w *controlv1.OpenContext) { w.Auth.AuthRevision = "other-revision" }},
		{name: "binding key", mutate: func(w *controlv1.OpenContext) { w.Binding.Key.TargetId = "other-target" }},
		{name: "binding owner", mutate: func(w *controlv1.OpenContext) { w.Binding.Ref.GatewayId = "" }},
		{name: "relay address", mutate: func(w *controlv1.OpenContext) { w.OwnerRelayAddress = "not-an-address" }},
		{name: "expiry", mutate: func(w *controlv1.OpenContext) { w.ExpiresAtUnixMillis = 0 }},
		{name: "oversized identity", mutate: func(w *controlv1.OpenContext) {
			w.Binding.Ref.ListenerBindingId = strings.Repeat("x", routing.MaxIdentityBytes+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire := proto.Clone(valid).(*controlv1.OpenContext)
			test.mutate(wire)
			if _, err := openContextFromProto(wire, "epoch-a", control, auth, key); !errors.Is(err, ErrOpenUnavailable) {
				t.Fatalf("openContextFromProto() = %v, want ErrOpenUnavailable", err)
			}
		})
	}
}
