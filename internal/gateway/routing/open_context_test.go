package routing

import (
	"testing"
	"time"
)

func TestOpenContextCloneSharesAttemptAndPreservesOwnerSession(t *testing.T) {
	binding := LiveBinding{
		Key: BindingKey{ClientID: "client-1", EndpointPattern: "/events", TargetID: "worker"},
		Ref: ListenerBindingRef{GatewayID: "owner", GatewayInstanceID: "owner-1", ListenerBindingID: "listener-1"},
	}
	openContext, err := NewForwardedOpenContext("epoch-1", "authority-1", "attempt-1", AuthContext{
		ClientSessionID: "caller",
		ClientID:        "client-1",
		APIKeyID:        "key-1",
		AuthRevision:    "revision-1",
	}, binding, ForwardingContext{
		IngressGatewayID:         "ingress",
		IngressGatewayInstanceID: "ingress-1",
		IngressControlSessionID:  "ingress-session",
		OwnerControlSessionID:    "owner-session",
		OwnerRelayAddress:        "127.0.0.1:9000",
		ExpiresAt:                time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	if openContext.OwnerControlSessionID != "owner-session" || openContext.Binding != binding {
		t.Fatalf("context = %#v", openContext)
	}
	if !openContext.Clone().TryConsume() || openContext.TryConsume() {
		t.Fatal("clones did not share one attempt token")
	}
}
