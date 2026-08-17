package authority

import (
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
)

func TestOpenContextCloneSharesAttemptAndPreservesOwnerSession(t *testing.T) {
	binding := routing.LiveBinding{Key: routing.BindingKey{ClientID: "client-1", EndpointPattern: "/events", TargetID: "worker"}, Ref: routing.ListenerBindingRef{GatewayID: "owner", GatewayInstanceID: "owner-1", ListenerBindingID: "listener-1"}}
	context, err := NewForwardedOpenContext("epoch-1", "authority-1", "attempt-1", testAuth(), binding, ForwardingContext{
		IngressGatewayID: "ingress", IngressGatewayInstanceID: "ingress-1", IngressControlSessionID: "ingress-session",
		OwnerControlSessionID: "owner-session", OwnerRelayAddress: "127.0.0.1:9000", ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	if context.OwnerControlSessionID != "owner-session" || context.Binding != binding {
		t.Fatalf("context = %#v", context)
	}
	if !context.Clone().TryConsume() || context.TryConsume() {
		t.Fatal("clones did not share one attempt token")
	}
}
