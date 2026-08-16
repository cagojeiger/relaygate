package authority

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/controlstate"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

const (
	ingressRelayAddress = "ingress.internal:7300"
	ownerRelayAddress   = "owner.internal:7300"
	admissionContextTTL = 30 * time.Second
)

var admissionNow = time.Unix(1_700_000_000, 123_000_000)

func TestResolveOpenReturnsExactContext(t *testing.T) {
	binding := admissionBinding("client-a", "/jobs/one", "worker")
	manager, _, ingress := newAdmissionManager(t, []controlstate.BindingSlot{binding}, []controlstate.BindingSlot{binding})
	auth := admissionAuth("client-a")

	first, err := manager.ResolveOpen(ingress, auth, "/jobs/one", "worker")
	if err != nil {
		t.Fatalf("ResolveOpen(): %v", err)
	}
	second, err := manager.ResolveOpen(ingress, auth, "/jobs/one", "worker")
	if err != nil {
		t.Fatalf("ResolveOpen(second): %v", err)
	}
	if first.ClusterEpoch != "epoch-1" || first.AuthorityID != ingress.AuthorityID ||
		first.AttemptID == "" || len(first.AttemptID) > controlstate.MaxIdentityBytes ||
		first.AttemptID == second.AttemptID || first.Auth != auth ||
		first.IngressGatewayID != ingress.GatewayID ||
		first.IngressGatewayInstanceID != ingress.GatewayInstanceID ||
		first.IngressControlSessionID != ingress.ControlSessionID ||
		first.OwnerRelayAddress != ownerRelayAddress ||
		!first.ExpiresAt.Equal(admissionNow.Add(admissionContextTTL)) ||
		!bindingSlotsEqual(first.Binding, binding) {
		t.Fatalf("Open context = %#v; second attempt = %q", first, second.AttemptID)
	}
	if first.Binding.Ref == binding.Ref {
		t.Fatal("ResolveOpen returned the durable binding ref pointer directly")
	}
}

func TestResolveOpenDoesNotReadFullControlState(t *testing.T) {
	binding := admissionBinding("client-a", "/jobs/one", "worker")
	manager, node, ingress := newAdmissionManager(t, []controlstate.BindingSlot{binding}, []controlstate.BindingSlot{binding})
	node.resetStateCalls()

	if _, err := manager.ResolveOpen(ingress, admissionAuth("client-a"), "/jobs/one", "worker"); err != nil {
		t.Fatalf("ResolveOpen(): %v", err)
	}
	if calls := node.stateCallCount(); calls != 0 {
		t.Fatalf("ResolveOpen() full State calls = %d, want 0", calls)
	}
}

func TestOpenContextConsumptionIsSharedByValueCopiesAndClones(t *testing.T) {
	openContext, err := NewOpenContext(
		"epoch-1",
		"authority-1",
		"attempt-1",
		admissionAuth("client-a"),
		admissionBinding("client-a", "/jobs/one", "worker"),
	)
	if err != nil {
		t.Fatalf("NewOpenContext(): %v", err)
	}
	valueCopy := openContext
	clone := openContext.Clone()
	if !clone.TryConsume() {
		t.Fatal("first TryConsume() = false, want true")
	}
	if openContext.TryConsume() || valueCopy.TryConsume() {
		t.Fatal("a copied Open context consumed the shared attempt twice")
	}
	if (OpenContext{}).TryConsume() {
		t.Fatal("zero Open context consumed without a constructor token")
	}
	if clone.Binding.Ref == openContext.Binding.Ref {
		t.Fatal("Clone() did not copy the binding ref value")
	}
}

func TestNewForwardedOpenContextValidatesForwardingFields(t *testing.T) {
	base := ForwardingContext{
		IngressGatewayID:         "gateway-ingress",
		IngressGatewayInstanceID: "instance-ingress",
		IngressControlSessionID:  "control-ingress",
		OwnerRelayAddress:        ownerRelayAddress,
		ExpiresAt:                time.UnixMilli(1),
	}
	open, err := NewForwardedOpenContext(
		"epoch-1",
		"authority-1",
		"attempt-forwarded",
		admissionAuth("client-a"),
		admissionBinding("client-a", "/jobs/one", "worker"),
		base,
	)
	if err != nil {
		t.Fatalf("NewForwardedOpenContext(): %v", err)
	}
	if open.IngressGatewayID != base.IngressGatewayID || open.IngressGatewayInstanceID != base.IngressGatewayInstanceID ||
		open.IngressControlSessionID != base.IngressControlSessionID || open.OwnerRelayAddress != base.OwnerRelayAddress ||
		!open.ExpiresAt.Equal(base.ExpiresAt) {
		t.Fatalf("forwarding context = %#v", open)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ForwardingContext)
	}{
		{name: "ingress gateway", mutate: func(context *ForwardingContext) { context.IngressGatewayID = "" }},
		{name: "ingress instance", mutate: func(context *ForwardingContext) {
			context.IngressGatewayInstanceID = strings.Repeat("i", controlstate.MaxIdentityBytes+1)
		}},
		{name: "ingress session", mutate: func(context *ForwardingContext) { context.IngressControlSessionID = "" }},
		{name: "owner relay", mutate: func(context *ForwardingContext) { context.OwnerRelayAddress = "0.0.0.0:7300" }},
		{name: "oversized owner relay", mutate: func(context *ForwardingContext) {
			context.OwnerRelayAddress = strings.Repeat("r", MaxRelayAddressBytes+1)
		}},
		{name: "expiry", mutate: func(context *ForwardingContext) { context.ExpiresAt = time.Time{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			forwarding := base
			test.mutate(&forwarding)
			if _, err := NewForwardedOpenContext(
				"epoch-1",
				"authority-1",
				"attempt-invalid",
				admissionAuth("client-a"),
				admissionBinding("client-a", "/jobs/one", "worker"),
				forwarding,
			); !errors.Is(err, ErrInvalidOpen) {
				t.Fatalf("NewForwardedOpenContext() error = %v, want ErrInvalidOpen", err)
			}
		})
	}
}

func TestResolveOpenFailsClosedWhenCommittedOrRevalidatedGateIsFalse(t *testing.T) {
	binding := admissionBinding("client-a", "/jobs/one", "worker")
	for _, test := range []struct {
		name          string
		committed     []controlstate.BindingSlot
		ownerSnapshot []controlstate.BindingSlot
	}{
		{name: "C false", ownerSnapshot: []controlstate.BindingSlot{binding}},
		{name: "V false", committed: []controlstate.BindingSlot{binding}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, _, ingress := newAdmissionManager(t, test.committed, test.ownerSnapshot)
			_, err := manager.ResolveOpen(ingress, admissionAuth("client-a"), "/jobs/one", "worker")
			if !errors.Is(err, ErrRouteNotFound) {
				t.Fatalf("ResolveOpen() error = %v, want ErrRouteNotFound", err)
			}
		})
	}
}

func TestResolveOpenDoesNotCrossClientNamespace(t *testing.T) {
	binding := admissionBinding("client-b", "/jobs/one", "worker")
	manager, _, ingress := newAdmissionManager(t, []controlstate.BindingSlot{binding}, []controlstate.BindingSlot{binding})

	_, err := manager.ResolveOpen(ingress, admissionAuth("client-a"), "/jobs/one", "worker")
	if !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("cross-client ResolveOpen() error = %v, want ErrRouteNotFound", err)
	}
	if _, err := manager.ResolveOpen(ingress, admissionAuth("client-b"), "/jobs/one", "worker"); err != nil {
		t.Fatalf("same-client ResolveOpen(): %v", err)
	}
}

func TestResolveOpenRejectsStaleIngressSession(t *testing.T) {
	binding := admissionBinding("client-a", "/jobs/one", "worker")
	manager, node, ingress := newAdmissionManager(t, []controlstate.BindingSlot{binding}, []controlstate.BindingSlot{binding})
	ingressSlot := node.State().Gateways[0]
	replacement, err := manager.OpenSession(ingressSlot, testRelayAddress)
	if err != nil {
		t.Fatalf("OpenSession(replacement): %v", err)
	}
	if err := manager.Revalidate(replacement.Ref, nil); err != nil {
		t.Fatalf("Revalidate(replacement): %v", err)
	}

	_, err = manager.ResolveOpen(ingress, admissionAuth("client-a"), "/jobs/one", "worker")
	if !errors.Is(err, ErrOpenUnavailable) || !errors.Is(err, ErrStaleSession) {
		t.Fatalf("ResolveOpen(stale ingress) error = %v, want unavailable stale session", err)
	}
}

func TestExactBindingKeyRejectsOmittedTargetAndOversizedIdentity(t *testing.T) {
	auth := admissionAuth("client-a")
	if _, err := ExactBindingKey(auth, "/jobs/one", ""); !errors.Is(err, ErrInvalidOpen) {
		t.Fatalf("ExactBindingKey(empty target) error = %v", err)
	}
	auth.APIKeyID = string(make([]byte, controlstate.MaxIdentityBytes+1))
	if _, err := ExactBindingKey(auth, "/jobs/one", "worker"); !errors.Is(err, ErrInvalidOpen) {
		t.Fatalf("ExactBindingKey(oversized API key ID) error = %v", err)
	}
}

func newAdmissionManager(
	t *testing.T,
	committed []controlstate.BindingSlot,
	ownerSnapshot []controlstate.BindingSlot,
) (*Manager, *fakeRaftNode, SessionRef) {
	t.Helper()
	ingressSlot := controlstate.GatewaySlot{
		GatewayID:  "gateway-ingress",
		Generation: 1,
		Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-ingress"},
	}
	ownerSlot := controlstate.GatewaySlot{
		GatewayID:  "gateway-owner",
		Generation: 1,
		Ref:        &controlstate.GatewayRegistrationRef{GatewayInstanceID: "instance-owner"},
	}
	node := &fakeRaftNode{
		status: raftnode.Status{Role: "Leader", Term: 1, ClusterEpoch: "epoch-1"},
		state: controlstate.State{
			ClusterEpoch: "epoch-1",
			Gateways:     []controlstate.GatewaySlot{ingressSlot, ownerSlot},
			Bindings:     committed,
		},
	}
	manager, err := New(Config{
		ClusterEpoch:        "epoch-1",
		ProbeInterval:       time.Hour,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
		OpenContextTTL:      admissionContextTTL,
	}, node)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(manager.Close)
	manager.now = func() time.Time { return admissionNow }
	if _, err := manager.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm(): %v", err)
	}
	ingress, err := manager.OpenSession(ingressSlot, ingressRelayAddress)
	if err != nil {
		t.Fatalf("OpenSession(ingress): %v", err)
	}
	if err := manager.Revalidate(ingress.Ref, nil); err != nil {
		t.Fatalf("Revalidate(ingress): %v", err)
	}
	owner, err := manager.OpenSession(ownerSlot, ownerRelayAddress)
	if err != nil {
		t.Fatalf("OpenSession(owner): %v", err)
	}
	if err := manager.Revalidate(owner.Ref, ownerSnapshot); err != nil {
		t.Fatalf("Revalidate(owner): %v", err)
	}
	return manager, node, ingress.Ref
}

func admissionBinding(clientID, endpoint, targetID string) controlstate.BindingSlot {
	return controlstate.BindingSlot{
		Key:        controlstate.BindingKey{ClientID: clientID, EndpointPattern: endpoint, TargetID: targetID},
		Generation: 7,
		Ref: &controlstate.ListenerBindingRef{
			GatewayID:         "gateway-owner",
			GatewayInstanceID: "instance-owner",
			ListenerBindingID: "listener-one",
		},
	}
}

func admissionAuth(clientID string) AuthContext {
	return AuthContext{
		ClientSessionID: "client-session-1",
		ClientID:        clientID,
		APIKeyID:        "key-1",
		AuthRevision:    "revision-1",
	}
}
