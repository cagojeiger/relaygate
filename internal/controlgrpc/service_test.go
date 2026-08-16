package controlgrpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

const testEpoch = "epoch-1"

func TestConnectRequiresSnapshotThenAppliesIdempotentMutations(t *testing.T) {
	harness := newHarness(t)
	stream, session := harness.openAndSync(t, "gateway-a", "instance-a")

	key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
	install := installRequest(session, key, 0, nil, ref)
	if err := stream.Send(install); err != nil {
		t.Fatalf("Send(install): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(install): %v", err)
	}
	result := response.GetMutationResult()
	if result.GetCode() != controlv1.MutationCode_MUTATION_CODE_APPLIED || result.GetSlot().GetGeneration() != 1 {
		t.Fatalf("install result = %#v", result)
	}

	if err := stream.Send(install); err != nil {
		t.Fatalf("Send(replay): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(replay): %v", err)
	}
	if code := response.GetMutationResult().GetCode(); code != controlv1.MutationCode_MUTATION_CODE_ALREADY_APPLIED {
		t.Fatalf("replay code = %v", code)
	}

	conflict := installRequest(session, key, 0, nil, &controlv1.ListenerBindingRef{
		GatewayInstanceId: "instance-a",
		ListenerBindingId: "listener-b",
	})
	if err := stream.Send(conflict); err != nil {
		t.Fatalf("Send(conflict): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(conflict): %v", err)
	}
	if code := response.GetMutationResult().GetCode(); code != controlv1.MutationCode_MUTATION_CODE_REJECTED {
		t.Fatalf("conflict code = %v", code)
	}
	current := response.GetMutationResult().GetSlot()
	if current.GetGeneration() != 1 || current.GetRef().GetListenerBindingId() != "listener-a" {
		t.Fatalf("conflict current slot = %#v", current)
	}

	remove := &controlv1.ControlRequest{Message: &controlv1.ControlRequest_BindingMutation{
		BindingMutation: &controlv1.BindingMutation{
			Session: session,
			Mutation: &controlv1.BindingMutation_Remove{Remove: &controlv1.RemoveBinding{
				Key: key, ExpectedGeneration: 1, ExpectedRef: ref,
			}},
		},
	}}
	if err := stream.Send(remove); err != nil {
		t.Fatalf("Send(remove): %v", err)
	}
	response, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv(remove): %v", err)
	}
	result = response.GetMutationResult()
	if result.GetCode() != controlv1.MutationCode_MUTATION_CODE_APPLIED || result.GetSlot().GetGeneration() != 2 || result.GetSlot().GetRef() != nil {
		t.Fatalf("remove result = %#v", result)
	}

	durable := harness.node.Lookup(controlstate.BindingKey{ClientID: "client-a", EndpointPattern: "/jobs/*", TargetID: "worker"})
	if durable.Generation != 2 || !durable.IsTombstone() {
		t.Fatalf("durable binding = %#v", durable)
	}
}

func TestNewGatewayInstanceFencesOldSession(t *testing.T) {
	harness := newHarness(t)
	oldStream, oldSession := harness.openAndSync(t, "gateway-a", "instance-a")
	newStream, newSession := harness.openAndSync(t, "gateway-a", "instance-b")

	if oldSession.GetControlSessionId() == newSession.GetControlSessionId() {
		t.Fatal("replacement reused the control session ID")
	}
	if _, err := oldStream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("old stream error = %v, want Unavailable", err)
	}
	slot := harness.node.LookupGateway("gateway-a")
	if slot.Generation != 2 || slot.Ref == nil || slot.Ref.GatewayInstanceID != "instance-b" {
		t.Fatalf("gateway slot = %#v", slot)
	}
	if err := newStream.CloseSend(); err != nil {
		t.Fatalf("CloseSend(new stream): %v", err)
	}
}

func TestAuthorityLossFencesSessionAndRecoveryCreatesNewAuthority(t *testing.T) {
	harness := newHarness(t)
	stream, oldSession := harness.openAndSync(t, "gateway-a", "instance-a")

	harness.node.setVerifyError(errors.New("quorum lost"))
	if _, err := harness.manager.Confirm(context.Background()); !errors.Is(err, authority.ErrNoAuthority) {
		t.Fatalf("Confirm() error = %v, want ErrNoAuthority", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("fenced stream error = %v, want Unavailable", err)
	}

	harness.node.setVerifyError(nil)
	newStream, newSession := harness.openAndSync(t, "gateway-a", "instance-a")
	if oldSession.GetAuthorityId() == newSession.GetAuthorityId() {
		t.Fatal("recovered authority reused its ID")
	}
	slot := harness.node.LookupGateway("gateway-a")
	if slot.Generation != 1 {
		t.Fatalf("same gateway instance changed registration generation: %#v", slot)
	}
	if err := newStream.Send(installRequest(oldSession,
		&controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/stale", TargetId: "worker"},
		0,
		nil,
		&controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"},
	)); err != nil {
		t.Fatalf("Send(stale session): %v", err)
	}
	if _, err := newStream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale session error = %v, want PermissionDenied", err)
	}
}

func TestSnapshotMustExactlyMatchCommittedBinding(t *testing.T) {
	harness := newHarness(t)
	stream, session := harness.open(t, "gateway-a", "instance-a")
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{
			Session: session,
			Bindings: []*controlv1.BindingSlot{{
				Key:        &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/missing", TargetId: "worker"},
				Generation: 1,
				Ref:        &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"},
			}},
		},
	}}); err != nil {
		t.Fatalf("Send(snapshot): %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("snapshot error = %v, want FailedPrecondition", err)
	}
}

func TestFullSnapshotRejectsOmittedCommittedBinding(t *testing.T) {
	harness := newHarness(t)
	stream, session := harness.openAndSync(t, "gateway-a", "instance-a")
	key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
	committed := installBinding(t, stream, session, key, ref)

	omittingStream, omittingSession := harness.open(t, "gateway-a", "instance-a")
	if err := omittingStream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: omittingSession},
	}}); err != nil {
		t.Fatalf("Send(omitting snapshot): %v", err)
	}
	if _, err := omittingStream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("omitting snapshot error = %v, want FailedPrecondition", err)
	}

	exactStream, exactSession := harness.open(t, "gateway-a", "instance-a")
	if err := exactStream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: exactSession, Bindings: []*controlv1.BindingSlot{committed}},
	}}); err != nil {
		t.Fatalf("Send(exact snapshot): %v", err)
	}
	response, err := exactStream.Recv()
	if err != nil {
		t.Fatalf("Recv(exact snapshot): %v", err)
	}
	if response.GetSnapshotAccepted() == nil {
		t.Fatalf("exact snapshot response = %#v", response)
	}
}

func TestSessionOpenedIncludesOwnedBindingOnSameInstanceReconnect(t *testing.T) {
	harness := newHarness(t)
	stream, session := harness.openAndSync(t, "gateway-a", "instance-a")
	key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
	committed := installBinding(t, stream, session, key, ref)

	_, opened := harness.openSession(t, "gateway-a", "instance-a")
	if bindings := opened.GetOwnedBindings(); len(bindings) != 1 || !proto.Equal(bindings[0], committed) {
		t.Fatalf("owned bindings = %#v, want committed slot %#v", bindings, committed)
	}
}

func TestSessionOpenedExcludesPriorInstanceBinding(t *testing.T) {
	harness := newHarness(t)
	stream, session := harness.openAndSync(t, "gateway-a", "instance-a")
	key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
	oldRef := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
	installBinding(t, stream, session, key, oldRef)

	_, opened := harness.openSession(t, "gateway-a", "instance-b")
	if opened.GetSession().GetGatewayInstanceId() != "instance-b" {
		t.Fatalf("new session = %#v", opened.GetSession())
	}
	if bindings := opened.GetOwnedBindings(); len(bindings) != 0 {
		t.Fatalf("owned bindings = %#v, want current-instance slots only", bindings)
	}
}

func TestReconnectMessagesFitControlEnvelopeAtProtocolLimits(t *testing.T) {
	identity := strings.Repeat("i", controlstate.MaxIdentityBytes)
	slots := make([]*controlv1.BindingSlot, 0, controlstate.MaxListenerBindingsPerGateway)
	for index := 0; index < controlstate.MaxListenerBindingsPerGateway; index++ {
		suffix := fmt.Sprintf("%04d", index)
		endpoint := strings.Repeat("e", controlstate.MaxEndpointPatternBytes-len(suffix)) + suffix
		slots = append(slots, &controlv1.BindingSlot{
			Key:        &controlv1.BindingKey{ClientId: identity, EndpointPattern: endpoint, TargetId: identity},
			Generation: uint64(index + 1),
			Ref:        &controlv1.ListenerBindingRef{GatewayInstanceId: identity, ListenerBindingId: identity},
		})
	}
	session := &controlv1.SessionRef{
		ClusterEpoch: identity, AuthorityId: identity, ControlSessionId: identity,
		GatewayId: identity, GatewayInstanceId: identity,
	}
	opened := &controlv1.SessionOpened{Session: session, GatewayGeneration: 1, OwnedBindings: slots}
	snapshot := &controlv1.FullSnapshot{Session: session, Bindings: slots}
	if size := proto.Size(opened); size >= maxMessageBytes {
		t.Fatalf("SessionOpened size = %d, must fit below %d", size, maxMessageBytes)
	}
	if size := proto.Size(snapshot); size >= maxMessageBytes {
		t.Fatalf("FullSnapshot size = %d, must fit below %d", size, maxMessageBytes)
	}
}

func TestSessionOpenedOwnedBindingsExcludeForeignAndTombstonesAndAreSorted(t *testing.T) {
	harness := newHarness(t)
	ownerStream, ownerSession := harness.openAndSync(t, "gateway-a", "instance-a")
	ownerRef := func(bindingID string) *controlv1.ListenerBindingRef {
		return &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: bindingID}
	}
	for _, binding := range []struct {
		key *controlv1.BindingKey
		ref *controlv1.ListenerBindingRef
	}{
		{
			key: &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/z", TargetId: "worker"},
			ref: ownerRef("listener-z"),
		},
		{
			key: &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/a", TargetId: "worker"},
			ref: ownerRef("listener-a"),
		},
	} {
		installBinding(t, ownerStream, ownerSession, binding.key, binding.ref)
	}

	tombstoneKey := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/removed", TargetId: "worker"}
	tombstoneRef := ownerRef("listener-removed")
	tombstone := installBinding(t, ownerStream, ownerSession, tombstoneKey, tombstoneRef)
	if err := ownerStream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_BindingMutation{
		BindingMutation: &controlv1.BindingMutation{
			Session: ownerSession,
			Mutation: &controlv1.BindingMutation_Remove{Remove: &controlv1.RemoveBinding{
				Key: tombstoneKey, ExpectedGeneration: tombstone.GetGeneration(), ExpectedRef: tombstoneRef,
			}},
		},
	}}); err != nil {
		t.Fatalf("Send(remove tombstone): %v", err)
	}
	response, err := ownerStream.Recv()
	if err != nil {
		t.Fatalf("Recv(remove tombstone): %v", err)
	}
	if result := response.GetMutationResult(); result.GetCode() != controlv1.MutationCode_MUTATION_CODE_APPLIED || result.GetSlot().GetRef() != nil {
		t.Fatalf("remove result = %#v", result)
	}

	foreignStream, foreignSession := harness.openAndSync(t, "gateway-b", "instance-b")
	installBinding(t, foreignStream, foreignSession,
		&controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/foreign", TargetId: "worker"},
		&controlv1.ListenerBindingRef{GatewayInstanceId: "instance-b", ListenerBindingId: "listener-foreign"},
	)

	_, opened := harness.openSession(t, "gateway-a", "instance-a")
	bindings := opened.GetOwnedBindings()
	if len(bindings) != 2 {
		t.Fatalf("owned bindings = %#v, want two live owner slots", bindings)
	}
	if first, second := bindings[0].GetKey().GetEndpointPattern(), bindings[1].GetKey().GetEndpointPattern(); first != "/a" || second != "/z" {
		t.Fatalf("owned binding order = [%q, %q], want [/a, /z]", first, second)
	}
}

func TestBindingMutationsCannotCrossGatewayOwnership(t *testing.T) {
	for _, test := range []struct {
		name    string
		request func(*controlv1.SessionRef, *controlv1.BindingKey, *controlv1.ListenerBindingRef) *controlv1.ControlRequest
	}{
		{
			name: "replace",
			request: func(session *controlv1.SessionRef, key *controlv1.BindingKey, ownerRef *controlv1.ListenerBindingRef) *controlv1.ControlRequest {
				return installRequest(session, key, 1, ownerRef, &controlv1.ListenerBindingRef{
					GatewayInstanceId: "instance-b",
					ListenerBindingId: "listener-b",
				})
			},
		},
		{
			name: "remove",
			request: func(session *controlv1.SessionRef, key *controlv1.BindingKey, ownerRef *controlv1.ListenerBindingRef) *controlv1.ControlRequest {
				return &controlv1.ControlRequest{Message: &controlv1.ControlRequest_BindingMutation{
					BindingMutation: &controlv1.BindingMutation{
						Session: session,
						Mutation: &controlv1.BindingMutation_Remove{Remove: &controlv1.RemoveBinding{
							Key: key, ExpectedGeneration: 1, ExpectedRef: ownerRef,
						}},
					},
				}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newHarness(t)
			ownerStream, ownerSession := harness.openAndSync(t, "gateway-a", "instance-a")
			key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
			ownerRef := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
			installBinding(t, ownerStream, ownerSession, key, ownerRef)

			attackerStream, attackerSession := harness.openAndSync(t, "gateway-b", "instance-b")
			if err := attackerStream.Send(test.request(attackerSession, key, ownerRef)); err != nil {
				t.Fatalf("Send(cross-owner mutation): %v", err)
			}
			response, err := attackerStream.Recv()
			if err != nil {
				t.Fatalf("Recv(cross-owner mutation): %v", err)
			}
			if code := response.GetMutationResult().GetCode(); code != controlv1.MutationCode_MUTATION_CODE_REJECTED {
				t.Fatalf("cross-owner mutation code = %v, want REJECTED", code)
			}
			durable := harness.node.Lookup(controlstate.BindingKey{ClientID: "client-a", EndpointPattern: "/jobs/*", TargetID: "worker"})
			if durable.Generation != 1 || durable.Ref == nil || durable.Ref.GatewayInstanceID != "instance-a" {
				t.Fatalf("durable binding after rejected mutation = %#v", durable)
			}
		})
	}
}

func TestNewInstanceCanRebindSameStableGatewayBinding(t *testing.T) {
	harness := newHarness(t)
	oldStream, oldSession := harness.openAndSync(t, "gateway-a", "instance-a")
	key := &controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs/*", TargetId: "worker"}
	oldRef := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"}
	installBinding(t, oldStream, oldSession, key, oldRef)

	newStream, newSession := harness.openAndSync(t, "gateway-a", "instance-b")
	newRef := &controlv1.ListenerBindingRef{GatewayInstanceId: "instance-b", ListenerBindingId: "listener-b"}
	if err := newStream.Send(installRequest(newSession, key, 0, nil, newRef)); err != nil {
		t.Fatalf("Send(initial rebind): %v", err)
	}
	response, err := newStream.Recv()
	if err != nil {
		t.Fatalf("Recv(initial rebind): %v", err)
	}
	if result := response.GetMutationResult(); result.GetCode() != controlv1.MutationCode_MUTATION_CODE_REJECTED ||
		!result.GetSameGatewayOwner() || result.GetSlot().GetRef().GetGatewayInstanceId() != "instance-a" {
		t.Fatalf("initial rebind result = %#v, want same-Gateway prior owner", result)
	}
	if err := newStream.Send(installRequest(newSession, key, 1, oldRef, newRef)); err != nil {
		t.Fatalf("Send(rebind): %v", err)
	}
	response, err = newStream.Recv()
	if err != nil {
		t.Fatalf("Recv(rebind): %v", err)
	}
	if result := response.GetMutationResult(); result.GetCode() != controlv1.MutationCode_MUTATION_CODE_APPLIED || result.GetSlot().GetGeneration() != 2 {
		t.Fatalf("rebind result = %#v", result)
	}
	durable := harness.node.Lookup(controlstate.BindingKey{ClientID: "client-a", EndpointPattern: "/jobs/*", TargetID: "worker"})
	if durable.Ref == nil || durable.Ref.GatewayID != "gateway-a" || durable.Ref.GatewayInstanceID != "instance-b" {
		t.Fatalf("durable binding after rebind = %#v", durable)
	}
}

func TestMutationBeforeSnapshotIsRejected(t *testing.T) {
	harness := newHarness(t)
	stream, session := harness.open(t, "gateway-a", "instance-a")
	if err := stream.Send(installRequest(session,
		&controlv1.BindingKey{ClientId: "client-a", EndpointPattern: "/jobs", TargetId: "worker"},
		0,
		nil,
		&controlv1.ListenerBindingRef{GatewayInstanceId: "instance-a", ListenerBindingId: "listener-a"},
	)); err != nil {
		t.Fatalf("Send(mutation): %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mutation error = %v, want FailedPrecondition", err)
	}
}

func TestControlProtocolRejectsOutOfOrderMessages(t *testing.T) {
	t.Run("snapshot before hello", func(t *testing.T) {
		harness := newHarness(t)
		stream, err := harness.client.Connect(harness.ctx)
		if err != nil {
			t.Fatalf("Connect(): %v", err)
		}
		if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
			FullSnapshot: &controlv1.FullSnapshot{},
		}}); err != nil {
			t.Fatalf("Send(snapshot): %v", err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("error = %v, want InvalidArgument", err)
		}
	})

	for _, test := range []struct {
		name    string
		request func(*controlv1.SessionRef) *controlv1.ControlRequest
	}{
		{
			name: "second hello",
			request: func(*controlv1.SessionRef) *controlv1.ControlRequest {
				return &controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{Hello: &controlv1.Hello{
					ClusterEpoch: testEpoch, GatewayId: "gateway-a", GatewayInstanceId: "instance-a",
				}}}
			},
		},
		{
			name: "second snapshot",
			request: func(session *controlv1.SessionRef) *controlv1.ControlRequest {
				return &controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
					FullSnapshot: &controlv1.FullSnapshot{Session: session},
				}}
			},
		},
		{
			name: "empty request",
			request: func(*controlv1.SessionRef) *controlv1.ControlRequest {
				return &controlv1.ControlRequest{}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newHarness(t)
			stream, session := harness.openAndSync(t, "gateway-a", "instance-a")
			if err := stream.Send(test.request(session)); err != nil {
				t.Fatalf("Send(): %v", err)
			}
			if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("error = %v, want FailedPrecondition", err)
			}
		})
	}
}

type harness struct {
	node    *fakeNode
	manager *authority.Manager
	client  controlv1.GatewayControlClient
	ctx     context.Context
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	node := newFakeNode(t)
	manager, err := authority.New(authority.Config{
		ClusterEpoch:        testEpoch,
		ProbeInterval:       time.Hour,
		ProbeTimeout:        time.Second,
		RevalidationTimeout: time.Minute,
	}, node)
	if err != nil {
		t.Fatalf("authority.New(): %v", err)
	}
	service, err := NewService(testEpoch, node, manager)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	server, err := Start(context.Background(), Config{BindAddress: "127.0.0.1:0"}, service)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	connection, err := grpc.NewClient(
		"passthrough:///"+server.Address(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
		_ = connection.Close()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
		manager.Close()
	})
	return &harness{
		node:    node,
		manager: manager,
		client:  controlv1.NewGatewayControlClient(connection),
		ctx:     ctx,
	}
}

func (h *harness) openAndSync(t *testing.T, gatewayID, instanceID string) (grpc.BidiStreamingClient[controlv1.ControlRequest, controlv1.ControlResponse], *controlv1.SessionRef) {
	t.Helper()
	stream, session := h.open(t, gatewayID, instanceID)
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: session},
	}}); err != nil {
		t.Fatalf("Send(snapshot): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(snapshot): %v", err)
	}
	accepted := response.GetSnapshotAccepted()
	if accepted == nil || accepted.GetPresence() != controlv1.PresenceState_PRESENCE_STATE_COMPLETE {
		t.Fatalf("snapshot response = %#v", response)
	}
	return stream, session
}

func (h *harness) open(t *testing.T, gatewayID, instanceID string) (grpc.BidiStreamingClient[controlv1.ControlRequest, controlv1.ControlResponse], *controlv1.SessionRef) {
	t.Helper()
	stream, opened := h.openSession(t, gatewayID, instanceID)
	return stream, opened.GetSession()
}

func (h *harness) openSession(t *testing.T, gatewayID, instanceID string) (grpc.BidiStreamingClient[controlv1.ControlRequest, controlv1.ControlResponse], *controlv1.SessionOpened) {
	t.Helper()
	stream, err := h.client.Connect(h.ctx)
	if err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{
		Hello: &controlv1.Hello{ClusterEpoch: testEpoch, GatewayId: gatewayID, GatewayInstanceId: instanceID},
	}}); err != nil {
		t.Fatalf("Send(hello): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(session): %v", err)
	}
	opened := response.GetSessionOpened()
	if opened == nil || opened.GetSession() == nil {
		t.Fatalf("session response = %#v", response)
	}
	return stream, opened
}

func installRequest(session *controlv1.SessionRef, key *controlv1.BindingKey, generation uint64, expected, next *controlv1.ListenerBindingRef) *controlv1.ControlRequest {
	return &controlv1.ControlRequest{Message: &controlv1.ControlRequest_BindingMutation{
		BindingMutation: &controlv1.BindingMutation{
			Session: session,
			Mutation: &controlv1.BindingMutation_Install{Install: &controlv1.InstallBinding{
				Key: key, ExpectedGeneration: generation, ExpectedRef: expected, NewRef: next,
			}},
		},
	}}
}

func installBinding(
	t *testing.T,
	stream grpc.BidiStreamingClient[controlv1.ControlRequest, controlv1.ControlResponse],
	session *controlv1.SessionRef,
	key *controlv1.BindingKey,
	ref *controlv1.ListenerBindingRef,
) *controlv1.BindingSlot {
	t.Helper()
	if err := stream.Send(installRequest(session, key, 0, nil, ref)); err != nil {
		t.Fatalf("Send(install): %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(install): %v", err)
	}
	result := response.GetMutationResult()
	if result.GetCode() != controlv1.MutationCode_MUTATION_CODE_APPLIED || result.GetSlot() == nil {
		t.Fatalf("install result = %#v", result)
	}
	return result.GetSlot()
}

type fakeNode struct {
	fsm *controlstate.FSM

	mu        sync.RWMutex
	verifyErr error
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	fsm := controlstate.NewFSM()
	command, err := controlstate.EncodeInitializeEpoch(controlstate.InitializeEpoch{
		ClusterEpoch:                   testEpoch,
		MaxDistinctBindingKeysPerEpoch: 100,
		MaxDistinctGatewayIDsPerEpoch:  10,
	})
	if err != nil {
		t.Fatalf("EncodeInitializeEpoch(): %v", err)
	}
	result := fsm.Apply(&raft.Log{Data: command}).(controlstate.ApplyResult)
	if !result.Applied() {
		t.Fatalf("initialize result = %#v", result)
	}
	return &fakeNode{fsm: fsm}
}

func (n *fakeNode) Status() raftnode.Status {
	return raftnode.Status{Role: "Leader", ClusterEpoch: testEpoch}
}

func (n *fakeNode) State() controlstate.State {
	return n.fsm.State()
}

func (n *fakeNode) VerifyLeader(context.Context) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.verifyErr
}

func (n *fakeNode) Apply(_ context.Context, command []byte) (controlstate.ApplyResult, error) {
	return n.fsm.Apply(&raft.Log{Data: command}).(controlstate.ApplyResult), nil
}

func (n *fakeNode) Lookup(key controlstate.BindingKey) controlstate.BindingSlot {
	return n.fsm.Lookup(key)
}

func (n *fakeNode) LookupGateway(gatewayID string) controlstate.GatewaySlot {
	return n.fsm.LookupGateway(gatewayID)
}

func (n *fakeNode) setVerifyError(err error) {
	n.mu.Lock()
	n.verifyErr = err
	n.mu.Unlock()
}
