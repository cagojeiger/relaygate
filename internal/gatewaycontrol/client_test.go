package gatewaycontrol

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/controlstate"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testRelayAddress = "relay-gateway-1.internal:7300"

func TestInstallFailsFastBeforeControlIsRevalidated(t *testing.T) {
	client := newTestClient(t, "127.0.0.1:1")
	_, err := client.Install(context.Background(), testBindingKey("one"), testBindingRef("one"))
	if !errors.Is(err, ErrControlUnavailable) {
		t.Fatalf("Install() error = %v, want %v", err, ErrControlUnavailable)
	}
}

func TestClientSerializesMutationRoundTrips(t *testing.T) {
	firstSeen := make(chan *controlv1.InstallBinding, 1)
	secondSeen := make(chan *controlv1.InstallBinding, 1)
	removeSeen := make(chan *controlv1.RemoveBinding, 1)
	releaseFirst := make(chan struct{})
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		_, _, err := openTestSession(stream, 1, nil)
		if err != nil {
			return err
		}
		first, err := stream.Recv()
		if err != nil {
			return err
		}
		firstSeen <- first.GetBindingMutation().GetInstall()
		<-releaseFirst
		if err := sendInstallResult(stream, first.GetBindingMutation().GetInstall(), controlv1.MutationCode_MUTATION_CODE_APPLIED, 1); err != nil {
			return err
		}
		second, err := stream.Recv()
		if err != nil {
			return err
		}
		secondSeen <- second.GetBindingMutation().GetInstall()
		if err := sendInstallResult(stream, second.GetBindingMutation().GetInstall(), controlv1.MutationCode_MUTATION_CODE_APPLIED, 1); err != nil {
			return err
		}
		remove, err := stream.Recv()
		if err != nil {
			return err
		}
		removeMutation := remove.GetBindingMutation().GetRemove()
		removeSeen <- removeMutation
		return stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: &controlv1.MutationResult{
			Code: controlv1.MutationCode_MUTATION_CODE_APPLIED,
			Slot: &controlv1.BindingSlot{Key: removeMutation.GetKey(), Generation: removeMutation.GetExpectedGeneration() + 1},
		}}})
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	firstResult := make(chan mutationOutcome, 1)
	go func() {
		slot, err := client.Install(context.Background(), testBindingKey("one"), testBindingRef("one"))
		firstResult <- mutationOutcome{slot: slot, err: err}
	}()
	first := receiveTestValue(t, firstSeen)
	if first.GetExpectedGeneration() != 0 || first.GetExpectedRef() != nil {
		t.Fatalf("first install expected slot = (%d, %v), want implicit tombstone", first.GetExpectedGeneration(), first.GetExpectedRef())
	}

	secondResult := make(chan mutationOutcome, 1)
	go func() {
		slot, err := client.Install(context.Background(), testBindingKey("two"), testBindingRef("two"))
		secondResult <- mutationOutcome{slot: slot, err: err}
	}()
	close(releaseFirst)
	firstOutcome := receiveTestValue(t, firstResult)
	second := receiveTestValue(t, secondSeen)
	secondOutcome := receiveTestValue(t, secondResult)
	if firstOutcome.err != nil || secondOutcome.err != nil {
		t.Fatalf("Install() errors = (%v, %v)", firstOutcome.err, secondOutcome.err)
	}
	if second.GetExpectedGeneration() != 0 || second.GetExpectedRef() != nil {
		t.Fatalf("second install expected slot = (%d, %v), want implicit tombstone", second.GetExpectedGeneration(), second.GetExpectedRef())
	}

	removeResult := make(chan error, 1)
	go func() { removeResult <- client.Remove(context.Background(), firstOutcome.slot) }()
	remove := receiveTestValue(t, removeSeen)
	if remove.GetExpectedGeneration() != firstOutcome.slot.Generation || remove.GetExpectedRef().GetListenerBindingId() != "listener-one" {
		t.Fatalf("remove mutation = %#v, want exact committed first slot", remove)
	}
	if err := receiveTestValue(t, removeResult); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
}

func TestInstallReplaysExactCASAfterDisconnectBeforeApply(t *testing.T) {
	var connections atomic.Int32
	firstMutation := make(chan *controlv1.InstallBinding, 1)
	replayedMutation := make(chan *controlv1.InstallBinding, 1)
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		connection := int(connections.Add(1))
		_, _, err := openTestSession(stream, connection, nil)
		if err != nil {
			return err
		}
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		install := request.GetBindingMutation().GetInstall()
		if connection == 1 {
			firstMutation <- install
			return status.Error(codes.Unavailable, "lost before apply")
		}
		replayedMutation <- install
		return sendInstallResult(stream, install, controlv1.MutationCode_MUTATION_CODE_APPLIED, 1)
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	completed := make(chan mutationOutcome, 1)
	go func() {
		slot, err := client.Install(context.Background(), testBindingKey("replay"), testBindingRef("replay"))
		completed <- mutationOutcome{slot: slot, err: err}
	}()
	first := receiveTestValue(t, firstMutation)
	replayed := receiveTestValue(t, replayedMutation)
	if !installMutationsEqual(first, replayed) {
		t.Fatalf("replayed CAS = %#v, want exact %#v", replayed, first)
	}
	outcome := receiveTestValue(t, completed)
	if outcome.err != nil || outcome.slot.Generation != 1 {
		t.Fatalf("Install() = (%#v, %v), want generation 1", outcome.slot, outcome.err)
	}
}

func TestInstallReconcilesCommitAfterResponseLoss(t *testing.T) {
	var connections atomic.Int32
	committed := make(chan *controlv1.BindingSlot, 1)
	secondSnapshot := make(chan *controlv1.FullSnapshot, 1)
	var durableMu sync.Mutex
	var durable *controlv1.BindingSlot
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		connection := int(connections.Add(1))
		durableMu.Lock()
		owned := []*controlv1.BindingSlot(nil)
		if durable != nil {
			owned = []*controlv1.BindingSlot{durable}
		}
		durableMu.Unlock()
		_, snapshot, err := openTestSession(stream, connection, owned)
		if err != nil {
			return err
		}
		if connection == 2 {
			secondSnapshot <- snapshot
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		install := request.GetBindingMutation().GetInstall()
		slot := &controlv1.BindingSlot{Key: install.GetKey(), Generation: install.GetExpectedGeneration() + 1, Ref: install.GetNewRef()}
		durableMu.Lock()
		durable = slot
		durableMu.Unlock()
		committed <- slot
		return status.Error(codes.Unavailable, "response lost after commit")
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	completed := make(chan mutationOutcome, 1)
	go func() {
		slot, err := client.Install(context.Background(), testBindingKey("committed"), testBindingRef("committed"))
		completed <- mutationOutcome{slot: slot, err: err}
	}()
	want := receiveTestValue(t, committed)
	snapshot := receiveTestValue(t, secondSnapshot)
	if len(snapshot.GetBindings()) != 1 || !wireSlotsEqual(snapshot.GetBindings()[0], want) {
		t.Fatalf("reconnect snapshot = %#v, want committed slot %#v", snapshot.GetBindings(), want)
	}
	outcome := receiveTestValue(t, completed)
	if outcome.err != nil || outcome.slot.Generation != want.GetGeneration() || outcome.slot.Ref.ListenerBindingID != want.GetRef().GetListenerBindingId() {
		t.Fatalf("Install() = (%#v, %v), want reconciled committed slot", outcome.slot, outcome.err)
	}
	client.mu.Lock()
	active := client.active
	client.mu.Unlock()
	if active != nil {
		t.Fatal("committed install remained active after authoritative reconciliation")
	}
}

func TestClientReconstructsExactCurrentInstanceSnapshot(t *testing.T) {
	keyA := testBindingKey("a")
	keyB := testBindingKey("b")
	owned := []*controlv1.BindingSlot{
		wireLiveSlot(keyB, 4, "instance-1", "listener-b"),
		wireLiveSlot(keyA, 3, "instance-1", "listener-a"),
	}
	snapshots := make(chan *controlv1.FullSnapshot, 1)
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		_, snapshot, err := openTestSession(stream, 1, owned)
		if err != nil {
			return err
		}
		snapshots <- snapshot
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	_, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)

	snapshot := receiveTestValue(t, snapshots)
	if len(snapshot.GetBindings()) != 2 {
		t.Fatalf("snapshot binding count = %d, want 2", len(snapshot.GetBindings()))
	}
	if snapshot.GetBindings()[0].GetKey().GetTargetId() != "target-a" || snapshot.GetBindings()[1].GetKey().GetTargetId() != "target-b" {
		t.Fatalf("snapshot order/contents = %#v, want current instance keys a,b", snapshot.GetBindings())
	}
}

func TestClientRejectsAuthoritativeBindingFromAnotherInstance(t *testing.T) {
	client := newTestClient(t, "127.0.0.1:1")
	if _, _, err := client.canonicalOwnedBindings([]*controlv1.BindingSlot{
		wireLiveSlot(testBindingKey("old"), 2, "instance-old", "listener-old"),
	}); err == nil {
		t.Fatal("canonicalOwnedBindings() accepted a prior-instance slot")
	}
}

func TestInstallRetriesOnceFromRejectedTombstone(t *testing.T) {
	mutations := make(chan *controlv1.InstallBinding, 2)
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		_, _, err := openTestSession(stream, 1, nil)
		if err != nil {
			return err
		}
		first, err := stream.Recv()
		if err != nil {
			return err
		}
		mutations <- first.GetBindingMutation().GetInstall()
		if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: &controlv1.MutationResult{
			Code: controlv1.MutationCode_MUTATION_CODE_REJECTED,
			Slot: &controlv1.BindingSlot{Key: first.GetBindingMutation().GetInstall().GetKey(), Generation: 4},
		}}}); err != nil {
			return err
		}
		second, err := stream.Recv()
		if err != nil {
			return err
		}
		install := second.GetBindingMutation().GetInstall()
		mutations <- install
		return sendInstallResult(stream, install, controlv1.MutationCode_MUTATION_CODE_APPLIED, 5)
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	completed := make(chan mutationOutcome, 1)
	go func() {
		slot, err := client.Install(context.Background(), testBindingKey("retry"), testBindingRef("retry"))
		completed <- mutationOutcome{slot: slot, err: err}
	}()
	first := receiveTestValue(t, mutations)
	second := receiveTestValue(t, mutations)
	if first.GetExpectedGeneration() != 0 || second.GetExpectedGeneration() != 4 || second.GetExpectedRef() != nil {
		t.Fatalf("install attempts expected generations = (%d, %d), want (0, 4) tombstones", first.GetExpectedGeneration(), second.GetExpectedGeneration())
	}
	if first.GetNewRef().GetListenerBindingId() != second.GetNewRef().GetListenerBindingId() {
		t.Fatal("tombstone retry changed the target binding ref")
	}
	outcome := receiveTestValue(t, completed)
	if outcome.err != nil || outcome.slot.Generation != 5 {
		t.Fatalf("Install() = (%#v, %v), want generation 5", outcome.slot, outcome.err)
	}
}

func TestInstallDoesNotOverwriteUnknownLiveOwner(t *testing.T) {
	requests := make(chan *controlv1.InstallBinding, 1)
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		_, _, err := openTestSession(stream, 1, nil)
		if err != nil {
			return err
		}
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		install := request.GetBindingMutation().GetInstall()
		requests <- install
		if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: &controlv1.MutationResult{
			Code:  controlv1.MutationCode_MUTATION_CODE_REJECTED,
			Slot:  wireLiveSlot(testBindingKey("foreign"), 8, "foreign-instance", "foreign-listener"),
			Error: "current binding has another owner",
		}}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	_, err := client.Install(context.Background(), testBindingKey("foreign"), testBindingRef("foreign"))
	_ = receiveTestValue(t, requests)
	if !errors.Is(err, ErrBindingConflict) {
		t.Fatalf("Install() error = %v, want %v", err, ErrBindingConflict)
	}
	if !errors.Is(err, controlstate.ErrCASMismatch) {
		t.Fatalf("Install() error = %v, want domain CAS mismatch", err)
	}
	client.mu.Lock()
	active, queued := client.active, len(client.queue)
	client.mu.Unlock()
	if active != nil || queued != 0 {
		t.Fatalf("foreign conflict remained pending: active=%v queued=%d", active != nil, queued)
	}
}

func TestInstallRetriesPriorInstanceOfSameStableGateway(t *testing.T) {
	mutations := make(chan *controlv1.InstallBinding, 2)
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		_, _, err := openTestSession(stream, 1, nil)
		if err != nil {
			return err
		}
		first, err := stream.Recv()
		if err != nil {
			return err
		}
		install := first.GetBindingMutation().GetInstall()
		mutations <- install
		if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: &controlv1.MutationResult{
			Code:             controlv1.MutationCode_MUTATION_CODE_REJECTED,
			Slot:             wireLiveSlot(testBindingKey("prior"), 7, "instance-old", "listener-old"),
			SameGatewayOwner: true,
		}}}); err != nil {
			return err
		}
		second, err := stream.Recv()
		if err != nil {
			return err
		}
		install = second.GetBindingMutation().GetInstall()
		mutations <- install
		return sendInstallResult(stream, install, controlv1.MutationCode_MUTATION_CODE_APPLIED, 8)
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	slot, err := client.Install(context.Background(), testBindingKey("prior"), testBindingRef("prior"))
	first := receiveTestValue(t, mutations)
	second := receiveTestValue(t, mutations)
	if err != nil || slot.Generation != 8 {
		t.Fatalf("Install() = (%#v, %v), want generation 8", slot, err)
	}
	if first.GetExpectedGeneration() != 0 || second.GetExpectedGeneration() != 7 ||
		second.GetExpectedRef().GetGatewayInstanceId() != "instance-old" ||
		second.GetExpectedRef().GetListenerBindingId() != "listener-old" {
		t.Fatalf("prior-instance retry = first %#v second %#v", first, second)
	}
}

func TestInstallMapsGatewayCapacity(t *testing.T) {
	address := startControlServer(t, controlServerFunc{connect: func(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
		_, _, err := openTestSession(stream, 1, nil)
		if err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: &controlv1.MutationResult{
			Code: controlv1.MutationCode_MUTATION_CODE_CAPACITY_REACHED,
		}}})
	}})
	client, cancel, done := startTestClient(t, address)
	defer stopTestClient(cancel, done)
	_ = waitForClientState(t, client, StateRevalidated)

	_, err := client.Install(context.Background(), testBindingKey("capacity"), testBindingRef("capacity"))
	if !errors.Is(err, ErrBindingCapacity) || !errors.Is(err, controlstate.ErrBindingCapacity) {
		t.Fatalf("Install() error = %v, want gateway and domain capacity sentinels", err)
	}
}

func TestClientRotatesEndpointsAndRevalidates(t *testing.T) {
	rejected := make(chan *controlv1.Hello, 1)
	rejectAddress := startControlServer(t, rejectingControlServer{hello: rejected})
	accepted := make(chan acceptedSession, 1)
	acceptAddress := startControlServer(t, &acceptingControlServer{accepted: accepted})

	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		RelayAddress:     testRelayAddress,
		ControlEndpoints: []string{rejectAddress, acceptAddress},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	rejectedHello := receiveTestValue(t, rejected)
	acceptedSession := receiveTestValue(t, accepted)
	if rejectedHello.GetGatewayInstanceId() != "instance-1" || acceptedSession.hello.GetGatewayInstanceId() != "instance-1" {
		t.Fatalf("instance IDs changed across endpoints: rejected=%q accepted=%q", rejectedHello.GetGatewayInstanceId(), acceptedSession.hello.GetGatewayInstanceId())
	}
	if rejectedHello.GetRelayAddress() != testRelayAddress || acceptedSession.hello.GetRelayAddress() != testRelayAddress {
		t.Fatalf("relay addresses = rejected %q accepted %q, want %q", rejectedHello.GetRelayAddress(), acceptedSession.hello.GetRelayAddress(), testRelayAddress)
	}
	status := waitForClientState(t, client, StateRevalidated)
	if status.Endpoint != acceptAddress || status.GatewayGeneration != 1 || !status.Ready() {
		t.Fatalf("client status = %#v", status)
	}
}

func TestClientReconnectKeepsProcessInstanceAndUsesNewSession(t *testing.T) {
	accepted := make(chan acceptedSession, 2)
	address := startControlServer(t, &acceptingControlServer{accepted: accepted, closeFirst: true})
	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		RelayAddress:     testRelayAddress,
		ControlEndpoints: []string{address},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	first := receiveTestValue(t, accepted)
	second := receiveTestValue(t, accepted)
	if first.hello.GetGatewayInstanceId() != second.hello.GetGatewayInstanceId() {
		t.Fatalf("gateway instance changed across reconnect: first=%q second=%q", first.hello.GetGatewayInstanceId(), second.hello.GetGatewayInstanceId())
	}
	if first.session.GetControlSessionId() == second.session.GetControlSessionId() {
		t.Fatalf("control session was reused: %q", first.session.GetControlSessionId())
	}
	status := waitForClientState(t, client, StateRevalidated)
	if status.ControlSessionID != second.session.GetControlSessionId() {
		t.Fatalf("client status = %#v, want session %q", status, second.session.GetControlSessionId())
	}
}

func TestClientLeavesRevalidatedStateWhenControlTransportStalls(t *testing.T) {
	accepted := make(chan acceptedSession, 1)
	serverAddress := startControlServer(t, &acceptingControlServer{accepted: accepted})
	proxy := startBlackholeProxy(t, serverAddress)
	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		RelayAddress:     testRelayAddress,
		ControlEndpoints: []string{proxy.Address()},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	client.keepalive.Timeout = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	_ = receiveTestValue(t, accepted)
	_ = waitForClientState(t, client, StateRevalidated)
	proxy.Blackhole()
	waitForClientToLeaveState(t, client, StateRevalidated, controlKeepaliveTime+5*time.Second)
}

func TestNewClientRejectsIncompleteConfig(t *testing.T) {
	_, err := newClient(Config{}, nil, "instance-1")
	if err == nil {
		t.Fatal("newClient() succeeded with empty config")
	}
	_, err = newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		RelayAddress:     testRelayAddress,
		ControlEndpoints: []string{"127.0.0.1:7100"},
		ConnectTimeout:   time.Second,
		RetryInterval:    time.Second,
	}, nil, "")
	if err == nil {
		t.Fatal("newClient() succeeded with empty instance ID")
	}
	_, err = newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		RelayAddress:     "0.0.0.0:7300",
		ControlEndpoints: []string{"127.0.0.1:7100"},
		ConnectTimeout:   time.Second,
		RetryInterval:    time.Second,
	}, nil, "instance-1")
	if err == nil {
		t.Fatal("newClient() accepted an unspecified relay address")
	}
}

type controlServerFunc struct {
	controlv1.UnimplementedGatewayControlServer
	connect func(grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error
}

func (s controlServerFunc) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	return s.connect(stream)
}

func newTestClient(t *testing.T, address string) *Client {
	t.Helper()
	client, err := newClient(Config{
		ClusterEpoch:     "epoch-1",
		GatewayID:        "gateway-1",
		RelayAddress:     testRelayAddress,
		ControlEndpoints: []string{address},
		ConnectTimeout:   time.Second,
		RetryInterval:    10 * time.Millisecond,
	}, nil, "instance-1")
	if err != nil {
		t.Fatalf("newClient(): %v", err)
	}
	return client
}

func startTestClient(t *testing.T, address string) (*Client, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	client := newTestClient(t, address)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	return client, cancel, done
}

func stopTestClient(cancel context.CancelFunc, done <-chan struct{}) {
	cancel()
	<-done
}

func openTestSession(
	stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse],
	sequence int,
	owned []*controlv1.BindingSlot,
) (*controlv1.SessionRef, *controlv1.FullSnapshot, error) {
	helloRequest, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	hello := helloRequest.GetHello()
	if hello == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "hello required")
	}
	session := &controlv1.SessionRef{
		ClusterEpoch:      hello.GetClusterEpoch(),
		AuthorityId:       fmt.Sprintf("authority-%d", sequence),
		ControlSessionId:  fmt.Sprintf("session-%d", sequence),
		GatewayId:         hello.GetGatewayId(),
		GatewayInstanceId: hello.GetGatewayInstanceId(),
	}
	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SessionOpened{
		SessionOpened: &controlv1.SessionOpened{Session: session, GatewayGeneration: uint64(sequence), OwnedBindings: owned},
	}}); err != nil {
		return nil, nil, err
	}
	snapshotRequest, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	snapshot := snapshotRequest.GetFullSnapshot()
	if snapshot == nil || snapshot.GetSession().GetControlSessionId() != session.GetControlSessionId() {
		return nil, nil, status.Error(codes.InvalidArgument, "exact session snapshot required")
	}
	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SnapshotAccepted{
		SnapshotAccepted: &controlv1.SnapshotAccepted{Presence: controlv1.PresenceState_PRESENCE_STATE_COMPLETE},
	}}); err != nil {
		return nil, nil, err
	}
	return session, snapshot, nil
}

func sendInstallResult(
	stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse],
	install *controlv1.InstallBinding,
	code controlv1.MutationCode,
	generation uint64,
) error {
	return stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: &controlv1.MutationResult{
		Code: code,
		Slot: &controlv1.BindingSlot{Key: install.GetKey(), Generation: generation, Ref: install.GetNewRef()},
	}}})
}

func testBindingKey(suffix string) controlstate.BindingKey {
	return controlstate.BindingKey{ClientID: "client-1", EndpointPattern: "/" + suffix, TargetID: "target-" + suffix}
}

func testBindingRef(suffix string) controlstate.ListenerBindingRef {
	return controlstate.ListenerBindingRef{GatewayID: "gateway-1", GatewayInstanceID: "instance-1", ListenerBindingID: "listener-" + suffix}
}

func wireLiveSlot(key controlstate.BindingKey, generation uint64, instanceID, bindingID string) *controlv1.BindingSlot {
	return &controlv1.BindingSlot{
		Key:        bindingKeyToProto(key),
		Generation: generation,
		Ref:        &controlv1.ListenerBindingRef{GatewayInstanceId: instanceID, ListenerBindingId: bindingID},
	}
}

func installMutationsEqual(left, right *controlv1.InstallBinding) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return wireKeysEqual(left.GetKey(), right.GetKey()) &&
		left.GetExpectedGeneration() == right.GetExpectedGeneration() &&
		wireRefsEqual(left.GetExpectedRef(), right.GetExpectedRef()) &&
		wireRefsEqual(left.GetNewRef(), right.GetNewRef())
}

func wireSlotsEqual(left, right *controlv1.BindingSlot) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return wireKeysEqual(left.GetKey(), right.GetKey()) && left.GetGeneration() == right.GetGeneration() && wireRefsEqual(left.GetRef(), right.GetRef())
}

func wireKeysEqual(left, right *controlv1.BindingKey) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.GetClientId() == right.GetClientId() && left.GetEndpointPattern() == right.GetEndpointPattern() && left.GetTargetId() == right.GetTargetId()
}

func wireRefsEqual(left, right *controlv1.ListenerBindingRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.GetGatewayInstanceId() == right.GetGatewayInstanceId() && left.GetListenerBindingId() == right.GetListenerBindingId()
}

type rejectingControlServer struct {
	controlv1.UnimplementedGatewayControlServer
	hello chan<- *controlv1.Hello
}

func (s rejectingControlServer) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetHello() == nil {
		return status.Error(codes.InvalidArgument, "hello required")
	}
	s.hello <- request.GetHello()
	return status.Error(codes.Unavailable, "not authority")
}

type acceptedSession struct {
	hello   *controlv1.Hello
	session *controlv1.SessionRef
}

type acceptingControlServer struct {
	controlv1.UnimplementedGatewayControlServer

	mu         sync.Mutex
	count      int
	accepted   chan<- acceptedSession
	closeFirst bool
}

func (s *acceptingControlServer) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	helloRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := helloRequest.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "hello required")
	}
	s.mu.Lock()
	s.count++
	count := s.count
	s.mu.Unlock()
	session := &controlv1.SessionRef{
		ClusterEpoch:      hello.GetClusterEpoch(),
		AuthorityId:       fmt.Sprintf("authority-%d", count),
		ControlSessionId:  fmt.Sprintf("session-%d", count),
		GatewayId:         hello.GetGatewayId(),
		GatewayInstanceId: hello.GetGatewayInstanceId(),
	}
	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SessionOpened{
		SessionOpened: &controlv1.SessionOpened{Session: session, GatewayGeneration: uint64(count)},
	}}); err != nil {
		return err
	}
	snapshotRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	snapshot := snapshotRequest.GetFullSnapshot()
	if snapshot == nil || snapshot.GetSession().GetControlSessionId() != session.GetControlSessionId() || len(snapshot.GetBindings()) != 0 {
		return status.Error(codes.InvalidArgument, "exact empty snapshot required")
	}
	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SnapshotAccepted{
		SnapshotAccepted: &controlv1.SnapshotAccepted{Presence: controlv1.PresenceState_PRESENCE_STATE_COMPLETE},
	}}); err != nil {
		return err
	}
	s.accepted <- acceptedSession{hello: hello, session: session}
	if s.closeFirst && count == 1 {
		return status.Error(codes.Unavailable, "authority changed")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startControlServer(t *testing.T, service controlv1.GatewayControlServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(): %v", err)
	}
	server := grpc.NewServer()
	controlv1.RegisterGatewayControlServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func receiveTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control session")
		var zero T
		return zero
	}
}

func waitForClientState(t *testing.T, client *Client, state State) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := client.Status()
		if status.State == state {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("client state = %q, want %q", client.Status().State, state)
	return Status{}
}

func waitForClientToLeaveState(t *testing.T, client *Client, state State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if client.Status().State != state {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("client state remained %q after %s", state, timeout)
}

type blackholeProxy struct {
	listener net.Listener
	target   string
	drop     atomic.Bool
	closed   atomic.Bool

	mu          sync.Mutex
	connections []net.Conn
}

func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for blackhole proxy: %v", err)
	}
	proxy := &blackholeProxy{listener: listener, target: target}
	go proxy.accept()
	t.Cleanup(proxy.Close)
	return proxy
}

func (p *blackholeProxy) Address() string {
	return p.listener.Addr().String()
}

func (p *blackholeProxy) Blackhole() {
	p.drop.Store(true)
}

func (p *blackholeProxy) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range p.connections {
		_ = connection.Close()
	}
}

func (p *blackholeProxy) accept() {
	for {
		downstream, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = downstream.Close()
			continue
		}
		if p.closed.Load() {
			_ = downstream.Close()
			_ = upstream.Close()
			return
		}
		p.mu.Lock()
		p.connections = append(p.connections, downstream, upstream)
		p.mu.Unlock()
		go p.forward(upstream, downstream)
		go p.forward(downstream, upstream)
	}
}

func (p *blackholeProxy) forward(destination, source net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		read, err := source.Read(buffer)
		if read > 0 && !p.drop.Load() {
			if _, writeErr := destination.Write(buffer[:read]); writeErr != nil {
				return
			}
		}
		if err != nil {
			if !p.drop.Load() {
				_ = destination.Close()
			}
			return
		}
	}
}
