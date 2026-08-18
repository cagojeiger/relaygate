package membership

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	operatorv1 "github.com/cagojeiger/relaygate/internal/gen/operator/v1"
)

func TestServiceRequiresLeaderAndMapsDeadline(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "not leader", err: fmt.Errorf("wrapped: %w", raft.ErrNotLeader), code: codes.FailedPrecondition},
		{name: "leadership lost", err: fmt.Errorf("wrapped: %w", raft.ErrLeadershipLost), code: codes.FailedPrecondition},
		{name: "deadline", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), code: codes.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := newFakeNode(nil)
			node.verifyErr = test.err
			service := newService(node)

			calls := []func() error{
				func() error {
					_, err := service.List(context.Background(), &operatorv1.ListRequest{})
					return err
				},
				func() error {
					_, err := service.Add(context.Background(), &operatorv1.AddRequest{})
					return err
				},
				func() error {
					_, err := service.Remove(context.Background(), &operatorv1.RemoveRequest{})
					return err
				},
			}
			for _, call := range calls {
				if got := status.Code(call()); got != test.code {
					t.Fatalf("status code = %v, want %v", got, test.code)
				}
			}
			if node.verifyCalls != len(calls) {
				t.Fatalf("VerifyLeader calls = %d, want %d", node.verifyCalls, len(calls))
			}
			if node.getCalls != 0 || node.addCalls != 0 || node.removeCalls != 0 {
				t.Fatalf("calls after failed leader verification = get:%d add:%d remove:%d", node.getCalls, node.addCalls, node.removeCalls)
			}
		})
	}
}

func TestServiceListReturnsSortedConfiguration(t *testing.T) {
	node := newFakeNode([]raft.Server{
		{ID: "node-c", Address: "127.0.0.1:3003", Suffrage: raft.Nonvoter},
		{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter},
		{ID: "node-b", Address: "127.0.0.1:3002", Suffrage: raft.Voter},
	})
	response, err := newService(node).List(context.Background(), &operatorv1.ListRequest{})
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	if response.GetChanged() {
		t.Fatal("List changed = true, want false")
	}
	got := protoMemberIDs(response)
	want := []string{"node-a", "node-b", "node-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List member IDs = %v, want %v", got, want)
	}
	if got := response.GetMembers()[2].GetSuffrage(); got != "Nonvoter" {
		t.Fatalf("node-c suffrage = %q, want Nonvoter", got)
	}
}

func TestServiceAddIsIdempotentAndRejectsConflictsAndCapacity(t *testing.T) {
	t.Run("exact voter", func(t *testing.T) {
		node := newFakeNode([]raft.Server{{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter}})
		response, err := newService(node).Add(context.Background(), &operatorv1.AddRequest{NodeId: "node-a", Address: "127.0.0.1:3001"})
		if err != nil {
			t.Fatalf("Add(): %v", err)
		}
		if response.GetChanged() || node.addCalls != 0 {
			t.Fatalf("exact Add = changed:%v calls:%d, want false and zero", response.GetChanged(), node.addCalls)
		}
	})

	tests := []struct {
		name    string
		servers []raft.Server
		request *operatorv1.AddRequest
		code    codes.Code
	}{
		{
			name:    "same ID different address",
			servers: []raft.Server{{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter}},
			request: &operatorv1.AddRequest{NodeId: "node-a", Address: "127.0.0.1:3999"},
			code:    codes.AlreadyExists,
		},
		{
			name:    "same address different ID",
			servers: []raft.Server{{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter}},
			request: &operatorv1.AddRequest{NodeId: "node-z", Address: "127.0.0.1:3001"},
			code:    codes.AlreadyExists,
		},
		{
			name:    "invalid",
			request: &operatorv1.AddRequest{NodeId: "node-z", Address: "not-an-address"},
			code:    codes.InvalidArgument,
		},
		{
			name:    "capacity",
			servers: voters(7),
			request: &operatorv1.AddRequest{NodeId: "node-8", Address: "127.0.0.1:3008"},
			code:    codes.ResourceExhausted,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := newFakeNode(test.servers)
			_, err := newService(node).Add(context.Background(), test.request)
			if got := status.Code(err); got != test.code {
				t.Fatalf("Add() code = %v, want %v (error %v)", got, test.code, err)
			}
			if node.addCalls != 0 {
				t.Fatalf("AddVoter calls = %d, want zero", node.addCalls)
			}
		})
	}
}

func TestServiceMutationsReturnSortedCurrentConfiguration(t *testing.T) {
	node := newFakeNode([]raft.Server{
		{ID: "node-b", Address: "127.0.0.1:3002", Suffrage: raft.Voter},
		{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter},
	})
	service := newService(node)

	added, err := service.Add(context.Background(), &operatorv1.AddRequest{NodeId: "node-c", Address: "127.0.0.1:3003"})
	if err != nil {
		t.Fatalf("Add(): %v", err)
	}
	if !added.GetChanged() {
		t.Fatal("Add changed = false, want true")
	}
	if got, want := protoMemberIDs(added), []string{"node-a", "node-b", "node-c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Add member IDs = %v, want %v", got, want)
	}

	removed, err := service.Remove(context.Background(), &operatorv1.RemoveRequest{NodeId: "node-b"})
	if err != nil {
		t.Fatalf("Remove(): %v", err)
	}
	if !removed.GetChanged() {
		t.Fatal("Remove changed = false, want true")
	}
	if got, want := protoMemberIDs(removed), []string{"node-a", "node-c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Remove member IDs = %v, want %v", got, want)
	}

	absent, err := service.Remove(context.Background(), &operatorv1.RemoveRequest{NodeId: "node-z"})
	if err != nil {
		t.Fatalf("Remove(absent): %v", err)
	}
	if absent.GetChanged() || node.removeCalls != 1 {
		t.Fatalf("absent Remove = changed:%v calls:%d, want false and one total mutation", absent.GetChanged(), node.removeCalls)
	}
}

func TestServiceRefusesToRemoveLastVoter(t *testing.T) {
	node := newFakeNode([]raft.Server{{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter}})
	_, err := newService(node).Remove(context.Background(), &operatorv1.RemoveRequest{NodeId: "node-a"})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("Remove(last voter) code = %v, want %v (error %v)", got, codes.FailedPrecondition, err)
	}
	if node.removeCalls != 0 {
		t.Fatalf("RemoveServer calls = %d, want zero", node.removeCalls)
	}
}

func TestServiceACKLossRetriesAreStateIdempotent(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		node := newFakeNode([]raft.Server{{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter}})
		node.addErrOnce = context.DeadlineExceeded
		service := newService(node)
		request := &operatorv1.AddRequest{NodeId: "node-b", Address: "127.0.0.1:3002"}

		if _, err := service.Add(context.Background(), request); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("first Add() error = %v, want DeadlineExceeded", err)
		}
		response, err := service.Add(context.Background(), request)
		if err != nil {
			t.Fatalf("retry Add(): %v", err)
		}
		if response.GetChanged() || node.addCalls != 1 {
			t.Fatalf("retry Add = changed:%v calls:%d, want false and one mutation", response.GetChanged(), node.addCalls)
		}
	})

	t.Run("remove", func(t *testing.T) {
		node := newFakeNode([]raft.Server{
			{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter},
			{ID: "node-b", Address: "127.0.0.1:3002", Suffrage: raft.Voter},
		})
		node.removeErrOnce = context.DeadlineExceeded
		service := newService(node)
		request := &operatorv1.RemoveRequest{NodeId: "node-b"}

		if _, err := service.Remove(context.Background(), request); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("first Remove() error = %v, want DeadlineExceeded", err)
		}
		response, err := service.Remove(context.Background(), request)
		if err != nil {
			t.Fatalf("retry Remove(): %v", err)
		}
		if response.GetChanged() || node.removeCalls != 1 {
			t.Fatalf("retry Remove = changed:%v calls:%d, want false and one mutation", response.GetChanged(), node.removeCalls)
		}
	})
}

func TestServiceSerializesConcurrentDuplicateAdd(t *testing.T) {
	node := newFakeNode([]raft.Server{{ID: "node-a", Address: "127.0.0.1:3001", Suffrage: raft.Voter}})
	service := newService(node)
	request := &operatorv1.AddRequest{NodeId: "node-b", Address: "127.0.0.1:3002"}

	responses := make(chan *operatorv1.MembershipResult, 2)
	errors := make(chan error, 2)
	var callers sync.WaitGroup
	callers.Add(2)
	for range 2 {
		go func() {
			defer callers.Done()
			response, err := service.Add(context.Background(), request)
			responses <- response
			errors <- err
		}()
	}
	callers.Wait()
	close(responses)
	close(errors)

	changed := 0
	for err := range errors {
		if err != nil {
			t.Fatalf("Add(): %v", err)
		}
	}
	for response := range responses {
		if response.GetChanged() {
			changed++
		}
	}
	if changed != 1 || node.addCalls != 1 {
		t.Fatalf("duplicate concurrent Add = changed:%d calls:%d, want one and one", changed, node.addCalls)
	}
}

func protoMemberIDs(response *operatorv1.MembershipResult) []string {
	ids := make([]string, 0, len(response.GetMembers()))
	for _, member := range response.GetMembers() {
		ids = append(ids, member.GetNodeId())
	}
	return ids
}

func voters(count int) []raft.Server {
	servers := make([]raft.Server, 0, count)
	for index := 1; index <= count; index++ {
		servers = append(servers, raft.Server{
			ID:       raft.ServerID(fmt.Sprintf("node-%d", index)),
			Address:  raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", 3000+index)),
			Suffrage: raft.Voter,
		})
	}
	return servers
}

type fakeNode struct {
	mu sync.Mutex

	configuration raft.Configuration
	verifyErr     error
	getErr        error
	addErrOnce    error
	removeErrOnce error

	verifyCalls int
	getCalls    int
	addCalls    int
	removeCalls int
}

func newFakeNode(servers []raft.Server) *fakeNode {
	return &fakeNode{configuration: raft.Configuration{Servers: append([]raft.Server(nil), servers...)}}
}

func (n *fakeNode) VerifyLeader(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.verifyCalls++
	return n.verifyErr
}

func (n *fakeNode) GetConfiguration(context.Context) (raft.Configuration, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.getCalls++
	return raft.Configuration{Servers: append([]raft.Server(nil), n.configuration.Servers...)}, n.getErr
}

func (n *fakeNode) AddVoter(_ context.Context, nodeID, address string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.addCalls++
	for index, member := range n.configuration.Servers {
		if member.ID == raft.ServerID(nodeID) {
			n.configuration.Servers[index].Address = raft.ServerAddress(address)
			n.configuration.Servers[index].Suffrage = raft.Voter
			return n.takeAddError()
		}
	}
	n.configuration.Servers = append(n.configuration.Servers, raft.Server{
		ID:       raft.ServerID(nodeID),
		Address:  raft.ServerAddress(address),
		Suffrage: raft.Voter,
	})
	return n.takeAddError()
}

func (n *fakeNode) RemoveServer(_ context.Context, nodeID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.removeCalls++
	for index, member := range n.configuration.Servers {
		if member.ID == raft.ServerID(nodeID) {
			n.configuration.Servers = append(n.configuration.Servers[:index], n.configuration.Servers[index+1:]...)
			break
		}
	}
	err := n.removeErrOnce
	n.removeErrOnce = nil
	return err
}

func (n *fakeNode) takeAddError() error {
	err := n.addErrOnce
	n.addErrOnce = nil
	return err
}

var _ Node = (*fakeNode)(nil)
