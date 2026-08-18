package membership

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	operatorv1 "github.com/cagojeiger/relaygate/internal/gen/operator/v1"
)

const maxVoters = 7

// Node is the Raft membership surface required by the local operator service.
type Node interface {
	VerifyLeader(context.Context) error
	GetConfiguration(context.Context) (raft.Configuration, error)
	AddVoter(context.Context, string, string) error
	RemoveServer(context.Context, string) error
}

type service struct {
	operatorv1.UnimplementedMembershipServer

	node       Node
	mutationMu sync.Mutex
}

func newService(node Node) *service {
	return &service{node: node}
}

func (s *service) List(ctx context.Context, _ *operatorv1.ListRequest) (*operatorv1.MembershipResult, error) {
	if err := s.verifyLeader(ctx, "list membership"); err != nil {
		return nil, err
	}
	configuration, err := s.node.GetConfiguration(ctx)
	if err != nil {
		return nil, mapNodeError("list membership", err)
	}
	return resultToProto(configuration, false), nil
}

func (s *service) Add(ctx context.Context, request *operatorv1.AddRequest) (*operatorv1.MembershipResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.verifyLeader(ctx, "add voter"); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "add request is required")
	}
	if err := validateAdd(request.GetNodeId(), request.GetAddress()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	configuration, err := s.node.GetConfiguration(ctx)
	if err != nil {
		return nil, mapNodeError("inspect membership before add", err)
	}
	exactVoter := false
	for _, member := range configuration.Servers {
		sameID := member.ID == raft.ServerID(request.GetNodeId())
		sameAddress := member.Address == raft.ServerAddress(request.GetAddress())
		switch {
		case sameID && sameAddress && member.Suffrage == raft.Voter:
			exactVoter = true
		case sameID && !sameAddress:
			return nil, status.Errorf(codes.AlreadyExists, "node ID %q already belongs to address %q", request.GetNodeId(), member.Address)
		case !sameID && sameAddress:
			return nil, status.Errorf(codes.AlreadyExists, "address %q already belongs to node ID %q", request.GetAddress(), member.ID)
		}
	}
	if exactVoter {
		return resultToProto(configuration, false), nil
	}
	if countVoters(configuration) >= maxVoters {
		return nil, status.Errorf(codes.ResourceExhausted, "raft voter limit is %d", maxVoters)
	}

	if err := s.node.AddVoter(ctx, request.GetNodeId(), request.GetAddress()); err != nil {
		return nil, mapNodeError("add voter", err)
	}
	configuration, err = s.node.GetConfiguration(ctx)
	if err != nil {
		return nil, mapNodeError("read membership after add", err)
	}
	return resultToProto(configuration, true), nil
}

func (s *service) Remove(ctx context.Context, request *operatorv1.RemoveRequest) (*operatorv1.MembershipResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.verifyLeader(ctx, "remove member"); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "remove request is required")
	}
	if strings.TrimSpace(request.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}

	configuration, err := s.node.GetConfiguration(ctx)
	if err != nil {
		return nil, mapNodeError("inspect membership before remove", err)
	}
	present := false
	removesLastVoter := false
	for _, member := range configuration.Servers {
		if member.ID == raft.ServerID(request.GetNodeId()) {
			present = true
			removesLastVoter = member.Suffrage == raft.Voter && countVoters(configuration) == 1
			break
		}
	}
	if !present {
		return resultToProto(configuration, false), nil
	}
	if removesLastVoter {
		return nil, status.Error(codes.FailedPrecondition, "cannot remove the last Raft voter")
	}

	if err := s.node.RemoveServer(ctx, request.GetNodeId()); err != nil {
		return nil, mapNodeError("remove member", err)
	}
	configuration, err = s.node.GetConfiguration(ctx)
	if err != nil {
		return nil, mapNodeError("read membership after remove", err)
	}
	return resultToProto(configuration, true), nil
}

func (s *service) verifyLeader(ctx context.Context, operation string) error {
	if err := s.node.VerifyLeader(ctx); err != nil {
		return mapNodeError(operation, err)
	}
	return nil
}

func validateAdd(nodeID, address string) error {
	if strings.TrimSpace(nodeID) == "" {
		return errors.New("node ID is required")
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("invalid voter address %q: %w", address, err)
	}
	return nil
}

func mapNodeError(operation string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, raft.ErrEnqueueTimeout):
		return status.Errorf(codes.DeadlineExceeded, "%s: %v", operation, err)
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s: %v", operation, err)
	case errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return status.Errorf(codes.FailedPrecondition, "%s requires the current Raft leader: %v", operation, err)
	case errors.Is(err, raft.ErrRaftShutdown):
		return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", operation, err)
	}
}

func countVoters(configuration raft.Configuration) int {
	count := 0
	for _, member := range configuration.Servers {
		if member.Suffrage == raft.Voter {
			count++
		}
	}
	return count
}

func resultToProto(configuration raft.Configuration, changed bool) *operatorv1.MembershipResult {
	members := make([]*operatorv1.Member, 0, len(configuration.Servers))
	for _, member := range configuration.Servers {
		members = append(members, &operatorv1.Member{
			NodeId:   string(member.ID),
			Address:  string(member.Address),
			Suffrage: member.Suffrage.String(),
		})
	}
	sort.Slice(members, func(left, right int) bool {
		if members[left].GetNodeId() != members[right].GetNodeId() {
			return members[left].GetNodeId() < members[right].GetNodeId()
		}
		if members[left].GetAddress() != members[right].GetAddress() {
			return members[left].GetAddress() < members[right].GetAddress()
		}
		return members[left].GetSuffrage() < members[right].GetSuffrage()
	})
	return &operatorv1.MembershipResult{Changed: changed, Members: members}
}
