package controlgrpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service owns only the ordered control-stream protocol. Authority translates
// accepted snapshots and mutations into the replicated current-state FSM while
// keeping control streams and relay addresses leader-local.
type Service struct {
	controlv1.UnimplementedGatewayControlServer

	clusterEpoch string
	authority    *authority.Manager
}

func NewService(clusterEpoch string, manager *authority.Manager) (*Service, error) {
	if err := routing.ValidateIdentity("cluster_epoch", clusterEpoch); err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, fmt.Errorf("authority manager is required")
	}
	return &Service{clusterEpoch: clusterEpoch, authority: manager}, nil
}

func (s *Service) Connect(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "hello is required")
		}
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "hello must be the first control message")
	}
	if err := s.validateHello(hello); err != nil {
		return err
	}
	session, err := s.openControlSession(stream.Context(), hello)
	if err != nil {
		return err
	}
	defer s.authority.EndSession(session.Ref)

	if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SessionOpened{
		SessionOpened: &controlv1.SessionOpened{Session: sessionRefToProto(session.Ref)},
	}}); err != nil {
		return err
	}

	received := receiveRequests(stream)
	snapshotAccepted := false
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-session.Done:
			return status.Error(codes.Unavailable, "control session was fenced")
		case item, ok := <-received:
			if !ok {
				return nil
			}
			if item.err != nil {
				if errors.Is(item.err, io.EOF) {
					return nil
				}
				return item.err
			}
			if !snapshotAccepted {
				snapshot := item.request.GetFullSnapshot()
				if snapshot == nil {
					return status.Error(codes.FailedPrecondition, "full snapshot must follow session_opened")
				}
				bindings, err := snapshotBindings(snapshot, session.Ref)
				if err != nil {
					return err
				}
				if err := s.revalidate(stream.Context(), session.Ref, bindings); err != nil {
					return err
				}
				snapshotAccepted = true
				bindingCount := uint32(len(bindings)) //nolint:gosec // snapshotBindings caps this slice at 512 entries.
				if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_SnapshotAccepted{
					SnapshotAccepted: &controlv1.SnapshotAccepted{BindingCount: bindingCount},
				}}); err != nil {
					return err
				}
				continue
			}

			mutation := item.request.GetBindingMutation()
			if mutation == nil {
				return status.Error(codes.FailedPrecondition, "only binding mutations are allowed after snapshot acceptance")
			}
			result, err := s.applyMutation(stream.Context(), session.Ref, mutation)
			if err != nil {
				return err
			}
			if err := stream.Send(&controlv1.ControlResponse{Message: &controlv1.ControlResponse_MutationResult{MutationResult: result}}); err != nil {
				return err
			}
		}
	}
}

// AdmitOpen confirms current authority and resolves an exact committed route
// plus current owner revalidation. It does not reserve, persist, or replay an
// Open attempt.
func (s *Service) AdmitOpen(ctx context.Context, request *controlv1.AdmitOpenRequest) (*controlv1.AdmitOpenResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "Open admission request is required")
	}
	wireSession := request.GetSession()
	if err := validateAdmissionSession(wireSession); err != nil {
		return nil, err
	}
	auth, err := authContextFromProto(request.GetAuth())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid auth context: %v", err)
	}
	ingress := sessionRefFromProto(wireSession)
	openContext, err := s.authority.AdmitOpen(ctx, ingress, auth, request.GetEndpoint(), request.GetTargetId())
	if err != nil {
		return nil, mapOpenAdmissionError(err)
	}
	return &controlv1.AdmitOpenResponse{Context: openContextToProto(openContext)}, nil
}

func (s *Service) openControlSession(ctx context.Context, hello *controlv1.Hello) (controlmodel.Session, error) {
	if _, err := s.authority.Confirm(ctx); err != nil {
		return controlmodel.Session{}, unavailable("confirm authority", err)
	}
	session, err := s.authority.OpenSession(ctx, hello.GetGatewayId(), hello.GetGatewayInstanceId(), hello.GetRelayAddress())
	if err != nil {
		return controlmodel.Session{}, mapAuthorityError("open control session", err)
	}
	return session, nil
}

func (s *Service) revalidate(ctx context.Context, ref controlmodel.SessionRef, bindings []routing.LiveBinding) error {
	if _, err := s.authority.Confirm(ctx); err != nil {
		return unavailable("confirm authority", err)
	}
	if err := s.authority.Revalidate(ctx, ref, bindings); err != nil {
		return mapAuthorityError("accept snapshot", err)
	}
	return nil
}

func (s *Service) applyMutation(ctx context.Context, ref controlmodel.SessionRef, mutation *controlv1.BindingMutation) (*controlv1.MutationResult, error) {
	if err := requireSession(mutation.GetSession(), ref); err != nil {
		return nil, err
	}
	bindingWire := mutation.GetDeclare()
	withdraw := bindingWire == nil
	if withdraw {
		bindingWire = mutation.GetWithdraw()
	}
	if bindingWire == nil {
		return nil, status.Error(codes.InvalidArgument, "binding mutation is required")
	}
	binding, err := liveBindingFromProto(bindingWire, ref.GatewayID, ref.GatewayInstanceID, false)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid live binding: %v", err)
	}

	if _, err := s.authority.Confirm(ctx); err != nil {
		return nil, unavailable("confirm authority", err)
	}
	var already bool
	if withdraw {
		already, err = s.authority.Withdraw(ctx, ref, binding)
	} else {
		already, err = s.authority.Declare(ctx, ref, binding)
	}
	if err != nil {
		switch {
		case errors.Is(err, routing.ErrConflict):
			return &controlv1.MutationResult{Code: controlv1.MutationCode_MUTATION_CODE_CONFLICT, Binding: liveBindingToProto(binding, false), Error: err.Error()}, nil
		case errors.Is(err, routing.ErrCapacity):
			return &controlv1.MutationResult{Code: controlv1.MutationCode_MUTATION_CODE_CAPACITY_REACHED, Binding: liveBindingToProto(binding, false), Error: err.Error()}, nil
		default:
			return nil, mapAuthorityError("apply live binding mutation", err)
		}
	}
	code := controlv1.MutationCode_MUTATION_CODE_APPLIED
	if already {
		code = controlv1.MutationCode_MUTATION_CODE_ALREADY_APPLIED
	}
	return &controlv1.MutationResult{Code: code, Binding: liveBindingToProto(binding, false)}, nil
}

type receivedRequest struct {
	request *controlv1.ControlRequest
	err     error
}

func receiveRequests(stream grpc.BidiStreamingServer[controlv1.ControlRequest, controlv1.ControlResponse]) <-chan receivedRequest {
	requests := make(chan receivedRequest, 1)
	go func() {
		defer close(requests)
		for {
			request, err := stream.Recv()
			select {
			case requests <- receivedRequest{request: request, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return requests
}
