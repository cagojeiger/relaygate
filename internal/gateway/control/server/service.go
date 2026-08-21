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

func (s *Service) validateHello(hello *controlv1.Hello) error {
	if hello.GetClusterEpoch() != s.clusterEpoch {
		return status.Error(codes.FailedPrecondition, "cluster epoch is not current")
	}
	if err := routing.ValidateIdentity("gateway_id", hello.GetGatewayId()); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := routing.ValidateIdentity("gateway_instance_id", hello.GetGatewayInstanceId()); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := routing.ValidateRelayAddress(hello.GetRelayAddress()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid relay_address: %v", err)
	}
	return nil
}

func snapshotBindings(snapshot *controlv1.FullSnapshot, ref controlmodel.SessionRef) ([]routing.LiveBinding, error) {
	if err := requireSession(snapshot.GetSession(), ref); err != nil {
		return nil, err
	}
	if len(snapshot.GetBindings()) > routing.MaxListenerBindingsPerGateway {
		return nil, status.Error(codes.ResourceExhausted, "snapshot binding capacity reached")
	}
	bindings := make([]routing.LiveBinding, 0, len(snapshot.GetBindings()))
	seen := make(map[routing.BindingKey]struct{}, len(snapshot.GetBindings()))
	for index, item := range snapshot.GetBindings() {
		binding, err := liveBindingFromProto(item, ref.GatewayID, ref.GatewayInstanceID, false)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot binding %d: %v", index, err)
		}
		if _, duplicate := seen[binding.Key]; duplicate {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot binding %d duplicates a key", index)
		}
		seen[binding.Key] = struct{}{}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func requireSession(wire *controlv1.SessionRef, ref controlmodel.SessionRef) error {
	if wire == nil || wire.GetClusterEpoch() != ref.ClusterEpoch || wire.GetAuthorityId() != ref.AuthorityID || wire.GetControlSessionId() != ref.ControlSessionID || wire.GetGatewayId() != ref.GatewayID || wire.GetGatewayInstanceId() != ref.GatewayInstanceID {
		return status.Error(codes.Unavailable, "control session is stale")
	}
	return nil
}

func validateAdmissionSession(wire *controlv1.SessionRef) error {
	if wire == nil {
		return status.Error(codes.InvalidArgument, "control session is required")
	}
	for _, field := range []struct{ name, value string }{{"cluster_epoch", wire.GetClusterEpoch()}, {"authority_id", wire.GetAuthorityId()}, {"control_session_id", wire.GetControlSessionId()}, {"gateway_id", wire.GetGatewayId()}, {"gateway_instance_id", wire.GetGatewayInstanceId()}} {
		if err := routing.ValidateIdentity(field.name, field.value); err != nil {
			return status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}
	return nil
}

func sessionRefFromProto(wire *controlv1.SessionRef) controlmodel.SessionRef {
	return controlmodel.SessionRef{ClusterEpoch: wire.GetClusterEpoch(), AuthorityID: wire.GetAuthorityId(), ControlSessionID: wire.GetControlSessionId(), GatewayID: wire.GetGatewayId(), GatewayInstanceID: wire.GetGatewayInstanceId()}
}

func sessionRefToProto(ref controlmodel.SessionRef) *controlv1.SessionRef {
	return &controlv1.SessionRef{ClusterEpoch: ref.ClusterEpoch, AuthorityId: ref.AuthorityID, ControlSessionId: ref.ControlSessionID, GatewayId: ref.GatewayID, GatewayInstanceId: ref.GatewayInstanceID}
}

func liveBindingToProto(binding routing.LiveBinding, includeGatewayID bool) *controlv1.LiveBinding {
	ref := &controlv1.ListenerBindingRef{GatewayInstanceId: binding.Ref.GatewayInstanceID, ListenerBindingId: binding.Ref.ListenerBindingID}
	if includeGatewayID {
		ref.GatewayId = binding.Ref.GatewayID
	}
	return &controlv1.LiveBinding{Key: &controlv1.BindingKey{ClientId: binding.Key.ClientID, EndpointPattern: binding.Key.EndpointPattern, TargetId: binding.Key.TargetID}, Ref: ref}
}

func liveBindingFromProto(wire *controlv1.LiveBinding, sessionGatewayID, sessionGatewayInstanceID string, requireGatewayID bool) (routing.LiveBinding, error) {
	if wire == nil || wire.GetKey() == nil || wire.GetRef() == nil {
		return routing.LiveBinding{}, errors.New("live binding is required")
	}
	gatewayID := wire.GetRef().GetGatewayId()
	if gatewayID == "" {
		gatewayID = sessionGatewayID
	}
	if requireGatewayID && gatewayID == "" {
		return routing.LiveBinding{}, errors.New("live binding gateway_id is required")
	}
	binding := routing.LiveBinding{Key: routing.BindingKey{ClientID: wire.GetKey().GetClientId(), EndpointPattern: wire.GetKey().GetEndpointPattern(), TargetID: wire.GetKey().GetTargetId()}, Ref: routing.ListenerBindingRef{GatewayID: gatewayID, GatewayInstanceID: wire.GetRef().GetGatewayInstanceId(), ListenerBindingID: wire.GetRef().GetListenerBindingId()}}
	if err := binding.Validate(); err != nil {
		return routing.LiveBinding{}, err
	}
	if sessionGatewayID != "" && (binding.Ref.GatewayID != sessionGatewayID || binding.Ref.GatewayInstanceID != sessionGatewayInstanceID) {
		return routing.LiveBinding{}, errors.New("live binding does not belong to current control session")
	}
	return binding, nil
}

func authContextFromProto(wire *controlv1.AuthContext) (routing.AuthContext, error) {
	if wire == nil {
		return routing.AuthContext{}, errors.New("auth context is required")
	}
	auth := routing.AuthContext{ClientSessionID: wire.GetClientSessionId(), ClientID: wire.GetClientId(), APIKeyID: wire.GetApiKeyId(), AuthRevision: wire.GetAuthRevision()}
	return auth, auth.Validate()
}

func openContextToProto(open routing.OpenContext) *controlv1.OpenContext {
	return &controlv1.OpenContext{ClusterEpoch: open.ClusterEpoch, AuthorityId: open.AuthorityID, AttemptId: open.AttemptID, Auth: &controlv1.AuthContext{ClientSessionId: open.Auth.ClientSessionID, ClientId: open.Auth.ClientID, ApiKeyId: open.Auth.APIKeyID, AuthRevision: open.Auth.AuthRevision}, Binding: liveBindingToProto(open.Binding, true), IngressGatewayId: open.IngressGatewayID, IngressGatewayInstanceId: open.IngressGatewayInstanceID, IngressControlSessionId: open.IngressControlSessionID, OwnerRelayAddress: open.OwnerRelayAddress, ExpiresAtUnixMillis: open.ExpiresAt.UnixMilli(), OwnerControlSessionId: open.OwnerControlSessionID}
}

func unavailable(operation string, err error) error {
	return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
}

func mapAuthorityError(operation string, err error) error {
	switch {
	case errors.Is(err, authority.ErrNoAuthority), errors.Is(err, authority.ErrStaleSession):
		return unavailable(operation, err)
	case errors.Is(err, authority.ErrSnapshotFirst):
		return status.Errorf(codes.FailedPrecondition, "%s: %v", operation, err)
	case errors.Is(err, routing.ErrInvalid):
		return status.Errorf(codes.InvalidArgument, "%s: %v", operation, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", operation, err)
	}
}

func mapOpenAdmissionError(err error) error {
	switch {
	case errors.Is(err, routing.ErrRouteNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, routing.ErrInvalidOpen), errors.Is(err, routing.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, authority.ErrNoAuthority), errors.Is(err, authority.ErrStaleSession), errors.Is(err, authority.ErrSnapshotFirst), errors.Is(err, routing.ErrOpenUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
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
