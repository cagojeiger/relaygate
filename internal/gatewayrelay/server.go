package gatewayrelay

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	gatewayKeepaliveTime    = 10 * time.Second
	gatewayKeepaliveTimeout = 5 * time.Second
)

type Server struct {
	grpcServer *grpc.Server
	service    *Service
	listener   net.Listener
	errors     chan error
}

func Start(ctx context.Context, config Config, service *Service) (*Server, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if service == nil {
		return nil, fmt.Errorf("%w: Gateway relay service is required", ErrInvalid)
	}
	if service.openTimeout != config.OpenTimeout || cap(service.slots) != int(config.MaxPipes) {
		return nil, fmt.Errorf("%w: server and service limits must match", ErrInvalid)
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on Gateway relay address: %w", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGatewayMessageBytes),
		grpc.MaxSendMsgSize(maxGatewayMessageBytes),
		grpc.MaxConcurrentStreams(config.MaxPipes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    gatewayKeepaliveTime,
			Timeout: gatewayKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: gatewayKeepaliveTime}),
	)
	gatewayv1.RegisterGatewayRelayServer(grpcServer, service)
	server := &Server{
		grpcServer: grpcServer,
		service:    service,
		listener:   listener,
		errors:     make(chan error, 1),
	}
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			server.errors <- fmt.Errorf("serve Gateway relay gRPC: %w", err)
		}
		close(server.errors)
	}()
	return server, nil
}

func (s *Server) Address() string {
	return s.listener.Addr().String()
}

func (s *Server) Errors() <-chan error {
	return s.errors
}

func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("shutdown Gateway relay gRPC: %w", ErrInvalid)
	}
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		if err := s.service.wait(ctx); err != nil {
			return fmt.Errorf("join Gateway relay workers: %w", err)
		}
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		<-stopped
		joinCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.service.openTimeout)
		_ = s.service.wait(joinCtx)
		cancel()
		return fmt.Errorf("shutdown Gateway relay gRPC: %w", ctx.Err())
	}
}
