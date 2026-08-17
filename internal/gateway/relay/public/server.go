package relaygrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	maxMessageBytes  = 64 << 10
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 5 * time.Second
)

type Config struct {
	BindAddress          string
	MaxConcurrentStreams uint32
}

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	errors     chan error
}

func Start(ctx context.Context, config Config, service *Service) (*Server, error) {
	if service == nil {
		return nil, fmt.Errorf("relay service is required")
	}
	if config.MaxConcurrentStreams == 0 {
		return nil, fmt.Errorf("max concurrent relay streams must be positive")
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on relay address: %w", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: keepaliveTime}),
	)
	relayv1.RegisterRelayServer(grpcServer, service)
	server := &Server{grpcServer: grpcServer, listener: listener, errors: make(chan error, 1)}
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			server.errors <- fmt.Errorf("serve relay gRPC: %w", err)
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
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		<-stopped
		return fmt.Errorf("shutdown relay gRPC: %w", ctx.Err())
	}
}
