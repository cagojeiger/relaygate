package controlgrpc

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/cagojeiger/relaygate/internal/controltransport"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const maxMessageBytes = 1 << 20

type Config struct {
	BindAddress string
}

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	errors     chan error
}

func Start(ctx context.Context, config Config, service *Service) (*Server, error) {
	if service == nil {
		return nil, fmt.Errorf("control service is required")
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on control address: %w", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    controltransport.KeepaliveTime,
			Timeout: controltransport.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: controltransport.KeepaliveMinPingTime,
		}),
	)
	controlv1.RegisterGatewayControlServer(grpcServer, service)
	server := &Server{grpcServer: grpcServer, listener: listener, errors: make(chan error, 1)}
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			server.errors <- fmt.Errorf("serve control gRPC: %w", err)
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
		return fmt.Errorf("shutdown control gRPC: %w", ctx.Err())
	}
}
