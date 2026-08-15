package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/gatewaycontrol"
	"github.com/cagojeiger/relaygate/internal/raftnode"
)

type Config struct {
	BindAddress  string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type Server struct {
	httpServer *http.Server
	listener   net.Listener
	errors     chan error
}

type StatusProvider interface {
	Status() raftnode.Status
}

type GatewayStatusProvider interface {
	Status() gatewaycontrol.Status
}

type PresenceProvider interface {
	Presence() authority.Presence
}

type RuntimeStatus struct {
	raftnode.Status
	GatewayControl gatewaycontrol.Status `json:"gateway_control"`
	Presence       authority.Presence    `json:"presence"`
}

func Start(
	ctx context.Context,
	config Config,
	node StatusProvider,
	gateway GatewayStatusProvider,
	presence PresenceProvider,
	gatherer prometheus.Gatherer,
) (*Server, error) {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on admin address: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz/live", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"status\":\"live\"}\n"))
	})
	mux.HandleFunc("GET /healthz/ready", func(writer http.ResponseWriter, _ *http.Request) {
		raftStatus := node.Status()
		gatewayStatus := gateway.Status()
		ready := raftStatus.Ready && gatewayStatus.Ready()
		writer.Header().Set("Content-Type", "application/json")
		if !ready {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"status":          map[bool]string{true: "ready", false: "not_ready"}[ready],
			"raft_role":       raftStatus.Role,
			"has_leader":      raftStatus.LeaderAddress != "",
			"gateway_control": gatewayStatus.State,
		})
	})
	mux.HandleFunc("GET /status", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(RuntimeStatus{
			Status:         node.Status(),
			GatewayControl: gateway.Status(),
			Presence:       presence.Presence(),
		})
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		MaxRequestsInFlight: 5,
		Timeout:             5 * time.Second,
	}))

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: config.ReadTimeout,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	server := &Server{
		httpServer: httpServer,
		listener:   listener,
		errors:     make(chan error, 1),
	}
	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			server.errors <- fmt.Errorf("serve admin HTTP: %w", err)
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
	if err := s.httpServer.Shutdown(ctx); err != nil {
		_ = s.httpServer.Close()
		return fmt.Errorf("shutdown admin HTTP: %w", err)
	}
	return nil
}
