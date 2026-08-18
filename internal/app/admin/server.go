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

	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/control/client"
	"github.com/cagojeiger/relaygate/internal/raft/node"
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
	Observe(context.Context) (authority.Ref, authority.Presence, error)
}

type AuthRevisionProvider interface {
	Revision() string
}

type RuntimeSources struct {
	Role         string
	ClusterEpoch string
	Raft         StatusProvider
	Gateway      GatewayStatusProvider
	Presence     PresenceProvider
}

type RuntimeStatus struct {
	RuntimeRole    string                 `json:"runtime_role"`
	ClusterEpoch   string                 `json:"cluster_epoch"`
	Raft           *raftnode.Status       `json:"raft,omitempty"`
	AuthorityID    string                 `json:"authority_id,omitempty"`
	AuthRevision   string                 `json:"auth_revision,omitempty"`
	GatewayControl *gatewaycontrol.Status `json:"gateway_control,omitempty"`
	Presence       *authority.Presence    `json:"presence,omitempty"`
}

// Start exposes an unauthenticated, read-only trusted-local observation
// surface. Shared or untrusted deployment requires an external auth boundary.
func Start(
	ctx context.Context,
	config Config,
	sources RuntimeSources,
	auth AuthRevisionProvider,
	gatherer prometheus.Gatherer,
) (*Server, error) {
	if sources.Role == "" || sources.ClusterEpoch == "" || gatherer == nil {
		return nil, fmt.Errorf("runtime role, cluster epoch and metrics provider are required")
	}
	if (sources.Raft == nil) != (sources.Presence == nil) {
		return nil, fmt.Errorf("raft and presence providers must be configured together")
	}
	controller := sources.Raft != nil
	gateway := sources.Gateway != nil
	if controller == gateway {
		return nil, fmt.Errorf("exactly one controller or Gateway status source is required")
	}
	if gateway && auth == nil {
		return nil, fmt.Errorf("gateway auth revision provider is required")
	}
	if controller && auth != nil {
		return nil, fmt.Errorf("controller must not configure Gateway auth state")
	}
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
		ready := true
		response := map[string]any{
			"runtime_role": sources.Role,
		}
		if sources.Gateway != nil {
			gatewayStatus := sources.Gateway.Status()
			ready = gatewayStatus.Ready()
			response["gateway_control"] = gatewayStatus.State
		}
		if sources.Raft != nil {
			raftStatus := sources.Raft.Status()
			ready = raftStatus.Ready
			response["raft_role"] = raftStatus.Role
			response["has_leader"] = raftStatus.LeaderAddress != ""
		}
		response["status"] = map[bool]string{true: "ready", false: "not_ready"}[ready]
		writer.Header().Set("Content-Type", "application/json")
		if !ready {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(writer).Encode(response)
	})
	mux.HandleFunc("GET /status", func(writer http.ResponseWriter, request *http.Request) {
		runtimeStatus := RuntimeStatus{
			RuntimeRole:  sources.Role,
			ClusterEpoch: sources.ClusterEpoch,
		}
		if sources.Gateway != nil {
			gatewayStatus := sources.Gateway.Status()
			runtimeStatus.GatewayControl = &gatewayStatus
			runtimeStatus.AuthRevision = auth.Revision()
		}
		var observationErr error
		if sources.Raft != nil {
			raftStatus := sources.Raft.Status()
			runtimeStatus.Raft = &raftStatus
			observationContext, cancelObservation := context.WithTimeout(request.Context(), config.ReadTimeout)
			authorityRef, currentPresence, err := sources.Presence.Observe(observationContext)
			cancelObservation()
			observationErr = err
			if observationErr == nil {
				runtimeStatus.AuthorityID = authorityRef.AuthorityID
				runtimeStatus.Presence = &currentPresence
			} else {
				noAuthority := authority.Presence{State: authority.PresenceNoAuthority}
				runtimeStatus.Presence = &noAuthority
			}
		} else if runtimeStatus.GatewayControl == nil || !runtimeStatus.GatewayControl.Ready() {
			observationErr = fmt.Errorf("gateway control is not revalidated")
		}
		writer.Header().Set("Content-Type", "application/json")
		if observationErr != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(writer).Encode(runtimeStatus)
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
