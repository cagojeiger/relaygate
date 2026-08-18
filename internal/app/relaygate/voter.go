package relaygate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cagojeiger/relaygate/internal/app/config"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	controlgrpc "github.com/cagojeiger/relaygate/internal/gateway/control/server"
	raftmembership "github.com/cagojeiger/relaygate/internal/raft/membership"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
	raftstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

type controllerRuntime struct {
	node          *raftnode.Node
	authority     *authority.Manager
	controlServer *controlgrpc.Server
	membership    *raftmembership.Server
	config        config.Config
	cancel        context.CancelFunc
	failures      chan error
	workers       sync.WaitGroup
	shutdownOnce  sync.Once
	shutdownErr   error
}

func startControllerRuntime(appConfig config.Config, registry prometheus.Registerer, logger *slog.Logger) (_ *controllerRuntime, resultErr error) {
	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "relaygate.raft",
		Level:      hclogLevel(appConfig.LogLevel()),
		Output:     os.Stdout,
		JSONFormat: true,
	})
	node, err := raftnode.Open(raftnode.Config{
		NodeID:            appConfig.Raft.NodeID,
		DataDir:           appConfig.Raft.DataDir,
		BindAddress:       appConfig.Raft.BindAddress,
		AdvertiseAddress:  appConfig.Raft.AdvertiseAddress,
		Bootstrap:         appConfig.Raft.Bootstrap,
		BootstrapVoters:   bootstrapVoters(appConfig.Raft.BootstrapVoters),
		ApplyTimeout:      appConfig.Raft.ApplyTimeout.Value(),
		TransportTimeout:  appConfig.Raft.TransportTimeout.Value(),
		ShutdownTimeout:   appConfig.Raft.ShutdownTimeout.Value(),
		SnapshotThreshold: appConfig.Raft.SnapshotThreshold,
		SnapshotInterval:  appConfig.Raft.SnapshotInterval.Value(),
		SnapshotRetain:    appConfig.Raft.SnapshotRetain,
		MaxPool:           appConfig.Raft.MaxPool,
		MaxCommandBytes:   appConfig.Raft.MaxCommandBytes,
	}, raftLogger, registry)
	if err != nil {
		return nil, err
	}
	runtimeContext, cancelRuntime := context.WithCancel(context.Background())
	runtime := &controllerRuntime{
		node:     node,
		config:   appConfig,
		cancel:   cancelRuntime,
		failures: make(chan error, 3),
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, runtime.shutdown())
		}
	}()

	authorityManager, err := authority.New(authority.Config{
		ClusterEpoch:               appConfig.Control.ClusterEpoch,
		ProbeInterval:              appConfig.Control.AuthorityProbeInterval.Value(),
		ProbeTimeout:               appConfig.Control.AuthorityProbeTimeout.Value(),
		ApplyTimeout:               appConfig.Raft.ApplyTimeout.Value(),
		GatewayRevalidationTimeout: appConfig.Control.GatewayRevalidationTimeout.Value(),
		OpenContextTTL:             appConfig.Control.OpenContextTTL.Value(),
	}, node)
	if err != nil {
		return nil, fmt.Errorf("configure authority manager: %w", err)
	}
	runtime.authority = authorityManager
	authorityManager.Start(runtimeContext)

	controlService, err := controlgrpc.NewService(appConfig.Control.ClusterEpoch, authorityManager)
	if err != nil {
		return nil, fmt.Errorf("configure control service: %w", err)
	}
	controlServer, err := controlgrpc.Start(runtimeContext, controlgrpc.Config{
		BindAddress: appConfig.Control.BindAddress,
	}, controlService)
	if err != nil {
		return nil, err
	}
	runtime.controlServer = controlServer
	membershipServer, err := raftmembership.Start(runtimeContext, appConfig.Raft.DataDir, node)
	if err != nil {
		return nil, fmt.Errorf("start membership operator: %w", err)
	}
	runtime.membership = membershipServer
	runtime.workers.Add(3)
	go runtime.initialize(runtimeContext, logger)
	go runtime.forwardControlErrors(runtimeContext)
	go runtime.forwardMembershipErrors(runtimeContext)
	return runtime, nil
}

func (r *controllerRuntime) initialize(ctx context.Context, logger *slog.Logger) {
	defer r.workers.Done()
	result, err := r.node.EnsureCluster(ctx, raftstate.InitializeCluster{
		ClusterEpoch:          r.config.Control.ClusterEpoch,
		MaxGatewaySessions:    r.config.Control.MaxGatewaySessions,
		MaxRoutes:             r.config.Control.MaxRoutes,
		MaxBindingsPerGateway: r.config.Control.MaxBindingsPerGateway,
	})
	if err != nil {
		if ctx.Err() == nil {
			r.reportFailure(fmt.Errorf("initialize or verify control cluster: %w", err))
		}
		return
	}
	logger.Info("control cluster ready", "cluster_epoch", r.config.Control.ClusterEpoch, "result", result.Code)
}

func (r *controllerRuntime) forwardControlErrors(ctx context.Context) {
	defer r.workers.Done()
	select {
	case <-ctx.Done():
		return
	case err, ok := <-r.controlServer.Errors():
		if ctx.Err() != nil {
			return
		}
		if !ok || err == nil {
			err = errors.New("control server stopped unexpectedly")
		}
		r.reportFailure(err)
	}
}

func (r *controllerRuntime) forwardMembershipErrors(ctx context.Context) {
	defer r.workers.Done()
	select {
	case <-ctx.Done():
		return
	case err, ok := <-r.membership.Errors():
		if ctx.Err() != nil {
			return
		}
		if !ok || err == nil {
			err = errors.New("membership operator stopped unexpectedly")
		}
		r.reportFailure(err)
	}
}

func (r *controllerRuntime) reportFailure(err error) {
	select {
	case r.failures <- err:
	default:
	}
}

func (r *controllerRuntime) beginShutdown() {
	if r == nil {
		return
	}
	if r.cancel != nil {
		r.cancel()
	}
	r.node.BeginShutdown()
	if r.authority != nil {
		r.authority.Close()
	}
}

func (r *controllerRuntime) shutdown() error {
	if r == nil {
		return nil
	}
	r.shutdownOnce.Do(func() {
		r.beginShutdown()
		if r.membership != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.config.Control.ShutdownTimeout.Value())
			r.shutdownErr = errors.Join(r.shutdownErr, r.membership.Shutdown(ctx))
			cancel()
		}
		if r.controlServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.config.Control.ShutdownTimeout.Value())
			r.shutdownErr = errors.Join(r.shutdownErr, r.controlServer.Shutdown(ctx))
			cancel()
		}
		if r.node != nil {
			r.shutdownErr = errors.Join(r.shutdownErr, r.node.Close())
		}
		r.workers.Wait()
	})
	return r.shutdownErr
}

func (r *controllerRuntime) errors() <-chan error {
	if r == nil {
		return nil
	}
	return r.failures
}

func (r *controllerRuntime) logFields() []any {
	if r == nil {
		return nil
	}
	return []any{
		"raft_address", r.config.Raft.AdvertiseAddress,
		"control_address", r.controlServer.Address(),
		"membership_socket", raftmembership.SocketPath(r.config.Raft.DataDir),
		"raft_role", r.node.Status().Role,
	}
}
