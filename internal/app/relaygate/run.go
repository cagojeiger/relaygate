package relaygate

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/cagojeiger/relaygate/internal/app/admin"
	"github.com/cagojeiger/relaygate/internal/app/config"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
)

func Run(configPath, version string) error {
	appConfig, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: appConfig.LogLevel()})).With(
		"service", "relaygate",
		"version", version,
		"runtime_role", appConfig.Runtime.Role,
	)
	if appConfig.Runtime.Role.HasController() {
		logger = logger.With("node_id", appConfig.Raft.NodeID)
	} else {
		logger = logger.With("gateway_id", appConfig.Gateway.ID)
	}
	slog.SetDefault(logger)

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)

	var controller *controllerRuntime
	var gateway *gatewayRuntime
	if appConfig.Runtime.Role.HasController() {
		controller, err = startControllerRuntime(appConfig, registry, logger)
		if err != nil {
			return err
		}
	} else {
		gateway, err = startGatewayRuntime(appConfig, logger)
		if err != nil {
			return err
		}
	}

	adminSources := admin.RuntimeSources{
		Role:         string(appConfig.Runtime.Role),
		ClusterEpoch: appConfig.Control.ClusterEpoch,
	}
	var authSource admin.AuthRevisionProvider
	if controller != nil {
		adminSources.Raft = controller.node
		adminSources.Presence = controller.authority
	} else {
		adminSources.Gateway = gateway.controlClient
		authSource = gateway.clients
	}
	adminServer, err := admin.Start(context.Background(), admin.Config{
		BindAddress:  appConfig.Admin.BindAddress,
		ReadTimeout:  appConfig.Admin.ReadTimeout.Value(),
		WriteTimeout: appConfig.Admin.WriteTimeout.Value(),
	}, adminSources, authSource, registry)
	if err != nil {
		return errors.Join(err, gateway.shutdown(), controller.shutdown())
	}

	fields := append([]any{
		"runtime_role", appConfig.Runtime.Role,
		"admin_address", adminServer.Address(),
	}, gateway.logFields()...)
	fields = append(fields, controller.logFields()...)
	logger.Info("relaygate started", fields...)

	signalContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	reloadSignals := make(chan os.Signal, 1)
	signal.Notify(reloadSignals, syscall.SIGHUP)
	defer signal.Stop(reloadSignals)
	runErr := (eventLoop{
		reloadSignals:      reloadSignals,
		adminErrors:        adminServer.Errors(),
		controlErrors:      controller.errors(),
		relayErrors:        gateway.publicErrors(),
		gatewayRelayErrors: gateway.peerErrors(),
		onShutdown: func() {
			logger.Info("shutdown requested")
		},
		onReload: func() {
			if gateway == nil {
				logger.Info("client config reload ignored by controller runtime")
				return
			}
			candidate, err := config.Load(configPath)
			if err != nil {
				logger.Error("client config reload rejected", "error", err)
				return
			}
			result, err := gateway.reload(candidate)
			if err != nil {
				logger.Error("client config reload rejected", "error", err)
				return
			}
			logger.Info("client config reloaded",
				"auth_revision", result.Revision,
				"removed_credentials", result.Removed,
				"retired_sessions", result.RetiredSessions,
				"retired_pipes", result.RetiredPipes,
				"retired_bindings", result.RetiredBindings,
			)
		},
	}).wait(signalContext)

	gateway.stopControl()
	controller.beginShutdown()
	shutdownErr := errors.Join(runErr, gateway.shutdown(), controller.shutdown())
	adminContext, cancelAdmin := context.WithTimeout(context.Background(), appConfig.Admin.ShutdownTimeout.Value())
	if err := adminServer.Shutdown(adminContext); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		logger.Error("admin shutdown failed", "error", err)
	}
	cancelAdmin()
	logger.Info("relaygate stopped")
	return shutdownErr
}

func bootstrapVoters(configured []config.RaftVoterConfig) []raftnode.BootstrapVoter {
	voters := make([]raftnode.BootstrapVoter, 0, len(configured))
	for _, voter := range configured {
		voters = append(voters, raftnode.BootstrapVoter{NodeID: voter.NodeID, Address: voter.Address})
	}
	return voters
}

func hclogLevel(level slog.Level) hclog.Level {
	switch {
	case level <= slog.LevelDebug:
		return hclog.Debug
	case level >= slog.LevelError:
		return hclog.Error
	case level >= slog.LevelWarn:
		return hclog.Warn
	default:
		return hclog.Info
	}
}
