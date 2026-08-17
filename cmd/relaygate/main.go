package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/cagojeiger/relaygate/internal/app/admin"
	"github.com/cagojeiger/relaygate/internal/app/config"
	"github.com/cagojeiger/relaygate/internal/gateway/access/runtime"
	"github.com/cagojeiger/relaygate/internal/gateway/control/authority"
	"github.com/cagojeiger/relaygate/internal/gateway/control/client"
	"github.com/cagojeiger/relaygate/internal/gateway/control/server"
	"github.com/cagojeiger/relaygate/internal/gateway/relay/peer"
	"github.com/cagojeiger/relaygate/internal/gateway/relay/public"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	"github.com/cagojeiger/relaygate/internal/raft/node"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "relaygate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := os.Getenv("RELAYGATE_CONFIG")
	if configPath == "" {
		configPath = "relaygate.yaml"
	}
	flag.StringVar(&configPath, "config", configPath, "path to RelayGate YAML config")
	flag.Parse()

	appConfig, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: appConfig.LogLevel()})).With(
		"service", "relaygate",
		"version", version,
		"node_id", appConfig.Raft.NodeID,
	)
	slog.SetDefault(logger)
	clientRuntime, err := clientruntime.New(appConfig)
	if err != nil {
		return err
	}
	defer clientRuntime.Close()

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "relaygate.raft",
		Level:      hclogLevel(appConfig.LogLevel()),
		Output:     os.Stdout,
		JSONFormat: true,
	})
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)

	node, err := raftnode.Open(raftnode.Config{
		NodeID:            appConfig.Raft.NodeID,
		BindAddress:       appConfig.Raft.BindAddress,
		AdvertiseAddress:  appConfig.Raft.AdvertiseAddress,
		DataDir:           appConfig.Raft.DataDir,
		Bootstrap:         appConfig.Raft.Bootstrap,
		BootstrapVoters:   bootstrapVoters(appConfig.Raft.BootstrapVoters),
		ApplyTimeout:      appConfig.Raft.ApplyTimeout.Value(),
		TransportTimeout:  appConfig.Raft.TransportTimeout.Value(),
		ShutdownTimeout:   appConfig.Raft.ShutdownTimeout.Value(),
		SnapshotRetain:    appConfig.Raft.SnapshotRetain,
		SnapshotThreshold: appConfig.Raft.SnapshotThreshold,
		SnapshotInterval:  appConfig.Raft.SnapshotInterval.Value(),
		MaxPool:           appConfig.Raft.MaxPool,
		MaxCommandBytes:   appConfig.Raft.MaxCommandBytes,
	}, raftLogger, registry)
	if err != nil {
		return err
	}
	defer node.Close()

	startupContext, cancelStartup := context.WithTimeout(context.Background(), appConfig.Raft.StartupTimeout.Value())
	if err := node.WaitForLeader(startupContext); err != nil {
		cancelStartup()
		return err
	}
	result, err := node.EnsureEpoch(startupContext, appConfig.Control.ClusterEpoch)
	if err != nil {
		cancelStartup()
		return fmt.Errorf("initialize or verify control epoch: %w", err)
	}
	logger.Info("control epoch ready", "cluster_epoch", appConfig.Control.ClusterEpoch, "result", result.Code)
	cancelStartup()

	authorityManager, err := authority.New(authority.Config{
		ClusterEpoch:   appConfig.Control.ClusterEpoch,
		ProbeInterval:  appConfig.Control.AuthorityProbeInterval.Value(),
		ProbeTimeout:   appConfig.Control.AuthorityProbeTimeout.Value(),
		OpenContextTTL: appConfig.Relay.OpenTimeout.Value(),
	}, node)
	if err != nil {
		return fmt.Errorf("configure authority manager: %w", err)
	}
	authorityManager.Start(context.Background())
	controlService, err := controlgrpc.NewService(appConfig.Control.ClusterEpoch, authorityManager)
	if err != nil {
		authorityManager.Close()
		return fmt.Errorf("configure control service: %w", err)
	}
	controlServer, err := controlgrpc.Start(context.Background(), controlgrpc.Config{
		BindAddress: appConfig.Control.BindAddress,
	}, controlService)
	if err != nil {
		authorityManager.Close()
		return err
	}
	gatewayClient, err := gatewaycontrol.New(gatewaycontrol.Config{
		ClusterEpoch:     appConfig.Control.ClusterEpoch,
		GatewayID:        appConfig.Gateway.ID,
		RelayAddress:     appConfig.InternalRelay.AdvertiseAddress,
		ControlEndpoints: appConfig.Gateway.ControlEndpoints,
		ConnectTimeout:   appConfig.Gateway.ConnectTimeout.Value(),
		RetryInterval:    appConfig.Gateway.RetryInterval.Value(),
	}, logger)
	if err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("configure gateway control client: %w", err)
	}
	bindingManager, err := localbinding.New(
		appConfig.Gateway.ID,
		gatewayClient.Status().GatewayInstanceID,
		appConfig.Relay.MaxListenerBindings,
		gatewayClient,
		clientRuntime.Sessions(),
	)
	if err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("configure listener binding runtime: %w", err)
	}
	defer bindingManager.Close()
	if err := gatewayClient.AttachSnapshotProvider(bindingManager); err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("attach current binding snapshot provider: %w", err)
	}
	if err := clientRuntime.AttachBindings(bindingManager); err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("attach listener binding runtime: %w", err)
	}
	gatewayRelayClient, err := gatewayrelay.NewClient(
		appConfig.InternalRelay.ConnectTimeout.Value(),
		appConfig.Relay.OpenTimeout.Value(),
		appConfig.Relay.MaxPipes,
	)
	if err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("configure Gateway relay client: %w", err)
	}
	defer gatewayRelayClient.Close()
	openingManager, err := opening.New(opening.Config{
		ClusterEpoch: appConfig.Control.ClusterEpoch,
		MaxPipes:     appConfig.Relay.MaxPipes,
		OpenTimeout:  appConfig.Relay.OpenTimeout.Value(),
	}, gatewayClient, bindingManager, gatewayRelayClient)
	if err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("configure Open runtime: %w", err)
	}
	defer openingManager.Close()
	if err := clientRuntime.AttachPipes(openingManager); err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("attach Open runtime: %w", err)
	}
	gatewayRelayService, err := gatewayrelay.NewService(
		openingManager,
		appConfig.Relay.OpenTimeout.Value(),
		appConfig.Relay.MaxPipes,
	)
	if err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("configure Gateway relay service: %w", err)
	}
	gatewayRelayServer, err := gatewayrelay.Start(context.Background(), gatewayrelay.Config{
		BindAddress: appConfig.InternalRelay.BindAddress,
		OpenTimeout: appConfig.Relay.OpenTimeout.Value(),
		MaxPipes:    appConfig.Relay.MaxPipes,
	}, gatewayRelayService)
	if err != nil {
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("start Gateway relay service: %w", err)
	}
	var stopGatewayRelayOnce sync.Once
	var gatewayRelayShutdownErr error
	stopGatewayRelay := func() error {
		stopGatewayRelayOnce.Do(func() {
			gatewayRelayClient.Close()
			shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.InternalRelay.ShutdownTimeout.Value())
			gatewayRelayShutdownErr = gatewayRelayServer.Shutdown(shutdownContext)
			cancelShutdown()
		})
		return gatewayRelayShutdownErr
	}
	defer func() { _ = stopGatewayRelay() }()
	gatewayContext, cancelGateway := context.WithCancel(context.Background())
	gatewayDone := make(chan struct{})
	go func() {
		gatewayClient.Run(gatewayContext)
		close(gatewayDone)
	}()
	var stopGatewayOnce sync.Once
	stopGateway := func() {
		stopGatewayOnce.Do(func() {
			cancelGateway()
			<-gatewayDone
		})
	}
	defer stopGateway()
	relayService, err := relaygrpc.NewService(
		clientRuntime.Sessions(),
		bindingManager,
		openingManager,
		appConfig.Relay.AuthenticationTimeout.Value(),
		appConfig.Relay.OpenTimeout.Value(),
		appConfig.Relay.MaxPipes,
	)
	if err != nil {
		stopGateway()
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return fmt.Errorf("configure relay service: %w", err)
	}
	relayServer, err := relaygrpc.Start(context.Background(), relaygrpc.Config{
		BindAddress:          appConfig.Relay.BindAddress,
		MaxConcurrentStreams: appConfig.Relay.MaxClientSessions,
	}, relayService)
	if err != nil {
		stopGateway()
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return err
	}

	adminServer, err := admin.Start(context.Background(), admin.Config{
		BindAddress:  appConfig.Admin.BindAddress,
		ReadTimeout:  appConfig.Admin.ReadTimeout.Value(),
		WriteTimeout: appConfig.Admin.WriteTimeout.Value(),
	}, node, gatewayClient, authorityManager, clientRuntime, registry)
	if err != nil {
		stopGateway()
		clientRuntime.Close()
		relayShutdownContext, cancelRelayShutdown := context.WithTimeout(context.Background(), appConfig.Relay.ShutdownTimeout.Value())
		_ = relayServer.Shutdown(relayShutdownContext)
		cancelRelayShutdown()
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return err
	}
	logger.Info("relaygate started",
		"raft_address", appConfig.Raft.AdvertiseAddress,
		"control_address", controlServer.Address(),
		"relay_address", relayServer.Address(),
		"internal_relay_address", gatewayRelayServer.Address(),
		"admin_address", adminServer.Address(),
		"gateway_id", appConfig.Gateway.ID,
		"gateway_instance_id", gatewayClient.Status().GatewayInstanceID,
		"auth_revision", clientRuntime.Revision(),
		"raft_role", node.Status().Role,
	)

	signalContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	reloadSignals := make(chan os.Signal, 1)
	signal.Notify(reloadSignals, syscall.SIGHUP)
	defer signal.Stop(reloadSignals)
	var runErr error
	running := true
	for running {
		select {
		case <-signalContext.Done():
			logger.Info("shutdown requested")
			running = false
		case <-reloadSignals:
			candidate, err := config.Load(configPath)
			if err != nil {
				logger.Error("client config reload rejected", "error", err)
				continue
			}
			result, err := clientRuntime.Apply(candidate)
			if err != nil {
				logger.Error("client config reload rejected", "error", err)
				continue
			}
			logger.Info("client config reloaded",
				"auth_revision", result.Revision,
				"removed_credentials", result.Removed,
				"retired_sessions", result.RetiredSessions,
				"retired_pipes", result.RetiredPipes,
				"retired_bindings", result.RetiredBindings,
			)
		case err := <-adminServer.Errors():
			if err != nil {
				runErr = err
			} else {
				runErr = fmt.Errorf("admin server stopped unexpectedly")
			}
			running = false
		case err := <-controlServer.Errors():
			if err != nil {
				runErr = err
			} else {
				runErr = fmt.Errorf("control server stopped unexpectedly")
			}
			running = false
		case err := <-relayServer.Errors():
			if err != nil {
				runErr = err
			} else {
				runErr = fmt.Errorf("relay server stopped unexpectedly")
			}
			running = false
		case err := <-gatewayRelayServer.Errors():
			if err != nil {
				runErr = err
			} else {
				runErr = fmt.Errorf("gateway relay server stopped unexpectedly")
			}
			running = false
		}
	}

	stopGateway()
	node.BeginShutdown()
	authorityManager.Close()
	clientRuntime.Close()
	shutdownErr := runErr
	relayShutdownContext, cancelRelayShutdown := context.WithTimeout(context.Background(), appConfig.Relay.ShutdownTimeout.Value())
	if err := relayServer.Shutdown(relayShutdownContext); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		logger.Error("relay shutdown failed", "error", err)
	}
	cancelRelayShutdown()
	if err := stopGatewayRelay(); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		logger.Error("Gateway relay shutdown failed", "error", err)
	}
	controlShutdownContext, cancelControlShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
	if err := controlServer.Shutdown(controlShutdownContext); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		logger.Error("control shutdown failed", "error", err)
	}
	cancelControlShutdown()
	adminShutdownContext, cancelAdminShutdown := context.WithTimeout(context.Background(), appConfig.Admin.ShutdownTimeout.Value())
	if err := adminServer.Shutdown(adminShutdownContext); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		logger.Error("admin shutdown failed", "error", err)
	}
	cancelAdminShutdown()
	if err := node.Close(); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
	}
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
