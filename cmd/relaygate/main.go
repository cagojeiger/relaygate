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

	"github.com/cagojeiger/relaygate/internal/admin"
	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/config"
	"github.com/cagojeiger/relaygate/internal/controlgrpc"
	"github.com/cagojeiger/relaygate/internal/gatewaycontrol"
	"github.com/cagojeiger/relaygate/internal/raftnode"
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
	result, err := node.EnsureEpoch(
		startupContext,
		appConfig.Control.ClusterEpoch,
		appConfig.Control.MaxDistinctBindingKeysPerEpoch,
		appConfig.Control.MaxDistinctGatewayIDsPerEpoch,
	)
	if err != nil {
		cancelStartup()
		return fmt.Errorf("initialize or verify control epoch: %w", err)
	}
	logger.Info("control epoch ready", "cluster_epoch", appConfig.Control.ClusterEpoch, "result", result.Code)
	cancelStartup()

	authorityManager, err := authority.New(authority.Config{
		ClusterEpoch:        appConfig.Control.ClusterEpoch,
		ProbeInterval:       appConfig.Control.AuthorityProbeInterval.Value(),
		ProbeTimeout:        appConfig.Control.AuthorityProbeTimeout.Value(),
		RevalidationTimeout: appConfig.Control.GatewayRevalidationTimeout.Value(),
	}, node)
	if err != nil {
		return fmt.Errorf("configure authority manager: %w", err)
	}
	authorityManager.Start(context.Background())
	controlService, err := controlgrpc.NewService(appConfig.Control.ClusterEpoch, node, authorityManager)
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

	adminServer, err := admin.Start(context.Background(), admin.Config{
		BindAddress:  appConfig.Admin.BindAddress,
		ReadTimeout:  appConfig.Admin.ReadTimeout.Value(),
		WriteTimeout: appConfig.Admin.WriteTimeout.Value(),
	}, node, gatewayClient, authorityManager, registry)
	if err != nil {
		stopGateway()
		authorityManager.Close()
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), appConfig.Control.ShutdownTimeout.Value())
		defer cancelShutdown()
		_ = controlServer.Shutdown(shutdownContext)
		return err
	}
	logger.Info("relaygate started",
		"raft_address", appConfig.Raft.AdvertiseAddress,
		"control_address", controlServer.Address(),
		"admin_address", adminServer.Address(),
		"gateway_id", appConfig.Gateway.ID,
		"gateway_instance_id", gatewayClient.Status().GatewayInstanceID,
		"raft_role", node.Status().Role,
	)

	signalContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	var runErr error
	select {
	case <-signalContext.Done():
		logger.Info("shutdown requested")
	case err := <-adminServer.Errors():
		if err != nil {
			runErr = err
		} else {
			runErr = fmt.Errorf("admin server stopped unexpectedly")
		}
	case err := <-controlServer.Errors():
		if err != nil {
			runErr = err
		} else {
			runErr = fmt.Errorf("control server stopped unexpectedly")
		}
	}

	stopGateway()
	node.BeginShutdown()
	authorityManager.Close()
	shutdownErr := runErr
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
