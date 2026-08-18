package relaygate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cagojeiger/relaygate/internal/app/config"
	clientruntime "github.com/cagojeiger/relaygate/internal/gateway/access/runtime"
	gatewaycontrol "github.com/cagojeiger/relaygate/internal/gateway/control/client"
	gatewayrelay "github.com/cagojeiger/relaygate/internal/gateway/relay/peer"
	relaygrpc "github.com/cagojeiger/relaygate/internal/gateway/relay/public"
	localbinding "github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
)

type gatewayRuntime struct {
	config          config.Config
	clients         *clientruntime.Runtime
	controlClient   *gatewaycontrol.Client
	bindings        *localbinding.Manager
	peerClient      *gatewayrelay.Client
	openings        *opening.Manager
	peerServer      *gatewayrelay.Server
	publicServer    *relaygrpc.Server
	cancelControl   context.CancelFunc
	controlDone     chan struct{}
	stopControlOnce sync.Once
	shutdownOnce    sync.Once
	shutdownErr     error
}

func startGatewayRuntime(appConfig config.Config, logger *slog.Logger) (_ *gatewayRuntime, resultErr error) {
	runtime := &gatewayRuntime{config: appConfig}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, runtime.shutdown())
		}
	}()

	clients, err := clientruntime.New(appConfig)
	if err != nil {
		return nil, err
	}
	runtime.clients = clients

	controlClient, err := gatewaycontrol.New(gatewaycontrol.Config{
		ClusterEpoch:     appConfig.Control.ClusterEpoch,
		GatewayID:        appConfig.Gateway.ID,
		RelayAddress:     appConfig.InternalRelay.AdvertiseAddress,
		ControlEndpoints: appConfig.Gateway.ControlEndpoints,
		ConnectTimeout:   appConfig.Gateway.ConnectTimeout.Value(),
		RetryInterval:    appConfig.Gateway.RetryInterval.Value(),
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("configure gateway control client: %w", err)
	}
	runtime.controlClient = controlClient

	bindings, err := localbinding.New(
		appConfig.Gateway.ID,
		controlClient.Status().GatewayInstanceID,
		appConfig.Relay.MaxListenerBindings,
		controlClient,
		clients.Sessions(),
	)
	if err != nil {
		return nil, fmt.Errorf("configure listener binding runtime: %w", err)
	}
	runtime.bindings = bindings
	if err := controlClient.AttachSnapshotProvider(bindings); err != nil {
		return nil, fmt.Errorf("attach current binding snapshot provider: %w", err)
	}
	if err := clients.AttachBindings(bindings); err != nil {
		return nil, fmt.Errorf("attach listener binding runtime: %w", err)
	}

	peerClient, err := gatewayrelay.NewClient(
		appConfig.InternalRelay.ConnectTimeout.Value(),
		appConfig.Relay.OpenTimeout.Value(),
		appConfig.Relay.MaxPipes,
	)
	if err != nil {
		return nil, fmt.Errorf("configure Gateway relay client: %w", err)
	}
	runtime.peerClient = peerClient

	openings, err := opening.New(opening.Config{
		ClusterEpoch: appConfig.Control.ClusterEpoch,
		MaxPipes:     appConfig.Relay.MaxPipes,
		OpenTimeout:  appConfig.Relay.OpenTimeout.Value(),
	}, controlClient, bindings, peerClient)
	if err != nil {
		return nil, fmt.Errorf("configure Open runtime: %w", err)
	}
	runtime.openings = openings
	if err := clients.AttachPipes(openings); err != nil {
		return nil, fmt.Errorf("attach Open runtime: %w", err)
	}

	peerService, err := gatewayrelay.NewService(openings, appConfig.Relay.OpenTimeout.Value(), appConfig.Relay.MaxPipes)
	if err != nil {
		return nil, fmt.Errorf("configure Gateway relay service: %w", err)
	}
	peerServer, err := gatewayrelay.Start(context.Background(), gatewayrelay.Config{
		BindAddress: appConfig.InternalRelay.BindAddress,
		OpenTimeout: appConfig.Relay.OpenTimeout.Value(),
		MaxPipes:    appConfig.Relay.MaxPipes,
	}, peerService)
	if err != nil {
		return nil, fmt.Errorf("start Gateway relay service: %w", err)
	}
	runtime.peerServer = peerServer

	controlContext, cancelControl := context.WithCancel(context.Background())
	runtime.cancelControl = cancelControl
	runtime.controlDone = make(chan struct{})
	go func() {
		controlClient.Run(controlContext)
		close(runtime.controlDone)
	}()

	publicService, err := relaygrpc.NewService(
		clients.Sessions(),
		bindings,
		openings,
		appConfig.Relay.AuthenticationTimeout.Value(),
		appConfig.Relay.OpenTimeout.Value(),
		appConfig.Relay.MaxPipes,
	)
	if err != nil {
		return nil, fmt.Errorf("configure relay service: %w", err)
	}
	publicServer, err := relaygrpc.Start(context.Background(), relaygrpc.Config{
		BindAddress:          appConfig.Relay.BindAddress,
		MaxConcurrentStreams: appConfig.Relay.MaxClientSessions,
	}, publicService)
	if err != nil {
		return nil, err
	}
	runtime.publicServer = publicServer
	return runtime, nil
}

func (r *gatewayRuntime) stopControl() {
	if r == nil {
		return
	}
	r.stopControlOnce.Do(func() {
		if r.cancelControl != nil {
			r.cancelControl()
			<-r.controlDone
		}
	})
}

func (r *gatewayRuntime) shutdown() error {
	if r == nil {
		return nil
	}
	r.shutdownOnce.Do(func() {
		r.stopControl()
		if r.clients != nil {
			r.clients.Close()
		}
		if r.publicServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.config.Relay.ShutdownTimeout.Value())
			r.shutdownErr = errors.Join(r.shutdownErr, r.publicServer.Shutdown(ctx))
			cancel()
		}
		if r.peerClient != nil {
			r.peerClient.Close()
		}
		if r.peerServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.config.InternalRelay.ShutdownTimeout.Value())
			r.shutdownErr = errors.Join(r.shutdownErr, r.peerServer.Shutdown(ctx))
			cancel()
		}
		if r.openings != nil {
			r.openings.Close()
		}
		if r.bindings != nil {
			r.bindings.Close()
		}
	})
	return r.shutdownErr
}

func (r *gatewayRuntime) reload(candidate config.Config) (clientruntime.ReloadResult, error) {
	return r.clients.Apply(candidate)
}

func (r *gatewayRuntime) publicErrors() <-chan error {
	if r == nil || r.publicServer == nil {
		return nil
	}
	return r.publicServer.Errors()
}

func (r *gatewayRuntime) peerErrors() <-chan error {
	if r == nil || r.peerServer == nil {
		return nil
	}
	return r.peerServer.Errors()
}

func (r *gatewayRuntime) logFields() []any {
	if r == nil {
		return nil
	}
	status := r.controlClient.Status()
	return []any{
		"relay_address", r.publicServer.Address(),
		"internal_relay_address", r.peerServer.Address(),
		"gateway_id", r.config.Gateway.ID,
		"gateway_instance_id", status.GatewayInstanceID,
		"auth_revision", r.clients.Revision(),
	}
}
