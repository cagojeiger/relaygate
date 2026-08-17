package relaygate

import (
	"context"
	"fmt"
	"os"
)

type eventLoop struct {
	reloadSignals      <-chan os.Signal
	adminErrors        <-chan error
	controlErrors      <-chan error
	relayErrors        <-chan error
	gatewayRelayErrors <-chan error
	onShutdown         func()
	onReload           func()
}

func (loop eventLoop) wait(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			loop.onShutdown()
			return nil
		case <-loop.reloadSignals:
			loop.onReload()
		case err := <-loop.adminErrors:
			return serverStopped("admin server", err)
		case err := <-loop.controlErrors:
			return serverStopped("control server", err)
		case err := <-loop.relayErrors:
			return serverStopped("relay server", err)
		case err := <-loop.gatewayRelayErrors:
			return serverStopped("gateway relay server", err)
		}
	}
}

func serverStopped(name string, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("%s stopped unexpectedly", name)
}
