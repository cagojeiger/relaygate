package gatewayrelay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cagojeiger/relaygate/internal/authority"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/localbinding"
	"github.com/cagojeiger/relaygate/internal/opening"
)

var (
	ErrInvalid      = errors.New("invalid Gateway relay configuration or request")
	ErrClosed       = errors.New("gateway relay is closed")
	ErrBackpressure = errors.New("gateway relay backpressure exhausted")
)

// Config owns only the internal Gateway-to-Gateway relay listener and its
// bounded volatile work. The transport is intentionally trusted-development
// plaintext until an authenticated Gateway transport is designed.
type Config struct {
	BindAddress string
	OpenTimeout time.Duration
	MaxPipes    uint32
}

func (c Config) validate() error {
	if c.BindAddress == "" {
		return fmt.Errorf("%w: bind address is required", ErrInvalid)
	}
	if c.OpenTimeout <= 0 {
		return fmt.Errorf("%w: Open timeout must be positive", ErrInvalid)
	}
	if c.MaxPipes == 0 {
		return fmt.Errorf("%w: maximum Pipes must be positive", ErrInvalid)
	}
	return nil
}

// Owner is the exact owner-Gateway boundary. It deliberately exposes no Raft,
// public Relay stream, or SDK types.
type Owner interface {
	OpenForwarded(context.Context, authority.OpenContext, localbinding.CallerEndpoint) (opening.Result, error)
	ActivatePipe(clientsession.Ref, string) bool
	RelayPayload(context.Context, clientsession.Ref, string, []byte) error
	ClosePipe(clientsession.Ref, string) bool
}
