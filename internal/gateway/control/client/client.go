package gatewaycontrol

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/control/transport"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc/keepalive"
)

var (
	ErrControlUnavailable      = errors.New("gateway control is not revalidated")
	ErrBindingConflict         = fmt.Errorf("live binding conflict: %w", routing.ErrConflict)
	ErrBindingCapacity         = fmt.Errorf("gateway listener binding capacity reached: %w", routing.ErrCapacity)
	ErrClientClosed            = errors.New("gateway control client is closed")
	ErrInvalidMutation         = errors.New("invalid binding mutation")
	ErrSnapshotProviderMissing = errors.New("gateway control snapshot provider is required before Run")
)

// SnapshotProvider returns this Gateway's current local bindings. It is read
// for every new control session: a reconnect never replays an older control
// operation or asks the authority to reconcile history.
type SnapshotProvider interface {
	LiveBindings() []routing.LiveBinding
}

type Config struct {
	ClusterEpoch     string
	GatewayID        string
	RelayAddress     string
	ControlEndpoints []string
	ConnectTimeout   time.Duration
	RetryInterval    time.Duration
}

type State string

const (
	StateDisconnected State = "Disconnected"
	StateConnecting   State = "Connecting"
	StateSyncing      State = "Syncing"
	StateRevalidated  State = "Revalidated"
)

type Status struct {
	GatewayID         string `json:"gateway_id"`
	GatewayInstanceID string `json:"gateway_instance_id"`
	State             State  `json:"state"`
	Endpoint          string `json:"endpoint,omitempty"`
	AuthorityID       string `json:"authority_id,omitempty"`
	ControlSessionID  string `json:"control_session_id,omitempty"`
}

func (s Status) Ready() bool { return s.State == StateRevalidated }

type mutationKind uint8

const (
	mutationDeclare mutationKind = iota + 1
	mutationWithdraw
)

type pendingMutation struct {
	kind    mutationKind
	ctx     context.Context //nolint:containedctx // The exact current-session mutation owns the caller deadline.
	done    chan error
	binding routing.LiveBinding
}

const maxPendingMutations = 2 * routing.MaxListenerBindingsPerGateway

type Client struct {
	config     Config
	logger     *slog.Logger
	instanceID string
	keepalive  keepalive.ClientParameters

	mu       sync.Mutex
	status   Status
	provider SnapshotProvider
	running  bool
	// admissionClient and admissionSession are published atomically with a
	// Revalidated status and cleared whenever that current session ends.
	admissionClient  controlv1.GatewayControlClient
	admissionSession *controlv1.SessionRef
	queue            []*pendingMutation
	active           *pendingMutation
	wake             chan struct{}
	stopped          bool
}

func New(config Config, logger *slog.Logger) (*Client, error) {
	instanceID, err := newInstanceID()
	if err != nil {
		return nil, err
	}
	return newClient(config, logger, instanceID)
}

func newClient(config Config, logger *slog.Logger, instanceID string) (*Client, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := routing.ValidateIdentity("gateway_instance_id", instanceID); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		config:     config,
		logger:     logger.With("component", "gateway_control", "gateway_id", config.GatewayID, "gateway_instance_id", instanceID),
		instanceID: instanceID,
		keepalive: keepalive.ClientParameters{
			Time:    controltransport.KeepaliveTime,
			Timeout: controltransport.KeepaliveTimeout,
		},
		status: Status{GatewayID: config.GatewayID, GatewayInstanceID: instanceID, State: StateDisconnected},
		wake:   make(chan struct{}, 1),
	}, nil
}

func (c Config) validate() error {
	if err := routing.ValidateIdentity("cluster_epoch", c.ClusterEpoch); err != nil {
		return err
	}
	if err := routing.ValidateIdentity("gateway_id", c.GatewayID); err != nil {
		return err
	}
	if err := routing.ValidateRelayAddress(c.RelayAddress); err != nil {
		return fmt.Errorf("relay address: %w", err)
	}
	if len(c.ControlEndpoints) == 0 {
		return fmt.Errorf("at least one control endpoint is required")
	}
	for index, endpoint := range c.ControlEndpoints {
		if endpoint == "" {
			return fmt.Errorf("control endpoint %d is empty", index)
		}
	}
	if c.ConnectTimeout <= 0 || c.RetryInterval <= 0 {
		return fmt.Errorf("connect timeout and retry interval must be positive")
	}
	return nil
}

// AttachSnapshotProvider must happen before Run. The provider is deliberately
// not copied: every reconnect takes a fresh declaration of local truth.
func (c *Client) AttachSnapshotProvider(provider SnapshotProvider) error {
	if provider == nil {
		return ErrSnapshotProviderMissing
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.stopped {
		return fmt.Errorf("%w: snapshot provider cannot change after Run", ErrClientClosed)
	}
	c.provider = provider
	return nil
}

func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// CurrentSession returns only a current, revalidated control session. It
// never returns a last-known session across reconnect/failover.
func (c *Client) CurrentSession() (controlmodel.SessionRef, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status.State != StateRevalidated || c.admissionSession == nil {
		return controlmodel.SessionRef{}, false
	}
	return controlmodel.SessionRef{
		ClusterEpoch:      c.admissionSession.GetClusterEpoch(),
		AuthorityID:       c.admissionSession.GetAuthorityId(),
		ControlSessionID:  c.admissionSession.GetControlSessionId(),
		GatewayID:         c.admissionSession.GetGatewayId(),
		GatewayInstanceID: c.admissionSession.GetGatewayInstanceId(),
	}, true
}

// Declare sends one exact live binding on the current control session. It is
// intentionally fail-fast while disconnected; reconnect is represented by a
// fresh full snapshot from SnapshotProvider, never mutation replay.
