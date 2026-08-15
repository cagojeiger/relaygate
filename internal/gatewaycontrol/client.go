package gatewaycontrol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	controlKeepaliveTime    = 10 * time.Second
	controlKeepaliveTimeout = 5 * time.Second
)

type Config struct {
	ClusterEpoch     string
	GatewayID        string
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
	GatewayGeneration uint64 `json:"gateway_generation,omitempty"`
}

func (s Status) Ready() bool {
	return s.State == StateRevalidated
}

type Client struct {
	config     Config
	logger     *slog.Logger
	instanceID string
	keepalive  keepalive.ClientParameters

	statusMu sync.RWMutex
	status   Status
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
	if instanceID == "" {
		return nil, fmt.Errorf("gateway instance ID is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	client := &Client{
		config:     config,
		logger:     logger.With("component", "gateway_control", "gateway_id", config.GatewayID, "gateway_instance_id", instanceID),
		instanceID: instanceID,
		keepalive: keepalive.ClientParameters{
			Time:    controlKeepaliveTime,
			Timeout: controlKeepaliveTimeout,
		},
		status: Status{
			GatewayID:         config.GatewayID,
			GatewayInstanceID: instanceID,
			State:             StateDisconnected,
		},
	}
	return client, nil
}

func (c Config) validate() error {
	if c.ClusterEpoch == "" {
		return fmt.Errorf("cluster epoch is required")
	}
	if c.GatewayID == "" {
		return fmt.Errorf("gateway ID is required")
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

func (c *Client) Status() Status {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

func (c *Client) Run(ctx context.Context) {
	defer c.setDisconnected()
	endpointIndex := 0
	for ctx.Err() == nil {
		endpoint := c.config.ControlEndpoints[endpointIndex]
		c.setConnecting(endpoint)
		revalidated, err := c.runEndpoint(ctx, endpoint)
		c.setDisconnected()
		if ctx.Err() != nil {
			return
		}
		if revalidated {
			c.logger.Warn("gateway control session ended", "endpoint", endpoint, "error", err)
		} else {
			c.logger.Debug("gateway control endpoint unavailable", "endpoint", endpoint, "error", err)
		}
		endpointIndex = (endpointIndex + 1) % len(c.config.ControlEndpoints)
		if !wait(ctx, c.config.RetryInterval) {
			return
		}
	}
}

func (c *Client) runEndpoint(ctx context.Context, endpoint string) (bool, error) {
	connection, err := grpc.NewClient(
		"passthrough:///"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(c.keepalive),
	)
	if err != nil {
		return false, fmt.Errorf("create control connection: %w", err)
	}
	defer connection.Close()
	if err := waitForReady(ctx, connection, c.config.ConnectTimeout); err != nil {
		return false, fmt.Errorf("connect to control endpoint: %w", err)
	}

	streamContext, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := controlv1.NewGatewayControlClient(connection).Connect(streamContext)
	if err != nil {
		return false, fmt.Errorf("open control stream: %w", err)
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{
		Hello: &controlv1.Hello{
			ClusterEpoch:      c.config.ClusterEpoch,
			GatewayId:         c.config.GatewayID,
			GatewayInstanceId: c.instanceID,
		},
	}}); err != nil {
		return false, fmt.Errorf("send gateway hello: %w", err)
	}
	response, err := receiveWithTimeout(streamContext, stream, c.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Errorf("receive control session: %w", err)
	}
	opened := response.GetSessionOpened()
	if err := c.validateSession(opened); err != nil {
		return false, err
	}
	session := opened.GetSession()
	c.setSyncing(endpoint, session, opened.GetGatewayGeneration())

	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: session},
	}}); err != nil {
		return false, fmt.Errorf("send gateway snapshot: %w", err)
	}
	response, err = receiveWithTimeout(streamContext, stream, c.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Errorf("receive snapshot acceptance: %w", err)
	}
	accepted := response.GetSnapshotAccepted()
	if accepted == nil || accepted.GetPresence() == controlv1.PresenceState_PRESENCE_STATE_UNSPECIFIED {
		return false, fmt.Errorf("control endpoint returned an invalid snapshot response")
	}
	c.setRevalidated(endpoint, session, opened.GetGatewayGeneration())
	c.logger.Info("gateway control session revalidated",
		"endpoint", endpoint,
		"authority_id", session.GetAuthorityId(),
		"control_session_id", session.GetControlSessionId(),
		"gateway_generation", opened.GetGatewayGeneration(),
		"presence", accepted.GetPresence().String(),
	)

	response, err = stream.Recv()
	if err != nil {
		return true, err
	}
	return true, fmt.Errorf("unexpected control response after snapshot acceptance: %T", response.GetMessage())
}

func (c *Client) validateSession(opened *controlv1.SessionOpened) error {
	if opened == nil || opened.GetSession() == nil {
		return fmt.Errorf("control endpoint did not open a session")
	}
	session := opened.GetSession()
	if session.GetClusterEpoch() != c.config.ClusterEpoch ||
		session.GetGatewayId() != c.config.GatewayID ||
		session.GetGatewayInstanceId() != c.instanceID ||
		session.GetAuthorityId() == "" ||
		session.GetControlSessionId() == "" ||
		opened.GetGatewayGeneration() == 0 {
		return fmt.Errorf("control endpoint returned a mismatched session")
	}
	return nil
}

func waitForReady(ctx context.Context, connection *grpc.ClientConn, timeout time.Duration) error {
	connectContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connection.Connect()
	for {
		state := connection.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("connection shut down")
		}
		if !connection.WaitForStateChange(connectContext, state) {
			if err := connectContext.Err(); err != nil {
				return err
			}
			return fmt.Errorf("connection state did not change")
		}
	}
}

type responseReceiver interface {
	Recv() (*controlv1.ControlResponse, error)
}

func receiveWithTimeout(ctx context.Context, receiver responseReceiver, timeout time.Duration) (*controlv1.ControlResponse, error) {
	type result struct {
		response *controlv1.ControlResponse
		err      error
	}
	completed := make(chan result, 1)
	go func() {
		response, err := receiver.Recv()
		completed <- result{response: response, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("control handshake timed out")
	case outcome := <-completed:
		return outcome.response, outcome.err
	}
}

func (c *Client) setConnecting(endpoint string) {
	c.replaceStatus(Status{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: c.instanceID,
		State:             StateConnecting,
		Endpoint:          endpoint,
	})
}

func (c *Client) setSyncing(endpoint string, session *controlv1.SessionRef, generation uint64) {
	c.replaceStatus(c.sessionStatus(StateSyncing, endpoint, session, generation))
}

func (c *Client) setRevalidated(endpoint string, session *controlv1.SessionRef, generation uint64) {
	c.replaceStatus(c.sessionStatus(StateRevalidated, endpoint, session, generation))
}

func (c *Client) sessionStatus(state State, endpoint string, session *controlv1.SessionRef, generation uint64) Status {
	return Status{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: c.instanceID,
		State:             state,
		Endpoint:          endpoint,
		AuthorityID:       session.GetAuthorityId(),
		ControlSessionID:  session.GetControlSessionId(),
		GatewayGeneration: generation,
	}
}

func (c *Client) setDisconnected() {
	c.replaceStatus(Status{
		GatewayID:         c.config.GatewayID,
		GatewayInstanceID: c.instanceID,
		State:             StateDisconnected,
	})
}

func (c *Client) replaceStatus(status Status) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status = status
}

func newInstanceID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate gateway instance ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
