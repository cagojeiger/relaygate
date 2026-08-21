package gatewaycontrol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	controlv1 "github.com/cagojeiger/relaygate/internal/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func (c *Client) Run(ctx context.Context) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	if c.provider == nil {
		c.mu.Unlock()
		c.stop(ErrSnapshotProviderMissing)
		return
	}
	c.running = true
	c.mu.Unlock()
	defer c.stop(ctx.Err())

	endpointIndex := 0
	for ctx.Err() == nil {
		endpoint := c.config.ControlEndpoints[endpointIndex]
		c.setConnecting(endpoint)
		revalidated, err := c.runEndpoint(ctx, endpoint)
		c.endCurrentSession(err)
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
		grpc.WithDisableRetry(),
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
	controlClient := controlv1.NewGatewayControlClient(connection)
	stream, err := controlClient.Connect(streamContext)
	if err != nil {
		return false, fmt.Errorf("open control stream: %w", err)
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_Hello{Hello: &controlv1.Hello{
		ClusterEpoch: c.config.ClusterEpoch, GatewayId: c.config.GatewayID, GatewayInstanceId: c.instanceID, RelayAddress: c.config.RelayAddress,
	}}}); err != nil {
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
	c.setSyncing(endpoint, session)
	snapshot, err := c.currentSnapshot()
	if err != nil {
		return false, err
	}
	if err := stream.Send(&controlv1.ControlRequest{Message: &controlv1.ControlRequest_FullSnapshot{
		FullSnapshot: &controlv1.FullSnapshot{Session: cloneSessionRef(session), Bindings: snapshot},
	}}); err != nil {
		return false, fmt.Errorf("send gateway snapshot: %w", err)
	}
	response, err = receiveWithTimeout(streamContext, stream, c.config.ConnectTimeout)
	if err != nil {
		return false, fmt.Errorf("receive snapshot acceptance: %w", err)
	}
	accepted := response.GetSnapshotAccepted()
	bindingCount := uint32(len(snapshot)) //nolint:gosec // currentSnapshot caps this slice at 512 entries.
	if accepted == nil || accepted.GetBindingCount() != bindingCount {
		return false, fmt.Errorf("control endpoint returned an invalid snapshot response")
	}
	c.setRevalidated(endpoint, session, controlClient)
	c.logger.Info("gateway control session revalidated", "endpoint", endpoint, "authority_id", session.GetAuthorityId(), "control_session_id", session.GetControlSessionId(), "binding_count", accepted.GetBindingCount())
	return true, c.serveMutations(streamContext, stream, session)
}

func (c *Client) currentSnapshot() ([]*controlv1.LiveBinding, error) {
	c.mu.Lock()
	provider := c.provider
	c.mu.Unlock()
	if provider == nil {
		return nil, ErrSnapshotProviderMissing
	}
	bindings := provider.LiveBindings()
	if len(bindings) > routing.MaxListenerBindingsPerGateway {
		return nil, fmt.Errorf("%w: snapshot has %d live bindings", ErrBindingCapacity, len(bindings))
	}
	seen := make(map[routing.BindingKey]struct{}, len(bindings))
	for index, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("snapshot binding %d: %w", index, err)
		}
		if binding.Ref.GatewayID != c.config.GatewayID || binding.Ref.GatewayInstanceID != c.instanceID {
			return nil, fmt.Errorf("snapshot binding %d does not belong to this gateway instance", index)
		}
		if _, exists := seen[binding.Key]; exists {
			return nil, fmt.Errorf("snapshot binding %d duplicates a key", index)
		}
		seen[binding.Key] = struct{}{}
	}
	sort.Slice(bindings, func(i, j int) bool { return bindingKeyLess(bindings[i].Key, bindings[j].Key) })
	wire := make([]*controlv1.LiveBinding, 0, len(bindings))
	for _, binding := range bindings {
		wire = append(wire, liveBindingToProto(binding, false))
	}
	return wire, nil
}

type receivedResponse struct {
	response *controlv1.ControlResponse
	err      error
}

func receiveResponses(ctx context.Context, stream controlv1.GatewayControl_ConnectClient) <-chan receivedResponse {
	received := make(chan receivedResponse, 1)
	go func() {
		defer close(received)
		for {
			response, err := stream.Recv()
			select {
			case received <- receivedResponse{response: response, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return received
}

func nextControlResponse(received receivedResponse, ok bool) (*controlv1.ControlResponse, error) {
	if !ok {
		return nil, fmt.Errorf("control response stream ended")
	}
	if received.err != nil {
		return nil, received.err
	}
	return received.response, nil
}

func (c *Client) validateSession(opened *controlv1.SessionOpened) error {
	if opened == nil || opened.GetSession() == nil {
		return fmt.Errorf("control endpoint did not open a session")
	}
	session := opened.GetSession()
	if session.GetClusterEpoch() != c.config.ClusterEpoch || session.GetGatewayId() != c.config.GatewayID || session.GetGatewayInstanceId() != c.instanceID ||
		session.GetAuthorityId() == "" || session.GetControlSessionId() == "" ||
		len(session.GetAuthorityId()) > routing.MaxIdentityBytes || len(session.GetControlSessionId()) > routing.MaxIdentityBytes {
		return fmt.Errorf("control endpoint returned a mismatched session")
	}
	return nil
}

func newInstanceID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate gateway instance ID: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
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
	go func() { response, err := receiver.Recv(); completed <- result{response, err} }()
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
