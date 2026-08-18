package membership

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	operatorv1 "github.com/cagojeiger/relaygate/internal/gen/operator/v1"
)

const clientTarget = "passthrough:///relaygate-membership"

// Client invokes the membership operator API through one exact Unix socket.
type Client struct {
	connection *grpc.ClientConn
	service    operatorv1.MembershipClient

	closeOnce sync.Once
	closeErr  error
}

// Dial connects to path over a Unix domain socket and waits until gRPC is ready.
func Dial(ctx context.Context, path string) (*Client, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("membership operator socket path is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("dial membership operator: %w", err)
	}

	connection, err := grpc.NewClient(
		clientTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create membership operator client: %w", err)
	}
	connection.Connect()
	for {
		state := connection.GetState()
		if state == connectivity.Ready {
			return &Client{
				connection: connection,
				service:    operatorv1.NewMembershipClient(connection),
			}, nil
		}
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("dial membership operator: connection shut down")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("dial membership operator socket %q: %w", path, ctx.Err())
		}
	}
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.connection.Close()
	})
	return c.closeErr
}

// List returns the leader's current sorted membership configuration.
func (c *Client) List(ctx context.Context) (Result, error) {
	response, err := c.service.List(ctx, &operatorv1.ListRequest{})
	if err != nil {
		return Result{}, err
	}
	return resultFromProto(response), nil
}

// Add adds or promotes the exact nodeID and address as a voter.
func (c *Client) Add(ctx context.Context, nodeID, address string) (Result, error) {
	response, err := c.service.Add(ctx, &operatorv1.AddRequest{NodeId: nodeID, Address: address})
	if err != nil {
		return Result{}, err
	}
	return resultFromProto(response), nil
}

// Remove removes the member with the exact nodeID.
func (c *Client) Remove(ctx context.Context, nodeID string) (Result, error) {
	response, err := c.service.Remove(ctx, &operatorv1.RemoveRequest{NodeId: nodeID})
	if err != nil {
		return Result{}, err
	}
	return resultFromProto(response), nil
}

func resultFromProto(response *operatorv1.MembershipResult) Result {
	result := Result{
		Changed: response.GetChanged(),
		Members: make([]Member, 0, len(response.GetMembers())),
	}
	for _, member := range response.GetMembers() {
		result.Members = append(result.Members, Member{
			NodeID:   member.GetNodeId(),
			Address:  member.GetAddress(),
			Suffrage: member.GetSuffrage(),
		})
	}
	return result
}
