package gatewayrelay

import (
	"fmt"
	"sync"

	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxIdlePeerConnections uint32 = 64

type peerConnectionKey struct {
	gatewayID         string
	gatewayInstanceID string
	address           string
}

type pooledConnection struct {
	key        peerConnectionKey
	connection *grpc.ClientConn
	references uint32
	retiring   bool
	lastUsed   uint64
}

type connectionLease struct {
	client *Client
	entry  *pooledConnection
	once   sync.Once
}

func newPeerConnection(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		"passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGatewayMessageBytes),
			grpc.MaxCallSendMsgSize(maxGatewayMessageBytes),
			grpc.WaitForReady(false),
		),
	)
}

func connectionKey(open routing.OpenContext) peerConnectionKey {
	return peerConnectionKey{
		gatewayID:         open.Binding.Ref.GatewayID,
		gatewayInstanceID: open.Binding.Ref.GatewayInstanceID,
		address:           open.OwnerRelayAddress,
	}
}

func (c *Client) acquireConnection(open routing.OpenContext) (*connectionLease, error) {
	key := connectionKey(open)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if current := c.connections[key.gatewayID]; current != nil && current.key == key {
		current.references++
		return &connectionLease{client: c, entry: current}, nil
	}
	connection, err := c.newConnection(key.address)
	if err != nil {
		return nil, fmt.Errorf("create shared owner Gateway connection: %w", err)
	}
	entry := &pooledConnection{key: key, connection: connection, references: 1}
	if previous := c.connections[key.gatewayID]; previous != nil {
		previous.retiring = true
		if previous.references == 0 {
			_ = previous.connection.Close()
			delete(c.allConnections, previous)
		}
	}
	c.connections[key.gatewayID] = entry
	c.allConnections[entry] = struct{}{}
	return &connectionLease{client: c, entry: entry}, nil
}

func (l *connectionLease) connection() *grpc.ClientConn {
	return l.entry.connection
}

func (l *connectionLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() { l.client.releaseConnection(l.entry) })
}

func (c *Client) releaseConnection(entry *pooledConnection) {
	c.mu.Lock()
	if entry.references == 0 {
		c.mu.Unlock()
		return
	}
	entry.references--
	shouldClose := c.closed || entry.retiring
	closeNow := shouldClose && entry.references == 0
	var idleEvictions []*grpc.ClientConn
	if closeNow {
		if c.connections[entry.key.gatewayID] == entry {
			delete(c.connections, entry.key.gatewayID)
		}
		delete(c.allConnections, entry)
	} else if entry.references == 0 {
		c.connectionSequence++
		entry.lastUsed = c.connectionSequence
		idleEvictions = c.evictIdleConnectionsLocked()
	}
	c.mu.Unlock()
	if closeNow {
		_ = entry.connection.Close()
	}
	for _, connection := range idleEvictions {
		_ = connection.Close()
	}
}

func (c *Client) evictIdleConnectionsLocked() []*grpc.ClientConn {
	var idle []*pooledConnection
	for entry := range c.allConnections {
		if !entry.retiring && entry.references == 0 {
			idle = append(idle, entry)
		}
	}
	var evicted []*grpc.ClientConn
	keep := c.maxIdleConnections
	for len(idle) > 0 && keep > 0 {
		newestIndex := 0
		for index := 1; index < len(idle); index++ {
			if idle[index].lastUsed > idle[newestIndex].lastUsed {
				newestIndex = index
			}
		}
		idle = append(idle[:newestIndex], idle[newestIndex+1:]...)
		keep--
	}
	for _, oldest := range idle {
		if c.connections[oldest.key.gatewayID] == oldest {
			delete(c.connections, oldest.key.gatewayID)
		}
		delete(c.allConnections, oldest)
		evicted = append(evicted, oldest.connection)
	}
	return evicted
}

func (c *Client) closeConnections() {
	c.mu.Lock()
	connections := make([]*grpc.ClientConn, 0, len(c.allConnections))
	for entry := range c.allConnections {
		connections = append(connections, entry.connection)
	}
	clear(c.connections)
	clear(c.allConnections)
	c.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}
