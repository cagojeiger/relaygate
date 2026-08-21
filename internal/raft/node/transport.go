package raftnode

import (
	"context"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type serverAddress string

func (a serverAddress) Network() string { return "tcp" }
func (a serverAddress) String() string  { return string(a) }

type tcpStreamLayer struct {
	net.Listener
	advertise net.Addr
}

func (s *tcpStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return (&net.Dialer{Timeout: timeout}).DialContext(context.Background(), "tcp", string(address))
}

func (s *tcpStreamLayer) Addr() net.Addr { return s.advertise }
