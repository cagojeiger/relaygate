package relaygrpc

import (
	"context"
	"io"
	"sync"

	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc/metadata"
)

type blockingRelayStream struct {
	ctx     context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingRelayStream() *blockingRelayStream {
	return &blockingRelayStream{
		ctx:     context.Background(),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingRelayStream) Send(*relayv1.ConnectResponse) error {
	s.once.Do(func() { s.entered <- struct{}{} })
	<-s.release
	return nil
}

func (*blockingRelayStream) Recv() (*relayv1.ConnectRequest, error) { return nil, io.EOF }
func (*blockingRelayStream) SetHeader(metadata.MD) error            { return nil }
func (*blockingRelayStream) SendHeader(metadata.MD) error           { return nil }
func (*blockingRelayStream) SetTrailer(metadata.MD)                 {}
func (s *blockingRelayStream) Context() context.Context             { return s.ctx }
func (*blockingRelayStream) SendMsg(any) error                      { return nil }
func (*blockingRelayStream) RecvMsg(any) error                      { return io.EOF }
