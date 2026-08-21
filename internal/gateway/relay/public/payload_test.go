package relaygrpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	relayv1 "github.com/cagojeiger/relaygate/internal/gen/relay/v1"
	"google.golang.org/grpc/metadata"
)

const payloadTestTimeout = 100 * time.Millisecond

type gateRecordingRelayStream struct {
	ctx          context.Context
	cancel       context.CancelFunc
	blockSend    int32
	entered      chan struct{}
	releaseFirst chan struct{}
	sent         chan *relayv1.ConnectResponse
	sends        atomic.Int32
	once         sync.Once
}

func newGateRecordingRelayStream(blockFirst bool) *gateRecordingRelayStream {
	if blockFirst {
		return newNthSendGateRecordingRelayStream(1)
	}
	return newNthSendGateRecordingRelayStream(0)
}

func newNthSendGateRecordingRelayStream(blockSend int32) *gateRecordingRelayStream {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &gateRecordingRelayStream{
		ctx:          ctx,
		cancel:       cancel,
		blockSend:    blockSend,
		entered:      make(chan struct{}, 1),
		releaseFirst: make(chan struct{}),
		sent:         make(chan *relayv1.ConnectResponse, 256),
	}
	if blockSend == 0 {
		close(stream.releaseFirst)
	}
	return stream
}

func (s *gateRecordingRelayStream) Send(response *relayv1.ConnectResponse) error {
	if s.sends.Add(1) == s.blockSend && s.blockSend != 0 {
		s.once.Do(func() { s.entered <- struct{}{} })
		select {
		case <-s.releaseFirst:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.sent <- response
	return nil
}

func (*gateRecordingRelayStream) Recv() (*relayv1.ConnectRequest, error) { return nil, io.EOF }
func (*gateRecordingRelayStream) SetHeader(metadata.MD) error            { return nil }
func (*gateRecordingRelayStream) SendHeader(metadata.MD) error           { return nil }
func (*gateRecordingRelayStream) SetTrailer(metadata.MD)                 {}
func (s *gateRecordingRelayStream) Context() context.Context             { return s.ctx }
func (*gateRecordingRelayStream) SendMsg(any) error                      { return nil }
func (*gateRecordingRelayStream) RecvMsg(any) error                      { return io.EOF }

type recordingPayloadSessionManager struct {
	session clientsession.Session
	ended   chan clientsession.Ref
}

func (m *recordingPayloadSessionManager) Authenticate(string, string, string) (clientsession.Session, error) {
	return m.session, nil
}

func (m *recordingPayloadSessionManager) End(ref clientsession.Ref) {
	m.ended <- ref
}

type connectBlockingRelayStream struct {
	ctx               context.Context
	cancel            context.CancelFunc
	requests          chan *relayv1.ConnectRequest
	sent              chan *relayv1.ConnectResponse
	pipeOpenedEntered chan struct{}
	releasePipeOpened chan struct{}
}

func newConnectBlockingRelayStream() *connectBlockingRelayStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &connectBlockingRelayStream{
		ctx:               ctx,
		cancel:            cancel,
		requests:          make(chan *relayv1.ConnectRequest, 2),
		sent:              make(chan *relayv1.ConnectResponse, 2),
		pipeOpenedEntered: make(chan struct{}, 1),
		releasePipeOpened: make(chan struct{}),
	}
}

func (s *connectBlockingRelayStream) Send(response *relayv1.ConnectResponse) error {
	if response.GetPipeOpened() != nil {
		s.pipeOpenedEntered <- struct{}{}
		<-s.releasePipeOpened
	}
	s.sent <- response
	return nil
}

func (s *connectBlockingRelayStream) Recv() (*relayv1.ConnectRequest, error) {
	select {
	case request := <-s.requests:
		return request, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*connectBlockingRelayStream) SetHeader(metadata.MD) error  { return nil }
func (*connectBlockingRelayStream) SendHeader(metadata.MD) error { return nil }
func (*connectBlockingRelayStream) SetTrailer(metadata.MD)       {}
func (s *connectBlockingRelayStream) Context() context.Context   { return s.ctx }
func (*connectBlockingRelayStream) SendMsg(any) error            { return nil }
func (*connectBlockingRelayStream) RecvMsg(any) error            { return nil }
