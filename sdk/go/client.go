package relaygate

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	sendQueueCapacity        = 32
	offerQueueCapacity       = 32
	pipePayloadQueueCapacity = 32
	maxListeners             = 512
	maxPendingOffers         = 1024
	maxPendingOpens          = 1024
	maxPipes                 = 1024
	maxReceivedPayloads      = 1024
	maxIdentityBytes         = 128
	maxEndpointBytes         = 1024
	maxPayloadBytes          = 60 * 1024
	openCancelDrainTimeout   = 500 * time.Millisecond
)

var (
	errExplicitClose  = errors.New("relaygate: explicit close")
	errProtocol       = errors.New("relaygate: invalid Relay protocol message")
	errCapacity       = errors.New("relaygate: SDK capacity reached")
	errAlreadyDecided = errors.New("relaygate: offer already decided")
)

type authResult struct {
	session Session
	err     error
}

type bindingOperation uint8

const (
	bindingBind bindingOperation = iota + 1
	bindingUnbind
)

type bindingCall struct {
	kind     bindingOperation
	endpoint string
	target   string
	id       string
	result   chan bindingResult
}

type bindingResult struct {
	listener *Listener
	err      error
}

type openCall struct {
	requestID          string
	endpoint           string
	target             string
	result             chan openResult
	mu                 sync.Mutex
	abandoned          bool
	cancelRequested    bool
	cancelAcknowledged bool
	cancelWasPending   bool
	outcomeReceived    bool
	outcome            openTombstone
	reserved           bool
	retired            chan struct{}
	retireOnce         sync.Once
}

type openResult struct {
	pipe *Pipe
	err  error
}

type closeCall struct {
	pipe         *Pipe
	result       chan error
	terminalSeen bool
}

type openOutcomeKind uint8

const (
	openOutcomeOpened openOutcomeKind = iota + 1
	openOutcomeFailed
	openOutcomeUnknown
	openOutcomeRejected
)

type openTombstone struct {
	endpoint   string
	target     string
	kind       openOutcomeKind
	attemptID  string
	pipeID     string
	failure    OpenFailure
	cancelAck  bool
	wasPending bool
}

type bindingRecord struct {
	id       string
	endpoint string
	target   string
	unbound  bool
}

type offerTombstone struct {
	pipeID          string
	decisionFailure relayv1.ListenerDecisionFailure
}

type sendCommand struct {
	ctx     context.Context //nolint:containedctx // The queued command preserves the exact caller deadline.
	request *relayv1.ConnectRequest
	result  chan error
	state   *atomic.Uint32
}

const (
	sendQueued uint32 = iota + 1
	sendWriting
	sendCompleted
	sendCanceled
)

type sendUncertainError struct{ cause error }

func (e *sendUncertainError) Error() string {
	return fmt.Sprintf("relaygate: send outcome uncertain: %v", e.cause)
}
func (e *sendUncertainError) Unwrap() error { return e.cause }

// Client owns one authenticated Relay.Connect stream.
type Client struct {
	ctx    context.Context //nolint:containedctx // The context is the Client lifetime.
	cancel context.CancelCauseFunc
	conn   *grpc.ClientConn
	stream grpc.BidiStreamingClient[relayv1.ConnectRequest, relayv1.ConnectResponse]

	sendQueue chan sendCommand
	pipeSlots chan struct{}
	done      chan struct{}
	tasks     sync.WaitGroup
	auth      chan authResult

	mu               sync.Mutex
	session          Session
	authenticated    bool
	expectedClientID string
	expectedAPIKeyID string
	listeners        map[string]*Listener
	bindingRecords   map[string]bindingRecord
	bindingHistory   []string
	offers           map[string]*Offer
	offerTombstones  map[string]offerTombstone
	offerHistory     []string
	opens            map[string]*openCall
	openTombstones   map[string]openTombstone
	openHistory      []string
	pipes            map[string]*Pipe
	pipeTombstones   map[string]*Pipe
	pipeHistory      []string
	pendingBinding   *bindingCall
	closeCalls       map[string]*closeCall
	closeTombstones  map[string]bool
	closeHistory     []string
	finalErr         error

	bindingMu sync.Mutex
}

// Connect opens and authenticates one Relay stream. The supplied context bounds
// setup only; Close or a stream/session failure ends the returned Client.
func Connect(ctx context.Context, config Config) (*Client, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	var transport credentials.TransportCredentials
	switch {
	case config.Insecure:
		transport = insecure.NewCredentials()
	case config.TLSConfig != nil:
		transport = credentials.NewTLS(config.TLSConfig.Clone())
	default:
		transport = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(transport))
	if err != nil {
		return nil, fmt.Errorf("relaygate: create gRPC client: %w", err)
	}

	clientCtx, cancel := context.WithCancelCause(context.Background())
	stream, err := relayv1.NewRelayClient(conn).Connect(clientCtx) //nolint:contextcheck // Client lifetime is intentionally detached from setup.
	if err != nil {
		cancel(err)
		_ = conn.Close()
		return nil, fmt.Errorf("relaygate: open Relay stream: %w", err)
	}
	c := &Client{
		ctx:              clientCtx,
		cancel:           cancel,
		conn:             conn,
		stream:           stream,
		sendQueue:        make(chan sendCommand, sendQueueCapacity),
		pipeSlots:        make(chan struct{}, maxPipes),
		done:             make(chan struct{}),
		auth:             make(chan authResult, 1),
		expectedClientID: config.ClientID,
		expectedAPIKeyID: config.APIKeyID,
		listeners:        make(map[string]*Listener),
		bindingRecords:   make(map[string]bindingRecord),
		offers:           make(map[string]*Offer),
		offerTombstones:  make(map[string]offerTombstone),
		opens:            make(map[string]*openCall),
		openTombstones:   make(map[string]openTombstone),
		pipes:            make(map[string]*Pipe),
		pipeTombstones:   make(map[string]*Pipe),
		closeCalls:       make(map[string]*closeCall),
		closeTombstones:  make(map[string]bool),
	}
	c.tasks.Add(2)
	go c.runSender()
	go c.runReceiver()
	go c.supervise()

	authenticate := &relayv1.Authenticate{
		ClientId: config.ClientID,
		ApiKeyId: config.APIKeyID,
		ApiKey:   config.apiKey,
	}
	request := &relayv1.ConnectRequest{Message: &relayv1.ConnectRequest_Authenticate{Authenticate: authenticate}}
	config.apiKey = ""
	err = c.send(ctx, request)
	authenticate.ApiKey = ""
	if err != nil {
		c.stop(err)
		<-c.done
		return nil, fmt.Errorf("relaygate: send authentication: %w", err)
	}

	select {
	case result := <-c.auth:
		if result.err != nil {
			c.stop(result.err)
			<-c.done
			return nil, result.err
		}
		return c, nil
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, ErrClientClosed
	case <-ctx.Done():
		c.stop(ctx.Err())
		<-c.done
		return nil, ctx.Err()
	}
}

func validateConfig(config Config) error {
	if config.Address == "" || config.ClientID == "" || config.APIKeyID == "" || config.apiKey == "" {
		return fmt.Errorf("relaygate: Address, ClientID, APIKeyID, and APIKey are required")
	}
	if len(config.ClientID) > maxIdentityBytes || len(config.APIKeyID) > maxIdentityBytes {
		return fmt.Errorf("relaygate: client credential identity is too long")
	}
	if config.TLSConfig != nil && config.Insecure {
		return fmt.Errorf("relaygate: TLSConfig and Insecure are mutually exclusive")
	}
	if config.Insecure && !isLoopbackTarget(config.Address) {
		return fmt.Errorf("relaygate: Insecure transport is limited to loopback addresses")
	}
	return nil
}

func isLoopbackTarget(target string) bool {
	if parsed, err := url.Parse(target); err == nil && parsed.Scheme != "" {
		target = strings.TrimPrefix(parsed.Path, "/")
		if parsed.Host != "" {
			target = parsed.Host
		}
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") || strings.EqualFold(host, "localhost.") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Client) Session() Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

func (c *Client) Done() <-chan struct{} { return c.done }

func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finalErr
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.stop(errExplicitClose)
	<-c.done
	return nil
}

func (c *Client) send(ctx context.Context, request *relayv1.ConnectRequest) error {
	command, err := c.enqueue(ctx, request)
	if err != nil {
		return err
	}
	return c.awaitSend(ctx, command)
}

func (c *Client) enqueue(ctx context.Context, request *relayv1.ConnectRequest) (sendCommand, error) {
	if ctx == nil || request == nil {
		return sendCommand{}, fmt.Errorf("relaygate: invalid send request")
	}
	if err := ctx.Err(); err != nil {
		return sendCommand{}, err
	}
	select {
	case <-c.done:
		return sendCommand{}, c.terminalError()
	default:
	}
	state := &atomic.Uint32{}
	state.Store(sendQueued)
	command := sendCommand{ctx: ctx, request: request, result: make(chan error, 1), state: state}
	select {
	case c.sendQueue <- command:
		return command, nil
	case <-ctx.Done():
		return sendCommand{}, ctx.Err()
	case <-c.done:
		return sendCommand{}, c.terminalError()
	}
}

func (c *Client) awaitSend(ctx context.Context, command sendCommand) error {
	for {
		select {
		case err := <-command.result:
			if err != nil {
				return &sendUncertainError{cause: err}
			}
			return nil
		case <-ctx.Done():
			if command.state.CompareAndSwap(sendQueued, sendCanceled) {
				return ctx.Err()
			}
			if command.state.Load() == sendCanceled {
				return ctx.Err()
			}
			c.stop(ctx.Err())
			<-c.done
			return &sendUncertainError{cause: ctx.Err()}
		case <-c.done:
			return &sendUncertainError{cause: c.terminalError()}
		}
	}
}

func (c *Client) runSender() {
	defer c.tasks.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case command := <-c.sendQueue:
			if !command.state.CompareAndSwap(sendQueued, sendWriting) {
				if command.state.Load() == sendCanceled {
					command.result <- command.ctx.Err()
				}
				continue
			}
			if err := command.ctx.Err(); err != nil {
				command.state.Store(sendCompleted)
				command.result <- err
				continue
			}
			err := c.stream.Send(command.request)
			command.state.Store(sendCompleted)
			command.result <- err
			if err != nil {
				c.stop(err)
				return
			}
		}
	}
}

func (c *Client) runReceiver() {
	defer c.tasks.Done()
	for {
		response, err := c.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = fmt.Errorf("relaygate: Relay stream ended: %w", io.EOF)
			}
			c.stop(err)
			return
		}
		if err := c.dispatch(response); err != nil {
			c.stop(err)
			return
		}
	}
}

func (c *Client) supervise() {
	<-c.ctx.Done()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.tasks.Wait()
	cause := context.Cause(c.ctx)
	c.mu.Lock()
	if !errors.Is(cause, errExplicitClose) {
		c.finalErr = cause
	}
	listeners := make([]*Listener, 0, len(c.listeners))
	for _, listener := range c.listeners {
		listeners = append(listeners, listener)
	}
	pipes := make([]*Pipe, 0, len(c.pipes))
	for _, pipe := range c.pipes {
		pipes = append(pipes, pipe)
	}
	offers := make([]*Offer, 0, len(c.offers))
	for _, offer := range c.offers {
		offers = append(offers, offer)
	}
	openReservations := 0
	for _, call := range c.opens {
		if call.reserved {
			call.reserved = false
			openReservations++
		}
	}
	c.mu.Unlock()
	terminal := cause
	if errors.Is(cause, errExplicitClose) {
		terminal = ErrClientClosed
	}
	for _, listener := range listeners {
		listener.end(terminal)
	}
	for _, offer := range offers {
		offer.terminate(terminal)
	}
	for range openReservations {
		c.releasePipeSlot()
	}
	for _, pipe := range pipes {
		pipe.terminate(terminal)
	}
	close(c.done)
}

func (c *Client) stop(err error) {
	if err == nil {
		err = ErrClientClosed
	}
	c.cancel(err)
}

func (c *Client) terminalError() error {
	if err := c.Err(); err != nil {
		return err
	}
	if cause := context.Cause(c.ctx); cause != nil && !errors.Is(cause, errExplicitClose) {
		return cause
	}
	return ErrClientClosed
}

func randomRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("relaygate: generate request ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
