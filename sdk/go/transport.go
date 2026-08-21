package relaygate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	relayv1 "github.com/cagojeiger/relaygate/sdk/go/internal/gen/relay/v1"
)

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
