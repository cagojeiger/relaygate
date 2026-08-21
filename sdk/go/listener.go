package relaygate

import (
	"context"
	"fmt"
	"sync"
)

type Listener struct {
	client   *Client
	id       string
	endpoint string
	target   string
	offers   chan *Offer
	done     chan struct{}

	endOnce sync.Once
	mu      sync.Mutex
	err     error
}

func newListener(client *Client, id, endpoint, target string) *Listener {
	return &Listener{client: client, id: id, endpoint: endpoint, target: target, offers: make(chan *Offer, offerQueueCapacity), done: make(chan struct{})}
}

func (l *Listener) ID() string       { return l.id }
func (l *Listener) Endpoint() string { return l.endpoint }
func (l *Listener) Target() string   { return l.target }

func (l *Listener) Next(ctx context.Context) (*Offer, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relaygate: context is required")
	}
	select {
	case offer := <-l.offers:
		return offer, nil
	default:
	}
	select {
	case offer := <-l.offers:
		return offer, nil
	case <-l.done:
		return nil, l.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *Listener) Unbind(ctx context.Context) error {
	if l == nil || l.client == nil {
		return ErrListenerEnded
	}
	return l.client.unbind(ctx, l)
}

func (l *Listener) enqueue(offer *Offer) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	select {
	case l.offers <- offer:
		return true
	default:
		return false
	}
}

func (l *Listener) end(err error) {
	l.endOnce.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}

func (l *Listener) terminalError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	return ErrListenerEnded
}

type offerState uint8

const (
	offerPending offerState = iota + 1
	offerAccepting
	offerTerminal
)
