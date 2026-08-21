package gatewayrelay

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/opening"
	gatewayv1 "github.com/cagojeiger/relaygate/internal/gen/gateway/v1"
)

var (
	ErrInvalid      = errors.New("invalid Gateway relay configuration or request")
	ErrClosed       = errors.New("gateway relay is closed")
	ErrBackpressure = errors.New("gateway relay backpressure exhausted")
)

// Config owns only the internal Gateway-to-Gateway relay listener and its
// bounded volatile work. The transport is intentionally trusted-development
// plaintext until an authenticated Gateway transport is designed.
type Config struct {
	BindAddress string
	OpenTimeout time.Duration
	MaxPipes    uint32
}

func (c Config) validate() error {
	if c.BindAddress == "" {
		return fmt.Errorf("%w: bind address is required", ErrInvalid)
	}
	if c.OpenTimeout <= 0 {
		return fmt.Errorf("%w: Open timeout must be positive", ErrInvalid)
	}
	if c.MaxPipes == 0 {
		return fmt.Errorf("%w: maximum Pipes must be positive", ErrInvalid)
	}
	return nil
}

// Owner is the exact owner-Gateway boundary. It deliberately exposes no Raft,
// public Relay stream, or SDK types.
type Owner interface {
	OpenForwarded(context.Context, routing.OpenContext, localbinding.CallerEndpoint) (opening.Result, error)
	ActivatePipe(clientsession.Ref, string) bool
	RelayPayload(context.Context, clientsession.Ref, string, string, []byte) error
	ClosePipe(clientsession.Ref, string) bool
}

type peerReceiptOutcome uint8

const (
	peerReceiptUnknown peerReceiptOutcome = iota + 1
	peerReceiptReceived
	peerReceiptRejected
)

type peerReceiptState struct {
	mu sync.Mutex

	pendingID     string
	pendingHash   [sha256.Size]byte
	pendingResult chan error
	lastID        string
	lastHash      [sha256.Size]byte
	lastOutcome   peerReceiptOutcome
	lastFailure   gatewayv1.PipePayloadFailure
}

func (s *peerReceiptState) begin(payload localbinding.PipePayload) (chan error, bool, error) {
	fingerprint := sha256.Sum256(payload.Data)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingID != "" {
		return nil, false, localbinding.ErrPayloadBackpressure
	}
	if s.lastID == payload.PayloadID {
		if s.lastHash != fingerprint {
			return nil, false, localbinding.ErrEndpointUnavailable
		}
		switch s.lastOutcome {
		case peerReceiptReceived:
			return nil, true, nil
		case peerReceiptRejected:
			return nil, true, peerPayloadError(s.lastFailure)
		default:
			return nil, true, localbinding.ErrEndpointUnavailable
		}
	}
	result := make(chan error, 1)
	s.pendingID = payload.PayloadID
	s.pendingHash = fingerprint
	s.pendingResult = result
	return result, false, nil
}

func (s *peerReceiptState) retireUnknown(payloadID string, result chan error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingID != payloadID || s.pendingResult != result {
		return
	}
	s.lastID = s.pendingID
	s.lastHash = s.pendingHash
	s.lastOutcome = peerReceiptUnknown
	s.lastFailure = gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNSPECIFIED
	s.pendingID = ""
	s.pendingResult = nil
}

func (s *peerReceiptState) acknowledge(payloadID string) error {
	if payloadID == "" {
		return localbinding.ErrEndpointUnavailable
	}
	s.mu.Lock()
	if s.pendingID == payloadID && s.pendingResult != nil {
		result := s.pendingResult
		s.lastID = s.pendingID
		s.lastHash = s.pendingHash
		s.lastOutcome = peerReceiptReceived
		s.lastFailure = gatewayv1.PipePayloadFailure_PIPE_PAYLOAD_FAILURE_UNSPECIFIED
		s.pendingID = ""
		s.pendingResult = nil
		s.mu.Unlock()
		result <- nil
		return nil
	}
	if s.lastID == payloadID && s.lastOutcome == peerReceiptUnknown {
		s.mu.Unlock()
		return nil
	}
	duplicate := s.lastID == payloadID && s.lastOutcome == peerReceiptReceived
	s.mu.Unlock()
	if duplicate {
		return nil
	}
	return localbinding.ErrEndpointUnavailable
}

func (s *peerReceiptState) reject(payloadID string, failure gatewayv1.PipePayloadFailure) error {
	rejection := peerPayloadError(failure)
	if payloadID == "" || rejection == nil {
		return localbinding.ErrEndpointUnavailable
	}
	s.mu.Lock()
	if s.pendingID == payloadID && s.pendingResult != nil {
		result := s.pendingResult
		s.lastID = s.pendingID
		s.lastHash = s.pendingHash
		s.lastOutcome = peerReceiptRejected
		s.lastFailure = failure
		s.pendingID = ""
		s.pendingResult = nil
		s.mu.Unlock()
		result <- rejection
		return nil
	}
	if s.lastID == payloadID && s.lastOutcome == peerReceiptUnknown {
		s.mu.Unlock()
		return nil
	}
	duplicate := s.lastID == payloadID && s.lastOutcome == peerReceiptRejected && s.lastFailure == failure
	s.mu.Unlock()
	if duplicate {
		return nil
	}
	return localbinding.ErrEndpointUnavailable
}
