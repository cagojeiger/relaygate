package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	controlmodel "github.com/cagojeiger/relaygate/internal/gateway/control/model"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	raftnode "github.com/cagojeiger/relaygate/internal/raft/node"
	controlstate "github.com/cagojeiger/relaygate/internal/raft/state"
)

var (
	ErrNoAuthority   = errors.New("no current authority")
	ErrStaleSession  = errors.New("stale control session")
	ErrSnapshotFirst = errors.New("full snapshot has not been accepted")
)

const DefaultGatewayRevalidationTimeout = 15 * time.Second

type Config struct {
	ClusterEpoch               string
	ProbeInterval              time.Duration
	ProbeTimeout               time.Duration
	ApplyTimeout               time.Duration
	GatewayRevalidationTimeout time.Duration
	OpenContextTTL             time.Duration
}

// RaftNode owns the durable, replicated current directory. The authority owns
// only leader-local control streams, advertised addresses, and the fact that a
// gateway has revalidated those durable records for this authority term.
type RaftNode interface {
	Status() raftnode.Status
	ClusterEpoch() string
	VerifyLeader(context.Context) error
	Apply(context.Context, []byte) (controlstate.ApplyResult, error)
	State() controlstate.State
	LookupGateway(string) (controlstate.GatewaySessionRef, bool)
	LookupRoute(controlstate.BindingKey) (controlstate.Route, bool)
}

type SessionState string

const (
	SessionSyncing     SessionState = "Syncing"
	SessionRevalidated SessionState = "Revalidated"
)

type PresenceState string

const (
	PresenceNoAuthority PresenceState = "NoAuthority"
	PresenceCurrent     PresenceState = "Current"
)

// Presence separates replicated current records (C) from this authority's
// freshly verified control streams (V). A route is eligible only when both
// conditions hold.
type Presence struct {
	State               PresenceState `json:"state"`
	CommittedGateways   int           `json:"committed_gateways"`
	CommittedRoutes     int           `json:"committed_routes"`
	RevalidatedGateways int           `json:"revalidated_gateways"`
	EligibleRoutes      int           `json:"eligible_routes"`
}

type sessionEntry struct {
	ref          controlmodel.SessionRef
	relayAddress string
	state        SessionState
	bindings     map[routing.BindingKey]routing.LiveBinding
	done         chan struct{}
	closed       bool
}

type Manager struct {
	config Config
	node   RaftNode

	// Raft applies are already ordered. This mutex gives the leader-local
	// session mirror the same order without serializing read-only Open
	// admission behind control-plane writes.
	mutationMu  sync.Mutex
	mu          sync.RWMutex
	current     *controlmodel.AuthorityRef
	currentTerm uint64
	sessions    map[string]*sessionEntry // leader-local current stream by GatewayID
	cleanup     map[controlstate.GatewaySessionRef]time.Time
	now         func() time.Time
	cancel      context.CancelFunc
	done        chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	doneOnce    sync.Once
	closed      bool
}

func New(config Config, node RaftNode) (*Manager, error) {
	if err := routing.ValidateIdentity("cluster_epoch", config.ClusterEpoch); err != nil {
		return nil, err
	}
	if config.GatewayRevalidationTimeout == 0 {
		config.GatewayRevalidationTimeout = DefaultGatewayRevalidationTimeout
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = config.ProbeTimeout
	}
	if config.ProbeInterval <= 0 || config.ProbeTimeout <= 0 || config.ApplyTimeout <= 0 || config.GatewayRevalidationTimeout <= 0 || config.OpenContextTTL <= 0 {
		return nil, fmt.Errorf("authority probe, revalidation, and Open context timeouts must be positive")
	}
	if node == nil {
		return nil, fmt.Errorf("raft node is required")
	}
	return &Manager{
		config:   config,
		node:     node,
		sessions: make(map[string]*sessionEntry),
		cleanup:  make(map[controlstate.GatewaySessionRef]time.Time),
		now:      time.Now,
		done:     make(chan struct{}),
	}, nil
}

func (m *Manager) Start(parent context.Context) {
	m.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			cancel()
			return
		}
		m.cancel = cancel
		m.mu.Unlock()
		go m.run(ctx)
	})
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		cancel := m.cancel
		m.mu.Unlock()
		if cancel != nil {
			cancel()
			<-m.done
			return
		}
		m.fence()
		m.finish()
	})
}

func (m *Manager) Current() (controlmodel.AuthorityRef, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil {
		return controlmodel.AuthorityRef{}, false
	}
	return *m.current, true
}
