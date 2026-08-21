package opening

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
	"github.com/cagojeiger/relaygate/internal/gateway/routing"
	"github.com/cagojeiger/relaygate/internal/gateway/routing/binding"
)

var (
	ErrInvalid                = errors.New("invalid open")
	ErrCapacity               = errors.New("open pipe capacity reached")
	ErrNotFound               = errors.New("open target not found")
	ErrUnavailable            = errors.New("open unavailable")
	ErrRemoteRelayUnavailable = errors.New("remote owner relay is unavailable")
	ErrListenerRejected       = errors.New("listener rejected open")
	ErrDeadline               = errors.New("open deadline exceeded")
	ErrUnknown                = errors.New("open outcome unknown")
	ErrSessionEnded           = errors.New("open session ended")
	ErrPayloadInvalid         = errors.New("invalid payload")
	ErrPipeNotOwned           = errors.New("pipe not owned")
	ErrPayloadBackpressure    = errors.New("payload backpressure exhausted")
	ErrContextExpired         = errors.New("open context expired")
	ErrAttemptReplay          = errors.New("forwarded open attempt replayed")
)

type Config struct {
	ClusterEpoch string
	MaxPipes     uint32
	OpenTimeout  time.Duration
}

// Admitter is implemented by gatewaycontrol.Client. It issues one exact,
// quorum-confirmed authority context and performs no owner-side reservation.
type Admitter interface {
	AdmitOpen(context.Context, clientsession.Ref, string, string) (routing.OpenContext, error)
}

type ReservationStore interface {
	GatewayID() string
	Reserve(routing.OpenContext, clientsession.Ref) (localbinding.Reservation, error)
	ReserveForwarded(routing.OpenContext, clientsession.Ref) (localbinding.Reservation, error)
}

// RemoteEndpoint is one ingress-owned hop to a remote owning Gateway. The
// endpoint is volatile: it is never reconnected or resumed after Done closes.
type RemoteEndpoint interface {
	localbinding.PayloadEndpoint
	Activate(context.Context) error
	Close(context.Context) error
	Done() <-chan struct{}
}

type RemoteResult struct {
	AttemptID string
	PipeID    string
	Binding   routing.LiveBinding
	Endpoint  RemoteEndpoint
}

// RemoteOpener creates one cross-Gateway stream for one attempt/Pipe. It must
// not replay the forwarded context when the stream outcome is unknown.
type RemoteOpener interface {
	Open(context.Context, routing.OpenContext, localbinding.CallerEndpoint) (RemoteResult, error)
}

type State string

const (
	StateOpening  State = "OpeningO"
	StateAdmitted State = "AdmittedO"
	StateAccepted State = "AcceptedO"
	StateTerminal State = "TerminalO"
)

type Result struct {
	AttemptID string
	PipeID    string
	Binding   routing.LiveBinding
}

type Snapshot struct {
	AttemptID string
	PipeID    string
	Caller    clientsession.Ref
	Listener  clientsession.Ref
	Binding   routing.LiveBinding
	State     State
	Err       error
}

type entry struct {
	caller         clientsession.Ref
	callerDone     <-chan struct{}
	callerEndpoint localbinding.CallerEndpoint

	attemptID    string
	pipeID       string
	binding      routing.LiveBinding
	listener     clientsession.Ref
	listenerDone <-chan struct{}
	endpoint     localbinding.ListenerEndpoint
	remote       RemoteEndpoint

	state           State
	terminalErr     error
	done            chan struct{}
	attemptDone     chan struct{}
	attemptDetached bool
	cancelPhase     context.CancelCauseFunc
	provisional     bool
	offerStarted    bool
	terminationHeld bool

	activation         chan struct{}
	activated          bool
	activationStarted  bool
	activationFinished chan struct{}
	activationOK       bool
	pipeCtx            context.Context //nolint:containedctx // Entry owns this accepted-Pipe lifetime context.
	cancelPipe         context.CancelCauseFunc
	callerToListenerMu sync.Mutex
	listenerToCallerMu sync.Mutex
}

type termination struct {
	listenerEndpoint localbinding.ListenerEndpoint
	callerEndpoint   localbinding.CallerEndpoint
	remoteEndpoint   RemoteEndpoint
	message          localbinding.Termination
	entry            *entry
}

type Manager struct {
	config   Config
	admitter Admitter
	bindings ReservationStore
	remote   RemoteOpener
	now      func() time.Time

	ctx       context.Context //nolint:containedctx // Manager owns one process-lifetime root context for opener and termination workers.
	cancel    context.CancelFunc
	closeOnce sync.Once
	opens     sync.WaitGroup
	watchers  sync.WaitGroup

	terminationMu      sync.Mutex
	terminations       chan termination
	terminationWorker  sync.WaitGroup
	terminationSenders sync.WaitGroup
	terminationClosed  bool

	mu            sync.Mutex
	entries       map[*entry]struct{}
	byAttempt     map[string]*entry
	byPipe        map[string]*entry
	terminalOrder []*entry
	// forwardedAttempts retains successful remote O reservations until their
	// absolute expiry. Expired entries are safe to evict because the context
	// itself then fails closed; the map never exceeds MaxPipes.
	forwardedAttempts map[string]time.Time
	active            uint32
	pending           uint32
	closed            bool
}
