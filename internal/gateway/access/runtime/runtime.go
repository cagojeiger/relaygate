package clientruntime

import (
	"fmt"
	"sync"

	"github.com/cagojeiger/relaygate/internal/app/config"
	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
	"github.com/cagojeiger/relaygate/internal/gateway/access/session"
)

type ReloadResult struct {
	Revision        string
	Removed         int
	RetiredSessions int
	RetiredPipes    int
	RetiredBindings int
}

type BindingRetirer interface {
	Retire(clientauth.ChangeSet) int
	RetireAll() int
}

type PipeRetirer interface {
	Retire(clientauth.ChangeSet) int
	RetireAll() int
}

type Runtime struct {
	mu       sync.Mutex
	current  config.Config
	auth     *clientauth.Store
	sessions *clientsession.Manager
	pipes    PipeRetirer
	bindings BindingRetirer
}

func New(current config.Config) (*Runtime, error) {
	auth, err := clientauth.NewStore(current.Clients)
	if err != nil {
		return nil, fmt.Errorf("configure client authentication: %w", err)
	}
	sessions, err := clientsession.NewManager(auth, current.Relay.MaxClientSessions)
	if err != nil {
		return nil, fmt.Errorf("configure client sessions: %w", err)
	}
	return &Runtime{current: current, auth: auth, sessions: sessions}, nil
}

func (r *Runtime) Sessions() *clientsession.Manager {
	return r.sessions
}

func (r *Runtime) Revision() string {
	return r.auth.Revision()
}

func (r *Runtime) AttachBindings(bindings BindingRetirer) error {
	if bindings == nil {
		return fmt.Errorf("binding retirer is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bindings != nil {
		return fmt.Errorf("binding retirer is already attached")
	}
	r.bindings = bindings
	return nil
}

func (r *Runtime) AttachPipes(pipes PipeRetirer) error {
	if pipes == nil {
		return fmt.Errorf("pipe retirer is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pipes != nil {
		return fmt.Errorf("pipe retirer is already attached")
	}
	r.pipes = pipes
	return nil
}

func (r *Runtime) Apply(candidate config.Config) (ReloadResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := config.ValidateClientReload(r.current, candidate); err != nil {
		return ReloadResult{}, err
	}
	change, err := r.auth.Reload(candidate.Clients)
	if err != nil {
		return ReloadResult{}, err
	}
	retired := r.sessions.Retire(change)
	retiredPipes := 0
	if r.pipes != nil {
		// Session retirement closes admission first. The explicit sweep is the
		// reload barrier: Apply does not return while a removed credential still
		// owns an opening or accepted Pipe.
		retiredPipes = r.pipes.Retire(change)
	}
	retiredBindings := 0
	if r.bindings != nil {
		// Session retirement happens first. Bind holds the local binding lock
		// across its final session validation, so the following sweep cannot
		// miss an overlapping pre-reload Bind.
		retiredBindings = r.bindings.Retire(change)
	}
	r.current.Clients = candidate.Clients
	return ReloadResult{
		Revision:        change.Revision,
		Removed:         len(change.Removed),
		RetiredSessions: retired,
		RetiredPipes:    retiredPipes,
		RetiredBindings: retiredBindings,
	}, nil
}

func (r *Runtime) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions.Close()
	if r.pipes != nil {
		r.pipes.RetireAll()
	}
	if r.bindings != nil {
		r.bindings.RetireAll()
	}
}
