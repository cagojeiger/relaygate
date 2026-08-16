package clientruntime

import (
	"fmt"
	"sync"

	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/config"
)

type ReloadResult struct {
	Revision        string
	Removed         int
	RetiredSessions int
}

type Runtime struct {
	mu       sync.Mutex
	current  config.Config
	auth     *clientauth.Store
	sessions *clientsession.Manager
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
	r.current.Clients = candidate.Clients
	return ReloadResult{
		Revision:        change.Revision,
		Removed:         len(change.Removed),
		RetiredSessions: retired,
	}, nil
}

func (r *Runtime) Close() {
	r.sessions.Close()
}
