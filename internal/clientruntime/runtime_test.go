package clientruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cagojeiger/relaygate/internal/clientauth"
	"github.com/cagojeiger/relaygate/internal/clientsession"
	"github.com/cagojeiger/relaygate/internal/config"
)

func TestApplySwapsAuthBeforeRetiringRemovedSessions(t *testing.T) {
	current := testConfig(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"remove": verifier("remove-secret"),
			"keep":   verifier("keep-secret"),
		}},
	})
	runtime, err := New(current)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(runtime.Close)
	removed, err := runtime.Sessions().Authenticate("client-a", "remove", "remove-secret")
	if err != nil {
		t.Fatalf("Authenticate(remove): %v", err)
	}
	kept, err := runtime.Sessions().Authenticate("client-a", "keep", "keep-secret")
	if err != nil {
		t.Fatalf("Authenticate(keep): %v", err)
	}

	candidate := current
	candidate.Clients = map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"keep": verifier("keep-secret")}},
	}
	result, err := runtime.Apply(candidate)
	if err != nil {
		t.Fatalf("Apply(): %v", err)
	}
	if result.Removed != 1 || result.RetiredSessions != 1 || result.Revision != runtime.Revision() {
		t.Fatalf("result = %#v", result)
	}
	select {
	case <-removed.Done:
	default:
		t.Fatal("removed credential session survived reload")
	}
	select {
	case <-kept.Done:
		t.Fatal("retained credential session was retired")
	default:
	}
	if _, err := runtime.Sessions().Authenticate("client-a", "remove", "remove-secret"); err == nil {
		t.Fatal("removed credential authenticated after reload")
	}
}

func TestApplyRejectsStaticConfigChangeWithoutChangingAuth(t *testing.T) {
	current := testConfig(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	runtime, err := New(current)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(runtime.Close)
	revision := runtime.Revision()

	candidate := current
	candidate.Admin.BindAddress = "127.0.0.1:9191"
	candidate.Clients = map[string]clientauth.ClientConfig{
		"client-b": {APIKeys: map[string]string{"key-b": verifier("secret-b")}},
	}
	if _, err := runtime.Apply(candidate); err == nil {
		t.Fatal("Apply() accepted a static config change")
	}
	if runtime.Revision() != revision {
		t.Fatal("rejected reload changed auth revision")
	}
	if _, err := runtime.Sessions().Authenticate("client-a", "key-a", "secret-a"); err != nil {
		t.Fatalf("old credential stopped working: %v", err)
	}
}

func TestApplyConcurrentWithAuthenticationEndsAtFinalSnapshot(t *testing.T) {
	enabled := testConfig(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"stable":   verifier("stable-secret"),
			"rotating": verifier("rotating-secret"),
		}},
	})
	disabled := enabled
	disabled.Clients = map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"stable": verifier("stable-secret")}},
	}
	runtime, err := New(enabled)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(runtime.Close)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		for range 500 {
			session, err := runtime.Sessions().Authenticate("client-a", "rotating", "rotating-secret")
			if err != nil {
				if errors.Is(err, clientsession.ErrAuthenticationFailed) || errors.Is(err, clientsession.ErrCredentialRevoked) {
					continue
				}
				results <- fmt.Errorf("authenticate: %w", err)
				return
			}
			if err := runtime.Sessions().Require(session.Ref); err != nil &&
				!errors.Is(err, clientsession.ErrCredentialRevoked) &&
				!errors.Is(err, clientsession.ErrStaleSession) {
				results <- fmt.Errorf("require: %w", err)
				return
			}
			runtime.Sessions().End(session.Ref)
		}
		results <- nil
	}()
	go func() {
		<-start
		for range 100 {
			if _, err := runtime.Apply(disabled); err != nil {
				results <- fmt.Errorf("disable credential: %w", err)
				return
			}
			if _, err := runtime.Apply(enabled); err != nil {
				results <- fmt.Errorf("enable credential: %w", err)
				return
			}
		}
		if _, err := runtime.Apply(disabled); err != nil {
			results <- fmt.Errorf("final credential removal: %w", err)
			return
		}
		results <- nil
	}()
	close(start)

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("concurrent operation failed: %v", err)
			}
		case <-timer.C:
			t.Fatal("concurrent reload and authentication did not complete")
		}
	}
	if runtime.Sessions().ActiveCount() != 0 {
		t.Fatalf("active sessions = %d, want 0", runtime.Sessions().ActiveCount())
	}
	if _, err := runtime.Sessions().Authenticate("client-a", "rotating", "rotating-secret"); !errors.Is(err, clientsession.ErrAuthenticationFailed) {
		t.Fatalf("removed credential error = %v, want authentication failure", err)
	}
}

func TestAuthenticationAllowedBeforeRemovalIsRetiredBySameApply(t *testing.T) {
	enabled, disabled := rotationConfigs()
	runtime, gate := newGatedRuntime(t, enabled, true)
	wantRevision := configRevision(t, disabled)

	authenticated := make(chan authenticationResult, 1)
	go func() {
		session, err := runtime.Sessions().Authenticate("client-a", "rotating", "rotating-secret")
		authenticated <- authenticationResult{session: session, err: err}
	}()
	if allowed := receiveWithin(t, gate.reached, "pre-swap credential check"); !allowed {
		t.Fatal("credential was not allowed before removal swap")
	}

	applied := make(chan applyResult, 1)
	go func() {
		result, err := runtime.Apply(disabled)
		applied <- applyResult{result: result, err: err}
	}()
	waitForRevision(t, runtime, wantRevision)
	close(gate.release)

	authResult := receiveWithin(t, authenticated, "pre-swap authentication")
	if authResult.err != nil {
		t.Fatalf("Authenticate(): %v", authResult.err)
	}
	applyResult := receiveWithin(t, applied, "credential removal")
	if applyResult.err != nil {
		t.Fatalf("Apply(): %v", applyResult.err)
	}
	if applyResult.result.RetiredSessions != 1 {
		t.Fatalf("retired sessions = %d, want 1", applyResult.result.RetiredSessions)
	}
	select {
	case <-authResult.session.Done:
	default:
		t.Fatal("pre-swap session survived removal Apply")
	}
	if runtime.Sessions().ActiveCount() != 0 {
		t.Fatalf("active sessions = %d, want 0", runtime.Sessions().ActiveCount())
	}
}

func TestRemovalBeforeFinalAllowedCheckRejectsAuthentication(t *testing.T) {
	enabled, disabled := rotationConfigs()
	runtime, gate := newGatedRuntime(t, enabled, false)
	wantRevision := configRevision(t, disabled)

	authenticated := make(chan authenticationResult, 1)
	go func() {
		session, err := runtime.Sessions().Authenticate("client-a", "rotating", "rotating-secret")
		authenticated <- authenticationResult{session: session, err: err}
	}()
	receiveWithin(t, gate.reached, "pending final credential check")

	applied := make(chan applyResult, 1)
	go func() {
		result, err := runtime.Apply(disabled)
		applied <- applyResult{result: result, err: err}
	}()
	waitForRevision(t, runtime, wantRevision)
	close(gate.release)

	authResult := receiveWithin(t, authenticated, "post-swap authentication")
	if !errors.Is(authResult.err, clientsession.ErrCredentialRevoked) {
		t.Fatalf("Authenticate() error = %v, want ErrCredentialRevoked", authResult.err)
	}
	applyResult := receiveWithin(t, applied, "credential removal")
	if applyResult.err != nil {
		t.Fatalf("Apply(): %v", applyResult.err)
	}
	if applyResult.result.RetiredSessions != 0 {
		t.Fatalf("retired sessions = %d, want 0", applyResult.result.RetiredSessions)
	}
	if runtime.Sessions().ActiveCount() != 0 {
		t.Fatalf("active sessions = %d, want 0", runtime.Sessions().ActiveCount())
	}
}

type gatedAuthenticator struct {
	store                *clientauth.Store
	observeBeforeRelease bool
	reached              chan bool
	release              chan struct{}
}

func (g *gatedAuthenticator) Authenticate(clientID, apiKeyID, presentedKey string) (clientauth.Context, error) {
	return g.store.Authenticate(clientID, apiKeyID, presentedKey)
}

func (g *gatedAuthenticator) Allowed(clientID, apiKeyID string) bool {
	if g.observeBeforeRelease {
		allowed := g.store.Allowed(clientID, apiKeyID)
		g.reached <- allowed
		<-g.release
		return allowed
	}
	g.reached <- false
	<-g.release
	return g.store.Allowed(clientID, apiKeyID)
}

type authenticationResult struct {
	session clientsession.Session
	err     error
}

type applyResult struct {
	result ReloadResult
	err    error
}

func newGatedRuntime(t *testing.T, current config.Config, observeBeforeRelease bool) (*Runtime, *gatedAuthenticator) {
	t.Helper()
	store, err := clientauth.NewStore(current.Clients)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	gate := &gatedAuthenticator{
		store:                store,
		observeBeforeRelease: observeBeforeRelease,
		reached:              make(chan bool, 1),
		release:              make(chan struct{}),
	}
	sessions, err := clientsession.NewManager(gate, current.Relay.MaxClientSessions)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	runtime := &Runtime{current: current, auth: store, sessions: sessions}
	t.Cleanup(runtime.Close)
	return runtime, gate
}

func rotationConfigs() (config.Config, config.Config) {
	enabled := testConfig(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"stable":   verifier("stable-secret"),
			"rotating": verifier("rotating-secret"),
		}},
	})
	disabled := enabled
	disabled.Clients = map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"stable": verifier("stable-secret")}},
	}
	return enabled, disabled
}

func configRevision(t *testing.T, configured config.Config) string {
	t.Helper()
	store, err := clientauth.NewStore(configured.Clients)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	return store.Revision()
}

func waitForRevision(t *testing.T, runtime *Runtime, want string) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if runtime.Revision() == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("auth revision = %q, want %q", runtime.Revision(), want)
		case <-ticker.C:
		}
	}
}

func receiveWithin[T any](t *testing.T, values <-chan T, operation string) T {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		var zero T
		t.Fatalf("%s did not complete", operation)
		return zero
	}
}

func testConfig(clients map[string]clientauth.ClientConfig) config.Config {
	configured := config.Defaults()
	configured.Clients = clients
	return configured
}

func verifier(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(digest[:])
}
