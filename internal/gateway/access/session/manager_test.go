package clientsession

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/cagojeiger/relaygate/internal/gateway/access/auth"
)

func TestAuthenticateCreatesExactSessionAndEndIsIdempotent(t *testing.T) {
	store := newStore(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	manager, err := NewManager(store, 10)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	t.Cleanup(manager.Close)

	session, err := manager.Authenticate("client-a", "key-a", "secret-a")
	if err != nil {
		t.Fatalf("Authenticate(): %v", err)
	}
	if session.Ref.ClientSessionID == "" || session.Ref.ClientID != "client-a" || session.Ref.APIKeyID != "key-a" || session.Ref.AuthRevision != store.Revision() {
		t.Fatalf("session ref = %#v", session.Ref)
	}
	if err := manager.Require(session.Ref); err != nil {
		t.Fatalf("Require(): %v", err)
	}

	manager.End(session.Ref)
	manager.End(session.Ref)
	select {
	case <-session.Done:
	default:
		t.Fatal("ended session Done was not closed")
	}
	if err := manager.Require(session.Ref); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("Require(ended) = %v", err)
	}
}

func TestFailedAuthenticationCreatesNoSession(t *testing.T) {
	store := newStore(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	manager, err := NewManager(store, 10)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	t.Cleanup(manager.Close)

	if _, err := manager.Authenticate("client-b", "key-a", "secret-a"); !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if manager.ActiveCount() != 0 {
		t.Fatalf("active sessions = %d", manager.ActiveCount())
	}
}

func TestCredentialRemovalRetiresOnlyMatchingSessions(t *testing.T) {
	store := newStore(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"remove": verifier("remove-secret"),
			"keep":   verifier("keep-secret"),
		}},
	})
	manager, err := NewManager(store, 10)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	t.Cleanup(manager.Close)
	removedSession, err := manager.Authenticate("client-a", "remove", "remove-secret")
	if err != nil {
		t.Fatalf("Authenticate(remove): %v", err)
	}
	keptSession, err := manager.Authenticate("client-a", "keep", "keep-secret")
	if err != nil {
		t.Fatalf("Authenticate(keep): %v", err)
	}

	change, err := store.Reload(map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"keep": verifier("keep-secret")}},
	})
	if err != nil {
		t.Fatalf("Reload(): %v", err)
	}
	if retired := manager.Retire(change); retired != 1 {
		t.Fatalf("Retire() = %d, want 1", retired)
	}
	select {
	case <-removedSession.Done:
	default:
		t.Fatal("removed credential session survived")
	}
	select {
	case <-keptSession.Done:
		t.Fatal("retained credential session was retired")
	default:
	}
	if err := manager.Require(keptSession.Ref); err != nil {
		t.Fatalf("Require(kept): %v", err)
	}
	if keptSession.Ref.AuthRevision == store.Revision() {
		t.Fatal("existing session revision changed across reload")
	}
}

func TestRequireFailsClosedBeforeRetirementSweep(t *testing.T) {
	store := newStore(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	manager, err := NewManager(store, 10)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	t.Cleanup(manager.Close)
	session, err := manager.Authenticate("client-a", "key-a", "secret-a")
	if err != nil {
		t.Fatalf("Authenticate(): %v", err)
	}

	change, err := store.Reload(map[string]clientauth.ClientConfig{
		"client-b": {APIKeys: map[string]string{"key-b": verifier("secret-b")}},
	})
	if err != nil {
		t.Fatalf("Reload(): %v", err)
	}
	if err := manager.Require(session.Ref); !errors.Is(err, ErrCredentialRevoked) {
		t.Fatalf("Require() = %v, want ErrCredentialRevoked", err)
	}
	if retired := manager.Retire(change); retired != 0 {
		t.Fatalf("Retire() after Require = %d", retired)
	}
	select {
	case <-session.Done:
	default:
		t.Fatal("revoked session Done was not closed")
	}
}

func TestCapacityRejectsOnlyNewSession(t *testing.T) {
	store := newStore(t, map[string]clientauth.ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("secret-a")}},
	})
	manager, err := NewManager(store, 1)
	if err != nil {
		t.Fatalf("NewManager(): %v", err)
	}
	t.Cleanup(manager.Close)
	first, err := manager.Authenticate("client-a", "key-a", "secret-a")
	if err != nil {
		t.Fatalf("Authenticate(first): %v", err)
	}
	if _, err := manager.Authenticate("client-a", "key-a", "secret-a"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Authenticate(second) = %v, want ErrCapacity", err)
	}
	if err := manager.Require(first.Ref); err != nil {
		t.Fatalf("existing session was affected: %v", err)
	}
}

func newStore(t *testing.T, config map[string]clientauth.ClientConfig) *clientauth.Store {
	t.Helper()
	store, err := clientauth.NewStore(config)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	return store
}

func verifier(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(digest[:])
}
