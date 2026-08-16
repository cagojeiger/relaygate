package clientauth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestAuthenticateRequiresExactClientAndKey(t *testing.T) {
	store, err := NewStore(map[string]ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"key-a1": verifier("secret-a1"),
			"key-a2": verifier("secret-a2"),
		}},
		"client-b": {APIKeys: map[string]string{"key-b1": verifier("secret-b1")}},
	})
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	context, err := store.Authenticate("client-a", "key-a2", "secret-a2")
	if err != nil {
		t.Fatalf("Authenticate(valid): %v", err)
	}
	if context.ClientID != "client-a" || context.APIKeyID != "key-a2" || context.AuthRevision != store.Revision() {
		t.Fatalf("context = %#v", context)
	}

	invalid := []struct {
		name     string
		clientID string
		apiKeyID string
		key      string
	}{
		{name: "missing client", clientID: "", apiKeyID: "key-a2", key: "secret-a2"},
		{name: "unknown client", clientID: "client-x", apiKeyID: "key-a2", key: "secret-a2"},
		{name: "wrong client", clientID: "client-b", apiKeyID: "key-a2", key: "secret-a2"},
		{name: "wrong key id", clientID: "client-a", apiKeyID: "key-a1", key: "secret-a2"},
		{name: "wrong key", clientID: "client-a", apiKeyID: "key-a2", key: "wrong"},
		{name: "empty key", clientID: "client-a", apiKeyID: "key-a2", key: ""},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.Authenticate(test.clientID, test.apiKeyID, test.key); !errors.Is(err, ErrInvalidCredential) {
				t.Fatalf("Authenticate() error = %v, want ErrInvalidCredential", err)
			}
		})
	}
}

func TestRevisionIsDeterministic(t *testing.T) {
	left, err := NewStore(map[string]ClientConfig{
		"client-b": {APIKeys: map[string]string{"key-b": verifier("secret-b")}},
		"client-a": {APIKeys: map[string]string{
			"key-a2": verifier("secret-a2"),
			"key-a1": verifier("secret-a1"),
		}},
	})
	if err != nil {
		t.Fatalf("NewStore(left): %v", err)
	}
	right, err := NewStore(map[string]ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"key-a1": verifier("secret-a1"),
			"key-a2": verifier("secret-a2"),
		}},
		"client-b": {APIKeys: map[string]string{"key-b": verifier("secret-b")}},
	})
	if err != nil {
		t.Fatalf("NewStore(right): %v", err)
	}
	if left.Revision() != right.Revision() {
		t.Fatalf("revisions differ: %q != %q", left.Revision(), right.Revision())
	}
}

func TestReloadRotatesAndRemovesCredentialsAtomically(t *testing.T) {
	store, err := NewStore(map[string]ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"old":  verifier("old-secret"),
			"keep": verifier("keep-secret"),
		}},
	})
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	oldRevision := store.Revision()

	change, err := store.Reload(map[string]ClientConfig{
		"client-a": {APIKeys: map[string]string{
			"keep": verifier("keep-secret"),
			"new":  verifier("new-secret"),
		}},
	})
	if err != nil {
		t.Fatalf("Reload(): %v", err)
	}
	if change.Revision == oldRevision || !change.Removes("client-a", "old") || len(change.Removed) != 1 {
		t.Fatalf("change = %#v", change)
	}
	if _, err := store.Authenticate("client-a", "old", "old-secret"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("removed credential error = %v", err)
	}
	if _, err := store.Authenticate("client-a", "keep", "keep-secret"); err != nil {
		t.Fatalf("retained credential: %v", err)
	}
	if _, err := store.Authenticate("client-a", "new", "new-secret"); err != nil {
		t.Fatalf("new credential: %v", err)
	}
}

func TestReloadRejectsVerifierMutationAndKeepsCurrentSnapshot(t *testing.T) {
	store, err := NewStore(map[string]ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("original")}},
	})
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	revision := store.Revision()

	if _, err := store.Reload(map[string]ClientConfig{
		"client-a": {APIKeys: map[string]string{"key-a": verifier("mutated")}},
	}); !errors.Is(err, ErrVerifierChanged) {
		t.Fatalf("Reload() error = %v, want ErrVerifierChanged", err)
	}
	if store.Revision() != revision {
		t.Fatalf("revision changed after rejected reload: %q", store.Revision())
	}
	if _, err := store.Authenticate("client-a", "key-a", "original"); err != nil {
		t.Fatalf("original credential stopped working: %v", err)
	}
	if _, err := store.Authenticate("client-a", "key-a", "mutated"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("mutated credential error = %v", err)
	}
}

func TestInvalidClientConfigFailsClosed(t *testing.T) {
	valid := verifier("valid")
	tests := []struct {
		name   string
		config map[string]ClientConfig
	}{
		{name: "missing clients", config: nil},
		{name: "blank client", config: map[string]ClientConfig{" ": {APIKeys: map[string]string{"key": valid}}}},
		{name: "missing keys", config: map[string]ClientConfig{"client": {}}},
		{name: "blank key id", config: map[string]ClientConfig{"client": {APIKeys: map[string]string{"": valid}}}},
		{name: "raw key", config: map[string]ClientConfig{"client": {APIKeys: map[string]string{"key": "secret"}}}},
		{name: "wrong algorithm", config: map[string]ClientConfig{"client": {APIKeys: map[string]string{"key": "md5:" + strings.Repeat("0", 64)}}}},
		{name: "uppercase digest", config: map[string]ClientConfig{"client": {APIKeys: map[string]string{"key": strings.ToUpper(valid)}}}},
		{name: "shared verifier", config: map[string]ClientConfig{
			"client-a": {APIKeys: map[string]string{"key-a": valid}},
			"client-b": {APIKeys: map[string]string{"key-b": valid}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewStore(test.config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewStore() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func verifier(raw string) string {
	digest := sha256.Sum256([]byte(raw))
	return verifierPrefix + hex.EncodeToString(digest[:])
}
