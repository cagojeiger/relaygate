package clientauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	verifierPrefix   = "sha256:"
	verifierHexBytes = sha256.Size * 2
	maxIdentityBytes = 256
	maxPresentedKey  = 4096
)

var (
	ErrInvalidConfig     = errors.New("invalid client authentication config")
	ErrInvalidCredential = errors.New("invalid client credential")
	ErrVerifierChanged   = errors.New("API key verifier changed")
)

type ClientConfig struct {
	APIKeys map[string]string `yaml:"api_keys"`
}

type CredentialID struct {
	ClientID string
	APIKeyID string
}

type Context struct {
	ClientID     string
	APIKeyID     string
	AuthRevision string
}

type ChangeSet struct {
	Revision string
	Removed  []CredentialID
}

func (c ChangeSet) Removes(clientID, apiKeyID string) bool {
	for _, removed := range c.Removed {
		if removed.ClientID == clientID && removed.APIKeyID == apiKeyID {
			return true
		}
	}
	return false
}

type snapshot struct {
	revision    string
	credentials map[CredentialID][sha256.Size]byte
}

type Store struct {
	mu      sync.RWMutex
	current *snapshot
	seen    map[CredentialID][sha256.Size]byte
}

func NewStore(config map[string]ClientConfig) (*Store, error) {
	initial, err := buildSnapshot(config)
	if err != nil {
		return nil, err
	}
	seen := make(map[CredentialID][sha256.Size]byte, len(initial.credentials))
	for id, digest := range initial.credentials {
		seen[id] = digest
	}
	return &Store{current: initial, seen: seen}, nil
}

func (s *Store) Authenticate(clientID, apiKeyID, presentedKey string) (Context, error) {
	digest := sha256.Sum256([]byte(presentedKey))
	var expected [sha256.Size]byte

	s.mu.RLock()
	configured, found := s.current.credentials[CredentialID{ClientID: clientID, APIKeyID: apiKeyID}]
	if found {
		expected = configured
	}
	revision := s.current.revision
	s.mu.RUnlock()

	match := subtle.ConstantTimeCompare(digest[:], expected[:]) == 1
	if presentedKey == "" || len(presentedKey) > maxPresentedKey || !found || !match {
		return Context{}, ErrInvalidCredential
	}
	return Context{ClientID: clientID, APIKeyID: apiKeyID, AuthRevision: revision}, nil
}

func (s *Store) Allowed(clientID, apiKeyID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.current.credentials[CredentialID{ClientID: clientID, APIKeyID: apiKeyID}]
	return ok
}

func (s *Store) Revision() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current.revision
}

func (s *Store) Reload(config map[string]ClientConfig) (ChangeSet, error) {
	candidate, err := buildSnapshot(config)
	if err != nil {
		return ChangeSet{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, digest := range candidate.credentials {
		if previous, ok := s.seen[id]; ok && previous != digest {
			return ChangeSet{}, fmt.Errorf("%w: client %q key %q", ErrVerifierChanged, id.ClientID, id.APIKeyID)
		}
	}
	removed := make([]CredentialID, 0)
	for id := range s.current.credentials {
		if _, ok := candidate.credentials[id]; !ok {
			removed = append(removed, id)
		}
	}
	sortCredentialIDs(removed)
	for id, digest := range candidate.credentials {
		s.seen[id] = digest
	}
	s.current = candidate
	return ChangeSet{Revision: candidate.revision, Removed: removed}, nil
}

func buildSnapshot(config map[string]ClientConfig) (*snapshot, error) {
	if len(config) == 0 {
		return nil, fmt.Errorf("%w: at least one client is required", ErrInvalidConfig)
	}
	credentials := make(map[CredentialID][sha256.Size]byte)
	owners := make(map[[sha256.Size]byte]CredentialID)
	for clientID, client := range config {
		if err := validateIdentity("client ID", clientID); err != nil {
			return nil, err
		}
		if len(client.APIKeys) == 0 {
			return nil, fmt.Errorf("%w: client %q must have at least one API key", ErrInvalidConfig, clientID)
		}
		for apiKeyID, verifier := range client.APIKeys {
			if err := validateIdentity("API key ID", apiKeyID); err != nil {
				return nil, fmt.Errorf("client %q: %w", clientID, err)
			}
			digest, err := parseVerifier(verifier)
			if err != nil {
				return nil, fmt.Errorf("client %q key %q: %w", clientID, apiKeyID, err)
			}
			id := CredentialID{ClientID: clientID, APIKeyID: apiKeyID}
			if owner, duplicate := owners[digest]; duplicate {
				return nil, fmt.Errorf("%w: client %q key %q shares a verifier with client %q key %q", ErrInvalidConfig, clientID, apiKeyID, owner.ClientID, owner.APIKeyID)
			}
			owners[digest] = id
			credentials[id] = digest
		}
	}
	return &snapshot{revision: revision(credentials), credentials: credentials}, nil
}

func validateIdentity(kind, value string) error {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) || len(value) > maxIdentityBytes {
		return fmt.Errorf("%w: %s must be non-empty valid UTF-8 without surrounding whitespace and at most %d bytes", ErrInvalidConfig, kind, maxIdentityBytes)
	}
	return nil
}

func parseVerifier(verifier string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if !strings.HasPrefix(verifier, verifierPrefix) || len(verifier) != len(verifierPrefix)+verifierHexBytes {
		return digest, fmt.Errorf("%w: verifier must be %s followed by %d lowercase hexadecimal characters", ErrInvalidConfig, verifierPrefix, verifierHexBytes)
	}
	encoded := strings.TrimPrefix(verifier, verifierPrefix)
	decoded, err := hex.DecodeString(encoded)
	if err != nil || hex.EncodeToString(decoded) != encoded {
		return digest, fmt.Errorf("%w: verifier digest must be lowercase hexadecimal", ErrInvalidConfig)
	}
	copy(digest[:], decoded)
	return digest, nil
}

func revision(credentials map[CredentialID][sha256.Size]byte) string {
	type canonicalCredential struct {
		ClientID string `json:"client_id"`
		APIKeyID string `json:"api_key_id"`
		Verifier string `json:"verifier"`
	}
	ids := make([]CredentialID, 0, len(credentials))
	for id := range credentials {
		ids = append(ids, id)
	}
	sortCredentialIDs(ids)
	canonical := make([]canonicalCredential, 0, len(ids))
	for _, id := range ids {
		digest := credentials[id]
		canonical = append(canonical, canonicalCredential{
			ClientID: id.ClientID,
			APIKeyID: id.APIKeyID,
			Verifier: verifierPrefix + hex.EncodeToString(digest[:]),
		})
	}
	encoded, _ := json.Marshal(struct {
		Version     int                   `json:"version"`
		Credentials []canonicalCredential `json:"credentials"`
	}{Version: 1, Credentials: canonical})
	digest := sha256.Sum256(encoded)
	return verifierPrefix + hex.EncodeToString(digest[:])
}

func sortCredentialIDs(ids []CredentialID) {
	sort.Slice(ids, func(left, right int) bool {
		if ids[left].ClientID != ids[right].ClientID {
			return ids[left].ClientID < ids[right].ClientID
		}
		return ids[left].APIKeyID < ids[right].APIKeyID
	})
}
