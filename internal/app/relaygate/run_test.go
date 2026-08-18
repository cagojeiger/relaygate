package relaygate

import (
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"

	"github.com/cagojeiger/relaygate/internal/app/config"
)

func TestRunRejectsMissingConfig(t *testing.T) {
	err := Run(filepath.Join(t.TempDir(), "missing.yaml"), "test")
	if err == nil {
		t.Fatal("Run() succeeded with missing config")
	}
}

func TestBootstrapVotersPreservesConfiguredOrder(t *testing.T) {
	configured := []config.RaftVoterConfig{
		{NodeID: "node-2", Address: "node-2:27400"},
		{NodeID: "node-1", Address: "node-1:27400"},
	}
	voters := bootstrapVoters(configured)
	if len(voters) != len(configured) {
		t.Fatalf("voters = %d, want %d", len(voters), len(configured))
	}
	for index := range configured {
		if voters[index].NodeID != configured[index].NodeID || voters[index].Address != configured[index].Address {
			t.Fatalf("voter[%d] = %#v, want %#v", index, voters[index], configured[index])
		}
	}
}

func TestHclogLevelMapping(t *testing.T) {
	for _, test := range []struct {
		name string
		in   slog.Level
		want hclog.Level
	}{
		{name: "debug", in: slog.LevelDebug, want: hclog.Debug},
		{name: "info", in: slog.LevelInfo, want: hclog.Info},
		{name: "warn", in: slog.LevelWarn, want: hclog.Warn},
		{name: "error", in: slog.LevelError, want: hclog.Error},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := hclogLevel(test.in); got != test.want {
				t.Fatalf("hclogLevel(%v) = %v, want %v", test.in, got, test.want)
			}
		})
	}
}
