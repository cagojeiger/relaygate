package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	raftmembership "github.com/cagojeiger/relaygate/internal/raft/membership"
)

func TestParseMembershipCommand(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		want   membershipCommand
		broken bool
	}{
		{name: "list", args: []string{"list"}, want: membershipCommand{action: "list", configPath: "env.yaml", timeout: defaultMembershipTimeout}},
		{name: "add", args: []string{"add", "-node-id", "node-4", "-raft-address", "node-4:27400", "-timeout", "9s"}, want: membershipCommand{action: "add", configPath: "env.yaml", nodeID: "node-4", raftAddress: "node-4:27400", timeout: 9 * time.Second}},
		{name: "remove", args: []string{"remove", "-node-id", "node-3", "-config", "other.yaml"}, want: membershipCommand{action: "remove", configPath: "other.yaml", nodeID: "node-3", timeout: defaultMembershipTimeout}},
		{name: "missing action", broken: true},
		{name: "unknown action", args: []string{"replace"}, broken: true},
		{name: "missing add node", args: []string{"add", "-raft-address", "node-4:27400"}, broken: true},
		{name: "missing add address", args: []string{"add", "-node-id", "node-4"}, broken: true},
		{name: "missing remove node", args: []string{"remove"}, broken: true},
		{name: "positional input", args: []string{"list", "extra"}, broken: true},
		{name: "invalid timeout", args: []string{"list", "-timeout", "0s"}, broken: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseMembershipCommand(test.args, func(key string) string {
				if key == "RELAYGATE_CONFIG" {
					return "env.yaml"
				}
				return ""
			})
			if test.broken {
				if err == nil {
					t.Fatalf("parseMembershipCommand() = %#v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMembershipCommand(): %v", err)
			}
			if got != test.want {
				t.Fatalf("parseMembershipCommand() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestRunMembershipListUsesControllerDataDirectoryAndWritesJSON(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("RELAYGATE_RAFT_DATA_DIR", dataDir)
	configPath := filepath.Join("..", "..", "configs", "relaygate.yaml")
	want := raftmembership.Result{Members: []raftmembership.Member{{NodeID: "node-1", Address: "127.0.0.1:27400", Suffrage: "Voter"}}}
	fake := &fakeMembershipClient{listResult: want}
	var dialedPath string
	var output bytes.Buffer

	err := runMembershipWithDial([]string{"list"}, func(key string) string {
		if key == "RELAYGATE_CONFIG" {
			return configPath
		}
		return ""
	}, &output, func(_ context.Context, path string) (membershipClient, error) {
		dialedPath = path
		return fake, nil
	})
	if err != nil {
		t.Fatalf("runMembershipWithDial(): %v", err)
	}
	if dialedPath != raftmembership.SocketPath(dataDir) {
		t.Fatalf("dialed path = %q, want %q", dialedPath, raftmembership.SocketPath(dataDir))
	}
	if !fake.closed {
		t.Fatal("membership client was not closed")
	}
	var got raftmembership.Result
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if len(got.Members) != 1 || got.Members[0] != want.Members[0] || got.Changed {
		t.Fatalf("output = %#v, want %#v", got, want)
	}
}

func TestRunMembershipDispatchesMutations(t *testing.T) {
	for _, test := range []struct {
		name        string
		args        []string
		wantNodeID  string
		wantAddress string
	}{
		{name: "add", args: []string{"add", "-node-id", "node-4", "-raft-address", "node-4:27400"}, wantNodeID: "node-4", wantAddress: "node-4:27400"},
		{name: "remove", args: []string{"remove", "-node-id", "node-3"}, wantNodeID: "node-3"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("RELAYGATE_RAFT_DATA_DIR", t.TempDir())
			configPath := filepath.Join("..", "..", "configs", "relaygate.yaml")
			fake := &fakeMembershipClient{mutationResult: raftmembership.Result{Changed: true}}
			var output bytes.Buffer
			err := runMembershipWithDial(test.args, func(key string) string {
				if key == "RELAYGATE_CONFIG" {
					return configPath
				}
				return ""
			}, &output, func(context.Context, string) (membershipClient, error) {
				return fake, nil
			})
			if err != nil {
				t.Fatalf("runMembershipWithDial(): %v", err)
			}
			if fake.nodeID != test.wantNodeID || fake.address != test.wantAddress {
				t.Fatalf("mutation = (%q, %q), want (%q, %q)", fake.nodeID, fake.address, test.wantNodeID, test.wantAddress)
			}
		})
	}
}

type fakeMembershipClient struct {
	listResult     raftmembership.Result
	mutationResult raftmembership.Result
	nodeID         string
	address        string
	closed         bool
}

func (f *fakeMembershipClient) List(context.Context) (raftmembership.Result, error) {
	return f.listResult, nil
}

func (f *fakeMembershipClient) Add(_ context.Context, nodeID, address string) (raftmembership.Result, error) {
	f.nodeID = nodeID
	f.address = address
	return f.mutationResult, nil
}

func (f *fakeMembershipClient) Remove(_ context.Context, nodeID string) (raftmembership.Result, error) {
	f.nodeID = nodeID
	return f.mutationResult, nil
}

func (f *fakeMembershipClient) Close() error {
	f.closed = true
	return nil
}
