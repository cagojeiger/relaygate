package membership

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestServerAndClientUsePrivateUnixSocket(t *testing.T) {
	baseDir := shortTempDir(t)
	dataDir := filepath.Join(baseDir, "data")
	if err := os.Mkdir(dataDir, 0o755); err != nil {
		t.Fatalf("Mkdir(data dir): %v", err)
	}
	if err := os.Chmod(dataDir, 0o755); err != nil {
		t.Fatalf("Chmod(data dir before Start): %v", err)
	}
	node := newFakeNode([]raft.Server{{ID: "node-b", Address: "127.0.0.1:3002", Suffrage: raft.Voter}})
	server, err := Start(context.Background(), dataDir, node)
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	t.Cleanup(func() { shutdownServer(t, server) })

	assertPermissions(t, dataDir, 0o700)
	assertPermissions(t, filepath.Join(dataDir, lockName), 0o600)
	path := SocketPath(dataDir)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(socket): %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket mode = %v, want Unix socket", info.Mode())
	}
	assertPermissions(t, path, 0o600)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := Dial(ctx, path)
	cancel()
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	listed, err := client.List(ctx)
	cancel()
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	if got, want := listed, (Result{Members: []Member{{NodeID: "node-b", Address: "127.0.0.1:3002", Suffrage: "Voter"}}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("List() = %#v, want %#v", got, want)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	added, err := client.Add(ctx, "node-a", "127.0.0.1:3001")
	cancel()
	if err != nil {
		t.Fatalf("Add(): %v", err)
	}
	if !added.Changed {
		t.Fatalf("Add() = %#v, want changed", added)
	}
	if got, want := memberIDs(added), []string{"node-a", "node-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Add() = %#v, want changed sorted node-a/node-b", added)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	removed, err := client.Remove(ctx, "node-b")
	cancel()
	if err != nil {
		t.Fatalf("Remove(): %v", err)
	}
	if !removed.Changed {
		t.Fatalf("Remove() = %#v, want changed", removed)
	}
	if got, want := memberIDs(removed), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Remove() = %#v, want changed node-a", removed)
	}
}

func TestStartDoesNotUnlinkLiveSocket(t *testing.T) {
	t.Run("package server lock", func(t *testing.T) {
		dataDir := shortTempDir(t)
		node := newFakeNode(voters(1))
		server, err := Start(context.Background(), dataDir, node)
		if err != nil {
			t.Fatalf("first Start(): %v", err)
		}
		defer shutdownServer(t, server)

		if _, err := Start(context.Background(), dataDir, node); err == nil || !strings.Contains(err.Error(), "already running") {
			t.Fatalf("second Start() error = %v, want already running", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		client, err := Dial(ctx, SocketPath(dataDir))
		cancel()
		if err != nil {
			t.Fatalf("Dial(first server after second Start): %v", err)
		}
		defer func() { _ = client.Close() }()
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		_, err = client.List(ctx)
		cancel()
		if err != nil {
			t.Fatalf("List(first server after second Start): %v", err)
		}
	})

	t.Run("external listener probe", func(t *testing.T) {
		dataDir := shortTempDir(t)
		path := SocketPath(dataDir)
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatalf("ListenUnix(): %v", err)
		}
		listener.SetUnlinkOnClose(false)
		defer func() {
			_ = listener.Close()
			_ = os.Remove(path)
		}()

		if _, err := Start(context.Background(), dataDir, newFakeNode(voters(1))); err == nil || !strings.Contains(err.Error(), "live listener") {
			t.Fatalf("Start() error = %v, want live listener", err)
		}
		connection, err := net.DialTimeout("unix", path, time.Second)
		if err != nil {
			t.Fatalf("DialUnix(existing listener): %v", err)
		}
		_ = connection.Close()
	})
}

func TestStartReplacesOnlyStaleSocket(t *testing.T) {
	dataDir := shortTempDir(t)
	path := SocketPath(dataDir)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("ListenUnix(): %v", err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatalf("Close(stale listener): %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket missing before Start(): %v", err)
	}

	server, err := Start(context.Background(), dataDir, newFakeNode(voters(1)))
	if err != nil {
		t.Fatalf("Start(stale socket): %v", err)
	}
	shutdownServer(t, server)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket after Shutdown() error = %v, want not exist", err)
	}
}

func TestStartRefusesNonSocketPath(t *testing.T) {
	dataDir := shortTempDir(t)
	path := SocketPath(dataDir)
	if err := os.WriteFile(path, []byte("do not remove"), 0o600); err != nil {
		t.Fatalf("WriteFile(): %v", err)
	}
	if _, err := Start(context.Background(), dataDir, newFakeNode(voters(1))); err == nil || !strings.Contains(err.Error(), "non-socket") {
		t.Fatalf("Start() error = %v, want non-socket refusal", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "do not remove" {
		t.Fatalf("occupied file = %q, %v; want preserved", contents, err)
	}
}

func TestDialUsesExactSocketPathAndDeadline(t *testing.T) {
	dataDir := shortTempDir(t)
	server, err := Start(context.Background(), dataDir, newFakeNode(voters(1)))
	if err != nil {
		t.Fatalf("Start(): %v", err)
	}
	defer shutdownServer(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = Dial(ctx, filepath.Join(dataDir, "other.sock"))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial(other path) error = %v, want DeadlineExceeded", err)
	}
}

func TestSocketPathBoundsLongDataDirectory(t *testing.T) {
	dataDir := filepath.Join("/tmp", strings.Repeat("long-membership-path-", 8))
	first := SocketPath(dataDir)
	second := SocketPath(dataDir)
	if first != second {
		t.Fatalf("SocketPath() = %q then %q, want deterministic", first, second)
	}
	if len(first) > maxUnixSocketPathBytes || !strings.HasPrefix(first, "/tmp/relaygate-membership-") {
		t.Fatalf("SocketPath(long) = %q (%d bytes), want bounded /tmp path", first, len(first))
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "relaygate-membership-")
	if err != nil {
		t.Fatalf("MkdirTemp(): %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod(temp dir): %v", err)
	}
	return directory
}

func shutdownServer(t *testing.T, server *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := server.Shutdown(ctx)
	cancel()
	if err != nil {
		t.Errorf("Shutdown(): %v", err)
	}
	for serveErr := range server.Errors() {
		t.Errorf("Server.Errors(): %v", serveErr)
	}
}

func assertPermissions(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q): %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("permissions for %q = %04o, want %04o", path, got, want)
	}
}

func memberIDs(result Result) []string {
	ids := make([]string, 0, len(result.Members))
	for _, member := range result.Members {
		ids = append(ids, member.NodeID)
	}
	return ids
}
