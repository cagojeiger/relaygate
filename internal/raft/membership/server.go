package membership

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	operatorv1 "github.com/cagojeiger/relaygate/internal/gen/operator/v1"
)

const (
	socketName             = "membership.sock"
	lockName               = "membership.lock"
	staleDialTimeout       = 100 * time.Millisecond
	maxUnixSocketPathBytes = 100
)

// Server hosts the controller membership operator API on a local Unix socket.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	path       string
	socketInfo os.FileInfo
	lockFile   *os.File

	errors chan error
	done   chan struct{}
	stop   sync.Once
}

// SocketPath returns the only socket path used by the membership operator API.
// Most data directories keep the socket beside the Raft store. Long paths use
// a deterministic short /tmp name because sockaddr_un is limited to roughly
// 100 bytes on supported Unix platforms. The exclusive lock remains in the
// protected data directory in both cases.
func SocketPath(dataDir string) string {
	canonical, err := filepath.Abs(dataDir)
	if err != nil {
		canonical = filepath.Clean(dataDir)
	}
	candidate := filepath.Join(canonical, socketName)
	if len(candidate) <= maxUnixSocketPathBytes {
		return candidate
	}
	digest := sha256.Sum256([]byte(canonical))
	return filepath.Join("/tmp", "relaygate-membership-"+hex.EncodeToString(digest[:16])+".sock")
}

// Start creates the private Unix socket and begins serving membership RPCs.
func Start(ctx context.Context, dataDir string, node Node) (*Server, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("start membership operator: %w", err)
	}
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("membership operator data directory is required")
	}
	if node == nil {
		return nil, errors.New("membership operator Raft node is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create membership operator data directory: %w", err)
	}
	if err := os.Chmod(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("secure membership operator data directory: %w", err)
	}

	lockFile, err := acquireLock(filepath.Join(dataDir, lockName))
	if err != nil {
		return nil, err
	}
	cleanupLock := true
	defer func() {
		if cleanupLock {
			releaseLock(lockFile)
		}
	}()

	path := SocketPath(dataDir)
	if err := removeStaleSocket(path); err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on membership operator socket %q: %w", path, err)
	}
	if unixListener, ok := listener.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	socketInfo, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("inspect membership operator socket: %w", err)
	}
	cleanupListener := true
	defer func() {
		if cleanupListener {
			_ = listener.Close()
			removeOwnedSocket(path, socketInfo)
		}
	}()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("secure membership operator socket: %w", err)
	}
	securedInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("reinspect secured membership operator socket: %w", err)
	}
	if !os.SameFile(socketInfo, securedInfo) {
		return nil, fmt.Errorf("membership operator socket %q changed while securing it", path)
	}

	grpcServer := grpc.NewServer()
	operatorv1.RegisterMembershipServer(grpcServer, newService(node))
	server := &Server{
		grpcServer: grpcServer,
		listener:   listener,
		path:       path,
		socketInfo: socketInfo,
		lockFile:   lockFile,
		errors:     make(chan error, 1),
		done:       make(chan struct{}),
	}
	cleanupLock = false
	cleanupListener = false
	go server.serve()
	return server, nil
}

// Errors reports an unexpected gRPC serve failure and closes when serving ends.
func (s *Server) Errors() <-chan error {
	return s.errors
}

// Shutdown gracefully stops the operator service until ctx expires.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stop.Do(func() {
		go s.grpcServer.GracefulStop()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		return fmt.Errorf("shutdown membership operator: %w", ctx.Err())
	}
}

func (s *Server) serve() {
	err := s.grpcServer.Serve(s.listener)
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		s.errors <- fmt.Errorf("serve membership operator: %w", err)
	}
	_ = s.listener.Close()
	removeOwnedSocket(s.path, s.socketInfo)
	releaseLock(s.lockFile)
	close(s.done)
	close(s.errors)
}

func acquireLock(path string) (*os.File, error) {
	lockFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open membership operator lock: %w", err)
	}
	if err := lockFile.Chmod(0o600); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("secure membership operator lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New("membership operator is already running for this data directory")
		}
		return nil, fmt.Errorf("lock membership operator data directory: %w", err)
	}
	return lockFile, nil
}

func releaseLock(lockFile *os.File) {
	if lockFile == nil {
		return
	}
	_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	_ = lockFile.Close()
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect membership operator socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("membership operator socket path %q is occupied by a non-socket file", path)
	}

	connection, dialErr := net.DialTimeout("unix", path, staleDialTimeout)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("membership operator socket %q already has a live listener", path)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
		return fmt.Errorf("refuse to remove membership operator socket %q after inconclusive probe: %w", path, dialErr)
	}

	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reinspect membership operator socket: %w", err)
	}
	if !os.SameFile(info, current) {
		return fmt.Errorf("membership operator socket %q changed during stale check", path)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale membership operator socket: %w", err)
	}
	return nil
}

func removeOwnedSocket(path string, owned os.FileInfo) {
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(owned, current) {
		return
	}
	_ = os.Remove(path)
}
