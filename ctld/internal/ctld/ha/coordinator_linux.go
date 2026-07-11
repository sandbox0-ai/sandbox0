//go:build linux

package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	protocolVersion = 1
	maxPacketSize   = 1 << 20
	connectInterval = 100 * time.Millisecond
	pingInterval    = time.Second
)

var ErrStandbyUnavailable = errors.New("ctld standby is unavailable")

type Role string

const (
	RoleStarting Role = "starting"
	RolePrimary  Role = "primary"
	RoleStandby  Role = "standby"
)

type State struct {
	Role         Role
	Epoch        uint64
	Synchronized bool
	Standbys     int
}

type Config struct {
	RootDir string
	Slot    string
	Logger  *zap.Logger
}

type Coordinator struct {
	rootDir string
	slot    string
	logger  *zap.Logger

	mu    sync.RWMutex
	state State
}

type RecoveredPortal struct {
	Manifest ctldportal.RecoveryManifest
	Channel  *os.File
}

type PrimaryLease struct {
	Epoch      uint64
	Replicator *Replicator
	Recovery   []RecoveredPortal

	coordinator *Coordinator
	lockFile    *os.File
	closeOnce   sync.Once
	closeErr    error
}

func NewCoordinator(cfg Config) (*Coordinator, error) {
	rootDir := strings.TrimSpace(cfg.RootDir)
	if rootDir == "" {
		return nil, fmt.Errorf("ctld HA root directory is required")
	}
	if err := os.MkdirAll(filepath.Join(rootDir, "ha"), 0o700); err != nil {
		return nil, fmt.Errorf("create ctld HA directory: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	coordinator := &Coordinator{
		rootDir: rootDir,
		slot:    strings.TrimSpace(cfg.Slot),
		logger:  logger,
		state:   State{Role: RoleStarting},
	}
	return coordinator, nil
}

func (c *Coordinator) State() State {
	if c == nil {
		return State{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *Coordinator) setState(update func(*State)) {
	if c == nil || update == nil {
		return
	}
	c.mu.Lock()
	update(&c.state)
	c.mu.Unlock()
}

func (c *Coordinator) WaitForPrimary(ctx context.Context) (*PrimaryLease, error) {
	if c == nil {
		return nil, fmt.Errorf("ctld HA coordinator is nil")
	}
	lockPath := filepath.Join(c.rootDir, "ha", "primary.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ctld primary lock: %w", err)
	}
	var standby *standbyState
	for {
		if err := ctx.Err(); err != nil {
			_ = lockFile.Close()
			if standby != nil {
				standby.close()
			}
			return nil, err
		}
		locked, err := tryLock(lockFile)
		if err != nil {
			_ = lockFile.Close()
			if standby != nil {
				standby.close()
			}
			return nil, err
		}
		if locked {
			epoch, err := c.advanceEpoch()
			if err != nil {
				_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
				_ = lockFile.Close()
				if standby != nil {
					standby.close()
				}
				return nil, err
			}
			replicator, err := newReplicator(c.socketPath(), epoch, c.logger, func(count int) {
				c.setState(func(state *State) {
					state.Standbys = count
					state.Synchronized = count > 0
				})
			})
			if err != nil {
				_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
				_ = lockFile.Close()
				if standby != nil {
					standby.close()
				}
				return nil, err
			}
			c.setState(func(state *State) {
				*state = State{Role: RolePrimary, Epoch: epoch}
			})
			lease := &PrimaryLease{
				Epoch:       epoch,
				Replicator:  replicator,
				coordinator: c,
				lockFile:    lockFile,
			}
			if standby != nil {
				lease.Recovery = standby.take()
			}
			c.logger.Info("ctld HA primary lock acquired", zap.String("slot", c.slot), zap.Uint64("epoch", epoch), zap.Int("recovered_portals", len(lease.Recovery)))
			return lease, nil
		}

		if standby != nil {
			currentEpoch, epochErr := c.currentEpoch()
			if epochErr != nil {
				standby.close()
				_ = lockFile.Close()
				return nil, epochErr
			}
			if currentEpoch > standby.epoch {
				standby.close()
				standby = nil
			}
		}
		standbyEpoch := uint64(0)
		if standby != nil {
			standbyEpoch = standby.epoch
		}
		c.setState(func(state *State) {
			*state = State{Role: RoleStandby, Epoch: standbyEpoch}
		})
		standby, err = c.followPrimary(ctx, standby)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.logger.Warn("ctld HA standby disconnected", zap.String("slot", c.slot), zap.Error(err))
		}
	}
}

func (c *Coordinator) followPrimary(ctx context.Context, previous *standbyState) (*standbyState, error) {
	if err := ctx.Err(); err != nil {
		return previous, err
	}
	conn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: c.socketPath(), Net: "unixpacket"})
	if err != nil {
		timer := time.NewTimer(connectInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return previous, ctx.Err()
		case <-timer.C:
			return previous, nil
		}
	}
	candidate := newStandbyState()
	swapped := false
	err = candidate.receive(ctx, conn, func(epoch uint64, synchronized bool) {
		if synchronized && !swapped {
			if previous != nil {
				previous.close()
				previous = nil
			}
			swapped = true
		}
		c.setState(func(current *State) {
			current.Role = RoleStandby
			current.Epoch = epoch
			current.Synchronized = synchronized
			current.Standbys = 0
		})
	})
	_ = conn.Close()
	if candidate.synchronized {
		return candidate, err
	}
	candidate.close()
	return previous, err
}

func (c *Coordinator) socketPath() string {
	return filepath.Join(c.rootDir, "ha", "primary.sock")
}

func (c *Coordinator) advanceEpoch() (uint64, error) {
	path := filepath.Join(c.rootDir, "ha", "epoch")
	current, err := c.currentEpoch()
	if err != nil {
		return 0, err
	}
	next := current + 1
	tmp, err := os.CreateTemp(filepath.Dir(path), ".epoch-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("create ctld HA epoch: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return 0, err
	}
	if _, err := fmt.Fprintf(tmp, "%d\n", next); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("write ctld HA epoch: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("sync ctld HA epoch: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return 0, fmt.Errorf("replace ctld HA epoch: %w", err)
	}
	return next, nil
}

func (c *Coordinator) currentEpoch() (uint64, error) {
	path := filepath.Join(c.rootDir, "ha", "epoch")
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read ctld HA epoch: %w", err)
	}
	current, err := strconv.ParseUint(strings.TrimSpace(string(payload)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse ctld HA epoch: %w", err)
	}
	return current, nil
}

func tryLock(file *os.File) (bool, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("acquire ctld primary lock: %w", err)
	}
	return true, nil
}

func (l *PrimaryLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		for _, recovered := range l.Recovery {
			if recovered.Channel != nil {
				_ = recovered.Channel.Close()
			}
		}
		if l.Replicator != nil {
			l.Replicator.Close()
		}
		if l.lockFile != nil {
			if err := unix.Flock(int(l.lockFile.Fd()), unix.LOCK_UN); err != nil {
				l.closeErr = err
			}
			if err := l.lockFile.Close(); err != nil && l.closeErr == nil {
				l.closeErr = err
			}
		}
		if l.coordinator != nil {
			l.coordinator.setState(func(state *State) { *state = State{Role: RoleStarting} })
		}
	})
	return l.closeErr
}

type wireMessage struct {
	Version  int                          `json:"version"`
	Type     string                       `json:"type"`
	Sequence uint64                       `json:"sequence"`
	Epoch    uint64                       `json:"epoch"`
	Manifest *ctldportal.RecoveryManifest `json:"manifest,omitempty"`
	Key      string                       `json:"key,omitempty"`
	Error    string                       `json:"error,omitempty"`
}

const (
	messagePublish     = "publish"
	messageUpdate      = "update"
	messageRemove      = "remove"
	messageSnapshotEnd = "snapshot_end"
	messagePing        = "ping"
	messageAck         = "ack"
)

type standbyState struct {
	epoch        uint64
	synchronized bool
	portals      map[string]RecoveredPortal
}

func newStandbyState() *standbyState {
	return &standbyState{portals: make(map[string]RecoveredPortal)}
}

func (s *standbyState) receive(ctx context.Context, conn *net.UnixConn, onState func(uint64, bool)) error {
	if conn == nil {
		return fmt.Errorf("ctld HA standby connection is nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		payload := make([]byte, maxPacketSize)
		oob := make([]byte, unix.CmsgSpace(4))
		n, oobn, _, _, err := conn.ReadMsgUnix(payload, oob)
		if err != nil {
			return err
		}
		var message wireMessage
		if err := json.Unmarshal(payload[:n], &message); err != nil {
			return fmt.Errorf("decode ctld HA message: %w", err)
		}
		ackErr := s.apply(message, oob[:oobn])
		ack := wireMessage{Version: protocolVersion, Type: messageAck, Sequence: message.Sequence, Epoch: message.Epoch}
		if ackErr != nil {
			ack.Error = ackErr.Error()
		}
		if err := writePacket(conn, ack, nil); err != nil {
			return err
		}
		if ackErr != nil {
			return ackErr
		}
		if onState != nil {
			onState(s.epoch, s.synchronized)
		}
	}
}

func (s *standbyState) apply(message wireMessage, oob []byte) error {
	if message.Version != protocolVersion {
		return fmt.Errorf("unsupported ctld HA protocol version %d", message.Version)
	}
	if message.Epoch == 0 || (s.epoch != 0 && message.Epoch != s.epoch) {
		return fmt.Errorf("invalid ctld HA epoch %d", message.Epoch)
	}
	s.epoch = message.Epoch
	switch message.Type {
	case messagePublish:
		if message.Manifest == nil || strings.TrimSpace(message.Manifest.Key) == "" {
			return fmt.Errorf("ctld HA publish manifest is required")
		}
		channel, err := fileFromRights(oob)
		if err != nil {
			return err
		}
		if existing := s.portals[message.Manifest.Key]; existing.Channel != nil {
			_ = existing.Channel.Close()
		}
		s.portals[message.Manifest.Key] = RecoveredPortal{Manifest: *message.Manifest, Channel: channel}
	case messageUpdate:
		if message.Manifest == nil || strings.TrimSpace(message.Manifest.Key) == "" {
			return fmt.Errorf("ctld HA update manifest is required")
		}
		existing, ok := s.portals[message.Manifest.Key]
		if !ok || existing.Channel == nil {
			return fmt.Errorf("ctld HA portal %q was not published", message.Manifest.Key)
		}
		existing.Manifest = *message.Manifest
		s.portals[message.Manifest.Key] = existing
	case messageRemove:
		existing := s.portals[message.Key]
		if existing.Channel != nil {
			_ = existing.Channel.Close()
		}
		delete(s.portals, message.Key)
	case messageSnapshotEnd:
		s.synchronized = true
	case messagePing:
		return nil
	default:
		return fmt.Errorf("unsupported ctld HA message type %q", message.Type)
	}
	return nil
}

func (s *standbyState) take() []RecoveredPortal {
	if s == nil {
		return nil
	}
	portals := make([]RecoveredPortal, 0, len(s.portals))
	for key, recovered := range s.portals {
		portals = append(portals, recovered)
		delete(s.portals, key)
	}
	return portals
}

func (s *standbyState) close() {
	if s == nil {
		return
	}
	for key, recovered := range s.portals {
		if recovered.Channel != nil {
			_ = recovered.Channel.Close()
		}
		delete(s.portals, key)
	}
}

func fileFromRights(oob []byte) (*os.File, error) {
	messages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("parse ctld HA descriptor message: %w", err)
	}
	var descriptors []int
	for _, message := range messages {
		fds, err := unix.ParseUnixRights(&message)
		if err != nil {
			return nil, fmt.Errorf("parse ctld HA descriptors: %w", err)
		}
		descriptors = append(descriptors, fds...)
	}
	if len(descriptors) != 1 {
		for _, fd := range descriptors {
			_ = unix.Close(fd)
		}
		return nil, fmt.Errorf("ctld HA publish contained %d descriptors, want 1", len(descriptors))
	}
	unix.CloseOnExec(descriptors[0])
	return os.NewFile(uintptr(descriptors[0]), "ctld-ha-fuse"), nil
}

func writePacket(conn *net.UnixConn, message wireMessage, file *os.File) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	var oob []byte
	if file != nil {
		oob = unix.UnixRights(int(file.Fd()))
	}
	if _, _, err := conn.WriteMsgUnix(payload, oob, nil); err != nil {
		return fmt.Errorf("write ctld HA packet: %w", err)
	}
	return nil
}
