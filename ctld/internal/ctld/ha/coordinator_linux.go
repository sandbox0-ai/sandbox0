//go:build linux

package ha

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const connectInterval = 100 * time.Millisecond

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

type RoleTransition struct {
	From Role
	To   Role
}

type LockIdentity struct {
	Device uint64
	Inode  uint64
	Known  bool
}

type MetricsSnapshot struct {
	State        State
	StateSince   time.Time
	Transitions  map[RoleTransition]uint64
	LockIdentity LockIdentity
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

	mu           sync.RWMutex
	state        State
	stateSince   time.Time
	transitions  map[RoleTransition]uint64
	lockIdentity LockIdentity
}

type PrimaryLease struct {
	Epoch uint64

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
	slot := strings.TrimSpace(cfg.Slot)
	if slot == "" {
		return nil, fmt.Errorf("ctld HA slot is required")
	}
	if err := os.MkdirAll(filepath.Join(rootDir, "ha"), 0o700); err != nil {
		return nil, fmt.Errorf("create ctld HA directory: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Coordinator{
		rootDir:     rootDir,
		slot:        slot,
		logger:      logger,
		state:       State{Role: RoleStarting},
		stateSince:  time.Now(),
		transitions: make(map[RoleTransition]uint64),
	}, nil
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
	previousRole := c.state.Role
	update(&c.state)
	if c.state.Role != previousRole {
		c.transitions[RoleTransition{From: previousRole, To: c.state.Role}]++
		c.stateSince = time.Now()
	}
	c.mu.Unlock()
}

func (c *Coordinator) MetricsSnapshot() MetricsSnapshot {
	if c == nil {
		return MetricsSnapshot{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	transitions := make(map[RoleTransition]uint64, len(c.transitions))
	for transition, count := range c.transitions {
		transitions[transition] = count
	}
	return MetricsSnapshot{State: c.state, StateSince: c.stateSince, Transitions: transitions, LockIdentity: c.lockIdentity}
}

func (c *Coordinator) recordLockIdentity(file *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fmt.Errorf("stat ctld primary lock: %w", err)
	}
	c.mu.Lock()
	c.lockIdentity = LockIdentity{Device: uint64(stat.Dev), Inode: stat.Ino, Known: true}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WaitForPrimary(ctx context.Context) (*PrimaryLease, error) {
	if c == nil {
		return nil, fmt.Errorf("ctld HA coordinator is nil")
	}
	lockFile, err := os.OpenFile(filepath.Join(c.rootDir, "ha", "primary.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ctld primary lock: %w", err)
	}
	if err := c.recordLockIdentity(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = lockFile.Close()
			return nil, err
		}
		locked, err := tryLock(lockFile)
		if err != nil {
			_ = lockFile.Close()
			return nil, err
		}
		if locked {
			epoch, err := c.advanceEpoch()
			if err != nil {
				_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
				_ = lockFile.Close()
				return nil, err
			}
			c.setState(func(state *State) { *state = State{Role: RolePrimary, Epoch: epoch, Synchronized: true} })
			c.logger.Info("ctld HA primary lock acquired", zap.String("slot", c.slot), zap.Uint64("epoch", epoch))
			return &PrimaryLease{Epoch: epoch, coordinator: c, lockFile: lockFile}, nil
		}
		epoch, epochErr := c.currentEpoch()
		if epochErr != nil {
			_ = lockFile.Close()
			return nil, epochErr
		}
		c.setState(func(state *State) { *state = State{Role: RoleStandby, Epoch: epoch} })
		timer := time.NewTimer(connectInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
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
	payload, err := os.ReadFile(filepath.Join(c.rootDir, "ha", "epoch"))
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
