package cgroup

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	freezeV2File   = "cgroup.freeze"
	freezeV1File   = "freezer.state"
	memoryV2File   = "memory.current"
	memoryV1File   = "memory.usage_in_bytes"
	defaultTimeout = 2 * time.Second
	defaultPoll    = 20 * time.Millisecond
)

type FS struct {
	SettleTimeout time.Duration
	PollInterval  time.Duration
}

func NewFS() *FS {
	return &FS{SettleTimeout: defaultTimeout, PollInterval: defaultPoll}
}

func (fs *FS) Freeze(dir string) error {
	return fs.setFrozen(dir, true)
}

func (fs *FS) Thaw(dir string) error {
	return fs.setFrozen(dir, false)
}

func (fs *FS) MemoryCurrent(dir string) (int64, error) {
	for _, candidate := range []string{memoryV2File, memoryV1File} {
		value, err := readInt64File(filepath.Join(dir, candidate))
		if err == nil {
			return value, nil
		}
		if !os.IsNotExist(err) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("no memory usage file found in %s", dir)
}

func (fs *FS) setFrozen(dir string, frozen bool) error {
	targetFile, freezeValue, expectedState, err := freezeControl(dir, frozen)
	if err != nil {
		return err
	}
	if err := os.WriteFile(targetFile, []byte(freezeValue), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", targetFile, err)
	}
	return fs.waitForState(filepath.Dir(targetFile), expectedState)
}

func freezeControl(dir string, frozen bool) (string, string, string, error) {
	v2Path := filepath.Join(dir, freezeV2File)
	if fileExists(v2Path) {
		if frozen {
			return v2Path, "1", "1", nil
		}
		return v2Path, "0", "0", nil
	}
	v1Path := filepath.Join(dir, freezeV1File)
	if fileExists(v1Path) {
		if frozen {
			return v1Path, "FROZEN", "FROZEN", nil
		}
		return v1Path, "THAWED", "THAWED", nil
	}
	return "", "", "", fmt.Errorf("no freezer control file found in %s", dir)
}

func (fs *FS) waitForState(dir, expected string) error {
	deadline := time.Now().Add(timeoutOrDefault(fs.SettleTimeout, defaultTimeout))
	poll := timeoutOrDefault(fs.PollInterval, defaultPoll)
	for {
		state, err := readFreezeState(dir)
		if err == nil && state == expected {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("cgroup state did not converge to %s", expected)
		}
		time.Sleep(poll)
	}
}

func readFreezeState(dir string) (string, error) {
	for _, candidate := range []string{freezeV2File, freezeV1File} {
		path := filepath.Join(dir, candidate)
		value, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSpace(string(value)), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
	}
	return "", fmt.Errorf("no freezer state file found in %s", dir)
}

func readInt64File(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	return value, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func timeoutOrDefault(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}
