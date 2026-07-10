// Package reaper removes orphaned zombie processes adopted by procd as PID 1.
package reaper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

var managedPIDs = struct {
	sync.RWMutex
	values map[int]struct{}
}{values: map[int]struct{}{}}

// StartManaged starts a child and registers its PID atomically with respect to
// orphan scans. pid must return the child PID after start succeeds.
func StartManaged(start func() error, pid func() int) error {
	managedPIDs.Lock()
	defer managedPIDs.Unlock()
	if err := start(); err != nil {
		return err
	}
	if childPID := pid(); childPID > 0 {
		managedPIDs.values[childPID] = struct{}{}
	}
	return nil
}

// Untrack removes a child after its owning wait path has completed.
func Untrack(pid int) {
	if pid <= 0 {
		return
	}
	managedPIDs.Lock()
	delete(managedPIDs.values, pid)
	managedPIDs.Unlock()
}

// Run periodically reaps untracked zombie children adopted by procd.
func Run(ctx context.Context, logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reapOrphans(logger)
		}
	}
}

func reapOrphans(logger *zap.Logger) {
	entries, err := filepath.Glob("/proc/[0-9]*/stat")
	if err != nil {
		return
	}
	parentPID := os.Getpid()
	for _, path := range entries {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		pid, ppid, state, ok := parseProcStat(string(data))
		if !ok || ppid != parentPID || state != "Z" {
			continue
		}
		managedPIDs.RLock()
		if _, managed := managedPIDs.values[pid]; managed {
			managedPIDs.RUnlock()
			continue
		}
		var status syscall.WaitStatus
		var usage syscall.Rusage
		reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, &usage)
		managedPIDs.RUnlock()
		if err != nil && !errors.Is(err, syscall.ECHILD) {
			logger.Debug("Failed to reap orphaned process", zap.Int("pid", pid), zap.Error(err))
			continue
		}
		if reaped == pid {
			logger.Debug("Reaped orphaned process", zap.Int("pid", pid))
		}
	}
}

func isManaged(pid int) bool {
	managedPIDs.RLock()
	_, ok := managedPIDs.values[pid]
	managedPIDs.RUnlock()
	return ok
}

func parseProcStat(value string) (pid, ppid int, state string, ok bool) {
	closing := strings.LastIndex(value, ")")
	opening := strings.Index(value, "(")
	if opening <= 0 || closing <= opening || closing+2 >= len(value) {
		return 0, 0, "", false
	}
	parsedPID, err := strconv.Atoi(strings.TrimSpace(value[:opening]))
	if err != nil {
		return 0, 0, "", false
	}
	fields := strings.Fields(value[closing+1:])
	if len(fields) < 2 {
		return 0, 0, "", false
	}
	parsedPPID, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, "", false
	}
	return parsedPID, parsedPPID, fields[0], true
}
