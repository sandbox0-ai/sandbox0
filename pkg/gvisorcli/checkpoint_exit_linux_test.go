//go:build linux

package gvisorcli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func checkpointLiveProcess(t *testing.T) (*exec.Cmd, func()) {
	t.Helper()
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = child.Process.Kill(); _ = child.Wait() }) }
	t.Cleanup(stop)
	return child, stop
}

func checkpointEarlyStoppedRunner(t *testing.T, pid int) (*Command, string) {
	t.Helper()
	runner, log := checkpointRunner(t, fmt.Sprintf(`case "$*" in
*' checkpoint '*) touch "$0.saved" ;;
*' state '*)
  if [ -e "$0.saved" ]; then
    printf '%%s' '{"id":"source","status":"stopped","pid":-1}'
  else
    printf '%%s' '{"id":"source","status":"running","pid":%d}'
  fi ;;
esac
`, pid))
	runner.checkpointSource = nil
	return runner, log
}

func awaitCheckpointSave(t *testing.T, runner *Command) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(runner.config.Path + ".saved"); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("checkpoint command did not run")
}

func TestCheckpointWaitsForPhysicalExitAfterEarlyStoppedObservation(t *testing.T) {
	child, stop := checkpointLiveProcess(t)
	runner, log := checkpointEarlyStoppedRunner(t, child.Process.Pid)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Checkpoint(ctx, "source", filepath.Join(t.TempDir(), "image")) }()
	awaitCheckpointSave(t, runner)
	select {
	case err := <-done:
		t.Fatalf("capture finished while its host process still existed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	calls := readCheckpointCalls(t, log)
	if strings.Count(calls, "\nstate\n") != 3 || strings.Count(calls, "\ncheckpoint\n") != 1 || strings.Contains(calls, "\nkill\n") {
		t.Fatalf("unexpected source capture sequence: %s", calls)
	}
}

func TestCheckpointPhysicalExitCancellationRetainsImageAndDoesNotKill(t *testing.T) {
	child, _ := checkpointLiveProcess(t)
	runner, log := checkpointEarlyStoppedRunner(t, child.Process.Pid)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	image := filepath.Join(t.TempDir(), "image")
	done := make(chan error, 1)
	go func() { done <- runner.Checkpoint(ctx, "source", image) }()
	awaitCheckpointSave(t, runner)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("capture ignored canceled physical-exit observation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("physical-exit observation did not stop")
	}
	if err := child.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("capture canceled by killing its source", err)
	}
	if _, err := os.Stat(image); err != nil {
		t.Fatal("capture discarded uncertain image custody", err)
	}
	if strings.Contains(readCheckpointCalls(t, log), "\nkill\n") {
		t.Fatal("capture invoked kill")
	}
}

func TestCheckpointRejectsChangedSourceWhilePinningPhysicalExit(t *testing.T) {
	child, _ := checkpointLiveProcess(t)
	runner, log := checkpointRunner(t, fmt.Sprintf(`case "$*" in
*' state '*)
  if [ -e "$0.observed" ]; then pid=%d; else pid=%d; touch "$0.observed"; fi
  printf '{"id":"source","status":"running","pid":%%s}' "$pid" ;;
esac
`, child.Process.Pid+1, child.Process.Pid))
	runner.checkpointSource = nil
	if err := runner.Checkpoint(t.Context(), "source", filepath.Join(t.TempDir(), "image")); err == nil {
		t.Fatal("changed source PID was accepted")
	}
	if strings.Contains(readCheckpointCalls(t, log), "\ncheckpoint\n") {
		t.Fatal("source changed before capture but checkpoint was invoked")
	}
}

func TestCheckpointRejectsUnpinnableSourceBeforeExecution(t *testing.T) {
	for _, state := range []string{
		`{"id":"source","status":"running","pid":0}`,
		`{"id":"source","status":"stopped","pid":-1}`,
		`{"id":"other","status":"running","pid":1}`,
	} {
		t.Run(state, func(t *testing.T) {
			runner, log := checkpointRunner(t, "printf '%s' '"+state+"'\n")
			runner.checkpointSource = nil
			if err := runner.Checkpoint(t.Context(), "source", filepath.Join(t.TempDir(), "image")); err == nil {
				t.Fatal("invalid source was accepted")
			}
			if strings.Contains(readCheckpointCalls(t, log), "\ncheckpoint\n") {
				t.Fatal("invalid source invoked checkpoint")
			}
		})
	}
}
