package process

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPTYRunner_FastCommandOutputReliableRepeat(t *testing.T) {
	const runs = 30

	for i := 0; i < runs; i++ {
		base := NewBaseProcess("pty-fast-test", ProcessTypeCMD, ProcessConfig{
			Type: ProcessTypeCMD,
		})
		runner := NewPTYRunner(base, nil, nil)
		ch := base.ReadOutput()

		cmd := exec.Command("/bin/echo", "Hello, Sandbox0!")
		if err := runner.Start(cmd, &PTYSize{Rows: 40, Cols: 120}); err != nil {
			t.Fatalf("run %d: runner.Start() failed: %v", i, err)
		}

		output := waitPTYOutput(t, ch, 3*time.Second)
		if !strings.Contains(output, "Hello, Sandbox0!") {
			t.Fatalf("run %d: output = %q, want to contain message", i, output)
		}
	}
}

func TestPTYRunner_StopWithOptionsKillsTermIgnoringProcessGroup(t *testing.T) {
	base := NewBaseProcess("pty-stop-test", ProcessTypeCMD, ProcessConfig{Type: ProcessTypeCMD})
	runner := NewPTYRunner(base, nil, nil)
	readyPath := t.TempDir() + "/ready"
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("trap '' TERM; : > %q; while :; do sleep 30; done", readyPath))
	if err := runner.Start(cmd, &PTYSize{Rows: 24, Cols: 80}); err != nil {
		t.Fatalf("runner.Start() failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(-base.PID(), syscall.SIGKILL)
			t.Fatal("term-ignoring process did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	pid := base.PID()
	started := time.Now()
	err := runner.StopWithOptions(StopOptions{GracePeriod: 100 * time.Millisecond, KillWait: time.Second})
	if err != nil {
		t.Fatalf("StopWithOptions() failed: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("StopWithOptions() took %s, want bounded termination", elapsed)
	}

	deadline = time.Now().Add(time.Second)
	for {
		err = syscall.Kill(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group %d still exists after stop: %v", pid, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPTYRunner_StopKillsChildrenAfterParentExits(t *testing.T) {
	base := NewBaseProcess("pty-child-stop-test", ProcessTypeCMD, ProcessConfig{Type: ProcessTypeCMD})
	runner := NewPTYRunner(base, nil, nil)
	readyPath := t.TempDir() + "/ready"
	script := fmt.Sprintf(`
sh -c 'trap "" TERM; while :; do sleep 30; done' &
trap 'exit 0' TERM
: > %q
while :; do sleep 30; done
`, readyPath)
	cmd := exec.Command("/bin/sh", "-c", script)
	if err := runner.Start(cmd, &PTYSize{Rows: 24, Cols: 80}); err != nil {
		t.Fatalf("runner.Start() failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(-base.PID(), syscall.SIGKILL)
			t.Fatal("process tree did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	pid := base.PID()
	if err := runner.StopWithOptions(StopOptions{GracePeriod: 100 * time.Millisecond, KillWait: time.Second}); err != nil {
		t.Fatalf("StopWithOptions() failed: %v", err)
	}
	if err := syscall.Kill(-pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process group %d survived parent exit: %v", pid, err)
	}
}

func waitPTYOutput(t *testing.T, ch <-chan ProcessOutput, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	var b strings.Builder
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for PTY output, got=%q", b.String())
		case msg, ok := <-ch:
			if !ok {
				return b.String()
			}
			if msg.Source == OutputSourcePTY && len(msg.Data) > 0 {
				b.Write(msg.Data)
			}
		}
	}
}
