package process

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestDirectRunner_WriterPathPublishesAndCapturesOutput(t *testing.T) {
	base := NewBaseProcess("direct-writer-test", ProcessTypeCMD, ProcessConfig{
		Type: ProcessTypeCMD,
	})
	runner := NewDirectRunner(base, context.Background(), nil)
	outputCh := base.ReadOutput()

	cmd := exec.Command("/bin/echo", "writer-path")
	if err := runner.Start(cmd); err != nil {
		t.Fatalf("runner.Start() failed: %v", err)
	}

	waitForRunnerStop(t, base, 5*time.Second)

	stdout, stderr := runner.GetOutput()
	if !strings.Contains(stdout, "writer-path") {
		t.Fatalf("stdout = %q, want to contain %q", stdout, "writer-path")
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}

	received := false
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-timeout:
			if !received {
				t.Fatal("timed out waiting for output channel data")
			}
			return
		case out, ok := <-outputCh:
			if !ok {
				if !received {
					t.Fatal("output channel closed without data")
				}
				return
			}
			if len(out.Data) > 0 {
				received = true
			}
		}
	}
}

func waitForRunnerStop(t *testing.T, base *BaseProcess, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("runner %s did not stop in time", base.ID())
		case <-ticker.C:
			if !base.IsRunning() {
				return
			}
		}
	}
}
