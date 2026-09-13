package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
)

type directChild struct {
	base   *process.BaseProcess
	runner *process.DirectRunner
	last   event
}

// Exercise the actual procd non-PTY runner with the same default PipeStdin=false
// as HTTP CMD creation. This is a subprocess unit test, not local runtime e2e.
func startDirectChild(t *testing.T, mode, lifetime, outputMode string) *directChild {
	t.Helper()
	base := process.NewBaseProcess("resident-control-unit", process.ProcessTypeCMD, process.ProcessConfig{Type: process.ProcessTypeCMD})
	runner := process.NewDirectRunner(base, context.Background(), nil)
	cmd := exec.Command(os.Args[0], "-test.run=^TestDirectControlChild$")
	cmd.Env = append(os.Environ(), "S0_RESIDENT_CONTROL_CHILD=1", "S0_RESIDENT_CONTROL_MODE="+mode, "S0_RESIDENT_CONTROL_LIFETIME="+lifetime, "S0_RESIDENT_CONTROL_OUTPUT="+outputMode, "GORACE=atexit_sleep_ms=0")
	if err := runner.Start(cmd); err != nil {
		t.Fatal(err)
	}
	c := &directChild{base: base, runner: runner}
	t.Cleanup(func() {
		if base.IsRunning() {
			_ = base.SendSignal(syscall.SIGKILL)
		}
		select {
		case <-base.OutputDone():
		case <-time.After(3 * time.Second):
			t.Error("owned test child was not reaped")
		}
	})
	if cmd.Stdin != nil || base.GetConfig().PipeStdin {
		t.Fatal("test accidentally supplied a stdin pipe")
	}
	return c
}

func (c *directChild) next(t *testing.T, state string) event {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		stdout, stderr := c.runner.GetOutput()
		if stderr != "" {
			t.Fatalf("unexpected stderr: %s", stderr)
		}
		// A runner write can end mid-line. Only decode complete telemetry.
		end := strings.LastIndexByte(stdout, '\n') + 1
		decoder := json.NewDecoder(strings.NewReader(stdout[:end]))
		for {
			var v event
			if err := decoder.Decode(&v); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if v.Sequence <= c.last.Sequence {
				continue
			}
			if v.ID != "direct-control-test" || v.PID != c.base.PID() || v.Version != 1 || v.Sequence != c.last.Sequence+1 || v.ElapsedNS < c.last.ElapsedNS || v.ElapsedNS >= v.LifetimeNS {
				t.Fatalf("identity or clock changed: previous=%+v current=%+v", c.last, v)
			}
			c.last = v
			if v.State == state {
				return v
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("missing phase %s", state)
		case <-c.base.OutputDone():
			t.Fatalf("process exited before phase %s", state)
		case <-ticker.C:
		}
	}
}

func (c *directChild) send(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := c.base.SendSignal(sig); err != nil {
		t.Fatal(err)
	}
}

func (c *directChild) exit(t *testing.T, code int, stderrPart string) {
	t.Helper()
	select {
	case <-c.base.OutputDone():
	case <-time.After(3 * time.Second):
		t.Fatal("finite no-stdin process did not exit")
	}
	got, err := c.base.ExitCode()
	_, stderr := c.runner.GetOutput()
	if err != nil || got != code || (stderrPart == "" && stderr != "") || !strings.Contains(stderr, stderrPart) {
		t.Fatalf("exit=%d error=%v stderr=%q", got, err, stderr)
	}
}

func TestDirectRunnerNoStdinDefaultStopsAtEOF(t *testing.T) {
	c := startDirectChild(t, "stdin", "5s", "normal")
	c.exit(t, 0, "")
	stdout, _ := c.runner.GetOutput()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	for _, state := range []string{"idle", "stopped"} {
		var v event
		if err := decoder.Decode(&v); err != nil || v.State != state || v.AllocatedBytes != 0 || v.Workers != 0 {
			t.Fatalf("default EOF state=%s event=%+v error=%v", state, v, err)
		}
	}
	if decoder.Decode(new(event)) != io.EOF {
		t.Fatal("unexpected events after stdin EOF")
	}
}

func TestDirectRunnerNoStdinSignalLifecycle(t *testing.T) {
	c := startDirectChild(t, "signals", "5s", "normal")
	c.next(t, "idle")
	// Waiting through an idle heartbeat proves EOF did not finish signal mode.
	c.next(t, "idle")
	c.send(t, syscall.SIGUSR1)
	loaded := c.next(t, "loaded")
	if loaded.AllocatedBytes != 16384 || loaded.Workers != 2 || len(loaded.Iterations) != 2 || loaded.Iterations[0] == 0 || loaded.Iterations[1] == 0 {
		t.Fatalf("unacknowledged worker or allocation: %+v", loaded)
	}
	c.next(t, "loaded")
	c.send(t, syscall.SIGUSR2)
	released := c.next(t, "released")
	if released.AllocatedBytes != 0 || released.Workers != 0 || len(released.Iterations) != 0 {
		t.Fatalf("release retained work: %+v", released)
	}
	c.next(t, "released")
	c.send(t, syscall.SIGTERM)
	c.exit(t, 0, "")
}

func TestDirectRunnerNoStdinRejectsInvalidSignalTransitions(t *testing.T) {
	for _, states := range [][]string{{"release"}, {"load", "load"}, {"load", "release", "load"}, {"load", "release", "release"}} {
		t.Run(strings.Join(states, "-"), func(t *testing.T) {
			c := startDirectChild(t, "signals", "5s", "normal")
			c.next(t, "idle")
			for i, state := range states {
				sig, ack := syscall.SIGUSR1, "loaded"
				if state == "release" {
					sig, ack = syscall.SIGUSR2, "released"
				}
				c.send(t, sig)
				if i < len(states)-1 {
					c.next(t, ack)
				}
			}
			c.exit(t, 1, "only accepted while")
		})
	}
}

func TestDirectRunnerSignalModeFiniteAndCancellable(t *testing.T) {
	for _, outputMode := range []string{"normal", "block-loaded"} {
		for _, stop := range []string{"expiry", "INT", "TERM"} {
			t.Run(outputMode+"-"+stop, func(t *testing.T) {
				c := startDirectChild(t, "signals", "1s", outputMode)
				c.next(t, "idle")
				c.next(t, "idle")
				c.send(t, syscall.SIGUSR1)
				c.next(t, "loaded")
				if stop != "expiry" {
					sig := syscall.SIGINT
					if stop == "TERM" {
						sig = syscall.SIGTERM
					}
					c.send(t, sig)
				}
				c.exit(t, 0, "")
			})
		}
	}
}

func TestDirectRunnerSignalModeOutputErrorTerminates(t *testing.T) {
	c := startDirectChild(t, "signals", "5s", "fail-loaded")
	c.next(t, "idle")
	c.send(t, syscall.SIGUSR1)
	c.exit(t, 1, "fixture output failure")
}

type controlledChildOutput struct {
	mode string
}

func (w controlledChildOutput) Write(p []byte) (int, error) {
	var v event
	if err := json.Unmarshal(p, &v); err != nil {
		return 0, err
	}
	if v.State == "loaded" && w.mode == "fail-loaded" {
		return 0, fmt.Errorf("fixture output failure")
	}
	n, err := os.Stdout.Write(p)
	if v.State == "loaded" && w.mode == "block-loaded" {
		// Publish the test marker but never return from the actual Write call.
		// The parent must observe process exit without unblocking this writer.
		select {}
	}
	return n, err
}

func TestDirectControlChild(t *testing.T) {
	if os.Getenv("S0_RESIDENT_CONTROL_CHILD") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	args := []string{"--id", "direct-control-test", "--memory-bytes", "16384", "--workers", "2", "--lifetime", os.Getenv("S0_RESIDENT_CONTROL_LIFETIME"), "--control", os.Getenv("S0_RESIDENT_CONTROL_MODE")}
	code := program(ctx, args, os.Stdin, controlledChildOutput{mode: os.Getenv("S0_RESIDENT_CONTROL_OUTPUT")}, os.Stderr)
	stop()
	os.Exit(code)
}
