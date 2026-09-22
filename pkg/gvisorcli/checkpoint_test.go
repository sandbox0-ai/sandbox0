package gvisorcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func checkpointRunner(t *testing.T, behavior string) (*Command, string) {
	t.Helper()
	dir := t.TempDir()
	// The generated executable has a fixed behavior and writes arguments next
	// to itself; no shell interpolation of the temporary directory is needed.
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"$0.calls\"\n" + behavior
	binary := filepath.Join(dir, "runsc")
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	log := binary + ".calls"
	runner := New(Config{Path: binary, Root: filepath.Join(dir, "root"),
		Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true}).(*Command)
	// These adapter tests control OCI observations. Linux-specific tests below
	// use real child processes to exercise physical exit and source pinning.
	runner.checkpointSource = func(context.Context, string) (checkpointExitWaiter, error) {
		return immediateCheckpointExit{}, nil
	}
	return runner, log
}

type immediateCheckpointExit struct{}

func (immediateCheckpointExit) Wait(ctx context.Context) error { return ctx.Err() }
func (immediateCheckpointExit) Close() error                   { return nil }

func readCheckpointCalls(t *testing.T, log string) string {
	t.Helper()
	data, err := os.ReadFile(log)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCheckpointStopsSourceAndPreservesImmutableRuntimeFlags(t *testing.T) {
	runner, log := checkpointRunner(t, "case \"$*\" in *' state '*) printf '%s' '{\"id\":\"source\",\"status\":\"stopped\"}';; esac\n")
	image := filepath.Join(t.TempDir(), "image with spaces")
	if err := runner.Checkpoint(t.Context(), "source", image); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(image)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("checkpoint permissions: %v, %v", info, err)
	}
	want := strings.Join(append(runner.GlobalArgs(), "checkpoint", "--image-path="+image,
		"--compression=none", "--exclude-committed-zero-pages", "source"), "\n") + "\n"
	want += strings.Join(append(runner.GlobalArgs(), "state", "source"), "\n") + "\n"
	if got := readCheckpointCalls(t, log); got != want {
		t.Fatalf("checkpoint arguments = %q, want %q", got, want)
	}
	if err := runner.Checkpoint(t.Context(), "source", image); !errors.Is(err, os.ErrExist) {
		t.Fatalf("retry must reject an existing image directory: %v", err)
	}
	if got := readCheckpointCalls(t, log); got != want {
		t.Fatal("existing image invoked runsc again")
	}
}

func TestCheckpointWaitsForSourceExitWithoutInventingCompletion(t *testing.T) {
	for _, outcome := range []string{"delayed-exit", "running", "wrong-container", "paused", "missing"} {
		t.Run(outcome, func(t *testing.T) {
			behavior := `case "$*" in
*' state '*)
`
			switch outcome {
			case "delayed-exit":
				behavior += `if [ -e "$0.observed" ]; then printf '%s' '{"id":"source","status":"stopped"}'; else touch "$0.observed"; printf '%s' '{"id":"source","status":"running"}'; fi`
			case "running":
				behavior += `printf '%s' '{"id":"source","status":"running"}'`
			case "wrong-container":
				behavior += `printf '%s' '{"id":"other","status":"stopped"}'`
			case "paused":
				behavior += `printf '%s' '{"id":"source","status":"paused"}'`
			case "missing":
				behavior += `exit 1`
			}
			behavior += "\n;; esac\n"
			runner, log := checkpointRunner(t, behavior)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			image := filepath.Join(t.TempDir(), "capture")
			err := runner.Checkpoint(ctx, "source", image)
			if (err == nil) != (outcome == "delayed-exit") {
				t.Fatalf("Checkpoint() = %v", err)
			}
			if _, err := os.Stat(image); err != nil {
				t.Fatal("checkpoint custody must survive failed exit confirmation", err)
			}
			calls := readCheckpointCalls(t, log)
			if strings.Count(calls, "\ncheckpoint\n") != 1 || strings.Contains(calls, "\nkill\n") || strings.Contains(calls, "\nstart\n") || strings.Contains(calls, "\nresume\n") {
				t.Fatalf("source-exit observation changed execution: %s", calls)
			}
		})
	}
}

func TestCheckpointRetainsFailedAttemptWithoutResumingSource(t *testing.T) {
	runner, log := checkpointRunner(t, "exit 1\n")
	image := filepath.Join(t.TempDir(), "partial")
	if err := runner.Checkpoint(t.Context(), "source", image); err == nil {
		t.Fatal("failed checkpoint was accepted")
	}
	if _, err := os.Stat(image); err != nil {
		t.Fatalf("failed attempt lost its custody marker: %v", err)
	}
	calls := readCheckpointCalls(t, log)
	if strings.Contains(calls, "leave-running") || strings.Contains(calls, "\nstart\n") ||
		strings.Contains(calls, "\nrestore\n") {
		t.Fatalf("checkpoint failure resumed the source: %q", calls)
	}
}

func TestRestoreRequiresExactCreatedContainerAndNeverStartsFresh(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		valid          bool
	}{
		{"created", `{"id":"target","status":"created"}`, true},
		{"running", `{"id":"target","status":"running"}`, false},
		{"stopped", `{"id":"target","status":"stopped"}`, false},
		{"wrong identity", `{"id":"other","status":"created"}`, false},
		{"invalid response", `{`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner, log := checkpointRunner(t, "case \"$*\" in\n*' state '*) printf '%s' '"+tc.response+"';;\nesac\n")
			image := t.TempDir()
			if err := os.Chmod(image, 0o700); err != nil {
				t.Fatal(err)
			}
			err := runner.Restore(t.Context(), "target", image)
			if (err == nil) != tc.valid {
				t.Fatalf("Restore() = %v", err)
			}
			calls := readCheckpointCalls(t, log)
			if strings.Contains(calls, "\nrestore\n") != tc.valid || strings.Contains(calls, "\nstart\n") {
				t.Fatalf("unexpected runtime calls: %q", calls)
			}
			if tc.valid {
				want := strings.Join(append(runner.GlobalArgs(), "restore", "--image-path="+image,
					"--detach", "target"), "\n") + "\n"
				if !strings.HasSuffix(calls, want) {
					t.Fatalf("restore arguments = %q, want suffix %q", calls, want)
				}
			}
		})
	}
}

func TestCheckpointRejectsInvalidInputBeforeInvokingRunsc(t *testing.T) {
	runner, log := checkpointRunner(t, "exit 0\n")
	for _, tc := range []struct{ id, path string }{
		{"", "/tmp/image"}, {"--help", "/tmp/image"}, {"a/b", "/tmp/image"},
		{"source", "relative"}, {"source", "/tmp/../image"}, {"source", "/"},
	} {
		if err := runner.Checkpoint(t.Context(), tc.id, tc.path); err == nil {
			t.Fatalf("accepted invalid request: %+v", tc)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	image := filepath.Join(t.TempDir(), "canceled")
	if err := runner.Checkpoint(ctx, "source", image); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled checkpoint: %v", err)
	}
	if _, err := os.Stat(image); !os.IsNotExist(err) {
		t.Fatalf("canceled checkpoint created a directory: %v", err)
	}
	if got := readCheckpointCalls(t, log); got != "" {
		t.Fatalf("invalid input reached runsc: %q", got)
	}
}

func TestRestoreRejectsUnprotectedOrRedirectedImages(t *testing.T) {
	runner, log := checkpointRunner(t, "exit 0\n")
	parent := t.TempDir()
	image := filepath.Join(parent, "image")
	if err := os.Mkdir(image, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(image, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{image, link, filepath.Join(parent, "missing")} {
		if err := runner.Restore(t.Context(), "target", path); err == nil {
			t.Fatalf("accepted unsafe image: %s", path)
		}
	}
	if got := readCheckpointCalls(t, log); got != "" {
		t.Fatalf("unsafe restore reached runsc: %q", got)
	}
}

func TestCheckpointWritebackErrorCannotBecomeCaptureSuccess(t *testing.T) {
	runner, _ := checkpointRunner(t, "case \"$*\" in *' state '*) printf '%s' '{\"id\":\"source\",\"status\":\"stopped\"}';; esac\n")
	joined := false
	runner.checkpointWriteback = func(context.Context, string) (func() error, error) {
		return func() error { joined = true; return errors.New("retained writeback failed") }, nil
	}
	err := runner.Checkpoint(t.Context(), "source", filepath.Join(t.TempDir(), "image"))
	if err == nil || !strings.Contains(err.Error(), "retained writeback failed") || !joined {
		t.Fatalf("checkpoint lost writeback completion: joined=%t, err=%v", joined, err)
	}
}
