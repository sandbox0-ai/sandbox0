package session

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestSupervisorConcurrentCreateDeduplicatesByKey(t *testing.T) {
	supervisor := newTestSupervisor(t)
	spec := SessionSpec{
		Command:   []string{"/bin/true"},
		Lifecycle: LifecycleSpec{DesiredState: DesiredStateStopped},
	}

	const workers = 32
	start := make(chan struct{})
	ids := make(chan string, workers)
	duplicates := make(chan bool, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			value, duplicate, err := supervisor.Create(spec, "concurrent-create")
			if err != nil {
				errs <- err
				return
			}
			ids <- value.ID
			duplicates <- duplicate
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	close(duplicates)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	firstID := ""
	count := 0
	for id := range ids {
		count++
		if firstID == "" {
			firstID = id
		}
		if id != firstID {
			t.Fatalf("create ids differ: %q and %q", firstID, id)
		}
	}
	if count != workers {
		t.Fatalf("create results = %d, want %d", count, workers)
	}
	nonDuplicates := 0
	for duplicate := range duplicates {
		if !duplicate {
			nonDuplicates++
		}
	}
	if nonDuplicates != 1 {
		t.Fatalf("non-duplicate creates = %d, want 1", nonDuplicates)
	}
	if values := supervisor.List(); len(values) != 1 {
		t.Fatalf("sessions = %d, want 1", len(values))
	}
}

func TestSupervisorPipeInputReplayAndDeduplication(t *testing.T) {
	supervisor := newTestSupervisor(t)
	value, duplicate, err := supervisor.Create(SessionSpec{
		Command: []string{"/bin/sh", "-c", "IFS= read -r line; printf 'out:%s' \"$line\"; printf 'err:%s' \"$line\" >&2"},
		IO:      IOSpec{Mode: IOModePipes},
	}, "create-1")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate {
		t.Fatal("first create was reported as duplicate")
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseRunning)
	value, err = supervisor.Get(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	attemptID := value.Attempt.ID
	input := InputRequest{
		InputID:           "input-1",
		ExpectedAttemptID: attemptID,
		DataBase64:        base64.StdEncoding.EncodeToString([]byte("hello\n")),
		EOF:               true,
	}
	response, err := supervisor.WriteInput(value.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Accepted || response.Duplicate {
		t.Fatalf("input response = %#v", response)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseExited)
	response, err = supervisor.WriteInput(value.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Duplicate {
		t.Fatalf("retried input response = %#v, want duplicate", response)
	}
	page, err := supervisor.Events(value.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr string
	for _, event := range page.Events {
		data, err := base64.StdEncoding.DecodeString(event.DataBase64)
		if err != nil {
			t.Fatal(err)
		}
		switch event.Stream {
		case "stdout":
			stdout += string(data)
		case "stderr":
			stderr += string(data)
		}
	}
	if stdout != "out:hello" || stderr != "err:hello" {
		t.Fatalf("streams = (%q, %q), want (%q, %q)", stdout, stderr, "out:hello", "err:hello")
	}
	createdAgain, duplicate, err := supervisor.Create(value.Spec, "create-1")
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate || createdAgain.ID != value.ID {
		t.Fatalf("idempotent create = (%s, %v), want (%s, true)", createdAgain.ID, duplicate, value.ID)
	}
}

func TestSupervisorRetainsBurstOutputWithoutLoss(t *testing.T) {
	const (
		helperEnv = "SANDBOX0_SESSION_BURST_HELPER"
		chunks    = 640
		chunkSize = 32 * 1024
	)
	if os.Getenv(helperEnv) == "1" {
		for i := range chunks {
			chunk := bytes.Repeat([]byte{byte(i % 251)}, chunkSize)
			if _, err := os.Stdout.Write(chunk); err != nil {
				os.Exit(2)
			}
		}
		os.Exit(0)
	}

	supervisor := newTestSupervisor(t)
	value, _, err := supervisor.Create(SessionSpec{
		Command: []string{os.Args[0], "-test.run=TestSupervisorRetainsBurstOutputWithoutLoss"},
		Env:     map[string]string{helperEnv: "1"},
		IO:      IOSpec{Mode: IOModePipes},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		current, err := supervisor.Get(value.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Phase == PhaseExited {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	current, err := supervisor.Get(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Phase != PhaseExited {
		t.Fatalf("session phase = %s, want exited", current.Phase)
	}

	page, err := supervisor.Events(value.ID, 0, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.New()
	for i := range chunks {
		_, _ = wantHash.Write(bytes.Repeat([]byte{byte(i % 251)}, chunkSize))
	}
	gotHash := sha256.New()
	gotBytes := 0
	for _, event := range page.Events {
		if event.Type != "output" || event.Stream != "stdout" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(event.DataBase64)
		if err != nil {
			t.Fatal(err)
		}
		gotBytes += len(data)
		_, _ = gotHash.Write(data)
	}
	if gotBytes != chunks*chunkSize || !bytes.Equal(gotHash.Sum(nil), wantHash.Sum(nil)) {
		t.Fatalf("retained output = %d bytes hash %x, want %d bytes hash %x", gotBytes, gotHash.Sum(nil), chunks*chunkSize, wantHash.Sum(nil))
	}
}

func TestSupervisorSerializesConcurrentDuplicateInput(t *testing.T) {
	supervisor := newTestSupervisor(t)
	value, _, err := supervisor.Create(SessionSpec{
		Command: []string{"/bin/cat"},
		IO:      IOSpec{Mode: IOModePipes},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseRunning)
	value, err = supervisor.Get(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := InputRequest{
		InputID:           "same-input",
		ExpectedAttemptID: value.Attempt.ID,
		DataBase64:        base64.StdEncoding.EncodeToString([]byte("once\n")),
	}

	start := make(chan struct{})
	responses := make(chan *InputResponse, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := supervisor.WriteInput(value.ID, request)
			if err != nil {
				errs <- err
				return
			}
			responses <- response
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	duplicates := 0
	for response := range responses {
		if response.Duplicate {
			duplicates++
		}
	}
	if duplicates != 1 {
		t.Fatalf("duplicate responses = %d, want 1", duplicates)
	}
	if _, err := supervisor.WriteInput(value.ID, InputRequest{
		InputID:           "eof",
		ExpectedAttemptID: value.Attempt.ID,
		EOF:               true,
	}); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseExited)
	page, err := supervisor.Events(value.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	stdout := ""
	for _, event := range page.Events {
		if event.Stream != "stdout" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(event.DataBase64)
		if err != nil {
			t.Fatal(err)
		}
		stdout += string(data)
	}
	if stdout != "once\n" {
		t.Fatalf("stdout = %q, want one input", stdout)
	}
}

func TestSupervisorRestartPolicyAndAttemptIdentity(t *testing.T) {
	supervisor := newTestSupervisor(t)
	value, _, err := supervisor.Create(SessionSpec{
		Command: []string{"/bin/sh", "-c", "exit 7"},
		Lifecycle: LifecycleSpec{
			DesiredState: DesiredStateRunning,
			Restart: RestartSpec{
				Policy: RestartPolicyOnFailure, MaxRestarts: 2, WindowSeconds: 60,
				InitialBackoffMS: 5, MaxBackoffMS: 10,
			},
			RuntimeRecovery:        RuntimeRecoveryRestart,
			StopGracePeriodSeconds: 1,
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseFailed)
	value, err = supervisor.Get(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if value.Attempt == nil || value.Attempt.Number != 3 || value.RestartCount != 2 {
		t.Fatalf("session = %#v, want three attempts and two restarts", value)
	}
	if _, err := supervisor.WriteInput(value.ID, InputRequest{
		InputID: "stale", ExpectedAttemptID: "att-stale", DataBase64: base64.StdEncoding.EncodeToString([]byte("x")),
	}); !errors.Is(err, ErrSessionNotRunning) && !errors.Is(err, ErrAttemptMismatch) {
		t.Fatalf("stale input error = %v", err)
	}
}

func TestSupervisorRecoversPersistedRunningSessionAsNewAttempt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sessions")
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	// The replacement process starts before manager applies the restored rootfs.
	supervisor, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	record := Session{
		ID: "ses-persisted",
		Spec: normalizeSpec(SessionSpec{
			Command: []string{"/bin/sh", "-c", "printf recovered"},
			Lifecycle: LifecycleSpec{
				DesiredState: DesiredStateRunning, RuntimeRecovery: RuntimeRecoveryRestart,
			},
		}),
		SpecVersion: 1,
		Phase:       PhaseRunning,
		Attempt: &Attempt{
			ID: "att-before", Number: 1, RuntimeGeneration: 4, PID: 123, StartedAt: &now,
		},
		RuntimeGeneration: 4,
		CreatedAt:         now,
		UpdatedAt:         now,
		LastActivityAt:    now,
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
	if err := supervisor.Initialize("sandbox-1", 5, nil); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, record.ID, PhaseExited)
	recovered, err := supervisor.Get(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.RuntimeGeneration != 5 || recovered.Attempt.Number != 2 || recovered.Attempt.ID == "att-before" {
		t.Fatalf("recovered session = %#v", recovered)
	}
	page, err := supervisor.Events(record.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var sawReplacement, sawRecovered bool
	for _, event := range page.Events {
		if event.AttemptID == "att-before" && event.Reason == "runtime_replaced" {
			sawReplacement = true
		}
		if event.Type == "output" {
			data, _ := base64.StdEncoding.DecodeString(event.DataBase64)
			if string(data) == "recovered" {
				sawRecovered = true
			}
		}
	}
	if !sawReplacement || !sawRecovered {
		t.Fatalf("events did not record recovery boundary and output: %#v", page.Events)
	}
}

func TestSupervisorDropsSessionsCopiedFromAnotherSandbox(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sessions")
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Initialize("sandbox-source", 1, nil); err != nil {
		t.Fatal(err)
	}
	created, _, err := source.Create(SessionSpec{
		Command: []string{"/bin/sh", "-c", "exit 0"},
		Lifecycle: LifecycleSpec{
			DesiredState:    DesiredStateStopped,
			RuntimeRecovery: RuntimeRecoveryRestart,
		},
	}, "copied-key")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	copiedStore, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewSupervisor(copiedStore, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, target) })
	if err := target.Initialize("sandbox-target", 1, nil); err != nil {
		t.Fatal(err)
	}
	if got := target.List(); len(got) != 0 {
		t.Fatalf("target sessions = %#v, want copied state cleared", got)
	}
	if _, err := os.Stat(filepath.Join(root, created.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("copied session directory still exists: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(root, sandboxIDFile))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "sandbox-target" {
		t.Fatalf("sandbox identity = %q, want sandbox-target", data)
	}
}

func TestSupervisorRejectsRebindingRunningProcessToAnotherSandbox(t *testing.T) {
	supervisor := newTestSupervisor(t)
	if err := supervisor.Initialize("sandbox-other", 2, nil); err == nil {
		t.Fatal("Initialize() rebound one procd process to another sandbox")
	}
}

func TestSupervisorEmitsSessionExpiryOnce(t *testing.T) {
	supervisor := newTestSupervisor(t)
	value, _, err := supervisor.Create(SessionSpec{
		Command: []string{"/bin/sh", "-c", "while :; do sleep 1; done"},
		Lifecycle: LifecycleSpec{
			DesiredState:       DesiredStateRunning,
			MaxLifetimeSeconds: 1,
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseRunning)
	supervisor.expireSessions(value.CreatedAt.Add(2 * time.Second))
	waitForSessionPhase(t, supervisor, value.ID, PhaseStopped)
	supervisor.expireSessions(value.CreatedAt.Add(3 * time.Second))
	page, err := supervisor.Events(value.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	expiredEvents := 0
	for _, event := range page.Events {
		if event.Type == "session.expired" {
			expiredEvents++
		}
	}
	if expiredEvents != 1 {
		t.Fatalf("session.expired events = %d, want 1", expiredEvents)
	}
}

func TestSupervisorPauseResumeKeepsAttempt(t *testing.T) {
	supervisor := newTestSupervisor(t)
	value, _, err := supervisor.Create(SessionSpec{
		Command:   []string{"/bin/sh", "-c", "while :; do sleep 1; done"},
		Lifecycle: LifecycleSpec{DesiredState: DesiredStateRunning, StopGracePeriodSeconds: 1},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseRunning)
	before, _ := supervisor.Get(value.ID)
	if err := supervisor.PauseAll(); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhasePaused)
	if err := supervisor.ResumeAll(); err != nil {
		t.Fatal(err)
	}
	waitForSessionPhase(t, supervisor, value.ID, PhaseRunning)
	after, _ := supervisor.Get(value.ID)
	if before.Attempt.ID != after.Attempt.ID {
		t.Fatalf("attempt changed across in-runtime pause: %s -> %s", before.Attempt.ID, after.Attempt.ID)
	}
}

func newTestSupervisor(t *testing.T) *Supervisor {
	t.Helper()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Initialize("sandbox-1", 1, map[string]string{"SANDBOX_DEFAULT": "true"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupSupervisor(t, supervisor) })
	return supervisor
}

func cleanupSupervisor(t *testing.T, supervisor *Supervisor) {
	t.Helper()
	for _, value := range supervisor.List() {
		if err := supervisor.Delete(value.ID); err != nil && !errors.Is(err, ErrSessionNotFound) {
			t.Errorf("delete session %s: %v", value.ID, err)
		}
	}
	if err := supervisor.Close(); err != nil {
		t.Errorf("close supervisor: %v", err)
	}
}

func waitForSessionPhase(t *testing.T, supervisor *Supervisor, id string, phase Phase) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		value, err := supervisor.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if value.Phase == phase {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	value, _ := supervisor.Get(id)
	t.Fatalf("session phase = %s, want %s; session=%#v", value.Phase, phase, value)
}
