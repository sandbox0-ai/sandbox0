package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func testConfig() loadConfig {
	return loadConfig{ID: "resident-test-1", Memory: 4 * pageBytes, Workers: 2, Lifetime: 5 * time.Second}
}

func TestConfigBounds(t *testing.T) {
	tests := []struct {
		name string
		edit func(*loadConfig)
	}{
		{"unknown-control", func(c *loadConfig) { c.Control = "pty" }},
		{"missing-owner", func(c *loadConfig) { c.ID = "" }},
		{"uppercase-owner", func(c *loadConfig) { c.ID = "OTHER" }},
		{"path-owner", func(c *loadConfig) { c.ID = "../other" }},
		{"oversize-owner", func(c *loadConfig) { c.ID = strings.Repeat("a", 65) }},
		{"missing-memory", func(c *loadConfig) { c.Memory = 0 }},
		{"negative-memory", func(c *loadConfig) { c.Memory = -pageBytes }},
		{"unaligned-memory", func(c *loadConfig) { c.Memory++ }},
		{"oversize-memory", func(c *loadConfig) { c.Memory = maximumMemory + pageBytes }},
		{"missing-workers", func(c *loadConfig) { c.Workers = 0 }},
		{"negative-workers", func(c *loadConfig) { c.Workers = -1 }},
		{"oversize-workers", func(c *loadConfig) { c.Workers = maximumWorkers + 1 }},
		{"short-lifetime", func(c *loadConfig) { c.Lifetime = time.Second - 1 }},
		{"oversize-lifetime", func(c *loadConfig) { c.Lifetime = maximumLifetime + 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.edit(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	for _, lifetime := range []time.Duration{time.Second, maximumLifetime} {
		cfg := testConfig()
		cfg.Lifetime = lifetime
		if err := cfg.validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoadTouchesEveryPageAndJoinsWorkers(t *testing.T) {
	cfg := testConfig()
	load, err := startLoad(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(load.stop)
	for offset := 0; offset < len(load.memory); offset += pageBytes {
		if got := binary.LittleEndian.Uint64(load.memory[offset:]); got != uint64(offset/pageBytes)+1 {
			t.Fatalf("page at %d was not touched: %d", offset, got)
		}
	}
	for i := range load.counters {
		if load.counters[i].Load() == 0 {
			t.Fatalf("worker %d was acknowledged before doing work", i)
		}
	}
	load.stop()
	if load.memory != nil {
		t.Fatal("memory retained after all workers stopped")
	}
	counts := make([]uint64, len(load.counters))
	for i := range load.counters {
		counts[i] = load.counters[i].Load()
	}
	// A second stop must be safe and cannot allow any worker to resume.
	load.stop()
	for i := range load.counters {
		if load.counters[i].Load() != counts[i] {
			t.Fatalf("worker %d continued after stop", i)
		}
	}
}

func TestLoadRejectsCanceledParentBeforeAllocation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	load, err := startLoad(ctx, testConfig())
	if load != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("got load=%v error=%v", load, err)
	}
}

type eventSink struct {
	events chan event
}

func (s *eventSink) Write(p []byte) (int, error) {
	var value event
	if err := json.Unmarshal(p, &value); err != nil {
		return 0, err
	}
	s.events <- value
	return len(p), nil
}

type session struct {
	cfg    loadConfig
	input  *io.PipeWriter
	events <-chan event
	done   <-chan error
	cancel context.CancelFunc
}

func newSession(t *testing.T, cfg loadConfig, heartbeat time.Duration) *session {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	sink := &eventSink{events: make(chan event, 256)}
	done := make(chan error, 1)
	go func() {
		done <- runLoad(ctx, cfg, reader, sink, heartbeat)
	}()
	s := &session{cfg: cfg, input: writer, events: sink.events, done: done, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		_ = writer.Close()
	})
	s.next(t, "idle")
	return s
}

func (s *session) send(t *testing.T, command string) {
	t.Helper()
	if err := json.NewEncoder(s.input).Encode(control{ID: s.cfg.ID, Command: command}); err != nil {
		t.Fatal(err)
	}
}

func (s *session) next(t *testing.T, state string) event {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case value := <-s.events:
			if value.State == state {
				if value.ID != s.cfg.ID || value.Version != 1 || value.PID <= 0 || value.Sequence == 0 || value.ElapsedNS < 0 || value.LifetimeNS != int64(s.cfg.Lifetime) {
					t.Fatalf("invalid identity or clock: %+v", value)
				}
				return value
			}
		case <-deadline.C:
			t.Fatalf("missing state %s", state)
		}
	}
}

func awaitResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("loader failed to exit within test bound")
		return nil
	}
}

func TestLoadReleaseStopProtocol(t *testing.T) {
	s := newSession(t, testConfig(), 5*time.Millisecond)
	s.send(t, "load")
	loaded := s.next(t, "loaded")
	if loaded.AllocatedBytes != s.cfg.Memory || loaded.Workers != s.cfg.Workers || len(loaded.Iterations) != s.cfg.Workers {
		t.Fatalf("invalid loaded allocation: %+v", loaded)
	}
	for _, count := range loaded.Iterations {
		if count == 0 {
			t.Fatal("worker did not start before loaded event")
		}
	}
	heartbeat := s.next(t, "loaded")
	if heartbeat.Sequence <= loaded.Sequence || heartbeat.ElapsedNS < loaded.ElapsedNS {
		t.Fatal("telemetry clock or sequence regressed")
	}
	for i, count := range heartbeat.Iterations {
		if count < loaded.Iterations[i] {
			t.Fatal("work count regressed")
		}
	}
	s.send(t, "release")
	released := s.next(t, "released")
	if released.Sequence <= heartbeat.Sequence || released.AllocatedBytes != 0 || released.Workers != 0 || len(released.Iterations) != 0 {
		t.Fatalf("release retained guest work: %+v", released)
	}
	s.send(t, "stop")
	stopped := s.next(t, "stopped")
	if stopped.Sequence <= released.Sequence || stopped.AllocatedBytes != 0 || stopped.Workers != 0 {
		t.Fatalf("invalid stopped event: %+v", stopped)
	}
	if err := awaitResult(t, s.done); err != nil {
		t.Fatal(err)
	}
}

func TestLoadedInputFailuresTerminate(t *testing.T) {
	for name, line := range map[string]string{
		"owner":     `{"id":"wrong","command":"stop"}`,
		"unknown":   `{"id":"resident-test-1","command":"wait"}`,
		"duplicate": `{"id":"resident-test-1","command":"load"}`,
		"field":     `{"id":"resident-test-1","command":"stop","secret":"not-echoed"}`,
		"malformed": `not-echoed`,
		"trailing":  `{"id":"resident-test-1","command":"stop"} {}`,
		"oversize":  strings.Repeat("x", 8192),
		"null":      `null`,
	} {
		t.Run(name, func(t *testing.T) {
			s := newSession(t, testConfig(), time.Second)
			s.send(t, "load")
			s.next(t, "loaded")
			// A bounded scanner can close its input before consuming an
			// oversized write. Both paths must terminate without echoing input.
			_, _ = io.WriteString(s.input, line+"\n")
			err := awaitResult(t, s.done)
			if err == nil || strings.Contains(err.Error(), "not-echoed") {
				t.Fatalf("invalid input result: %v", err)
			}
		})
	}
}

func TestInvalidStateTransitionsTerminate(t *testing.T) {
	for _, commands := range [][]string{{"release"}, {"load", "release", "load"}, {"load", "release", "release"}} {
		t.Run(strings.Join(commands, "-"), func(t *testing.T) {
			s := newSession(t, testConfig(), time.Second)
			for i, command := range commands {
				s.send(t, command)
				if i < len(commands)-1 {
					state := "loaded"
					if command == "release" {
						state = "released"
					}
					s.next(t, state)
				}
			}
			if err := awaitResult(t, s.done); err == nil {
				t.Fatal("invalid state transition succeeded")
			}
		})
	}
}

func TestEOFStopsActiveLoad(t *testing.T) {
	s := newSession(t, testConfig(), time.Second)
	s.send(t, "load")
	s.next(t, "loaded")
	if err := s.input.Close(); err != nil {
		t.Fatal(err)
	}
	stopped := s.next(t, "stopped")
	if stopped.AllocatedBytes != 0 || stopped.Workers != 0 {
		t.Fatal("EOF retained work")
	}
	if err := awaitResult(t, s.done); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationStopsLoadAndClosesInput(t *testing.T) {
	s := newSession(t, testConfig(), time.Second)
	s.send(t, "load")
	s.next(t, "loaded")
	s.cancel()
	if err := awaitResult(t, s.done); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := io.WriteString(s.input, "{}\n"); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("input not closed: %v", err)
	}
}

func TestLifetimeIncludesIdleAndIsNotRefreshed(t *testing.T) {
	cfg := testConfig()
	cfg.Lifetime = time.Second
	started := time.Now()
	s := newSession(t, cfg, 10*time.Millisecond)
	// Wait for the program's own clock, not a separate unbounded sleep.
	for value := s.next(t, "idle"); value.ElapsedNS < int64(300*time.Millisecond); value = s.next(t, "idle") {
	}
	s.send(t, "load")
	s.next(t, "loaded")
	if err := awaitResult(t, s.done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected expiry, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 1250*time.Millisecond {
		t.Fatalf("load reset lifetime or expiry was delayed: %s", elapsed)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("telemetry failed") }

func TestOutputErrorTerminates(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	if err := runLoad(context.Background(), testConfig(), reader, failingWriter{}, time.Second); err == nil || err.Error() != "telemetry failed" {
		t.Fatalf("unexpected output failure: %v", err)
	}
}

type blockingWriter struct {
	entered    chan struct{}
	release    chan struct{}
	exited     chan struct{}
	once       sync.Once
	loadedOnly bool
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	if w.loadedOnly {
		var value event
		if err := json.Unmarshal(p, &value); err != nil {
			return 0, err
		}
		if value.State != "loaded" {
			return len(p), nil
		}
	}
	w.once.Do(func() { close(w.entered) })
	<-w.release
	close(w.exited)
	return len(p), nil
}

func TestBlockedOutputDoesNotPreventExpiry(t *testing.T) {
	cfg := testConfig()
	cfg.Lifetime = time.Second
	reader, writer := io.Pipe()
	defer writer.Close()
	output := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	defer close(output.release)
	done := make(chan error, 1)
	go func() { done <- runLoad(context.Background(), cfg, reader, output, time.Second) }()
	select {
	case <-output.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("output not attempted")
	}
	if err := awaitResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked output prevented expiry: %v", err)
	}
	if _, err := io.WriteString(writer, "{}\n"); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("blocked output retained input: %v", err)
	}
}

func TestBlockedLoadedAcknowledgementDoesNotPreventCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	output := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{}), loadedOnly: true}
	defer close(output.release)
	done := make(chan error, 1)
	go func() { done <- runLoad(ctx, testConfig(), reader, output, time.Second) }()
	if err := json.NewEncoder(writer).Encode(control{ID: testConfig().ID, Command: "load"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-output.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no loaded acknowledgement attempted")
	}
	cancel()
	if err := awaitResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("loaded output prevented cancellation: %v", err)
	}
}

func TestProgramRequiresExplicitResources(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--id", "test", "--memory-bytes", "4096"},
		{"--id", "test", "--workers", "1"},
		{"--memory-bytes", "4096", "--workers", "1"},
		{"--id", "test", "--memory-bytes", "4096", "--workers", "1", "--lifetime", "51s"},
		{"--id", "test", "--memory-bytes", "4096", "--workers", "1", "extra"},
		{"--id", "test", "--memory-bytes", "4096", "--workers", "1", "--control", "unknown"},
	} {
		var output, stderr bytes.Buffer
		if got := program(context.Background(), args, io.NopCloser(strings.NewReader("")), &output, &stderr); got == 0 || output.Len() != 0 {
			t.Fatalf("unsafe CLI config accepted: %v, exit %d", args, got)
		}
	}
}

func TestProgramCleanStop(t *testing.T) {
	var output, stderr bytes.Buffer
	args := []string{"--id", "test", "--memory-bytes", "4096", "--workers", "1"}
	input := io.NopCloser(strings.NewReader("{\"id\":\"test\",\"command\":\"stop\"}\n"))
	if got := program(context.Background(), args, input, &output, &stderr); got != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%s", got, &stderr)
	}
	decoder := json.NewDecoder(&output)
	for _, state := range []string{"idle", "stopped"} {
		var value event
		if err := decoder.Decode(&value); err != nil || value.State != state {
			t.Fatalf("state=%s value=%+v err=%v", state, value, err)
		}
	}
	if err := decoder.Decode(new(event)); err != io.EOF {
		t.Fatalf("unexpected trailing output: %v", err)
	}
}
