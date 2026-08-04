package sandboxobservability

import (
	"context"
	"reflect"
	"testing"
	"time"

	shared "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
)

func TestLogWorkerWritesLogEntries(t *testing.T) {
	writer := recordingLogWriter{entries: make(chan []shared.LogEntry, 1)}
	worker, err := NewLogWorker(writer, ingest.Config{BatchSize: 1, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewLogWorker() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("log worker did not stop")
		}
	})

	entry := shared.LogEntry{SandboxID: "sandbox-a", Message: "hello"}
	if !worker.TryEnqueue(entry) {
		t.Fatal("TryEnqueue() returned false")
	}
	select {
	case got := <-writer.entries:
		if !reflect.DeepEqual(got, []shared.LogEntry{entry}) {
			t.Fatalf("inserted entries = %#v, want %#v", got, []shared.LogEntry{entry})
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for log insert")
	}
}

func TestNewLogWorkerRejectsNilWriter(t *testing.T) {
	if _, err := NewLogWorker(nil, ingest.Config{}); err == nil {
		t.Fatal("NewLogWorker() error = nil, want error")
	}
}

type recordingLogWriter struct {
	entries chan []shared.LogEntry
}

func (w recordingLogWriter) InsertLogs(_ context.Context, entries []shared.LogEntry) error {
	w.entries <- append([]shared.LogEntry(nil), entries...)
	return nil
}
