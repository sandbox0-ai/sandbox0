package runtimemetrics

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
)

func TestRuntimeSampleWorkerWritesSamples(t *testing.T) {
	writer := recordingRuntimeSampleWriter{samples: make(chan []sandboxobservability.RuntimeSample, 1)}
	worker, err := NewRuntimeSampleWorker(writer, ingest.Config{BatchSize: 1, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewRuntimeSampleWorker() error = %v", err)
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
			t.Error("runtime sample worker did not stop")
		}
	})

	sample := sandboxobservability.RuntimeSample{SandboxID: "sandbox-a", SampleID: "sample-a"}
	if !worker.TryEnqueue(sample) {
		t.Fatal("TryEnqueue() returned false")
	}
	select {
	case got := <-writer.samples:
		if !reflect.DeepEqual(got, []sandboxobservability.RuntimeSample{sample}) {
			t.Fatalf("inserted samples = %#v, want %#v", got, []sandboxobservability.RuntimeSample{sample})
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runtime sample insert")
	}
}

func TestNewRuntimeSampleWorkerRejectsNilWriter(t *testing.T) {
	if _, err := NewRuntimeSampleWorker(nil, ingest.Config{}); err == nil {
		t.Fatal("NewRuntimeSampleWorker() error = nil, want error")
	}
}

type recordingRuntimeSampleWriter struct {
	samples chan []sandboxobservability.RuntimeSample
}

func (w recordingRuntimeSampleWriter) InsertRuntimeSamples(_ context.Context, samples []sandboxobservability.RuntimeSample) error {
	w.samples <- append([]sandboxobservability.RuntimeSample(nil), samples...)
	return nil
}
