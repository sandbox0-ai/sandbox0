package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	s0k8s "github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"go.uber.org/zap"
	"k8s.io/client-go/rest"
)

type recordingTemplateReconcilerQuiescer struct {
	called  chan struct{}
	release chan struct{}
}

func (q *recordingTemplateReconcilerQuiescer) Quiesce(context.Context) error {
	close(q.called)
	<-q.release
	return nil
}

func TestServeTemplateReconcilerQuiesceSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	quiescer := &recordingTemplateReconcilerQuiescer{
		called:  make(chan struct{}),
		release: make(chan struct{}),
	}
	tempDir := t.TempDir()
	supportedMarkerPath := filepath.Join(tempDir, "supported")
	quiescedMarkerPath := filepath.Join(tempDir, "quiesced")

	go serveTemplateReconcilerQuiesceSignals(
		ctx,
		signals,
		quiescer,
		supportedMarkerPath,
		quiescedMarkerPath,
		zap.NewNop(),
	)
	waitForTestFile(t, supportedMarkerPath)
	signals <- syscall.SIGUSR1

	select {
	case <-quiescer.called:
	case <-time.After(time.Second):
		t.Fatal("quiesce signal was not handled")
	}
	if _, err := os.Stat(quiescedMarkerPath); !os.IsNotExist(err) {
		t.Fatalf("quiesced marker exists before reconciliation drained: %v", err)
	}
	close(quiescer.release)
	waitForTestFile(t, quiescedMarkerPath)
}

func waitForTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("marker %q was not created", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConfigureK8sClientRateLimiterUsesConfiguredValues(t *testing.T) {
	cfg := &rest.Config{}

	configureK8sClientRateLimiter(cfg, 25, 50)

	if cfg.QPS != 25 {
		t.Fatalf("qps = %v, want 25", cfg.QPS)
	}
	if cfg.Burst != 50 {
		t.Fatalf("burst = %d, want 50", cfg.Burst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}

func TestConfigureK8sClientRateLimiterDefaultsWhenUnset(t *testing.T) {
	cfg := &rest.Config{}

	configureK8sClientRateLimiter(cfg, 0, 0)

	if cfg.QPS != s0k8s.DefaultClientQPS {
		t.Fatalf("qps = %v, want %v", cfg.QPS, s0k8s.DefaultClientQPS)
	}
	if cfg.Burst != s0k8s.DefaultClientBurst {
		t.Fatalf("burst = %d, want %d", cfg.Burst, s0k8s.DefaultClientBurst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}
