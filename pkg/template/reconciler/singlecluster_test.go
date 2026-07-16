package reconciler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

type countingTemplateStore struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func (s *countingTemplateStore) ListTemplates(ctx context.Context) ([]*template.Template, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()

	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, nil
}

func (s *countingTemplateStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type emptyTemplateApplier struct{}

func (emptyTemplateApplier) ListTemplates(context.Context) ([]*v1alpha1.SandboxTemplate, error) {
	return nil, nil
}

func (emptyTemplateApplier) GetTemplate(context.Context, string) (*v1alpha1.SandboxTemplate, error) {
	return nil, nil
}

func (emptyTemplateApplier) CreateTemplate(context.Context, *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	return nil, nil
}

func (emptyTemplateApplier) UpdateTemplate(context.Context, *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	return nil, nil
}

func (emptyTemplateApplier) DeleteTemplate(context.Context, string) error { return nil }

func TestSingleClusterReconcilerQuiesceBlocksNewWork(t *testing.T) {
	store := &countingTemplateStore{}
	reconciler := NewSingleClusterReconciler(
		store,
		emptyTemplateApplier{},
		"cluster-a",
		time.Minute,
		nil,
		zap.NewNop(),
	)

	reconciler.runReconcile(context.Background())
	if err := reconciler.Quiesce(context.Background()); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	reconciler.runReconcile(context.Background())

	if got := store.callCount(); got != 1 {
		t.Fatalf("ListTemplates calls = %d, want 1", got)
	}
}

func TestSingleClusterReconcilerQuiesceWaitsForInFlightWork(t *testing.T) {
	store := &countingTemplateStore{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	reconciler := NewSingleClusterReconciler(
		store,
		emptyTemplateApplier{},
		"cluster-a",
		time.Minute,
		nil,
		zap.NewNop(),
	)

	go reconciler.runReconcile(context.Background())
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not start")
	}

	quiesced := make(chan error, 1)
	go func() {
		quiesced <- reconciler.Quiesce(context.Background())
	}()

	select {
	case err := <-quiesced:
		t.Fatalf("Quiesce returned before in-flight work completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(store.release)
	select {
	case err := <-quiesced:
		if err != nil {
			t.Fatalf("Quiesce: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Quiesce did not return after in-flight work completed")
	}
}
