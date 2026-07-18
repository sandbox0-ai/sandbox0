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

type creationTemplateStore struct {
	templates  []*template.Template
	readyCalls int
}

func (s *creationTemplateStore) ListTemplates(context.Context) ([]*template.Template, error) {
	return s.templates, nil
}

func (s *creationTemplateStore) MarkTemplateCreationReady(context.Context, string, string, string, string, time.Time) (bool, error) {
	s.readyCalls++
	return true, nil
}

type recordingTemplateApplier struct {
	existing *v1alpha1.SandboxTemplate
	creates  int
	updates  int
}

func (a *recordingTemplateApplier) ListTemplates(context.Context) ([]*v1alpha1.SandboxTemplate, error) {
	return nil, nil
}

func (a *recordingTemplateApplier) GetTemplate(context.Context, string) (*v1alpha1.SandboxTemplate, error) {
	return a.existing, nil
}

func (a *recordingTemplateApplier) CreateTemplate(_ context.Context, tpl *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	a.creates++
	return tpl, nil
}

func (a *recordingTemplateApplier) UpdateTemplate(_ context.Context, tpl *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	a.updates++
	return tpl, nil
}

func (*recordingTemplateApplier) DeleteTemplate(context.Context, string) error { return nil }

func TestSingleClusterReconcilerDefersCapturingTemplate(t *testing.T) {
	store := &creationTemplateStore{templates: []*template.Template{{
		TemplateID: "derived",
		Scope:      "team",
		TeamID:     "team-1",
		Status: &v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State: v1alpha1.TemplateCreationStateCreating,
				Stage: v1alpha1.TemplateCreationStageCapturing,
			},
		},
	}}}
	applier := &recordingTemplateApplier{}
	reconciler := NewSingleClusterReconciler(store, applier, "cluster-a", time.Minute, nil, zap.NewNop())

	reconciler.reconcile(context.Background())

	if applier.creates != 0 || applier.updates != 0 {
		t.Fatalf("applier calls = create:%d update:%d, want none", applier.creates, applier.updates)
	}
	if store.readyCalls != 0 {
		t.Fatalf("ready calls = %d, want 0", store.readyCalls)
	}
}

func TestSingleClusterReconcilerFinalizesVisibleReconcilingTemplate(t *testing.T) {
	tpl := &template.Template{
		TemplateID:      "derived",
		Scope:           "team",
		TeamID:          "team-1",
		CreationBuildID: "build-1",
		Status: &v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State: v1alpha1.TemplateCreationStateCreating,
				Stage: v1alpha1.TemplateCreationStageReconciling,
			},
		},
	}
	store := &creationTemplateStore{templates: []*template.Template{tpl}}
	applier := &recordingTemplateApplier{existing: &v1alpha1.SandboxTemplate{}}
	reconciler := NewSingleClusterReconciler(store, applier, "cluster-a", time.Minute, nil, zap.NewNop())

	reconciler.reconcile(context.Background())

	if applier.updates != 1 {
		t.Fatalf("update calls = %d, want 1", applier.updates)
	}
	if store.readyCalls != 1 {
		t.Fatalf("ready calls = %d, want 1", store.readyCalls)
	}
}

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
