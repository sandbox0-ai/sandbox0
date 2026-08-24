package templatebuild

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestTemplateBuildWorkerRetainsPublishedSnapshot(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	capture := templateBuildWorkerTestCapture(build)
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{capture: capture}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)

	worked, err := worker.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatalf("RunOnce() = worked %v, error %v", worked, err)
	}
	if queue.rootFSSource == nil || queue.rootFSSource.SnapshotID != build.SnapshotID ||
		queue.rootFSSource.GenerationID != capture.HeadGenerationID {
		t.Fatalf("published RootFS source = %#v", queue.rootFSSource)
	}
	if queue.finished || queue.released || queue.failed || len(capturer.deleted) != 0 {
		t.Fatalf("retained capture was cleaned: finished=%v released=%v failed=%v deleted=%#v",
			queue.finished, queue.released, queue.failed, capturer.deleted)
	}
	if queue.capturedMetadata == nil {
		t.Fatal("capture metadata was not persisted before publication")
	}
	var persisted CaptureMetadata
	if err := json.Unmarshal(queue.capturedMetadata, &persisted); err != nil {
		t.Fatalf("decode persisted capture metadata: %v", err)
	}
	if persisted.Version != CaptureMetadataVersion || persisted.HeadGenerationID != capture.HeadGenerationID {
		t.Fatalf("persisted capture = %#v", persisted)
	}
}

func TestTemplateBuildWorkerPublishesDurableCaptureFromAnotherCluster(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.TargetClusterID = "source-cluster"
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	capture := templateBuildWorkerTestCapture(build)
	build.CaptureMetadata, _ = json.Marshal(capture)
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)
	worker.config.ClusterID = "recovery-cluster"

	worked, err := worker.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatalf("RunOnce() = worked %v, error %v", worked, err)
	}
	if queue.claimClusterID != "recovery-cluster" {
		t.Fatalf("claim cluster = %q, want recovery-cluster", queue.claimClusterID)
	}
	if capturer.ensureCalls != 0 {
		t.Fatalf("capture calls = %d, want 0", capturer.ensureCalls)
	}
	if queue.rootFSSource == nil || queue.rootFSSource.GenerationID != capture.HeadGenerationID {
		t.Fatalf("published RootFS source = %#v", queue.rootFSSource)
	}
}

func TestTemplateBuildWorkerCancellationInterruptsPublicationAndCleansUp(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	build.CaptureMetadata, _ = json.Marshal(templateBuildWorkerTestCapture(build))
	queue := &fakeTemplateBuildQueue{build: build, cancelAfterChecks: 2, waitForPublishCancellation: true}
	capturer := &fakeTemplateBuildCapturer{}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)
	worker.config.HeartbeatInterval = time.Millisecond
	worker.config.LeaseDuration = 50 * time.Millisecond

	worked, err := worker.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatalf("RunOnce() = worked %v, error %v", worked, err)
	}
	if !queue.finished || len(capturer.deleted) != 1 || capturer.deleted[0] != build.SnapshotID {
		t.Fatalf("cancel cleanup state: finished=%v deleted=%#v", queue.finished, capturer.deleted)
	}
	if queue.released || queue.failed {
		t.Fatalf("cancelled build was released or failed: released=%v failed=%v", queue.released, queue.failed)
	}
}

func TestTemplateBuildWorkerInvalidCaptureFailsWithoutRetry(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{err: fmt.Errorf("%w: mixed rootfs platform", ErrCaptureInvalid)}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)

	worked, err := worker.RunOnce(context.Background())
	if !worked || !errors.Is(err, ErrCaptureInvalid) {
		t.Fatalf("RunOnce() = worked %v, error %v; want terminal capture error", worked, err)
	}
	if !queue.failed || queue.released || !queue.finished || len(capturer.deleted) != 1 {
		t.Fatalf("terminal capture state: failed=%v released=%v finished=%v deleted=%#v",
			queue.failed, queue.released, queue.finished, capturer.deleted)
	}
}

func TestTemplateBuildWorkerRejectsLegacyCaptureVersion(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	capture := templateBuildWorkerTestCapture(build)
	capture.Version = 1
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{capture: capture}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)

	worked, err := worker.RunOnce(context.Background())
	if !worked || !errors.Is(err, ErrCaptureInvalid) {
		t.Fatalf("RunOnce() = worked %v, error %v; want invalid version", worked, err)
	}
	if !queue.failed || !queue.finished || queue.rootFSSource != nil {
		t.Fatalf("legacy capture state: failed=%v finished=%v source=%#v",
			queue.failed, queue.finished, queue.rootFSSource)
	}
}

func TestTemplateBuildWorkerPublishingWithoutMetadataNeverRecapturesSource(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.TargetClusterID = "source-cluster"
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{capture: templateBuildWorkerTestCapture(build)}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)
	worker.config.ClusterID = "recovery-cluster"

	worked, err := worker.RunOnce(context.Background())
	if !worked || !errors.Is(err, ErrCaptureInvalid) {
		t.Fatalf("RunOnce() = worked %v, error %v; want invalid durable capture", worked, err)
	}
	if capturer.ensureCalls != 0 || !queue.failed || !queue.finished {
		t.Fatalf("missing metadata state: captures=%d failed=%v finished=%v",
			capturer.ensureCalls, queue.failed, queue.finished)
	}
}

func TestTemplateBuildWorkerNeverCleansUncertainPublication(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	queue := &fakeTemplateBuildQueue{
		build: build, rootFSPublishErr: fmt.Errorf("%w: commit response lost", template.ErrTemplateRootFSPublicationUncertain),
	}
	capturer := &fakeTemplateBuildCapturer{capture: templateBuildWorkerTestCapture(build)}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)

	worked, err := worker.RunOnce(context.Background())
	if !worked || !errors.Is(err, template.ErrTemplateRootFSPublicationUncertain) {
		t.Fatalf("RunOnce() = worked %v, error %v; want uncertain publish error", worked, err)
	}
	if queue.finished || queue.released || queue.failed || len(capturer.deleted) != 0 {
		t.Fatalf("uncertain publish was destructively handled: finished=%v released=%v failed=%v deleted=%#v",
			queue.finished, queue.released, queue.failed, capturer.deleted)
	}
}

func TestTemplateBuildWorkerReleasesDefinitePublicationFailure(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	queue := &fakeTemplateBuildQueue{build: build, rootFSPublishErr: errors.New("publication rejected before commit")}
	capturer := &fakeTemplateBuildCapturer{capture: templateBuildWorkerTestCapture(build)}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer)

	worked, err := worker.RunOnce(context.Background())
	if !worked || err == nil {
		t.Fatalf("RunOnce() = worked %v, error %v; want publication error", worked, err)
	}
	if !queue.released || queue.finished || queue.failed || len(capturer.deleted) != 0 {
		t.Fatalf("definite publication failure state: released=%v finished=%v failed=%v deleted=%#v",
			queue.released, queue.finished, queue.failed, capturer.deleted)
	}
}

func newTemplateBuildWorkerForTest(
	t *testing.T,
	queue *fakeTemplateBuildQueue,
	capturer *fakeTemplateBuildCapturer,
) *TemplateBuildWorker {
	t.Helper()
	worker, err := NewTemplateBuildWorker(queue, capturer, TemplateBuildWorkerConfig{
		ClusterID: "cluster-1", WorkerID: "worker-1", PollInterval: time.Hour,
		LeaseDuration: time.Hour, HeartbeatInterval: 30 * time.Minute, MaxAttempts: 3,
	}, nil)
	if err != nil {
		t.Fatalf("NewTemplateBuildWorker() error = %v", err)
	}
	return worker
}

func templateBuildWorkerTestBuild() *template.TemplateBuild {
	return &template.TemplateBuild{
		BuildID: "build-1", TeamID: "team-1", TemplateID: "template-1",
		SourceSandboxID: "sandbox-1", TargetClusterID: "cluster-1",
		DesiredSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "source:tag"},
		},
		Stage: v1alpha1.TemplateCreationStageCapturing, SnapshotID: "template-build-build-1", AttemptCount: 1,
	}
}

func templateBuildWorkerTestCapture(build *template.TemplateBuild) *TemplateBuildCaptureMetadata {
	return &TemplateBuildCaptureMetadata{
		Version: CaptureMetadataVersion, SnapshotID: build.SnapshotID,
		StorageFormat:    template.RootFSTemplateStorageFormatBlockCOWV1,
		HeadGenerationID: "generation-1", SourceOCIDigest: digest.FromString("source").String(),
		BaseArtifactDigest: digest.FromString("artifact").String(), FormatGeneration: 1,
		Platform:   ocispec.Platform{OS: "linux", Architecture: "amd64"},
		CapturedAt: time.Unix(100, 0).UTC(),
	}
}

type fakeTemplateBuildQueue struct {
	mu                         sync.Mutex
	build                      *template.TemplateBuild
	claimed                    bool
	cancelAfterChecks          int
	cancelChecks               int
	capturedMetadata           json.RawMessage
	finished                   bool
	released                   bool
	failed                     bool
	renewed                    int
	claimClusterID             string
	rootFSSource               *template.RootFSTemplateSource
	rootFSPublishErr           error
	waitForPublishCancellation bool
}

func (q *fakeTemplateBuildQueue) ClaimTemplateBuild(
	_ context.Context,
	clusterID, _ string,
	_ time.Duration,
) (*template.TemplateBuild, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.claimed {
		return nil, nil
	}
	q.claimed = true
	q.claimClusterID = clusterID
	copy := *q.build
	copy.CaptureMetadata = append([]byte(nil), q.build.CaptureMetadata...)
	return &copy, nil
}

func (q *fakeTemplateBuildQueue) RenewTemplateBuildLease(context.Context, string, string, time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.renewed++
	return nil
}

func (q *fakeTemplateBuildQueue) MarkTemplateBuildCaptured(_ context.Context, _, _, _ string, metadata json.RawMessage, _ time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.capturedMetadata = append([]byte(nil), metadata...)
	return nil
}

func (q *fakeTemplateBuildQueue) PublishRootFSTemplateBuild(
	ctx context.Context,
	_, _ string,
	source template.RootFSTemplateSource,
	_ time.Time,
) error {
	if q.waitForPublishCancellation {
		<-ctx.Done()
		return ctx.Err()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	copy := source
	q.rootFSSource = &copy
	return q.rootFSPublishErr
}

func (q *fakeTemplateBuildQueue) FailTemplateBuild(context.Context, string, string, string, string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failed = true
	return nil
}

func (q *fakeTemplateBuildQueue) ReleaseTemplateBuild(context.Context, string, string, time.Time, string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.released = true
	return nil
}

func (q *fakeTemplateBuildQueue) TemplateBuildCancelled(context.Context, string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cancelChecks++
	return q.cancelAfterChecks > 0 && q.cancelChecks >= q.cancelAfterChecks, nil
}

func (q *fakeTemplateBuildQueue) FinishTemplateBuild(context.Context, string, string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.finished = true
	return nil
}

type fakeTemplateBuildCapturer struct {
	capture     *TemplateBuildCaptureMetadata
	err         error
	deleted     []string
	ensureCalls int
}

func (c *fakeTemplateBuildCapturer) EnsureTemplateBuildCapture(context.Context, string, string, string, v1alpha1.SandboxTemplateSpec) (*TemplateBuildCaptureMetadata, error) {
	c.ensureCalls++
	return c.capture, c.err
}

func (c *fakeTemplateBuildCapturer) DeleteTemplateBuildCapture(_ context.Context, snapshotID, _ string) error {
	c.deleted = append(c.deleted, snapshotID)
	return nil
}
