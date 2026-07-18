package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestTemplateBuildWorkerReconcilingClaimOnlyCleansUp(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.Stage = v1alpha1.TemplateCreationStageReconciling
	build.OutputImage = "registry.example.com/template@sha256:published"
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{}
	publisher := &fakeTemplateImagePublisher{err: fmt.Errorf("publisher must not be called")}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer, publisher, emptyTemplateObjectReader{})

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !worked {
		t.Fatal("RunOnce() worked = false, want true")
	}
	if publisher.calls != 0 {
		t.Fatalf("publisher calls = %d, want 0", publisher.calls)
	}
	if got := capturer.deleted; len(got) != 1 || got[0] != build.SnapshotID {
		t.Fatalf("deleted snapshots = %#v, want %q", got, build.SnapshotID)
	}
	if !queue.finished {
		t.Fatal("reconciling build queue row was not finished")
	}
}

func TestTemplateBuildWorkerCapturesLegacyDiffIDPublishesAndCleansUp(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	uncompressed := []byte("legacy rootfs layer")
	compressed := templateBuildGzip(t, uncompressed)
	objects := &templateBuildObjectReader{objects: map[string][]byte{"rootfs/layer": compressed}}
	capture := &TemplateBuildCaptureMetadata{
		Version:         templateBuildCaptureMetadataVersion,
		SnapshotID:      build.SnapshotID,
		HeadLayerID:     "layer-1",
		BaseImageRef:    "docker.io/library/busybox:1.36",
		BaseImageDigest: digest.FromString("base-index").String(),
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Layers: []templateimage.Layer{{
			ID:        "layer-1",
			ObjectKey: "rootfs/layer",
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Digest:    digest.FromBytes(compressed).String(),
			Size:      int64(len(compressed)),
		}},
		CapturedAt: time.Unix(100, 0).UTC(),
	}
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{capture: capture}
	publisher := &fakeTemplateImagePublisher{result: &templateimage.Result{
		PullReference:  "registry.internal/t-team/template@sha256:published",
		ManifestDigest: digest.FromString("published"),
	}}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer, publisher, objects)

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !worked {
		t.Fatal("RunOnce() worked = false, want true")
	}
	if queue.capturedMetadata == nil {
		t.Fatal("capture metadata was not persisted")
	}
	var persisted TemplateBuildCaptureMetadata
	if err := json.Unmarshal(queue.capturedMetadata, &persisted); err != nil {
		t.Fatalf("decode persisted capture metadata: %v", err)
	}
	if got, want := persisted.Layers[0].DiffID, digest.FromBytes(uncompressed).String(); got != want {
		t.Fatalf("legacy DiffID = %s, want %s", got, want)
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}
	if got := publisher.request.Platform.Architecture; got != "amd64" {
		t.Fatalf("publisher platform architecture = %q, want amd64", got)
	}
	if got := publisher.request.Layers[0].DiffID; got != digest.FromBytes(uncompressed).String() {
		t.Fatalf("publisher DiffID = %q", got)
	}
	if queue.publishedSpec.MainContainer.Image != publisher.result.PullReference {
		t.Fatalf("published template image = %q", queue.publishedSpec.MainContainer.Image)
	}
	if !queue.finished || len(capturer.deleted) != 1 {
		t.Fatalf("cleanup state: finished=%v deleted=%#v", queue.finished, capturer.deleted)
	}
}

func TestTemplateBuildWorkerCancellationInterruptsPublisherAndCleansUp(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	capture := templateBuildWorkerTestCapture(build)
	build.CaptureMetadata, _ = json.Marshal(capture)
	queue := &fakeTemplateBuildQueue{
		build:             build,
		cancelAfterChecks: 2,
	}
	capturer := &fakeTemplateBuildCapturer{}
	publisher := &fakeTemplateImagePublisher{waitForCancellation: true}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer, publisher, emptyTemplateObjectReader{})
	worker.config.HeartbeatInterval = time.Millisecond
	worker.config.LeaseDuration = 50 * time.Millisecond

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !worked {
		t.Fatal("RunOnce() worked = false, want true")
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}
	if !queue.finished || len(capturer.deleted) != 1 {
		t.Fatalf("cancel cleanup state: finished=%v deleted=%#v", queue.finished, capturer.deleted)
	}
	if queue.released || queue.failed {
		t.Fatalf("cancelled build was released or failed: released=%v failed=%v", queue.released, queue.failed)
	}
}

func TestTemplateBuildWorkerPublishesCapturedBuildClaimedByAnotherCluster(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.TargetClusterID = "source-cluster"
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	capture := templateBuildWorkerTestCapture(build)
	build.CaptureMetadata, _ = json.Marshal(capture)
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{}
	publisher := &fakeTemplateImagePublisher{result: &templateimage.Result{
		PullReference:  "registry.internal/t-team/template@sha256:takeover",
		ManifestDigest: digest.FromString("takeover"),
	}}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer, publisher, emptyTemplateObjectReader{})
	worker.config.ClusterID = "recovery-cluster"

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !worked {
		t.Fatal("RunOnce() worked = false, want true")
	}
	if got := queue.claimClusterID; got != "recovery-cluster" {
		t.Fatalf("claim cluster = %q, want recovery-cluster", got)
	}
	if capturer.ensureCalls != 0 {
		t.Fatalf("capture calls = %d, want 0 for durable captured metadata takeover", capturer.ensureCalls)
	}
	if publisher.calls != 1 || publisher.request.BaseImageDigest != capture.BaseImageDigest {
		t.Fatalf("publisher takeover request = %#v, calls=%d", publisher.request, publisher.calls)
	}
	if !queue.finished || len(capturer.deleted) != 1 || capturer.deleted[0] != build.SnapshotID {
		t.Fatalf("takeover cleanup state: finished=%v deleted=%#v", queue.finished, capturer.deleted)
	}
}

func TestTemplateBuildWorkerInvalidCaptureFailsWithoutRetry(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{
		err: fmt.Errorf("%w: mixed rootfs platform", errTemplateBuildCaptureInvalid),
	}
	publisher := &fakeTemplateImagePublisher{err: fmt.Errorf("publisher must not be called")}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer, publisher, emptyTemplateObjectReader{})

	worked, err := worker.RunOnce(context.Background())
	if !worked || !errors.Is(err, errTemplateBuildCaptureInvalid) {
		t.Fatalf("RunOnce() = worked %v, error %v; want terminal capture error", worked, err)
	}
	if !queue.failed || queue.released {
		t.Fatalf("terminal capture queue state: failed=%v released=%v", queue.failed, queue.released)
	}
	if !queue.finished || len(capturer.deleted) != 1 {
		t.Fatalf("terminal capture cleanup state: finished=%v deleted=%#v", queue.finished, capturer.deleted)
	}
	if publisher.calls != 0 {
		t.Fatalf("publisher calls = %d, want 0", publisher.calls)
	}
}

func TestTemplateBuildWorkerPublishingWithoutMetadataNeverRecapturesSource(t *testing.T) {
	t.Parallel()

	build := templateBuildWorkerTestBuild()
	build.TargetClusterID = "source-cluster"
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	queue := &fakeTemplateBuildQueue{build: build}
	capturer := &fakeTemplateBuildCapturer{
		capture: templateBuildWorkerTestCapture(build),
	}
	publisher := &fakeTemplateImagePublisher{err: fmt.Errorf("publisher must not be called")}
	worker := newTemplateBuildWorkerForTest(t, queue, capturer, publisher, emptyTemplateObjectReader{})
	worker.config.ClusterID = "recovery-cluster"

	worked, err := worker.RunOnce(context.Background())
	if !worked || !errors.Is(err, errTemplateBuildCaptureInvalid) {
		t.Fatalf("RunOnce() = worked %v, error %v; want invalid durable capture", worked, err)
	}
	if capturer.ensureCalls != 0 {
		t.Fatalf("capture calls = %d, want 0 outside source-cluster capturing stage", capturer.ensureCalls)
	}
	if publisher.calls != 0 || !queue.failed || !queue.finished {
		t.Fatalf(
			"missing metadata recovery state: publisher=%d failed=%v finished=%v",
			publisher.calls,
			queue.failed,
			queue.finished,
		)
	}
}

func newTemplateBuildWorkerForTest(
	t *testing.T,
	queue *fakeTemplateBuildQueue,
	capturer *fakeTemplateBuildCapturer,
	publisher *fakeTemplateImagePublisher,
	objects templateimage.ObjectReader,
) *TemplateBuildWorker {
	t.Helper()
	worker, err := NewTemplateBuildWorker(queue, capturer, publisher, objects, TemplateBuildWorkerConfig{
		ClusterID:         "cluster-1",
		WorkerID:          "worker-1",
		PollInterval:      time.Hour,
		LeaseDuration:     time.Hour,
		HeartbeatInterval: 30 * time.Minute,
		MaxAttempts:       3,
	}, nil)
	if err != nil {
		t.Fatalf("NewTemplateBuildWorker() error = %v", err)
	}
	return worker
}

func templateBuildWorkerTestBuild() *template.TemplateBuild {
	return &template.TemplateBuild{
		BuildID:         "build-1",
		TeamID:          "team-1",
		TemplateID:      "template-1",
		SourceSandboxID: "sandbox-1",
		TargetClusterID: "cluster-1",
		DesiredSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "source:tag"},
		},
		Stage:        v1alpha1.TemplateCreationStageCapturing,
		SnapshotID:   "template-build-build-1",
		AttemptCount: 1,
	}
}

func templateBuildWorkerTestCapture(build *template.TemplateBuild) *TemplateBuildCaptureMetadata {
	layer := []byte("rootfs")
	return &TemplateBuildCaptureMetadata{
		Version:         templateBuildCaptureMetadataVersion,
		SnapshotID:      build.SnapshotID,
		HeadLayerID:     "layer-1",
		BaseImageRef:    "busybox:1.36",
		BaseImageDigest: digest.FromString("base").String(),
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Layers: []templateimage.Layer{{
			ID:        "layer-1",
			ObjectKey: "rootfs/layer",
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(layer).String(),
			DiffID:    digest.FromBytes(layer).String(),
			Size:      int64(len(layer)),
		}},
		CapturedAt: time.Unix(100, 0).UTC(),
	}
}

type fakeTemplateBuildQueue struct {
	mu                sync.Mutex
	build             *template.TemplateBuild
	claimed           bool
	cancelAfterChecks int
	cancelChecks      int
	capturedMetadata  json.RawMessage
	publishedSpec     v1alpha1.SandboxTemplateSpec
	outputImage       string
	finished          bool
	released          bool
	failed            bool
	renewed           int
	claimClusterID    string
}

func (q *fakeTemplateBuildQueue) ClaimTemplateBuild(_ context.Context, clusterID, _ string, _ time.Duration) (*template.TemplateBuild, error) {
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

func (q *fakeTemplateBuildQueue) PublishTemplateBuild(_ context.Context, _, _ string, spec v1alpha1.SandboxTemplateSpec, outputImage string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.publishedSpec = spec
	q.outputImage = outputImage
	return nil
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

type fakeTemplateImagePublisher struct {
	result              *templateimage.Result
	err                 error
	waitForCancellation bool
	request             templateimage.BuildRequest
	calls               int
}

func (p *fakeTemplateImagePublisher) Publish(ctx context.Context, req templateimage.BuildRequest) (*templateimage.Result, error) {
	p.calls++
	p.request = req
	if p.waitForCancellation {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return p.result, p.err
}

type emptyTemplateObjectReader struct{}

func (emptyTemplateObjectReader) Get(string, int64, int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unexpected object read")
}

type templateBuildObjectReader struct {
	objects map[string][]byte
}

func (r *templateBuildObjectReader) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	payload, ok := r.objects[key]
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	end := int64(len(payload))
	if limit >= 0 && offset+limit < end {
		end = offset + limit
	}
	return io.NopCloser(bytes.NewReader(payload[offset:end])), nil
}

func templateBuildGzip(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return buffer.Bytes()
}
