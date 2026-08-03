package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

const templateBuildFailureReason = "TemplateImageBuildFailed"

type templateBuildQueue interface {
	ClaimTemplateBuild(ctx context.Context, targetClusterID, workerID string, leaseDuration time.Duration) (*template.TemplateBuild, error)
	RenewTemplateBuildLease(ctx context.Context, buildID, workerID string, leaseDuration time.Duration) error
	MarkTemplateBuildCaptured(ctx context.Context, buildID, workerID, snapshotID string, captureMetadata json.RawMessage, capturedAt time.Time) error
	PublishTemplateBuild(ctx context.Context, buildID, workerID string, spec v1alpha1.SandboxTemplateSpec, outputImage string) error
	FailTemplateBuild(ctx context.Context, buildID, workerID, reason, message string) error
	ReleaseTemplateBuild(ctx context.Context, buildID, workerID string, retryAt time.Time, lastError string) error
	TemplateBuildCancelled(ctx context.Context, buildID string) (bool, error)
	FinishTemplateBuild(ctx context.Context, buildID, workerID string) error
}

var _ templateBuildQueue = (templatestore.TemplateBuildStore)(nil)

type templateBuildCapturer interface {
	EnsureTemplateBuildCapture(ctx context.Context, sandboxID, teamID, snapshotID string, desiredSpec v1alpha1.SandboxTemplateSpec) (*TemplateBuildCaptureMetadata, error)
	DeleteTemplateBuildCapture(ctx context.Context, snapshotID, teamID string) error
}

type templateImagePublisher interface {
	Publish(ctx context.Context, req templateimage.BuildRequest) (*templateimage.Result, error)
}

// TemplateBuildWorkerConfig controls one durable manager-side build worker.
type TemplateBuildWorkerConfig struct {
	ClusterID         string
	WorkerID          string
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	RetryBase         time.Duration
	RetryMax          time.Duration
	MaxAttempts       int
}

// TemplateBuildWorker checkpoints source sandboxes and publishes template
// images for builds targeted at the local cluster.
type TemplateBuildWorker struct {
	queue     templateBuildQueue
	capturer  templateBuildCapturer
	publisher templateImagePublisher
	objects   templateimage.ObjectReader
	config    TemplateBuildWorkerConfig
	logger    *zap.Logger
}

func NewTemplateBuildWorker(
	queue templateBuildQueue,
	capturer templateBuildCapturer,
	publisher templateImagePublisher,
	objects templateimage.ObjectReader,
	config TemplateBuildWorkerConfig,
	logger *zap.Logger,
) (*TemplateBuildWorker, error) {
	if queue == nil || capturer == nil || publisher == nil || objects == nil {
		return nil, fmt.Errorf("template build queue, capturer, publisher, and object reader are required")
	}
	if strings.TrimSpace(config.ClusterID) == "" {
		return nil, fmt.Errorf("template build cluster_id is required")
	}
	if strings.TrimSpace(config.WorkerID) == "" {
		config.WorkerID = "manager-" + uuid.NewString()
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 2 * time.Minute
	}
	if config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.LeaseDuration {
		config.HeartbeatInterval = config.LeaseDuration / 3
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = time.Second
	}
	if config.RetryBase <= 0 {
		config.RetryBase = 5 * time.Second
	}
	if config.RetryMax <= 0 {
		config.RetryMax = 5 * time.Minute
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 8
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TemplateBuildWorker{
		queue:     queue,
		capturer:  capturer,
		publisher: publisher,
		objects:   objects,
		config:    config,
		logger:    logger,
	}, nil
}

// Run claims builds until ctx is cancelled.
func (w *TemplateBuildWorker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		worked, err := w.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("Template image build iteration failed", zap.Error(err))
		}
		delay := w.config.PollInterval
		if worked {
			delay = 0
		}
		timer.Reset(delay)
	}
}

// RunOnce claims and processes at most one build.
func (w *TemplateBuildWorker) RunOnce(ctx context.Context) (bool, error) {
	build, err := w.queue.ClaimTemplateBuild(ctx, w.config.ClusterID, w.config.WorkerID, w.config.LeaseDuration)
	if err != nil {
		return false, err
	}
	if build == nil {
		return false, nil
	}
	return true, w.processClaim(ctx, build)
}

type templateBuildLeaseState struct {
	cancelled atomic.Bool
	mu        sync.Mutex
	err       error
}

func (s *templateBuildLeaseState) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *templateBuildLeaseState) error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (w *TemplateBuildWorker) processClaim(ctx context.Context, build *template.TemplateBuild) error {
	workCtx, cancelWork := context.WithCancel(ctx)
	state := &templateBuildLeaseState{}
	monitorDone := make(chan struct{})
	go w.monitorLease(workCtx, cancelWork, build.BuildID, state, monitorDone)
	defer func() {
		cancelWork()
		<-monitorDone
	}()

	cancelled, err := w.queue.TemplateBuildCancelled(ctx, build.BuildID)
	if err != nil {
		return err
	}
	if cancelled || !build.CancelRequestedAt.IsZero() {
		state.cancelled.Store(true)
		return w.cleanupAndFinish(ctx, build)
	}
	if build.Stage == v1alpha1.TemplateCreationStageReconciling {
		return w.cleanupAndFinish(ctx, build)
	}

	err = w.captureAndPublish(workCtx, build)
	if leaseErr := state.error(); leaseErr != nil {
		return leaseErr
	}
	if state.cancelled.Load() {
		return w.cleanupAndFinish(ctx, build)
	}
	if err == nil {
		return w.cleanupAndFinish(ctx, build)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	message := publicTemplateBuildError(err)
	if errors.Is(err, errTemplateBuildCaptureInvalid) || build.AttemptCount >= w.config.MaxAttempts {
		if failErr := w.queue.FailTemplateBuild(ctx, build.BuildID, w.config.WorkerID, templateBuildFailureReason, message); failErr != nil {
			return errors.Join(err, failErr)
		}
		if cleanupErr := w.cleanupAndFinish(ctx, build); cleanupErr != nil {
			return errors.Join(err, cleanupErr)
		}
		return err
	}
	retryAt := time.Now().UTC().Add(w.retryDelay(build.AttemptCount))
	if releaseErr := w.queue.ReleaseTemplateBuild(ctx, build.BuildID, w.config.WorkerID, retryAt, message); releaseErr != nil {
		return errors.Join(err, releaseErr)
	}
	return err
}

func (w *TemplateBuildWorker) captureAndPublish(ctx context.Context, build *template.TemplateBuild) error {
	capture, err := w.captureMetadata(ctx, build)
	if err != nil {
		return err
	}
	finalSpec := *build.DesiredSpec.DeepCopy()
	result, err := w.publisher.Publish(ctx, templateimage.BuildRequest{
		BuildID:         build.BuildID,
		TeamID:          build.TeamID,
		TemplateID:      build.TemplateID,
		SourceSandboxID: build.SourceSandboxID,
		BaseImageRef:    capture.BaseImageRef,
		BaseImageDigest: capture.BaseImageDigest,
		Platform:        capture.Platform,
		Layers:          capture.Layers,
		CreatedAt:       capture.CapturedAt,
	})
	if err != nil {
		return err
	}
	if result == nil || strings.TrimSpace(result.PullReference) == "" {
		return fmt.Errorf("template image publisher returned no pull reference")
	}
	finalSpec.MainContainer.Image = result.PullReference
	if err := w.queue.PublishTemplateBuild(ctx, build.BuildID, w.config.WorkerID, finalSpec, result.PullReference); err != nil {
		return err
	}
	build.Stage = v1alpha1.TemplateCreationStageReconciling
	build.OutputImage = result.PullReference
	return nil
}

func (w *TemplateBuildWorker) captureMetadata(ctx context.Context, build *template.TemplateBuild) (*TemplateBuildCaptureMetadata, error) {
	if len(build.CaptureMetadata) > 0 {
		var capture TemplateBuildCaptureMetadata
		if err := json.Unmarshal(build.CaptureMetadata, &capture); err != nil {
			return nil, fmt.Errorf("%w: decode template build capture metadata: %v", errTemplateBuildCaptureInvalid, err)
		}
		if err := validateTemplateBuildCapture(build, &capture); err != nil {
			return nil, fmt.Errorf("%w: %v", errTemplateBuildCaptureInvalid, err)
		}
		return &capture, nil
	}
	if build.Stage != v1alpha1.TemplateCreationStageCapturing {
		return nil, fmt.Errorf(
			"%w: build stage %q has no durable capture metadata",
			errTemplateBuildCaptureInvalid,
			build.Stage,
		)
	}
	capture, err := w.capturer.EnsureTemplateBuildCapture(
		ctx,
		build.SourceSandboxID,
		build.TeamID,
		build.SnapshotID,
		build.DesiredSpec,
	)
	if err != nil {
		return nil, err
	}
	capture.Layers, err = templateimage.ResolveLayerDiffIDs(ctx, w.objects, capture.Layers)
	if err != nil {
		return nil, err
	}
	if err := validateTemplateBuildCapture(build, capture); err != nil {
		return nil, fmt.Errorf("%w: %v", errTemplateBuildCaptureInvalid, err)
	}
	metadata, err := json.Marshal(capture)
	if err != nil {
		return nil, fmt.Errorf("encode template build capture metadata: %w", err)
	}
	if err := w.queue.MarkTemplateBuildCaptured(ctx, build.BuildID, w.config.WorkerID, capture.SnapshotID, metadata, capture.CapturedAt); err != nil {
		return nil, err
	}
	build.Stage = v1alpha1.TemplateCreationStagePublishing
	build.SnapshotID = capture.SnapshotID
	build.CaptureMetadata = metadata
	return capture, nil
}

func validateTemplateBuildCapture(build *template.TemplateBuild, capture *TemplateBuildCaptureMetadata) error {
	if capture == nil {
		return fmt.Errorf("template build capture metadata is required")
	}
	if capture.Version != templateBuildCaptureMetadataVersion {
		return fmt.Errorf("unsupported template build capture metadata version %d", capture.Version)
	}
	if strings.TrimSpace(capture.SnapshotID) == "" || capture.SnapshotID != build.SnapshotID {
		return fmt.Errorf("template build capture snapshot does not match build")
	}
	if strings.TrimSpace(capture.HeadLayerID) == "" || len(capture.Layers) == 0 {
		return fmt.Errorf("template build capture has no rootfs layer chain")
	}
	if capture.Platform.OS == "" || capture.Platform.Architecture == "" {
		return fmt.Errorf("template build capture has no source platform")
	}
	if capture.BaseImageRef == "" || capture.BaseImageDigest == "" {
		return fmt.Errorf("template build capture has no base image identity")
	}
	return nil
}

func (w *TemplateBuildWorker) cleanupAndFinish(ctx context.Context, build *template.TemplateBuild) error {
	if snapshotID := strings.TrimSpace(build.SnapshotID); snapshotID != "" {
		if err := w.capturer.DeleteTemplateBuildCapture(ctx, snapshotID, build.TeamID); err != nil {
			return fmt.Errorf("delete template build snapshot: %w", err)
		}
	}
	return w.queue.FinishTemplateBuild(ctx, build.BuildID, w.config.WorkerID)
}

func (w *TemplateBuildWorker) monitorLease(ctx context.Context, cancel context.CancelFunc, buildID string, state *templateBuildLeaseState, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cancelled, err := w.queue.TemplateBuildCancelled(ctx, buildID)
			if err != nil {
				state.setError(err)
				cancel()
				return
			}
			if cancelled {
				state.cancelled.Store(true)
				cancel()
				return
			}
			if err := w.queue.RenewTemplateBuildLease(ctx, buildID, w.config.WorkerID, w.config.LeaseDuration); err != nil {
				state.setError(err)
				cancel()
				return
			}
		}
	}
}

func (w *TemplateBuildWorker) retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	multiplier := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(w.config.RetryBase) * multiplier)
	if delay > w.config.RetryMax || delay < 0 {
		return w.config.RetryMax
	}
	return delay
}

func publicTemplateBuildError(err error) string {
	message := strings.TrimSpace(err.Error())
	const maxLength = 1024
	if len(message) > maxLength {
		message = message[:maxLength]
	}
	return message
}
