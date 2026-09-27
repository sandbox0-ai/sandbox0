package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/stretchr/testify/require"
)

type resizeServiceTestStore struct {
	*memorySandboxStore
	request    sandboxstore.RequestSandboxResourceResize
	beginCalls int
	resize     *sandboxstore.SandboxResourceResize
}

func (s *resizeServiceTestStore) BeginSandboxResourceResize(_ context.Context, r sandboxstore.RequestSandboxResourceResize) (*sandboxstore.SandboxResourceResize, error) {
	s.request = r
	s.beginCalls++
	return s.resize, nil
}
func (s *resizeServiceTestStore) PrepareSandboxResourceResize(context.Context, string) (*sandboxstore.SandboxResourceResize, error) {
	return s.resize, nil
}
func (s *resizeServiceTestStore) ListPendingSandboxResourceResizes(context.Context, string, int) ([]*sandboxstore.SandboxResourceResize, error) {
	return []*sandboxstore.SandboxResourceResize{s.resize}, nil
}

type resizeServiceTestRuntime struct {
	store           *resizeServiceTestStore
	pauseOperations []string
	resumeCalls     int
	resumeErr       error
	resumeWait      bool
}

func (r *resizeServiceTestRuntime) PauseSandboxForResourceResize(_ context.Context, _ string, operation string) error {
	r.pauseOperations = append(r.pauseOperations, operation)
	r.store.resize.Phase = sandboxstore.SandboxResourceResizeResuming
	return nil
}
func (r *resizeServiceTestRuntime) ResumeSandboxAndWait(ctx context.Context, _ string) (*managerapi.ResumeSandboxResponse, error) {
	r.resumeCalls++
	if r.resumeWait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if r.resumeErr != nil {
		return nil, r.resumeErr
	}
	r.store.resize.Phase = sandboxstore.SandboxResourceResizeApplied
	return &managerapi.ResumeSandboxResponse{SandboxID: "sandbox-a", Resumed: true}, nil
}

func TestSandboxResourceResizeReturnsPendingBeforeGatewayDeadline(t *testing.T) {
	service, store, runtime := newResizeServiceFixture(t)
	store.resize = &sandboxstore.SandboxResourceResize{SandboxID: "sandbox-a", OperationID: "resize-one", WasActive: true, Phase: sandboxstore.SandboxResourceResizePausing}
	runtime.resumeWait = true
	var enqueued string
	service.SetEnqueuer(func(id string) { enqueued = id })
	gatewayCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := service.UpdateResources(gatewayCtx, "sandbox-a", &managerapi.SandboxResourceConfig{Memory: "512Mi"})
	require.ErrorIs(t, err, ErrSandboxRuntimeUpdateUnavailable)
	require.NoError(t, gatewayCtx.Err(), "a pending response must arrive before the gateway abandons the request")
	require.Equal(t, "sandbox-a", enqueued)
	require.Equal(t, sandboxstore.SandboxResourceResizeResuming, store.resize.Phase)
	// The HTTP wait expires without canceling the durable operation. A later
	// reconciliation completes it without pausing the sandbox a second time.
	runtime.resumeWait = false
	require.NoError(t, service.CompleteSandboxResourceResize(context.Background(), "sandbox-a"))
	require.Equal(t, []string{"resize-one"}, runtime.pauseOperations)
	require.Equal(t, sandboxstore.SandboxResourceResizeApplied, store.resize.Phase)
}

func newResizeServiceFixture(t *testing.T) (*SandboxResourceResizeService, *resizeServiceTestStore, *resizeServiceTestRuntime) {
	t.Helper()
	base := newNomadSandboxUpdaterTestStore(time.Now(), sandboxstore.SandboxDesiredStateActive)
	base.records["sandbox-a"].TemplateSpec.MainContainer.Resources = sandboxspec.ResourceQuota{CPU: "500m", Memory: "2Gi"}
	base.records["sandbox-a"].ResourceMillicpu = 500
	base.records["sandbox-a"].ResourceMemoryMiB = 2048
	store := &resizeServiceTestStore{memorySandboxStore: base}
	runtime := &resizeServiceTestRuntime{store: store}
	service, err := NewSandboxResourceResizeService(store, runtime, template.NewResourcePolicy("4Gi", "16Gi"))
	require.NoError(t, err)
	return service, store, runtime
}

func TestSandboxResourceResizeValidatesBeforeAdmissionAndUsesSharedCPUFloor(t *testing.T) {
	for _, memory := range []string{"", "large", "127Mi", "17Gi"} {
		t.Run(memory, func(t *testing.T) {
			service, store, _ := newResizeServiceFixture(t)
			_, err := service.UpdateResources(context.Background(), "sandbox-a", &managerapi.SandboxResourceConfig{Memory: memory})
			require.ErrorIs(t, err, ErrInvalidClaimRequest)
			require.Zero(t, store.beginCalls)
		})
	}
	for _, test := range []struct {
		memory     string
		cpu, bytes int64
	}{{"4096Mi", 1000, 4 << 30}, {"128Mi", 500, 128 << 20}} {
		service, store, _ := newResizeServiceFixture(t)
		_, err := service.UpdateResources(context.Background(), "sandbox-a", &managerapi.SandboxResourceConfig{Memory: test.memory})
		require.NoError(t, err)
		require.Equal(t, test.cpu, store.request.CPUMillicores)
		require.Equal(t, test.bytes, store.request.MemoryBytes)
		require.False(t, store.request.NoChange)
	}
	service, store, runtime := newResizeServiceFixture(t)
	_, err := service.UpdateResources(context.Background(), "sandbox-a", &managerapi.SandboxResourceConfig{Memory: "2048Mi"})
	require.NoError(t, err)
	require.True(t, store.request.NoChange)
	// Identical applied memory never stops or starts compute.
	require.Empty(t, runtime.pauseOperations)
	require.Zero(t, runtime.resumeCalls)
}

func TestSandboxResourceResizeRecoversAfterLostResumeReplyWithoutRepeatingPause(t *testing.T) {
	service, store, runtime := newResizeServiceFixture(t)
	store.resize = &sandboxstore.SandboxResourceResize{SandboxID: "sandbox-a", OperationID: "resize-one", WasActive: true, Phase: sandboxstore.SandboxResourceResizePausing}
	err := service.CompleteSandboxResourceResize(context.Background(), "sandbox-a")
	require.ErrorIs(t, err, sandboxstore.ErrSandboxResourceResizePending)
	require.Equal(t, []string{"resize-one"}, runtime.pauseOperations)
	runtime.resumeErr = errors.New("resume response lost")
	err = service.CompleteSandboxResourceResize(context.Background(), "sandbox-a")
	require.ErrorIs(t, err, sandboxstore.ErrSandboxResourceResizePending)
	// A replacement manager reads durable resuming state and retries the cold
	// resume. It does not start another pause or create a new desired operation.
	recovered, err := NewSandboxResourceResizeService(store, runtime, template.ResourcePolicy{})
	require.NoError(t, err)
	runtime.resumeErr = nil
	require.NoError(t, recovered.CompleteSandboxResourceResize(context.Background(), "sandbox-a"))
	require.Equal(t, []string{"resize-one"}, runtime.pauseOperations)
	require.Equal(t, 2, runtime.resumeCalls)
}

func TestNomadSandboxUpdaterResourcePatchDoesNotPartiallyChangeMetadata(t *testing.T) {
	service, store, _ := newResizeServiceFixture(t)
	updater, err := NewNomadSandboxUpdater(store, time.Minute, time.Now)
	require.NoError(t, err)
	updater.SetResourceResizeService(service)
	ttl := int32(90)
	_, err = updater.UpdateSandbox(context.Background(), "sandbox-a", &SandboxUpdateConfig{Resources: &managerapi.SandboxResourceConfig{Memory: "4Gi"}, TTL: &ttl})
	require.ErrorIs(t, err, ErrInvalidClaimRequest)
	require.Zero(t, store.beginCalls)
	require.Equal(t, int32(60), *store.records["sandbox-a"].Config.TTL)
}
