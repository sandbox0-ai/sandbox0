package power

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/internal/ctld/cgroup"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticResolver struct {
	target Target
	err    error
}

func (r staticResolver) Resolve(_ *http.Request, _ string) (Target, error) {
	return r.target, r.err
}

type staticStatsProvider struct {
	usage *ctldapi.SandboxResourceUsage
	err   error
}

func (p staticStatsProvider) SandboxResourceUsage(_ context.Context, _ Target) (*ctldapi.SandboxResourceUsage, error) {
	return p.usage, p.err
}

func TestControllerPauseAndResume(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte("123\n"), 0o644))
	controller := NewController(staticResolver{target: Target{SandboxID: "sandbox-1", CgroupDir: dir}}, &cgroup.FS{SettleTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)

	pauseResp, status := controller.Pause(req, "sandbox-1")
	assert.Equal(t, http.StatusOK, status)
	assert.True(t, pauseResp.Paused)
	assert.Equal(t, int64(123), pauseResp.ResourceUsage.ContainerMemoryWorkingSet)

	resumeResp, status := controller.Resume(req, "sandbox-1")
	assert.Equal(t, http.StatusOK, status)
	assert.True(t, resumeResp.Resumed)

	state, err := os.ReadFile(filepath.Join(dir, "cgroup.freeze"))
	require.NoError(t, err)
	assert.Equal(t, "0", string(state))
}

func TestControllerPausePrefersCRIStatsWhenAvailable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte("123\n"), 0o644))
	controller := NewController(staticResolver{target: Target{SandboxID: "sandbox-1", CgroupDir: dir, PodNamespace: "default", PodName: "sandbox", PodUID: "uid-1"}}, &cgroup.FS{SettleTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond})
	controller.StatsProvider = staticStatsProvider{usage: &ctldapi.SandboxResourceUsage{ContainerMemoryUsage: 456, ContainerMemoryWorkingSet: 400, TotalMemoryRSS: 300, TotalThreadCount: 8}}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)

	resp, status := controller.Pause(req, "sandbox-1")
	assert.Equal(t, http.StatusOK, status)
	assert.True(t, resp.Paused)
	assert.Equal(t, int64(456), resp.ResourceUsage.ContainerMemoryUsage)
	assert.Equal(t, int64(400), resp.ResourceUsage.ContainerMemoryWorkingSet)
	assert.Equal(t, int64(300), resp.ResourceUsage.TotalMemoryRSS)
	assert.Equal(t, 8, resp.ResourceUsage.TotalThreadCount)
	assert.Equal(t, int64(123), resp.ResourceUsage.ContainerMemoryLimit)
}

func TestControllerMapsResolverErrors(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
	t.Run("not implemented", func(t *testing.T) {
		controller := NewController(staticResolver{err: ErrNotImplemented}, nil)
		resp, status := controller.Pause(req, "sandbox-1")
		assert.Equal(t, http.StatusNotImplemented, status)
		assert.False(t, resp.Paused)
	})
	t.Run("not found", func(t *testing.T) {
		controller := NewController(staticResolver{err: ErrSandboxNotFound}, nil)
		resp, status := controller.Pause(req, "sandbox-1")
		assert.Equal(t, http.StatusNotFound, status)
		assert.False(t, resp.Paused)
	})
}
