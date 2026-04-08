package power

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/internal/ctld/cgroup"
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
