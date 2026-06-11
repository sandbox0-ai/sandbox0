package power

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
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

func (r staticResolver) ResolvePod(_ *http.Request, _, _ string) (Target, error) {
	return r.target, r.err
}

func TestControllerPauseAndResumeAreRemoved(t *testing.T) {
	controller := NewController(staticResolver{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)

	pauseResp, status := controller.Pause(req, "sandbox-1")
	assert.Equal(t, http.StatusNotImplemented, status)
	assert.False(t, pauseResp.Paused)
	assert.Contains(t, pauseResp.Error, "removed")

	resumeResp, status := controller.Resume(req, "sandbox-1")
	assert.Equal(t, http.StatusNotImplemented, status)
	assert.False(t, resumeResp.Resumed)
	assert.Contains(t, resumeResp.Error, "removed")
}

func TestControllerProbeForwardsToProcd(t *testing.T) {
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/sandbox-probes/readiness", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindReadiness, "SandboxProbePassed", "sandbox probe passed", nil))
	}))
	defer procd.Close()
	host, port := splitTestServerHostPort(t, procd.URL)

	controller := NewController(staticResolver{target: Target{SandboxID: "sandbox-1", PodIP: host, ProcdPort: int32(port)}})
	controller.HTTPClient = procd.Client()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/probes/readiness", nil)

	resp, status := controller.Probe(req, "sandbox-1", sandboxprobe.KindReadiness)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, sandboxprobe.KindReadiness, resp.Kind)
	assert.Equal(t, sandboxprobe.StatusPassed, resp.Status)
}

func TestControllerProbeMapsResolverErrors(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/probes/readiness", nil)

	controller := NewController(staticResolver{err: ErrSandboxNotFound})
	resp, status := controller.Probe(req, "sandbox-1", sandboxprobe.KindReadiness)

	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, sandboxprobe.StatusFailed, resp.Status)
	assert.Equal(t, "SandboxResolveFailed", resp.Reason)
}

func TestControllerProbePodForwardsToProcd(t *testing.T) {
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/sandbox-probes/liveness", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sandboxprobe.Passed(sandboxprobe.KindLiveness, "SandboxProbePassed", "sandbox probe passed", nil))
	}))
	defer procd.Close()
	host, port := splitTestServerHostPort(t, procd.URL)

	controller := NewController(staticResolver{target: Target{PodNamespace: "tpl-a", PodName: "pod-a", PodIP: host, ProcdPort: int32(port)}})
	controller.HTTPClient = procd.Client()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pods/tpl-a/pod-a/probes/liveness", nil)

	resp, status := controller.ProbePod(req, "tpl-a", "pod-a", sandboxprobe.KindLiveness)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, sandboxprobe.KindLiveness, resp.Kind)
	assert.Equal(t, sandboxprobe.StatusPassed, resp.Status)
}

func splitTestServerHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	host, portRaw, err := net.SplitHostPort(parsed.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portRaw)
	require.NoError(t, err)
	return host, port
}
