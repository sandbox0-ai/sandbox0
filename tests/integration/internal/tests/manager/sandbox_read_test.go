package manager

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/sandbox0-ai/infra/manager/pkg/service"
	corev1 "k8s.io/api/core/v1"
)

func TestManagerIntegration_GetSandboxForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-forbidden", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/sbx-forbidden", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}

func TestManagerIntegration_GetSandboxOK(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-ok", "team-1", "user-1", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/sbx-ok", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}
	var sandbox service.Sandbox
	if err := json.Unmarshal(body, &sandbox); err != nil {
		t.Fatalf("failed to decode sandbox: %v", err)
	}
	if sandbox.ID != "sbx-ok" || sandbox.TeamID != "team-1" || sandbox.UserID != "user-1" {
		t.Fatalf("unexpected sandbox payload: %+v", sandbox)
	}
}

func TestManagerIntegration_GetSandboxStatusOK(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-status", "team-1", "user-1", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/sbx-status/status", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}
	if payload["sandbox_id"] != "sbx-status" || payload["status"] != service.SandboxStatusRunning {
		t.Fatalf("unexpected status payload: %v", payload)
	}
}

func TestManagerIntegration_GetSandboxNotFound(t *testing.T) {
	env := newManagerTestEnv(t)

	resp, _ := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/missing", env.token, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected not found, got %d", resp.StatusCode)
	}
}

func TestManagerIntegration_GetSandboxStatusForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-status-forbidden", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/sbx-status-forbidden/status", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}
