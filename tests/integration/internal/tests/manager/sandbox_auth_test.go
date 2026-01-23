package manager

import (
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestManagerIntegration_ClaimSandboxRequiresAuth(t *testing.T) {
	env := newManagerTestEnv(t)

	resp, _ := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes", "", map[string]any{
		"team_id": "team-1",
	})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", resp.StatusCode)
	}
}

func TestManagerIntegration_ClaimSandboxRequiresTeamID(t *testing.T) {
	env := newManagerTestEnv(t)

	resp, _ := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes", env.token, map[string]any{
		"template": "default",
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d", resp.StatusCode)
	}
}

func TestManagerIntegration_PauseSandboxForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-pause", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes/sbx-pause/pause", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}

func TestManagerIntegration_ResumeSandboxForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-resume", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes/sbx-resume/resume", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}

func TestManagerIntegration_RefreshSandboxForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-refresh", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes/sbx-refresh/refresh", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}

func TestManagerIntegration_GetSandboxStatsForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-stats", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/sbx-stats/stats", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}

func TestManagerIntegration_TerminateSandboxForbidden(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-terminate-forbidden", "team-2", "user-2", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodDelete, env.server.URL+"/api/v1/sandboxes/sbx-terminate-forbidden", env.token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d: %s", resp.StatusCode, string(body))
	}
}
