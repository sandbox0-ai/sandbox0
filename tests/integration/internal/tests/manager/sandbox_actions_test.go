package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestManagerIntegration_RefreshSandboxOK(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-refresh-ok", "team-1", "user-1", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes/sbx-refresh-ok/refresh", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}
	var payload service.RefreshResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode refresh response: %v", err)
	}
	if payload.SandboxID != "sbx-refresh-ok" || payload.ExpiresAt.IsZero() {
		t.Fatalf("unexpected refresh response: %+v", payload)
	}

	updated, err := env.k8sClient.CoreV1().Pods("").Get(context.Background(), "sbx-refresh-ok", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated pod: %v", err)
	}
	if updated.Annotations[controller.AnnotationExpiresAt] == "" {
		t.Fatalf("expected expires-at annotation to be set")
	}
}

func TestManagerIntegration_TerminateSandboxOK(t *testing.T) {
	env := newManagerTestEnv(t)

	addSandboxPod(t, env, "sbx-terminate", "team-1", "user-1", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodDelete, env.server.URL+"/api/v1/sandboxes/sbx-terminate", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}

	_, err := env.k8sClient.CoreV1().Pods("").Get(context.Background(), "sbx-terminate", metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Fatalf("expected pod to be deleted, got err=%v", err)
	}
}

func TestManagerIntegration_PauseSandboxOK(t *testing.T) {
	env := newManagerTestEnvWithProcd(t)

	addSandboxPod(t, env, "sbx-pause-ok", "team-1", "user-1", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes/sbx-pause-ok/pause", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}
	var payload service.PauseSandboxResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode pause response: %v", err)
	}
	if !payload.Paused || payload.SandboxID != "sbx-pause-ok" {
		t.Fatalf("unexpected pause response: %+v", payload)
	}

	updated, err := env.k8sClient.CoreV1().Pods("").Get(context.Background(), "sbx-pause-ok", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated pod: %v", err)
	}
	if updated.Annotations[controller.AnnotationPaused] != "true" {
		t.Fatalf("expected paused annotation to be true")
	}
}

func TestManagerIntegration_ResumeSandboxOK(t *testing.T) {
	env := newManagerTestEnvWithProcd(t)

	pausedState := service.PausedState{
		Resources: map[string]service.ContainerResources{
			"procd": {
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("256Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		},
		OriginalTTL: 120,
	}
	pausedStateJSON, err := json.Marshal(pausedState)
	if err != nil {
		t.Fatalf("marshal paused state: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-resume-ok",
			Namespace: "",
			Labels: map[string]string{
				controller.LabelSandboxID: "sbx-resume-ok",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:      "team-1",
				controller.AnnotationUserID:      "user-1",
				controller.AnnotationPaused:      "true",
				controller.AnnotationPausedState: string(pausedStateJSON),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "procd",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				},
			},
		},
	}

	_, err = env.k8sClient.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod: %v", err)
	}
	if err := env.podIndexer.Add(pod); err != nil {
		t.Fatalf("add pod to indexer: %v", err)
	}

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes/sbx-resume-ok/resume", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}
	var payload service.ResumeSandboxResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode resume response: %v", err)
	}
	if !payload.Resumed || payload.SandboxID != "sbx-resume-ok" {
		t.Fatalf("unexpected resume response: %+v", payload)
	}
	if payload.RestoredMemory != "256Mi" {
		t.Fatalf("expected restored memory 256Mi, got %s", payload.RestoredMemory)
	}

	updated, err := env.k8sClient.CoreV1().Pods("").Get(context.Background(), "sbx-resume-ok", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated pod: %v", err)
	}
	if updated.Annotations[controller.AnnotationPaused] != "" {
		t.Fatalf("expected paused annotation cleared")
	}
	if updated.Annotations[controller.AnnotationExpiresAt] == "" {
		t.Fatalf("expected expires-at annotation to be set")
	}
}

func TestManagerIntegration_GetSandboxStatsOK(t *testing.T) {
	env := newManagerTestEnvWithProcd(t)

	addSandboxPod(t, env, "sbx-stats-ok", "team-1", "user-1", corev1.PodRunning)

	resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxes/sbx-stats-ok/stats", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}
	var stats service.SandboxResourceUsage
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("failed to decode stats response: %v", err)
	}
	if stats.ContainerMemoryWorkingSet == 0 {
		t.Fatalf("expected working set in stats response")
	}
}

func TestManagerIntegration_ClaimSandboxOK(t *testing.T) {
	env := newManagerTestEnvWithProcd(t)

	template := v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "claim-template",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image:     "sandbox0ai/infra:latest",
				Resources: v1alpha1.ResourceQuota{},
			},
			Pool: v1alpha1.PoolStrategy{
				MinIdle:   0,
				MaxIdle:   1,
				AutoScale: false,
			},
		},
	}

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/templates", env.token, template)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d: %s", http.StatusCreated, resp.StatusCode, string(body))
	}

	resp, body = doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes", env.token, map[string]any{
		"team_id":  "team-1",
		"user_id":  "user-1",
		"template": template.Name,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected created, got %d: %s", resp.StatusCode, string(body))
	}
	var payload service.ClaimResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode claim response: %v", err)
	}
	if payload.SandboxID == "" || payload.Status != "starting" {
		t.Fatalf("unexpected claim response: %+v", payload)
	}
	if payload.Template != template.Name || payload.Namespace != "sandbox0" {
		t.Fatalf("unexpected claim template/namespace: %+v", payload)
	}
	if payload.ProcdAddress == "" {
		t.Fatalf("expected procd address in response")
	}

	_, err := env.k8sClient.CoreV1().Pods("sandbox0").Get(context.Background(), payload.SandboxID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected created pod: %v", err)
	}
}
