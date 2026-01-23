package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/tests/integration/internal/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestManagerIntegration_TemplateLifecycle(t *testing.T) {
	env := newManagerTestEnv(t)

	template := v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "basic-template",
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

	err := utils.WaitUntil(context.Background(), 2*time.Second, 100*time.Millisecond, func(ctx context.Context) (bool, error) {
		resp, body := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", env.token, nil)
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		var payload struct {
			Templates []v1alpha1.SandboxTemplate `json:"templates"`
			Count     int                        `json:"count"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return false, err
		}
		if payload.Count != 1 {
			return false, nil
		}
		if len(payload.Templates) != 1 || payload.Templates[0].Name != template.Name {
			return false, nil
		}
		return true, nil
	})
	utils.RequireNoError(t, err, "waiting for template to appear in list")
}

func TestManagerIntegration_TemplatesRequireAuth(t *testing.T) {
	env := newManagerTestEnv(t)

	resp, _ := doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", resp.StatusCode)
	}
}

func TestManagerIntegration_TemplateUpdateAndDelete(t *testing.T) {
	env := newManagerTestEnv(t)

	template := v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "update-template",
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

	template.Spec.Pool.MaxIdle = 3
	resp, body = doRequest(t, env.server.Client(), http.MethodPut, env.server.URL+"/api/v1/templates/"+template.Name, env.token, template)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected update status %d, got %d: %s", http.StatusOK, resp.StatusCode, string(body))
	}

	var updated v1alpha1.SandboxTemplate
	if err := json.Unmarshal(body, &updated); err != nil {
		t.Fatalf("failed to decode update response: %v", err)
	}
	if updated.Spec.Pool.MaxIdle != 3 {
		t.Fatalf("expected maxIdle 3, got %d", updated.Spec.Pool.MaxIdle)
	}

	resp, body = doRequest(t, env.server.Client(), http.MethodDelete, env.server.URL+"/api/v1/templates/"+template.Name, env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d: %s", http.StatusOK, resp.StatusCode, string(body))
	}

	resp, body = doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected list status %d, got %d: %s", http.StatusOK, resp.StatusCode, string(body))
	}
	var payload struct {
		Templates []v1alpha1.SandboxTemplate `json:"templates"`
		Count     int                        `json:"count"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode list response: %v", err)
	}
	if payload.Count != 0 || len(payload.Templates) != 0 {
		t.Fatalf("expected empty list after delete, got %d templates", payload.Count)
	}
}

func TestManagerIntegration_TemplateGet(t *testing.T) {
	env := newManagerTestEnv(t)

	template := v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "get-template",
		},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image:     "sandbox0ai/infra:latest",
				Resources: v1alpha1.ResourceQuota{},
			},
			Pool: v1alpha1.PoolStrategy{
				MinIdle:   1,
				MaxIdle:   2,
				AutoScale: false,
			},
		},
	}

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/templates", env.token, template)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d: %s", http.StatusCreated, resp.StatusCode, string(body))
	}

	resp, body = doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates/"+template.Name, env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}

	var fetched v1alpha1.SandboxTemplate
	if err := json.Unmarshal(body, &fetched); err != nil {
		t.Fatalf("failed to decode template: %v", err)
	}
	if fetched.Name != template.Name {
		t.Fatalf("expected template %s, got %s", template.Name, fetched.Name)
	}
	if fetched.Spec.Pool.MinIdle != 1 || fetched.Spec.Pool.MaxIdle != 2 {
		t.Fatalf("unexpected pool settings: %+v", fetched.Spec.Pool)
	}
}

func TestManagerIntegration_TemplateWarmPool(t *testing.T) {
	env := newManagerTestEnv(t)

	template := v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-template",
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

	resp, body = doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/templates/"+template.Name+"/pool/warm", env.token, map[string]any{
		"count": 2,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected warm status %d, got %d: %s", http.StatusOK, resp.StatusCode, string(body))
	}

	resp, body = doRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates/"+template.Name, env.token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected ok, got %d: %s", resp.StatusCode, string(body))
	}

	var fetched v1alpha1.SandboxTemplate
	if err := json.Unmarshal(body, &fetched); err != nil {
		t.Fatalf("failed to decode template: %v", err)
	}
	if fetched.Spec.Pool.MinIdle != 2 || fetched.Spec.Pool.MaxIdle != 2 {
		t.Fatalf("unexpected warm pool settings: %+v", fetched.Spec.Pool)
	}
}
