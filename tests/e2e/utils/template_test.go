package utils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func TestCreateTemplateFromSandboxDetailedSendsIdempotencyKeyAndDecodesCreation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/templates/from-sandbox" {
			t.Errorf("request = %s %s, want POST /api/v1/templates/from-sandbox", r.Method, r.URL.Path)
			return
		}
		if got := r.Header.Get("Idempotency-Key"); got != "request-1" {
			t.Errorf("Idempotency-Key = %q, want request-1", got)
			return
		}
		var request TemplateFromSandboxCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if request.TemplateID != "derived" || request.SandboxID != "sandbox-1" {
			t.Errorf("request = %#v", request)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{
			"success": true,
			"data": {
				"template_id": "derived",
				"team_id": "team-1",
				"status": {
					"creation": {
						"state": "creating",
						"stage": "capturing"
					}
				}
			}
		}`))
	}))
	defer server.Close()

	session := &Session{
		baseURL: server.URL,
		token:   "token-1",
		teamID:  "team-1",
		client:  server.Client(),
	}
	view, status, err := session.CreateTemplateFromSandboxDetailed(
		context.Background(),
		nil,
		TemplateFromSandboxCreateRequest{TemplateID: "derived", SandboxID: "sandbox-1"},
		"request-1",
	)
	if err != nil {
		t.Fatalf("CreateTemplateFromSandboxDetailed() error = %v", err)
	}
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", status, http.StatusAccepted)
	}
	if view == nil || view.Status == nil || view.Status.Creation == nil {
		t.Fatalf("view = %#v, want creation status", view)
	}
	if view.Status.Creation.State != "creating" || view.Status.Creation.Stage != "capturing" {
		t.Fatalf("creation = %#v, want creating/capturing", view.Status.Creation)
	}
}

func TestCloneTemplateForCreatePreservesMemory(t *testing.T) {
	t.Parallel()

	base := apispec.Template{
		Spec: apispec.SandboxTemplateSpec{
			MainContainer: &apispec.ContainerSpec{
				Image: "nginx:1.27-alpine",
				Resources: apispec.ResourceQuota{
					Memory: "512Mi",
				},
			},
		},
	}

	created := CloneTemplateForCreate(base, "tpl-e2e")

	if created.Spec.MainContainer == nil {
		t.Fatal("main container should be set")
	}
	if got := created.Spec.MainContainer.Resources.Memory; got != "512Mi" {
		t.Fatalf("main memory = %q, want %q", got, "512Mi")
	}
}
