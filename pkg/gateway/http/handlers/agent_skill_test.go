package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/agentskill"
	"go.uber.org/zap"
)

func TestAgentSkillHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	metadata := &agentskill.Metadata{
		Name:           "sandbox0",
		ReleaseVersion: "0.1.0",
		ReleaseTag:     "v0.1.0",
		ArtifactPrefix: "sandbox0-agent-skill",
		SourcePriority: []string{"source-code"},
		DownloadURL:    "https://example.invalid/download",
		ChecksumURL:    "https://example.invalid/checksum",
		ManifestURL:    "https://example.invalid/manifest",
	}
	handler := NewAgentSkillHandler(metadata, zap.NewNop())

	t.Run("get", func(t *testing.T) {
		router := gin.New()
		router.GET("/api/v1/agent-skills/:name", handler.Get)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/agent-skills/sandbox0", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status %d", rec.Code)
		}
		var body struct {
			Success bool                `json:"success"`
			Data    agentskill.Metadata `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if !body.Success || body.Data.ReleaseVersion != "0.1.0" {
			t.Fatalf("unexpected response %+v", body)
		}
	})

	t.Run("download redirect", func(t *testing.T) {
		router := gin.New()
		router.GET("/api/v1/agent-skills/:name/download", handler.Download)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/agent-skills/sandbox0/download", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusTemporaryRedirect {
			t.Fatalf("unexpected status %d", rec.Code)
		}
		if got := rec.Header().Get("Location"); got != metadata.DownloadURL {
			t.Fatalf("unexpected location %q", got)
		}
	})

	t.Run("not found", func(t *testing.T) {
		router := gin.New()
		router.GET("/api/v1/agent-skills/:name", handler.Get)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/agent-skills/other", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("unexpected status %d", rec.Code)
		}
	})
}
