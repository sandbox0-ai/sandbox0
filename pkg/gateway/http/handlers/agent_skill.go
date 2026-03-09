package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/agentskill"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

type AgentSkillHandler struct {
	metadata *agentskill.Metadata
	logger   *zap.Logger
}

func NewAgentSkillHandler(metadata *agentskill.Metadata, logger *zap.Logger) *AgentSkillHandler {
	return &AgentSkillHandler{
		metadata: metadata,
		logger:   logger,
	}
}

func (h *AgentSkillHandler) List(c *gin.Context) {
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"skills": []any{h.metadata},
	})
}

func (h *AgentSkillHandler) Get(c *gin.Context) {
	if !h.matches(c.Param("name")) {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "agent skill not found")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, h.metadata)
}

func (h *AgentSkillHandler) Download(c *gin.Context) {
	h.redirect(c, c.Param("name"), h.metadata.DownloadURL)
}

func (h *AgentSkillHandler) Checksum(c *gin.Context) {
	h.redirect(c, c.Param("name"), h.metadata.ChecksumURL)
}

func (h *AgentSkillHandler) Manifest(c *gin.Context) {
	h.redirect(c, c.Param("name"), h.metadata.ManifestURL)
}

func (h *AgentSkillHandler) redirect(c *gin.Context, name, url string) {
	if !h.matches(name) {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "agent skill not found")
		return
	}
	c.Redirect(http.StatusTemporaryRedirect, url)
}

func (h *AgentSkillHandler) matches(name string) bool {
	return strings.EqualFold(strings.TrimSpace(name), h.metadata.Name)
}
