package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ClusterStore provides cluster lookup for delete warnings.
type ClusterStore interface {
	GetCluster(ctx context.Context, clusterID string) (*template.Cluster, error)
}

// Reconciler triggers template reconciliation.
type Reconciler interface {
	TriggerReconcile(ctx context.Context)
}

// TemplateStat contains pool counters for a synced template.
type TemplateStat struct {
	TemplateID  string
	Namespace   string
	IdleCount   int32
	ActiveCount int32
}

// TemplateStats is a container for all template stats.
type TemplateStats struct {
	Templates []TemplateStat
}

// TemplateStatsProvider resolves current pool status for templates.
type TemplateStatsProvider interface {
	GetTemplateStats(ctx context.Context) (*TemplateStats, error)
}

// Handler provides template HTTP handlers backed by a template store.
type Handler struct {
	Store                store.TemplateStore
	AllocationStore      store.AllocationStore
	ClusterStore         ClusterStore
	Reconciler           Reconciler
	StatsProvider        TemplateStatsProvider
	PrivateRegistryHosts []string
	Logger               *zap.Logger
}

// TemplateRequest represents the request body for updating a template.
type TemplateRequest struct {
	Spec json.RawMessage `json:"spec"`
}

// ListTemplates lists all templates.
func (h *Handler) ListTemplates(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	_, teamID, err := templateScopeForClaims(claims)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	templates, err := h.Store.ListVisibleTemplates(c.Request.Context(), teamID)
	if err != nil {
		h.Logger.Error("Failed to list templates", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list templates")
		return
	}
	h.enrichTemplatesStatus(c.Request.Context(), templates)
	responseTemplates, err := templatesForResponse(templates, claims)
	if err != nil {
		h.Logger.Error("Failed to project template response", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list templates")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"templates": responseTemplates,
		"count":     len(templates),
	})
}

// GetTemplate gets a template by ID.
func (h *Handler) GetTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	scope, teamID, err := templateScopeForClaims(claims)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	var tpl *template.Template
	if scope == naming.ScopePublic {
		tpl, err = h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	} else {
		tpl, err = h.Store.GetTemplateForTeam(c.Request.Context(), teamID, templateID)
	}
	if err != nil {
		h.Logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template")
		return
	}

	if tpl == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}
	h.enrichTemplatesStatus(c.Request.Context(), []*template.Template{tpl})

	responseTemplate, err := templateForResponse(tpl, claims)
	if err != nil {
		h.Logger.Error("Failed to project template response", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, responseTemplate)
}

func (h *Handler) enrichTemplatesStatus(ctx context.Context, templates []*template.Template) {
	if len(templates) == 0 || h.StatsProvider == nil {
		return
	}

	stats, err := h.StatsProvider.GetTemplateStats(ctx)
	if err != nil {
		h.Logger.Warn("Failed to load template status", zap.Error(err))
		return
	}
	if stats == nil || len(stats.Templates) == 0 {
		return
	}

	statsMap := make(map[string]TemplateStat, len(stats.Templates))
	for _, stat := range stats.Templates {
		statsMap[templateStatKey(stat.Namespace, stat.TemplateID)] = stat
	}

	for _, tpl := range templates {
		namespace, err := templateNamespaceForScope(tpl.Scope, tpl.TeamID, tpl.TemplateID)
		if err != nil {
			h.Logger.Warn("Failed to resolve template namespace for status",
				zap.String("template_id", tpl.TemplateID),
				zap.String("scope", tpl.Scope),
				zap.String("team_id", tpl.TeamID),
				zap.Error(err),
			)
			continue
		}
		clusterTemplateID := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
		stat, ok := statsMap[templateStatKey(namespace, clusterTemplateID)]
		if !ok {
			continue
		}
		tpl.Status = &v1alpha1.SandboxTemplateStatus{
			IdleCount:   stat.IdleCount,
			ActiveCount: stat.ActiveCount,
		}
	}
}

func templateNamespaceForScope(scope, teamID, templateID string) (string, error) {
	if scope == naming.ScopeTeam {
		return naming.TemplateNamespaceForTeam(teamID)
	}
	return naming.TemplateNamespaceForBuiltin(templateID)
}

func templateStatKey(namespace, templateID string) string {
	return namespace + "\x00" + templateID
}

func templatesForResponse(templates []*template.Template, claims *internalauth.Claims) ([]*apispec.Template, error) {
	out := make([]*apispec.Template, 0, len(templates))
	for _, tpl := range templates {
		projected, err := templateForResponse(tpl, claims)
		if err != nil {
			return nil, err
		}
		out = append(out, projected)
	}
	return out, nil
}

// templateForResponse projects the internal template model through the generated
// public API type so internal fields such as the platform-derived CPU are omitted.
func templateForResponse(tpl *template.Template, claims *internalauth.Claims) (*apispec.Template, error) {
	if tpl == nil {
		return nil, nil
	}
	out := *tpl
	out.Spec = *tpl.Spec.DeepCopy()
	if tpl.Status != nil {
		out.Status = tpl.Status.DeepCopy()
	}
	if claims == nil || !claims.IsSystemToken() {
		out.Spec.Pod = nil
		out.Spec.MainContainer.SecurityContext = nil
		out.Spec.MainContainer.ImagePullPolicy = ""
		out.Spec.ClusterId = nil
	}
	raw, err := json.Marshal(&out)
	if err != nil {
		return nil, fmt.Errorf("marshal internal template: %w", err)
	}
	var projected apispec.Template
	if err := json.Unmarshal(raw, &projected); err != nil {
		return nil, fmt.Errorf("decode public template: %w", err)
	}
	return &projected, nil
}

func templateScopeForClaims(claims *internalauth.Claims) (string, string, error) {
	if claims == nil {
		return "", "", errors.New("missing authentication")
	}
	if teamID := strings.TrimSpace(claims.TeamID); teamID != "" {
		return naming.ScopeTeam, teamID, nil
	}
	if claims.IsSystemToken() {
		return naming.ScopePublic, "", nil
	}
	return "", "", errors.New("team_id is required for custom templates")
}

func decodeTemplateRequestSpec(raw json.RawMessage) (v1alpha1.SandboxTemplateSpec, error) {
	var out v1alpha1.SandboxTemplateSpec
	if len(raw) == 0 {
		return out, nil
	}
	if err := rejectUnsupportedTemplateSpecFields(raw); err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("invalid spec: %w", err)
	}
	return out, nil
}

// deriveTemplateCPU materializes the platform-managed CPU before persistence.
func deriveTemplateCPU(spec *v1alpha1.SandboxTemplateSpec, memoryPerCPU resource.Quantity) {
	if spec == nil || spec.MainContainer.Resources.Memory.Sign() <= 0 {
		return
	}
	spec.MainContainer.Resources.CPU = template.CPUForMemory(
		spec.MainContainer.Resources.Memory,
		memoryPerCPU,
	)
}

func rejectUnsupportedTemplateSpecFields(raw json.RawMessage) error {
	if strings.TrimSpace(string(raw)) == "null" {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("spec must be an object: %w", err)
	}
	for _, field := range []string{"lifecycle", "public", "allowedTeams"} {
		if _, ok := fields[field]; ok {
			return fmt.Errorf("spec.%s is not supported", field)
		}
	}
	var mainContainerFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["mainContainer"], &mainContainerFields); err != nil {
		return nil
	}
	var resourceFields map[string]json.RawMessage
	if err := json.Unmarshal(mainContainerFields["resources"], &resourceFields); err != nil {
		return nil
	}
	for field := range resourceFields {
		if strings.EqualFold(field, "cpu") {
			return fmt.Errorf("spec.mainContainer.resources.cpu is not supported; set memory only")
		}
	}
	return nil
}

// CreateTemplate creates a new template.
func (h *Handler) CreateTemplate(c *gin.Context) {
	var req struct {
		TemplateID string          `json:"template_id"`
		Spec       json.RawMessage `json:"spec"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}
	templateSpec, err := decodeTemplateRequestSpec(req.Spec)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	canonicalTemplateID, err := naming.CanonicalTemplateID(req.TemplateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.TemplateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	scope, teamID, err := templateScopeForClaims(claims)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	memoryPerCPU := configuredTemplateMemoryPerCPU()
	deriveTemplateCPU(&templateSpec, memoryPerCPU)
	if err := validateTemplateSpec(templateSpec); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := validateTemplateSpecForClaimsWithMemoryPerCPU(templateSpec, claims, memoryPerCPU); err != nil {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
		return
	}
	if err := validateTemplateImagesForClaims(templateSpec, claims, h.PrivateRegistryHosts); err != nil {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
		return
	}
	if err := validateTemplateClaimNameBudget(scope, teamID, req.TemplateID, templateSpec); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	templateID := req.TemplateID

	existing, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to check existing template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	if existing != nil {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "template already exists")
		return
	}

	tpl := &template.Template{
		TemplateID: templateID,
		Scope:      scope,
		TeamID:     teamID,
		UserID:     claims.UserID,
		Spec:       templateSpec,
	}

	if err := h.Store.CreateTemplate(c.Request.Context(), tpl); err != nil {
		h.Logger.Error("Failed to create template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}

	h.Logger.Info("Template created",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	h.triggerReconcile()

	created, _ := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if created == nil {
		created = tpl
	}
	responseTemplate, err := templateForResponse(created, claims)
	if err != nil {
		h.Logger.Error("Failed to project template response", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, responseTemplate)
}

// UpdateTemplate updates an existing template.
func (h *Handler) UpdateTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	scope, teamID, err := templateScopeForClaims(claims)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	var req TemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}
	templateSpec, err := decodeTemplateRequestSpec(req.Spec)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	memoryPerCPU := configuredTemplateMemoryPerCPU()
	deriveTemplateCPU(&templateSpec, memoryPerCPU)
	if err := validateTemplateSpec(templateSpec); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := validateTemplateSpecForClaimsWithMemoryPerCPU(templateSpec, claims, memoryPerCPU); err != nil {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
		return
	}
	if err := validateTemplateImagesForClaims(templateSpec, claims, h.PrivateRegistryHosts); err != nil {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
		return
	}
	if err := validateTemplateClaimNameBudget(scope, teamID, templateID, templateSpec); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	existing, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	tpl := &template.Template{
		TemplateID: templateID,
		Scope:      scope,
		TeamID:     teamID,
		UserID:     claims.UserID,
		Spec:       templateSpec,
	}

	if err := h.Store.UpdateTemplate(c.Request.Context(), tpl); err != nil {
		h.Logger.Error("Failed to update template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}

	h.Logger.Info("Template updated",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	h.triggerReconcile()

	updated, _ := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if updated == nil {
		updated = tpl
	}
	responseTemplate, err := templateForResponse(updated, claims)
	if err != nil {
		h.Logger.Error("Failed to project template response", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, responseTemplate)
}

// DeleteTemplate deletes a template.
func (h *Handler) DeleteTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	scope, teamID, err := templateScopeForClaims(claims)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	existing, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	var cleanupErrors []string
	if h.AllocationStore != nil {
		allocations, err := h.AllocationStore.ListAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID)
		if err != nil {
			h.Logger.Error("Failed to get template allocations", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
			return
		}

		if h.ClusterStore != nil {
			for _, alloc := range allocations {
				cluster, err := h.ClusterStore.GetCluster(c.Request.Context(), alloc.ClusterID)
				if err != nil {
					h.Logger.Warn("Failed to get cluster info for cleanup",
						zap.String("cluster_id", alloc.ClusterID),
						zap.Error(err),
					)
					cleanupErrors = append(cleanupErrors, alloc.ClusterID+": failed to get cluster info")
					continue
				}
				if cluster == nil {
					h.Logger.Warn("Cluster not found for cleanup",
						zap.String("cluster_id", alloc.ClusterID),
					)
					continue
				}

				h.Logger.Info("Template will be cleaned from cluster via reconcile",
					zap.String("cluster_id", alloc.ClusterID),
					zap.String("template_id", templateID),
				)
			}
		}

		if err := h.AllocationStore.DeleteAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
			h.Logger.Error("Failed to delete template allocations", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
			return
		}
	}

	if err := h.Store.DeleteTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
		h.Logger.Error("Failed to delete template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	h.Logger.Info("Template deleted from database",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	h.triggerReconcile()

	response := gin.H{"message": "template deleted"}
	if len(cleanupErrors) > 0 {
		response["cleanup_warnings"] = cleanupErrors
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

func (h *Handler) triggerReconcile() {
	if h.Reconciler == nil {
		return
	}
	go h.Reconciler.TriggerReconcile(context.Background())
}
