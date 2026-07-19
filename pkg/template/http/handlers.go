package http

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// SandboxTemplateSourceResolver resolves durable source sandbox metadata in the
// sandbox's owning data-plane cluster.
type SandboxTemplateSourceResolver interface {
	ResolveSandboxTemplateSource(ctx context.Context, sandboxID, teamID string) (*template.SandboxTemplateSource, error)
}

// Handler provides template HTTP handlers backed by a template store.
type Handler struct {
	Store                store.TemplateStore
	BuildStore           store.TemplateBuildStore
	SourceResolver       SandboxTemplateSourceResolver
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

// TemplateFromSandboxRequest creates a template from a durable sandbox rootfs.
type TemplateFromSandboxRequest struct {
	TemplateID    string                            `json:"template_id"`
	SandboxID     string                            `json:"sandbox_id"`
	SpecOverrides *TemplateFromSandboxSpecOverrides `json:"spec_overrides,omitempty"`
}

// TemplateFromSandboxSpecOverrides contains the safe overrides accepted by the
// from-sandbox constructor.
type TemplateFromSandboxSpecOverrides struct {
	Description *string                `json:"description,omitempty"`
	DisplayName *string                `json:"displayName,omitempty"`
	Tags        *[]string              `json:"tags,omitempty"`
	Pool        *v1alpha1.PoolStrategy `json:"pool,omitempty"`
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
		if tpl.Status == nil {
			tpl.Status = &v1alpha1.SandboxTemplateStatus{}
		}
		tpl.Status.IdleCount = stat.IdleCount
		tpl.Status.ActiveCount = stat.ActiveCount
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
	if !limitJSONRequestBody(c, "template request body", template.MaxObjectRequestBytes) {
		return
	}
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
	if err := template.ValidateTemplateSpecSize(&templateSpec); err != nil {
		if writeResourceTooLarge(c, err, "template spec") {
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
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
		if writeTeamQuotaMutationError(c, err) {
			return
		}
		if writeResourceTooLarge(c, err, "template spec") {
			return
		}
		if errors.Is(err, template.ErrTemplateImageCleanupPending) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
			return
		}
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

// CreateTemplateFromSandbox creates a template and enqueues its image build.
func (h *Handler) CreateTemplateFromSandbox(c *gin.Context) {
	if !limitJSONRequestBody(c, "template request body", template.MaxObjectRequestBytes) {
		return
	}
	var req TemplateFromSandboxRequest
	if err := decodeStrictJSON(c, &req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(req.TemplateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.TemplateID = canonicalTemplateID
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	if req.SandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}
	if _, err := naming.ParseSandboxName(req.SandboxID); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid sandbox_id")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if !claims.IsSystemToken() && !internalauth.HasAllPermissions(
		c.Request.Context(),
		gatewayauthn.PermTemplateCreate,
		gatewayauthn.PermSandboxRead,
	) {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "template:create and sandbox:read permissions are required")
		return
	}
	scope, teamID, err := templateScopeForClaims(claims)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if scope != naming.ScopeTeam || strings.TrimSpace(teamID) == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for templates created from a sandbox")
		return
	}
	if h.BuildStore == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template image builds are unavailable")
		return
	}

	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if len(idempotencyKey) > 255 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "Idempotency-Key must be at most 255 characters")
		return
	}
	requestHash, err := templateFromSandboxRequestHash(req)
	if err != nil {
		h.Logger.Error("Failed to hash template creation request", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	if idempotencyKey != "" {
		existing, err := h.BuildStore.GetTemplateByIdempotencyKey(c.Request.Context(), scope, teamID, idempotencyKey)
		if err != nil {
			h.Logger.Error("Failed to resolve template creation idempotency key", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
			return
		}
		if existing != nil {
			if existing.CreationRequestHash != requestHash {
				spec.JSONError(c, http.StatusConflict, spec.CodeConflict, template.ErrTemplateIdempotencyConflict.Error())
				return
			}
			h.writeAcceptedTemplate(c, existing, claims)
			return
		}
	}
	if h.Store == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template store is unavailable")
		return
	}
	existingTemplate, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, req.TemplateID)
	if err != nil {
		h.Logger.Error("Failed to check template ID", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	if existingTemplate != nil {
		if idempotencyKey != "" {
			replayed, err := h.BuildStore.GetTemplateByIdempotencyKey(c.Request.Context(), scope, teamID, idempotencyKey)
			if err != nil {
				h.Logger.Error("Failed to recheck template creation idempotency key", zap.Error(err))
				spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
				return
			}
			if replayed != nil {
				if replayed.CreationRequestHash != requestHash {
					spec.JSONError(c, http.StatusConflict, spec.CodeConflict, template.ErrTemplateIdempotencyConflict.Error())
					return
				}
				h.writeAcceptedTemplate(c, replayed, claims)
				return
			}
		}
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, template.ErrTemplateAlreadyExists.Error())
		return
	}
	if h.SourceResolver == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template image builds are unavailable")
		return
	}

	source, err := h.SourceResolver.ResolveSandboxTemplateSource(c.Request.Context(), req.SandboxID, teamID)
	if err != nil {
		h.writeTemplateSourceError(c, err)
		return
	}
	if source == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "source sandbox not found")
		return
	}
	if source.TeamID != teamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "source sandbox belongs to a different team")
		return
	}
	if strings.TrimSpace(source.ClusterID) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "source sandbox has no owning cluster")
		return
	}

	templateSpec := templateSpecFromSandboxSource(source.Spec, req.SpecOverrides)
	memoryPerCPU := configuredTemplateMemoryPerCPU()
	templateSpec.MainContainer.Resources.CPU = resource.Quantity{}
	deriveTemplateCPU(&templateSpec, memoryPerCPU)
	if err := template.ValidateTemplateSpecSize(&templateSpec); err != nil {
		if writeResourceTooLarge(c, err, "template spec") {
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := validateTemplateSpec(templateSpec); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "source template is not reusable: "+err.Error())
		return
	}
	if err := validateTemplateSpecForClaimsWithMemoryPerCPU(templateSpec, claims, memoryPerCPU); err != nil {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
		return
	}
	if err := validateTemplateClaimNameBudget(scope, teamID, req.TemplateID, templateSpec); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	now := time.Now().UTC()
	buildID := uuid.NewString()
	startedAt := metav1.NewTime(now)
	tpl := &template.Template{
		TemplateID: req.TemplateID,
		Scope:      scope,
		TeamID:     teamID,
		UserID:     claims.UserID,
		Spec:       templateSpec,
		Status: &v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State:     v1alpha1.TemplateCreationStateCreating,
				Stage:     v1alpha1.TemplateCreationStageCapturing,
				StartedAt: &startedAt,
			},
		},
		CreatedAt:              now,
		UpdatedAt:              now,
		CreationBuildID:        buildID,
		CreationIdempotencyKey: idempotencyKey,
		CreationRequestHash:    requestHash,
	}
	build := &template.TemplateBuild{
		BuildID:         buildID,
		Scope:           scope,
		TeamID:          teamID,
		UserID:          claims.UserID,
		TemplateID:      req.TemplateID,
		SourceSandboxID: req.SandboxID,
		TargetClusterID: source.ClusterID,
		DesiredSpec:     templateSpec,
		RequestHash:     requestHash,
		IdempotencyKey:  idempotencyKey,
		Status:          template.TemplateBuildStatusQueued,
		Stage:           v1alpha1.TemplateCreationStageCapturing,
		SnapshotID:      template.BuildSnapshotID(buildID),
		NextAttemptAt:   now,
	}
	created, _, err := h.BuildStore.CreateTemplateBuild(c.Request.Context(), tpl, build)
	if err != nil {
		switch {
		case writeTeamQuotaMutationError(c, err):
		case writeResourceTooLarge(c, err, "template spec"):
		case errors.Is(err, template.ErrTemplateAlreadyExists),
			errors.Is(err, template.ErrTemplateIdempotencyConflict),
			errors.Is(err, template.ErrTemplateImageCleanupPending):
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
		default:
			h.Logger.Error("Failed to create template build", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		}
		return
	}
	if created == nil {
		created = tpl
	}
	h.writeAcceptedTemplate(c, created, claims)
}

func (h *Handler) writeAcceptedTemplate(c *gin.Context, tpl *template.Template, claims *internalauth.Claims) {
	responseTemplate, err := templateForResponse(tpl, claims)
	if err != nil {
		h.Logger.Error("Failed to project template response", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	c.Header("Location", "/api/v1/templates/"+tpl.TemplateID)
	if !tpl.ReadyForClaim() && tpl.Status != nil && tpl.Status.Creation != nil &&
		tpl.Status.Creation.State == v1alpha1.TemplateCreationStateCreating {
		c.Header("Retry-After", "1")
	}
	spec.JSONSuccess(c, http.StatusAccepted, responseTemplate)
}

func templateSpecFromSandboxSource(source v1alpha1.SandboxTemplateSpec, overrides *TemplateFromSandboxSpecOverrides) v1alpha1.SandboxTemplateSpec {
	out := *source.DeepCopy()
	out.Pod = nil
	out.MainContainer.SecurityContext = nil
	out.MainContainer.ImagePullPolicy = ""
	out.ClusterId = nil
	out.VolumeMounts = nil
	out.Pool = v1alpha1.PoolStrategy{}
	if overrides == nil {
		return out
	}
	if overrides.Description != nil {
		out.Description = *overrides.Description
	}
	if overrides.DisplayName != nil {
		out.DisplayName = *overrides.DisplayName
	}
	if overrides.Tags != nil {
		out.Tags = append([]string(nil), (*overrides.Tags)...)
	}
	if overrides.Pool != nil {
		out.Pool = *overrides.Pool
	}
	return out
}

func templateFromSandboxRequestHash(req TemplateFromSandboxRequest) (string, error) {
	normalized, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(normalized)
	return fmt.Sprintf("%x", sum[:]), nil
}

func decodeStrictJSON(c *gin.Context, target any) error {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("request body must contain one JSON value")
		}
		return err
	}
	return nil
}

func (h *Handler) writeTemplateSourceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, template.ErrTemplateSourceNotFound):
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, err.Error())
	case errors.Is(err, template.ErrTemplateSourceForbidden):
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, err.Error())
	case errors.Is(err, template.ErrTemplateSourceNotReady):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, template.ErrTemplateSourceUnavailable):
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
	default:
		h.Logger.Error("Failed to resolve source sandbox", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to resolve source sandbox")
	}
}

// UpdateTemplate updates an existing template.
func (h *Handler) UpdateTemplate(c *gin.Context) {
	if !limitJSONRequestBody(c, "template request body", template.MaxObjectRequestBytes) {
		return
	}
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
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}
	if !existing.ReadyForClaim() {
		message := "template is not ready"
		if existing.Status != nil && existing.Status.Creation != nil {
			creation := existing.Status.Creation
			switch creation.State {
			case v1alpha1.TemplateCreationStateCreating:
				c.Header("Retry-After", "1")
				message = "template creation is still in progress"
			case v1alpha1.TemplateCreationStateFailed:
				message = "template creation failed; delete and recreate the template"
			}
		}
		spec.JSONError(c, http.StatusConflict, spec.CodeTemplateNotReady, message)
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
	if err := template.ValidateTemplateSpecSize(&templateSpec); err != nil {
		if writeResourceTooLarge(c, err, "template spec") {
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
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

	tpl := &template.Template{
		TemplateID: templateID,
		Scope:      scope,
		TeamID:     teamID,
		UserID:     claims.UserID,
		Spec:       templateSpec,
	}

	if err := h.Store.UpdateTemplate(c.Request.Context(), tpl); err != nil {
		if writeResourceTooLarge(c, err, "template spec") {
			return
		}
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

	if existing.CreationBuildID != "" && h.BuildStore != nil {
		if _, err := h.BuildStore.CancelTemplateBuildAndDeleteTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
			if writeTeamQuotaMutationError(c, err) {
				return
			}
			h.Logger.Error("Failed to cancel template build", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
			return
		}
	} else {
		if err := h.Store.DeleteTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
			if writeTeamQuotaMutationError(c, err) {
				return
			}
			h.Logger.Error("Failed to delete template", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
			return
		}
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

func writeTeamQuotaMutationError(c *gin.Context, err error) bool {
	switch {
	case teamquota.IsExceeded(err):
		spec.JSONError(c, http.StatusTooManyRequests, spec.CodeQuotaExceeded, "team quota exceeded")
		return true
	case teamquota.IsUnavailable(err):
		c.Header("Retry-After", "1")
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "team quota is unavailable")
		return true
	default:
		return false
	}
}
