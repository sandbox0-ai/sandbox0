package functionapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"

	"github.com/gin-gonic/gin"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

const defaultFunctionRootDomain = "sandbox0.site"

type Config struct {
	DefaultClusterGatewayURL string
	FunctionRootDomain       string
	FunctionRegionID         string
	PublicRegionID           string
	RegionID                 string
}

type SandboxLookup func(ctx context.Context, sandboxID string) (*mgr.Sandbox, error)

type PermissionMiddleware func(permission string) gin.HandlerFunc

type Handler struct {
	repo          *functions.Repository
	cfg           Config
	sandboxLookup SandboxLookup
	logger        *zap.Logger
}

func New(repo *functions.Repository, cfg Config, lookup SandboxLookup, logger *zap.Logger) *Handler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Handler{
		repo:          repo,
		cfg:           cfg,
		sandboxLookup: lookup,
		logger:        logger,
	}
}

func (h *Handler) RegisterRoutes(group *gin.RouterGroup, require PermissionMiddleware) {
	if require == nil {
		require = func(string) gin.HandlerFunc {
			return func(c *gin.Context) { c.Next() }
		}
	}
	group.GET("", require(authn.PermFunctionRead), h.listFunctions)
	group.POST("", require(authn.PermFunctionCreate), h.createFunction)
	group.GET("/:id", require(authn.PermFunctionRead), h.getFunction)
	group.GET("/:id/revisions", require(authn.PermFunctionRead), h.listFunctionRevisions)
	group.POST("/:id/revisions", require(authn.PermFunctionWrite), h.createFunctionRevision)
	group.PUT("/:id/aliases/:alias", require(authn.PermFunctionWrite), h.setFunctionAlias)
}

func NewClusterGatewaySandboxLookup(clusterGatewayURL string, internalAuthGen *internalauth.Generator, httpClient *http.Client, logger *zap.Logger) SandboxLookup {
	if logger == nil {
		logger = zap.NewNop()
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return func(ctx context.Context, sandboxID string) (*mgr.Sandbox, error) {
		clusterGatewayURL := strings.TrimRight(strings.TrimSpace(clusterGatewayURL), "/")
		if clusterGatewayURL == "" {
			return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "cluster gateway is not configured"}
		}
		if internalAuthGen == nil {
			return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "internal auth generator is not configured"}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, clusterGatewayURL+"/internal/v1/sandboxes/"+neturl.PathEscape(sandboxID), nil)
		if err != nil {
			return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "failed to create cluster gateway request"}
		}
		token, err := internalAuthGen.GenerateSystem(internalauth.ServiceClusterGateway, internalauth.GenerateOptions{})
		if err != nil {
			return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "failed to generate internal token"}
		}
		req.Header.Set(internalauth.DefaultTokenHeader, token)

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "cluster gateway unavailable"}
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return nil, publishError{status: http.StatusNotFound, code: spec.CodeNotFound, message: "sandbox not found"}
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			logger.Warn("Cluster gateway sandbox lookup failed",
				zap.String("sandbox_id", sandboxID),
				zap.Int("status", resp.StatusCode),
				zap.String("body", strings.TrimSpace(string(body))),
			)
			return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "sandbox unavailable"}
		}

		sandbox, apiErr, err := spec.DecodeResponse[mgr.Sandbox](resp.Body)
		if err != nil {
			return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "failed to decode sandbox response"}
		}
		if apiErr != nil {
			return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: apiErr.Message}
		}
		if sandbox == nil {
			return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "sandbox response was empty"}
		}
		return sandbox, nil
	}
}

func SandboxNotFoundError() error {
	return publishError{status: http.StatusNotFound, code: spec.CodeNotFound, message: "sandbox not found"}
}

func SandboxUnavailableError(message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "sandbox unavailable"
	}
	return publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: message}
}

type functionSourceRequest struct {
	SandboxID string `json:"sandbox_id"`
	ServiceID string `json:"service_id"`
}

type createFunctionRequest struct {
	Name   string                `json:"name"`
	Source functionSourceRequest `json:"source"`
}

type createFunctionRevisionRequest struct {
	Source  functionSourceRequest `json:"source"`
	Promote *bool                 `json:"promote,omitempty"`
}

type setFunctionAliasRequest struct {
	RevisionNumber int `json:"revision_number"`
}

type functionRecord struct {
	*functions.Function
	Host string `json:"host"`
	URL  string `json:"url"`
}

func (h *Handler) listFunctions(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	fns, err := h.repo.ListFunctions(c.Request.Context(), authCtx.TeamID)
	if err != nil {
		h.logger.Error("Failed to list functions", zap.String("team_id", authCtx.TeamID), zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list functions")
		return
	}

	records := make([]functionRecord, 0, len(fns))
	for _, fn := range fns {
		records = append(records, h.functionRecord(fn))
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"functions": records})
}

func (h *Handler) createFunction(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req createFunctionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	sandbox, serviceSnapshot, err := h.loadPublishableService(c.Request.Context(), authCtx, req.Source)
	if err != nil {
		h.writePublishError(c, err)
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = strings.TrimSpace(serviceSnapshot.DisplayName)
	}
	if name == "" {
		name = serviceSnapshot.ID
	}

	userID := principalID(authCtx)
	fn := functions.NewFunction(authCtx.TeamID, name, userID)
	rev, err := functions.NewRevision(authCtx.TeamID, sandbox.ID, serviceSnapshot.ID, sandbox.TemplateID, serviceSnapshot, restoreMountsFromSandbox(sandbox), userID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create revision snapshot")
		return
	}

	fn, rev, err = h.repo.CreateFunctionWithRevision(c.Request.Context(), fn, rev, userID)
	if err != nil {
		if errors.Is(err, functions.ErrAlreadyExists) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "function name already exists")
			return
		}
		h.logger.Error("Failed to create function", zap.String("team_id", authCtx.TeamID), zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create function")
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, gin.H{
		"function": h.functionRecord(fn),
		"revision": rev,
		"alias": functions.Alias{
			FunctionID:     fn.ID,
			Alias:          functions.ProductionAlias,
			RevisionID:     rev.ID,
			RevisionNumber: rev.RevisionNumber,
			UpdatedBy:      userID,
			UpdatedAt:      fn.UpdatedAt,
		},
	})
}

func (h *Handler) getFunction(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	fn, err := h.repo.GetFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get function")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"function": h.functionRecord(fn)})
}

func (h *Handler) listFunctionRevisions(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	revisions, err := h.repo.ListRevisions(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list revisions")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"revisions": revisions})
}

func (h *Handler) createFunctionRevision(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req createFunctionRevisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	sandbox, serviceSnapshot, err := h.loadPublishableService(c.Request.Context(), authCtx, req.Source)
	if err != nil {
		h.writePublishError(c, err)
		return
	}

	userID := principalID(authCtx)
	rev, err := functions.NewRevision(authCtx.TeamID, sandbox.ID, serviceSnapshot.ID, sandbox.TemplateID, serviceSnapshot, restoreMountsFromSandbox(sandbox), userID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create revision snapshot")
		return
	}
	promote := true
	if req.Promote != nil {
		promote = *req.Promote
	}

	rev, err = h.repo.CreateRevision(c.Request.Context(), authCtx.TeamID, c.Param("id"), rev, promote, userID)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create revision")
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, gin.H{"revision": rev, "promoted": promote})
}

func (h *Handler) setFunctionAlias(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req setFunctionAliasRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	if req.RevisionNumber <= 0 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "revision_number must be greater than zero")
		return
	}

	alias, err := h.repo.SetAlias(c.Request.Context(), authCtx.TeamID, c.Param("id"), c.Param("alias"), req.RevisionNumber, principalID(authCtx))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function or revision not found")
			return
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"alias": alias})
}

func (h *Handler) loadPublishableService(ctx context.Context, authCtx *authn.AuthContext, source functionSourceRequest) (*mgr.Sandbox, mgr.SandboxAppService, error) {
	if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
		return nil, mgr.SandboxAppService{}, publishError{status: http.StatusForbidden, code: spec.CodeForbidden, message: "team context is required"}
	}
	source.SandboxID = strings.TrimSpace(source.SandboxID)
	source.ServiceID = strings.TrimSpace(source.ServiceID)
	if source.SandboxID == "" {
		return nil, mgr.SandboxAppService{}, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: "source.sandbox_id is required"}
	}
	if source.ServiceID == "" {
		return nil, mgr.SandboxAppService{}, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: "source.service_id is required"}
	}
	if h.sandboxLookup == nil {
		return nil, mgr.SandboxAppService{}, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "sandbox lookup is not configured"}
	}

	sandbox, err := h.sandboxLookup(ctx, source.SandboxID)
	if err != nil {
		return nil, mgr.SandboxAppService{}, err
	}
	if sandbox.TeamID != authCtx.TeamID {
		return nil, mgr.SandboxAppService{}, publishError{status: http.StatusForbidden, code: spec.CodeForbidden, message: "sandbox belongs to a different team"}
	}

	serviceSnapshot, ok := findSandboxService(sandbox, source.ServiceID)
	if !ok {
		return nil, mgr.SandboxAppService{}, publishError{status: http.StatusNotFound, code: spec.CodeNotFound, message: "sandbox service not found"}
	}
	if blockers := mgr.SandboxAppServicePublishBlockers(serviceSnapshot); len(blockers) > 0 {
		return nil, mgr.SandboxAppService{}, publishError{
			status:  http.StatusBadRequest,
			code:    spec.CodeBadRequest,
			message: "sandbox service is not publishable",
			details: gin.H{"blockers": blockers},
		}
	}
	return sandbox, serviceSnapshot, nil
}

func findSandboxService(sandbox *mgr.Sandbox, serviceID string) (mgr.SandboxAppService, bool) {
	if sandbox == nil {
		return mgr.SandboxAppService{}, false
	}
	serviceID = strings.TrimSpace(serviceID)
	for _, service := range sandbox.Services {
		if service.ID == serviceID {
			return service, true
		}
	}
	return mgr.SandboxAppService{}, false
}

func restoreMountsFromSandbox(sandbox *mgr.Sandbox) []functions.RestoreMount {
	if sandbox == nil || len(sandbox.Mounts) == 0 {
		return nil
	}
	mounts := make([]functions.RestoreMount, 0, len(sandbox.Mounts))
	for _, mount := range sandbox.Mounts {
		mounts = append(mounts, functions.RestoreMount{
			SandboxVolumeID: strings.TrimSpace(mount.SandboxVolumeID),
			MountPoint:      strings.TrimSpace(mount.MountPoint),
		})
	}
	return mounts
}

func (h *Handler) functionRecord(fn *functions.Function) functionRecord {
	regionID := strings.TrimSpace(h.cfg.FunctionRegionID)
	if regionID == "" {
		regionID = strings.TrimSpace(h.cfg.PublicRegionID)
	}
	if regionID == "" {
		regionID = strings.TrimSpace(h.cfg.RegionID)
	}
	host := functionHost(fn.DomainLabel, regionID, h.cfg.FunctionRootDomain)
	return functionRecord{
		Function: fn,
		Host:     host,
		URL:      "https://" + host,
	}
}

func principalID(authCtx *authn.AuthContext) string {
	if authCtx == nil {
		return ""
	}
	if strings.TrimSpace(authCtx.UserID) != "" {
		return strings.TrimSpace(authCtx.UserID)
	}
	return strings.TrimSpace(authCtx.APIKeyID)
}

type publishError struct {
	status  int
	code    string
	message string
	details any
}

func (e publishError) Error() string {
	return e.message
}

func (h *Handler) writePublishError(c *gin.Context, err error) {
	var publishErr publishError
	if errors.As(err, &publishErr) {
		if publishErr.details != nil {
			spec.JSONError(c, publishErr.status, publishErr.code, publishErr.message, publishErr.details)
			return
		}
		spec.JSONError(c, publishErr.status, publishErr.code, publishErr.message)
		return
	}
	h.logger.Error("Function publish failed", zap.Error(err))
	spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to publish function")
}

func functionHost(domainLabel, regionID, rootDomain string) string {
	rootDomain = strings.Trim(strings.ToLower(rootDomain), ".")
	if rootDomain == "" {
		rootDomain = defaultFunctionRootDomain
	}
	regionID = strings.Trim(strings.ToLower(regionID), ".")
	if regionID == "" {
		return strings.ToLower(domainLabel) + "." + rootDomain
	}
	return strings.ToLower(domainLabel) + "." + regionID + "." + rootDomain
}
