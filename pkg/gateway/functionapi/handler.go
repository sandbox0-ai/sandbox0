package functionapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/functionruntime"
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

type RevisionVolumeStore interface {
	PrepareRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, metadata functionruntime.Metadata) ([]functions.RestoreMount, error)
	DeleteRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, mounts []functions.RestoreMount) error
}

type RuntimeController interface {
	DeleteRuntimeSandbox(ctx context.Context, authCtx *authn.AuthContext, sandboxID string) error
}

type PermissionMiddleware func(permission string) gin.HandlerFunc

type Handler struct {
	repo          *functions.Repository
	cfg           Config
	sandboxLookup SandboxLookup
	volumeStore   RevisionVolumeStore
	runtime       RuntimeController
	logger        *zap.Logger

	revisionPublishLocks sync.Map
}

func New(repo *functions.Repository, cfg Config, lookup SandboxLookup, volumeStore RevisionVolumeStore, runtime RuntimeController, logger *zap.Logger) *Handler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Handler{
		repo:          repo,
		cfg:           cfg,
		sandboxLookup: lookup,
		volumeStore:   volumeStore,
		runtime:       runtime,
		logger:        logger,
	}
}

func (h *Handler) tryLockFunctionRevisionPublish(teamID, functionID string) (func(), bool) {
	key := teamID + ":" + functionID
	newLock := &sync.Mutex{}
	actual, _ := h.revisionPublishLocks.LoadOrStore(key, newLock)
	lock := actual.(*sync.Mutex)
	if !lock.TryLock() {
		return nil, false
	}
	return func() {
		lock.Unlock()
	}, true
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
	group.PUT("/:id", require(authn.PermFunctionWrite), h.updateFunction)
	group.DELETE("/:id", require(authn.PermFunctionDelete), h.deleteFunction)
	group.GET("/:id/aliases", require(authn.PermFunctionRead), h.listFunctionAliases)
	group.GET("/:id/aliases/:alias", require(authn.PermFunctionRead), h.getFunctionAlias)
	group.PUT("/:id/aliases/:alias", require(authn.PermFunctionWrite), h.setFunctionAlias)
	group.GET("/:id/revisions", require(authn.PermFunctionRead), h.listFunctionRevisions)
	group.POST("/:id/revisions", require(authn.PermFunctionWrite), h.createFunctionRevision)
	group.GET("/:id/revisions/:revision_number", require(authn.PermFunctionRead), h.getFunctionRevision)
	group.GET("/:id/runtime", require(authn.PermFunctionRead), h.getFunctionRuntime)
	group.POST("/:id/runtime/restart", require(authn.PermFunctionWrite), h.restartFunctionRuntime)
	group.POST("/:id/runtime/recycle", require(authn.PermFunctionWrite), h.recycleFunctionRuntime)
}

func NewClusterGatewaySandboxLookup(clusterGatewayURL string, internalAuthGen *internalauth.Generator, httpClient *http.Client, logger *zap.Logger) SandboxLookup {
	if logger == nil {
		logger = zap.NewNop()
	}
	httpClient = resolveHTTPClient(httpClient)
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
	Type           functions.RevisionSourceType    `json:"type,omitempty"`
	SandboxID      string                          `json:"sandbox_id,omitempty"`
	ServiceID      string                          `json:"service_id,omitempty"`
	SandboxService *sandboxServiceSourceRequest    `json:"sandbox_service,omitempty"`
	RevisionSpec   *functions.FunctionRevisionSpec `json:"revision_spec,omitempty"`
	Provenance     json.RawMessage                 `json:"provenance,omitempty"`
}

type sandboxServiceSourceRequest struct {
	SandboxID string `json:"sandbox_id"`
	ServiceID string `json:"service_id"`
}

type createFunctionRequest struct {
	Name        string                 `json:"name"`
	Source      functionSourceRequest  `json:"source"`
	Autoscaling *functions.Autoscaling `json:"autoscaling,omitempty"`
}

type createFunctionRevisionRequest struct {
	Source  functionSourceRequest `json:"source"`
	Promote *bool                 `json:"promote,omitempty"`
}

type updateFunctionRequest struct {
	Name        *string                `json:"name,omitempty"`
	Enabled     *bool                  `json:"enabled,omitempty"`
	Autoscaling *functions.Autoscaling `json:"autoscaling,omitempty"`
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

	functionID := uuid.NewString()
	revisionID := uuid.NewString()
	rev, defaultName, cleanupRevision, err := h.prepareRevisionFromSource(c.Request.Context(), authCtx, req.Source, functionruntime.Metadata{
		FunctionID:         functionID,
		FunctionRevisionID: revisionID,
	})
	if err != nil {
		h.writePublishError(c, err)
		return
	}
	cleanupRevisionAfterFailure := true
	if cleanupRevision != nil {
		defer func() {
			if cleanupRevisionAfterFailure {
				cleanupRevision()
			}
		}()
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = defaultName
	}

	userID := principalID(authCtx)
	fn := functions.NewFunction(authCtx.TeamID, name, userID)
	fn.ID = functionID
	if req.Autoscaling != nil {
		fn.Autoscaling = functions.NormalizeAutoscaling(*req.Autoscaling)
	}
	rev.ID = revisionID
	rev.FunctionID = functionID

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
	cleanupRevisionAfterFailure = false

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

func (h *Handler) updateFunction(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req updateFunctionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	fn, err := h.repo.UpdateFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"), req.Name, req.Enabled, req.Autoscaling)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update function")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"function": h.functionRecord(fn)})
}

func (h *Handler) deleteFunction(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	deleted, revisions, err := h.repo.DeleteFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete function")
		return
	}
	go h.cleanupDeletedFunctionResources(authCtx, revisions)
	spec.JSONSuccess(c, http.StatusOK, gin.H{"function": h.functionRecord(deleted), "message": "function deleted"})
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

func (h *Handler) getFunctionRevision(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	revisionNumber, err := strconv.Atoi(c.Param("revision_number"))
	if err != nil || revisionNumber <= 0 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "revision_number must be greater than zero")
		return
	}
	rev, err := h.repo.GetRevisionByNumber(c.Request.Context(), authCtx.TeamID, c.Param("id"), revisionNumber)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function revision not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get revision")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"revision": rev})
}

func (h *Handler) createFunctionRevision(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req createFunctionRevisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	fn, err := h.repo.GetFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get function")
		return
	}
	unlockRevisionPublish, ok := h.tryLockFunctionRevisionPublish(authCtx.TeamID, fn.ID)
	if !ok {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "function revision publish already in progress")
		return
	}
	defer unlockRevisionPublish()

	revisionID := uuid.NewString()
	rev, _, cleanupRevision, err := h.prepareRevisionFromSource(c.Request.Context(), authCtx, req.Source, functionruntime.Metadata{
		FunctionID:         fn.ID,
		FunctionRevisionID: revisionID,
	})
	if err != nil {
		h.writePublishError(c, err)
		return
	}
	cleanupRevisionAfterFailure := true
	if cleanupRevision != nil {
		defer func() {
			if cleanupRevisionAfterFailure {
				cleanupRevision()
			}
		}()
	}

	userID := principalID(authCtx)
	promote := true
	if req.Promote != nil {
		promote = *req.Promote
	}
	rev.ID = revisionID
	rev.FunctionID = fn.ID

	rev, err = h.repo.CreateRevision(c.Request.Context(), authCtx.TeamID, fn.ID, rev, promote, userID)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create revision")
		return
	}
	cleanupRevisionAfterFailure = false
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

func (h *Handler) listFunctionAliases(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	aliases, err := h.repo.ListAliases(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list aliases")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"aliases": aliases})
}

func (h *Handler) getFunctionAlias(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	alias, err := h.repo.GetAlias(c.Request.Context(), authCtx.TeamID, c.Param("id"), c.Param("alias"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function alias not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get alias")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"alias": alias})
}

func (h *Handler) getFunctionRuntime(c *gin.Context) {
	fn, rev, ok := h.loadActiveFunctionRevision(c)
	if !ok {
		return
	}
	instances, err := h.repo.ListRuntimeInstances(c.Request.Context(), fn.TeamID, fn.ID, rev.ID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list function runtime instances")
		return
	}
	events, err := h.repo.ListRuntimeEvents(c.Request.Context(), fn.TeamID, fn.ID, rev.ID, 20)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list function runtime events")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"runtime": runtimeStatus(fn, rev, instances, events)})
}

func (h *Handler) restartFunctionRuntime(c *gin.Context) {
	h.clearFunctionRuntime(c)
}

func (h *Handler) recycleFunctionRuntime(c *gin.Context) {
	h.clearFunctionRuntime(c)
}

func (h *Handler) clearFunctionRuntime(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	fn, rev, ok := h.loadActiveFunctionRevision(c)
	if !ok {
		return
	}
	instances, err := h.repo.ListRuntimeInstances(c.Request.Context(), fn.TeamID, fn.ID, rev.ID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list function runtime instances")
		return
	}
	deletedLegacy := false
	needsRuntimeDelete := false
	for _, inst := range instances {
		if inst == nil || strings.TrimSpace(inst.SandboxID) == "" {
			continue
		}
		needsRuntimeDelete = true
		if h.runtime == nil {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function runtime cleanup is not configured")
			return
		}
		if err := h.runtime.DeleteRuntimeSandbox(c.Request.Context(), authCtx, strings.TrimSpace(inst.SandboxID)); err != nil {
			h.logger.Warn("Failed to delete function runtime sandbox",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("runtime_sandbox_id", strings.TrimSpace(inst.SandboxID)),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "failed to recycle function runtime")
			return
		}
		if rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) == strings.TrimSpace(inst.SandboxID) {
			deletedLegacy = true
		}
	}
	if len(instances) == 0 && h.runtime != nil && rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) != "" {
		needsRuntimeDelete = true
		if err := h.runtime.DeleteRuntimeSandbox(c.Request.Context(), authCtx, strings.TrimSpace(*rev.RuntimeSandboxID)); err != nil {
			h.logger.Warn("Failed to delete function runtime sandbox",
				zap.String("function_id", fn.ID),
				zap.String("revision_id", rev.ID),
				zap.String("runtime_sandbox_id", strings.TrimSpace(*rev.RuntimeSandboxID)),
				zap.Error(err),
			)
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "failed to recycle function runtime")
			return
		}
		deletedLegacy = true
	}
	if !needsRuntimeDelete && h.runtime == nil && rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) != "" {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "function runtime cleanup is not configured")
		return
	}
	if !deletedLegacy && rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) != "" {
		h.logger.Warn("Legacy function runtime mapping did not match runtime pool instances",
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.String("runtime_sandbox_id", strings.TrimSpace(*rev.RuntimeSandboxID)),
		)
	}
	if err := h.repo.ClearRevisionRuntime(c.Request.Context(), fn.TeamID, fn.ID, rev.ID); err != nil && !errors.Is(err, functions.ErrNotFound) {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to clear function runtime")
		return
	}
	if _, err := h.repo.AppendRuntimeEvent(c.Request.Context(), &functions.RuntimeEvent{
		TeamID:         fn.TeamID,
		FunctionID:     fn.ID,
		RevisionID:     rev.ID,
		Phase:          functions.RuntimePhaseIdle,
		ReadinessState: functions.RuntimeReadinessStateUnknown,
		Reason:         "runtime_cleared",
	}); err != nil {
		h.logger.Warn("Failed to append function runtime clear event",
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.Error(err),
		)
	}
	rev.RuntimeSandboxID = nil
	rev.RuntimeContextID = nil
	rev.RuntimeUpdatedAt = nil
	spec.JSONSuccess(c, http.StatusOK, gin.H{"runtime": runtimeStatus(fn, rev, nil, nil)})
}

func (h *Handler) prepareRevisionFromSource(ctx context.Context, authCtx *authn.AuthContext, source functionSourceRequest, storageMetadata functionruntime.Metadata) (*functions.Revision, string, func(), error) {
	sourceType := normalizeFunctionSourceType(source)
	switch sourceType {
	case functions.RevisionSourceTypeSandboxService:
		sandboxSource := normalizeSandboxServiceSource(source)
		sandbox, serviceSnapshot, err := h.loadPublishableService(ctx, authCtx, sandboxSource)
		if err != nil {
			return nil, "", nil, err
		}
		restoreMounts, err := h.prepareRestoreMounts(ctx, authCtx, sandbox, storageMetadata)
		if err != nil {
			return nil, "", nil, err
		}
		cleanup := func() {
			h.deletePreparedRestoreMounts(context.Background(), authCtx, sandbox, restoreMounts, "revision create failed")
		}
		rev, err := functions.NewRevision(authCtx.TeamID, sandbox.ID, serviceSnapshot.ID, sandbox.TemplateID, serviceSnapshot, restoreMounts, principalID(authCtx))
		if err != nil {
			cleanup()
			return nil, "", nil, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: err.Error()}
		}
		return rev, defaultFunctionName(serviceSnapshot), cleanup, nil
	case functions.RevisionSourceTypeRevisionSpec:
		if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
			return nil, "", nil, publishError{status: http.StatusForbidden, code: spec.CodeForbidden, message: "team context is required"}
		}
		if source.RevisionSpec == nil {
			return nil, "", nil, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: "source.revision_spec is required"}
		}
		serviceSnapshot, err := validatePublishableRevisionSpec(*source.RevisionSpec)
		if err != nil {
			return nil, "", nil, err
		}
		provenance := any(functions.RevisionProvenance{Type: functions.RevisionSourceTypeRevisionSpec})
		if len(source.Provenance) > 0 {
			provenance = source.Provenance
		}
		rev, err := functions.NewRevisionFromSpec(authCtx.TeamID, functions.RevisionSourceTypeRevisionSpec, *source.RevisionSpec, provenance, principalID(authCtx))
		if err != nil {
			return nil, "", nil, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: err.Error()}
		}
		return rev, defaultFunctionName(serviceSnapshot), nil, nil
	default:
		return nil, "", nil, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: "source.type is invalid"}
	}
}

func normalizeFunctionSourceType(source functionSourceRequest) functions.RevisionSourceType {
	if source.Type != "" {
		return source.Type
	}
	if source.RevisionSpec != nil {
		return functions.RevisionSourceTypeRevisionSpec
	}
	return functions.RevisionSourceTypeSandboxService
}

func normalizeSandboxServiceSource(source functionSourceRequest) sandboxServiceSourceRequest {
	out := sandboxServiceSourceRequest{
		SandboxID: source.SandboxID,
		ServiceID: source.ServiceID,
	}
	if source.SandboxService != nil {
		if strings.TrimSpace(source.SandboxService.SandboxID) != "" {
			out.SandboxID = source.SandboxService.SandboxID
		}
		if strings.TrimSpace(source.SandboxService.ServiceID) != "" {
			out.ServiceID = source.SandboxService.ServiceID
		}
	}
	return out
}

func validatePublishableRevisionSpec(revisionSpec functions.FunctionRevisionSpec) (mgr.SandboxAppService, error) {
	if err := revisionSpec.Validate(); err != nil {
		return mgr.SandboxAppService{}, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: err.Error()}
	}
	var serviceSnapshot mgr.SandboxAppService
	if err := json.Unmarshal(revisionSpec.RuntimeService, &serviceSnapshot); err != nil {
		return mgr.SandboxAppService{}, publishError{status: http.StatusBadRequest, code: spec.CodeBadRequest, message: "source.revision_spec.runtime_service is invalid"}
	}
	if blockers := mgr.SandboxAppServicePublishBlockers(serviceSnapshot); len(blockers) > 0 {
		return mgr.SandboxAppService{}, publishError{
			status:  http.StatusBadRequest,
			code:    spec.CodeBadRequest,
			message: "revision spec runtime service is not publishable",
			details: gin.H{"blockers": blockers},
		}
	}
	return serviceSnapshot, nil
}

func defaultFunctionName(service mgr.SandboxAppService) string {
	name := strings.TrimSpace(service.DisplayName)
	if name == "" {
		name = strings.TrimSpace(service.ID)
	}
	if name == "" {
		name = "function"
	}
	return name
}

func (h *Handler) loadPublishableService(ctx context.Context, authCtx *authn.AuthContext, source sandboxServiceSourceRequest) (*mgr.Sandbox, mgr.SandboxAppService, error) {
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

func (h *Handler) prepareRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, storageMetadata functionruntime.Metadata) ([]functions.RestoreMount, error) {
	if sandbox == nil || len(sandbox.Mounts) == 0 {
		return nil, nil
	}
	if h.volumeStore == nil {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "function revision volume store is not configured"}
	}
	mounts, err := h.volumeStore.PrepareRestoreMounts(ctx, authCtx, sandbox, storageMetadata)
	if err != nil {
		h.logger.Warn("Failed to prepare function restore mounts",
			zap.String("sandbox_id", sandbox.ID),
			zap.Error(err),
		)
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "failed to prepare function volumes"}
	}
	return mounts, nil
}

func (h *Handler) deletePreparedRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, mounts []functions.RestoreMount, reason string) {
	if h == nil || h.volumeStore == nil || len(mounts) == 0 {
		return
	}
	if err := h.volumeStore.DeleteRestoreMounts(ctx, authCtx, sandbox, mounts); err != nil {
		h.logger.Warn("Failed to clean up prepared function restore mounts",
			zap.String("sandbox_id", sandbox.ID),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func (h *Handler) loadActiveFunctionRevision(c *gin.Context) (*functions.Function, *functions.Revision, bool) {
	authCtx := middleware.GetAuthContext(c)
	fn, err := h.repo.GetFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return nil, nil, false
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get function")
		return nil, nil, false
	}
	rev, err := h.repo.GetActiveRevision(c.Request.Context(), fn)
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function revision not found")
			return nil, nil, false
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get function revision")
		return nil, nil, false
	}
	return fn, rev, true
}

func runtimeStatus(fn *functions.Function, rev *functions.Revision, instances []*functions.RuntimeInstance, events []*functions.RuntimeEvent) functions.RuntimeStatus {
	status := functions.RuntimeStatus{}
	if fn != nil {
		status.FunctionID = fn.ID
		status.Autoscaling = functions.NormalizeAutoscaling(fn.Autoscaling)
	}
	if rev != nil {
		status.RevisionID = rev.ID
		status.RevisionNumber = rev.RevisionNumber
		status.RuntimeSandboxID = rev.RuntimeSandboxID
		status.RuntimeContextID = rev.RuntimeContextID
		status.RuntimeUpdatedAt = rev.RuntimeUpdatedAt
	}
	if len(instances) > 0 {
		status.Instances = make([]functions.RuntimeInstance, 0, len(instances))
		for _, inst := range instances {
			if inst != nil {
				status.Instances = append(status.Instances, *inst)
			}
		}
	}
	if len(events) > 0 {
		status.RecentEvents = make([]functions.RuntimeEvent, 0, len(events))
		for _, event := range events {
			if event != nil {
				status.RecentEvents = append(status.RecentEvents, *event)
			}
		}
	}
	latestEvent := latestRuntimeEvent(events)
	latestFailed := latestRuntimeInstanceByState(instances, functions.RuntimeInstanceStateFailed)
	latestReady := latestRuntimeInstanceByState(instances, functions.RuntimeInstanceStateReady)
	if latestReady != nil {
		status.StartupDurationMS = latestReady.StartupDurationMS
		status.ReadinessState = latestReady.ReadinessState
	}
	if latestFailed != nil {
		status.LastError = latestFailed.LastError
		status.LastErrorAt = latestFailed.LastErrorAt
		if status.LastErrorAt == nil {
			status.LastErrorAt = latestFailed.FailedAt
		}
		if status.StartupDurationMS == nil {
			status.StartupDurationMS = latestFailed.StartupDurationMS
		}
	}
	if latestEvent != nil {
		if status.StartupDurationMS == nil {
			status.StartupDurationMS = latestEvent.StartupDurationMS
		}
		if status.LastError == nil && latestEvent.Phase == functions.RuntimePhaseFailed && strings.TrimSpace(latestEvent.Message) != "" {
			status.LastError = &latestEvent.Message
			status.LastErrorAt = &latestEvent.CreatedAt
		}
	}
	switch {
	case fn != nil && !fn.Enabled:
		status.State = functions.RuntimeStateDisabled
		status.Phase = functions.RuntimePhaseDisabled
		status.ReadinessState = functions.RuntimeReadinessStateUnknown
	case latestReady != nil:
		status.State = functions.RuntimeStateActive
		status.Phase = functions.RuntimePhaseReady
		status.ReadinessState = functions.RuntimeReadinessStateReady
	case hasRuntimeInstanceState(instances, functions.RuntimeInstanceStateStarting):
		status.State = functions.RuntimeStateIdle
		status.Phase = functions.RuntimePhaseStarting
		status.ReadinessState = functions.RuntimeReadinessStateChecking
	case hasRuntimeInstanceState(instances, functions.RuntimeInstanceStateDraining):
		status.State = functions.RuntimeStateIdle
		status.Phase = functions.RuntimePhaseDraining
		status.ReadinessState = functions.RuntimeReadinessStateUnknown
	case latestFailed != nil:
		status.State = functions.RuntimeStateIdle
		status.Phase = functions.RuntimePhaseFailed
		status.ReadinessState = functions.RuntimeReadinessStateFailed
	case runtimeEventIsCurrent(latestEvent):
		status.State = runtimeStateForPhase(latestEvent.Phase)
		status.Phase = latestEvent.Phase
		status.ReadinessState = latestEvent.ReadinessState
	case rev != nil && rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) != "":
		status.State = functions.RuntimeStateActive
		status.Phase = functions.RuntimePhaseReady
		status.ReadinessState = functions.RuntimeReadinessStateUnknown
	default:
		status.State = functions.RuntimeStateIdle
		status.Phase = functions.RuntimePhaseIdle
		status.ReadinessState = functions.RuntimeReadinessStateUnknown
	}
	return status
}

func hasRuntimeInstanceState(instances []*functions.RuntimeInstance, state functions.RuntimeInstanceState) bool {
	for _, inst := range instances {
		if inst != nil && inst.State == state {
			return true
		}
	}
	return false
}

func latestRuntimeInstanceByState(instances []*functions.RuntimeInstance, state functions.RuntimeInstanceState) *functions.RuntimeInstance {
	var latest *functions.RuntimeInstance
	for _, inst := range instances {
		if inst == nil || inst.State != state {
			continue
		}
		if latest == nil || inst.UpdatedAt.After(latest.UpdatedAt) {
			latest = inst
		}
	}
	return latest
}

func latestRuntimeEvent(events []*functions.RuntimeEvent) *functions.RuntimeEvent {
	var latest *functions.RuntimeEvent
	for _, event := range events {
		if event == nil {
			continue
		}
		if latest == nil || event.CreatedAt.After(latest.CreatedAt) {
			latest = event
		}
	}
	return latest
}

func runtimeEventIsCurrent(event *functions.RuntimeEvent) bool {
	if event == nil {
		return false
	}
	switch event.Phase {
	case functions.RuntimePhaseProvisioning, functions.RuntimePhaseStarting:
		return time.Since(event.CreatedAt) <= 5*time.Minute
	case functions.RuntimePhaseFailed, functions.RuntimePhaseIdle:
		return true
	default:
		return false
	}
}

func runtimeStateForPhase(phase functions.RuntimePhase) functions.RuntimeState {
	switch phase {
	case functions.RuntimePhaseDisabled:
		return functions.RuntimeStateDisabled
	case functions.RuntimePhaseReady:
		return functions.RuntimeStateActive
	default:
		return functions.RuntimeStateIdle
	}
}

func (h *Handler) cleanupDeletedFunctionResources(authCtx *authn.AuthContext, revisions []*functions.Revision) {
	if len(revisions) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, rev := range revisions {
		if rev == nil {
			continue
		}
		runtimeSandboxIDs := map[string]struct{}{}
		if h.repo != nil {
			instances, err := h.repo.ListRuntimeInstances(ctx, rev.TeamID, rev.FunctionID, rev.ID)
			if err != nil {
				h.logger.Warn("Failed to list deleted function runtime instances",
					zap.String("function_id", rev.FunctionID),
					zap.String("revision_id", rev.ID),
					zap.Error(err),
				)
			}
			for _, inst := range instances {
				if inst != nil && strings.TrimSpace(inst.SandboxID) != "" {
					runtimeSandboxIDs[strings.TrimSpace(inst.SandboxID)] = struct{}{}
				}
			}
		}
		if rev.RuntimeSandboxID != nil && strings.TrimSpace(*rev.RuntimeSandboxID) != "" {
			runtimeSandboxIDs[strings.TrimSpace(*rev.RuntimeSandboxID)] = struct{}{}
		}
		runtimeCleanupComplete := h.deleteRuntimeSandboxesForRevision(ctx, authCtx, rev, runtimeSandboxIDs)
		if h.repo != nil && runtimeCleanupComplete {
			if err := h.repo.ClearRevisionRuntime(ctx, rev.TeamID, rev.FunctionID, rev.ID); err != nil && !errors.Is(err, functions.ErrNotFound) {
				h.logger.Warn("Failed to clear deleted function runtime mapping",
					zap.String("function_id", rev.FunctionID),
					zap.String("revision_id", rev.ID),
					zap.Error(err),
				)
			}
		}
		if h.volumeStore != nil && len(rev.RestoreMounts) > 0 {
			sandbox := &mgr.Sandbox{ID: rev.SourceSandboxID}
			if err := h.volumeStore.DeleteRestoreMounts(ctx, authCtx, sandbox, rev.RestoreMounts); err != nil {
				h.logger.Warn("Failed to clean up deleted function revision volumes",
					zap.String("function_id", rev.FunctionID),
					zap.String("revision_id", rev.ID),
					zap.Error(err),
				)
			}
		}
	}
}

func (h *Handler) deleteRuntimeSandboxesForRevision(ctx context.Context, authCtx *authn.AuthContext, rev *functions.Revision, sandboxIDs map[string]struct{}) bool {
	if len(sandboxIDs) == 0 {
		return true
	}
	if h.runtime == nil {
		return false
	}
	complete := true
	for sandboxID := range sandboxIDs {
		if err := h.runtime.DeleteRuntimeSandbox(ctx, authCtx, sandboxID); err != nil {
			complete = false
			h.logger.Warn("Failed to clean up deleted function runtime sandbox",
				zap.String("function_id", rev.FunctionID),
				zap.String("revision_id", rev.ID),
				zap.String("runtime_sandbox_id", sandboxID),
				zap.Error(err),
			)
		}
	}
	return complete
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
