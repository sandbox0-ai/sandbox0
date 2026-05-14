package http

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

func (s *Server) listFunctions(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	fns, err := s.functionRepo.ListFunctions(c.Request.Context(), authCtx.TeamID)
	if err != nil {
		s.logger.Error("Failed to list functions", zap.String("team_id", authCtx.TeamID), zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list functions")
		return
	}

	records := make([]functionRecord, 0, len(fns))
	for _, fn := range fns {
		records = append(records, s.functionRecord(fn))
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"functions": records})
}

func (s *Server) createFunction(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req createFunctionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	sandbox, serviceSnapshot, err := s.loadPublishableService(c.Request.Context(), authCtx, req.Source)
	if err != nil {
		s.writePublishError(c, err)
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

	fn, rev, err = s.functionRepo.CreateFunctionWithRevision(c.Request.Context(), fn, rev, userID)
	if err != nil {
		if errors.Is(err, functions.ErrAlreadyExists) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "function name already exists")
			return
		}
		s.logger.Error("Failed to create function", zap.String("team_id", authCtx.TeamID), zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create function")
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, gin.H{
		"function": s.functionRecord(fn),
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

func (s *Server) getFunction(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	fn, err := s.functionRepo.GetFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		if errors.Is(err, functions.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get function")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"function": s.functionRecord(fn)})
}

func (s *Server) listFunctionRevisions(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	revisions, err := s.functionRepo.ListRevisions(c.Request.Context(), authCtx.TeamID, c.Param("id"))
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

func (s *Server) createFunctionRevision(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	var req createFunctionRevisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	sandbox, serviceSnapshot, err := s.loadPublishableService(c.Request.Context(), authCtx, req.Source)
	if err != nil {
		s.writePublishError(c, err)
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

	rev, err = s.functionRepo.CreateRevision(c.Request.Context(), authCtx.TeamID, c.Param("id"), rev, promote, userID)
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

func (s *Server) setFunctionAlias(c *gin.Context) {
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

	alias, err := s.functionRepo.SetAlias(c.Request.Context(), authCtx.TeamID, c.Param("id"), c.Param("alias"), req.RevisionNumber, principalID(authCtx))
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

func (s *Server) loadPublishableService(ctx context.Context, authCtx *authn.AuthContext, source functionSourceRequest) (*mgr.Sandbox, mgr.SandboxAppService, error) {
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

	sandbox, err := s.getSandboxFromClusterGateway(ctx, source.SandboxID)
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
	services := sandbox.Services
	if len(services) == 0 && sandbox.PublicGateway != nil {
		if converted, err := mgr.PublicGatewayConfigToSandboxAppServices(sandbox.PublicGateway); err == nil {
			services = converted
		}
	}
	serviceID = strings.TrimSpace(serviceID)
	for _, service := range services {
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

func (s *Server) getSandboxFromClusterGateway(ctx context.Context, sandboxID string) (*mgr.Sandbox, error) {
	clusterGatewayURL := strings.TrimRight(strings.TrimSpace(s.cfg.DefaultClusterGatewayURL), "/")
	if clusterGatewayURL == "" {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "cluster gateway is not configured"}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clusterGatewayURL+"/internal/v1/sandboxes/"+neturl.PathEscape(sandboxID), nil)
	if err != nil {
		return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "failed to create cluster gateway request"}
	}
	token, err := s.internalAuthGen.GenerateSystem(internalauth.ServiceClusterGateway, internalauth.GenerateOptions{})
	if err != nil {
		return nil, publishError{status: http.StatusInternalServerError, code: spec.CodeInternal, message: "failed to generate internal token"}
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)

	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return nil, publishError{status: http.StatusServiceUnavailable, code: spec.CodeUnavailable, message: "cluster gateway unavailable"}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, publishError{status: http.StatusNotFound, code: spec.CodeNotFound, message: "sandbox not found"}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warn("Cluster gateway sandbox lookup failed",
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

func (s *Server) functionRecord(fn *functions.Function) functionRecord {
	regionID := strings.TrimSpace(s.cfg.FunctionRegionID)
	if regionID == "" {
		regionID = strings.TrimSpace(s.cfg.PublicRegionID)
	}
	if regionID == "" {
		regionID = strings.TrimSpace(s.cfg.RegionID)
	}
	host := functionHost(fn.DomainLabel, regionID, s.cfg.FunctionRootDomain)
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

func (s *Server) writePublishError(c *gin.Context, err error) {
	var publishErr publishError
	if errors.As(err, &publishErr) {
		if publishErr.details != nil {
			spec.JSONError(c, publishErr.status, publishErr.code, publishErr.message, publishErr.details)
			return
		}
		spec.JSONError(c, publishErr.status, publishErr.code, publishErr.message)
		return
	}
	s.logger.Error("Function publish failed", zap.Error(err))
	spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to publish function")
}
