package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
)

type functionUpdateRequest struct {
	Name    string                         `json:"name,omitempty"`
	Enabled *bool                          `json:"enabled,omitempty"`
	Scale   *functions.FunctionScalePolicy `json:"scale,omitempty"`
}

type activateFunctionRevisionRequest struct {
	RevisionID string `json:"revision_id"`
}

func (s *Server) listFunctions(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	if authCtx == nil || authCtx.TeamID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	items, err := s.functionRepo.ListFunctions(c.Request.Context(), authCtx.TeamID)
	if err != nil {
		s.logger.Error("Failed to list functions", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal server error")
		return
	}
	for i := range items {
		s.decorateFunction(&items[i])
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"functions": items})
}

func (s *Server) getFunction(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	fn, err := s.functionRepo.GetFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		writeFunctionError(c, err)
		return
	}
	s.decorateFunction(fn)
	spec.JSONSuccess(c, http.StatusOK, fn)
}

func (s *Server) updateFunction(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	var req functionUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	fn, err := s.functionRepo.UpdateFunction(c.Request.Context(), authCtx.TeamID, c.Param("id"), req.Name, req.Enabled, req.Scale)
	if err != nil {
		writeFunctionError(c, err)
		return
	}
	s.decorateFunction(fn)
	spec.JSONSuccess(c, http.StatusOK, fn)
}

func (s *Server) deleteFunction(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	if err := s.functionRepo.DeleteFunction(c.Request.Context(), authCtx.TeamID, c.Param("id")); err != nil {
		writeFunctionError(c, err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"deleted": true})
}

func (s *Server) deployFunction(c *gin.Context) {
	s.deployFunctionFromRequest(c, "")
}

func (s *Server) deployFunctionRevision(c *gin.Context) {
	s.deployFunctionFromRequest(c, c.Param("id"))
}

func (s *Server) publishSandboxServiceFunction(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	var req functions.FunctionDeployRequest
	if c.Request.Body != nil {
		body, _ := io.ReadAll(c.Request.Body)
		if len(bytes.TrimSpace(body)) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
				return
			}
		}
	}
	req.Source = functions.FunctionSource{
		Type: functions.RevisionSourceSandboxService,
		SandboxService: &functions.SandboxServiceSource{
			SandboxID: c.Param("id"),
			ServiceID: c.Param("service_id"),
		},
	}
	result, err := s.prepareAndDeployFunction(c.Request.Context(), authCtx, "", req)
	if err != nil {
		s.writeDeployError(c, err)
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, result)
}

func (s *Server) deployFunctionFromRequest(c *gin.Context, functionIDOrSlug string) {
	authCtx := authn.FromContext(c.Request.Context())
	var req functions.FunctionDeployRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	result, err := s.prepareAndDeployFunction(c.Request.Context(), authCtx, functionIDOrSlug, req)
	if err != nil {
		s.writeDeployError(c, err)
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, result)
}

func (s *Server) listFunctionRevisions(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	revisions, err := s.functionRepo.ListRevisions(c.Request.Context(), authCtx.TeamID, c.Param("id"))
	if err != nil {
		writeFunctionError(c, err)
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"revisions": revisions})
}

func (s *Server) activateFunctionRevision(c *gin.Context) {
	authCtx := authn.FromContext(c.Request.Context())
	var req activateFunctionRevisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}
	result, err := s.functionRepo.ActivateRevision(c.Request.Context(), authCtx.TeamID, c.Param("id"), req.RevisionID)
	if err != nil {
		writeFunctionError(c, err)
		return
	}
	s.decorateDeployResult(result)
	spec.JSONSuccess(c, http.StatusOK, result)
}

func (s *Server) prepareAndDeployFunction(ctx context.Context, authCtx *authn.AuthContext, functionIDOrSlug string, req functions.FunctionDeployRequest) (*functions.FunctionDeployResult, error) {
	if authCtx == nil || authCtx.TeamID == "" {
		return nil, errFunctionUnauthorized
	}
	if strings.TrimSpace(functionIDOrSlug) != "" {
		existing, err := s.functionRepo.GetFunction(ctx, authCtx.TeamID, functionIDOrSlug)
		if err != nil {
			return nil, err
		}
		if req.Name == "" {
			req.Name = existing.Name
		}
		if req.Slug == "" {
			req.Slug = existing.Slug
		}
	}
	activate := true
	if req.Activate != nil {
		activate = *req.Activate
	}
	specValue, source, err := s.compileFunctionRevision(ctx, authCtx, req)
	if err != nil {
		return nil, err
	}
	result, err := s.functionRepo.DeployRevision(ctx, functions.DeployInput{
		TeamID:   authCtx.TeamID,
		UserID:   authCtx.UserID,
		Name:     req.Name,
		Slug:     req.Slug,
		Scale:    req.Scale,
		Source:   source,
		Spec:     specValue,
		Activate: activate,
	})
	if err != nil {
		return nil, err
	}
	s.decorateDeployResult(result)
	return result, nil
}

func (s *Server) compileFunctionRevision(ctx context.Context, authCtx *authn.AuthContext, req functions.FunctionDeployRequest) (functions.FunctionRevisionSpec, functions.FunctionSource, error) {
	switch strings.TrimSpace(req.Source.Type) {
	case functions.RevisionSourceSandboxService:
		if req.Source.SandboxService == nil {
			return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("source.sandbox_service is required")
		}
		specValue, source, err := s.compileSandboxServiceRevision(ctx, authCtx, *req.Source.SandboxService, req.Slug)
		return specValue, source, err
	case "", functions.RevisionSourceSnapshot:
		if req.Spec == nil {
			return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("spec is required")
		}
		specValue, err := functions.NormalizeRevisionSpec(*req.Spec)
		if err != nil {
			return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, err
		}
		snapshotIDs := make([]string, 0, len(specValue.Mounts))
		for _, mount := range specValue.Mounts {
			snapshotIDs = append(snapshotIDs, mount.SnapshotID)
		}
		return specValue, functions.FunctionSource{
			Type:     functions.RevisionSourceSnapshot,
			Snapshot: &functions.SnapshotRevisionSource{SnapshotIDs: snapshotIDs},
		}, nil
	default:
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("unsupported source type %q", req.Source.Type)
	}
}

func (s *Server) compileSandboxServiceRevision(ctx context.Context, authCtx *authn.AuthContext, source functions.SandboxServiceSource, slug string) (functions.FunctionRevisionSpec, functions.FunctionSource, error) {
	sandboxID := strings.TrimSpace(source.SandboxID)
	serviceID := strings.TrimSpace(source.ServiceID)
	if sandboxID == "" || serviceID == "" {
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("sandbox_id and service_id are required")
	}
	sandbox, err := s.getFunctionSourceSandbox(ctx, authCtx, sandboxID)
	if err != nil {
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, err
	}
	if sandbox.TeamID != authCtx.TeamID {
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("sandbox belongs to a different team")
	}
	var selected *service.SandboxAppService
	for i := range sandbox.Services {
		if sandbox.Services[i].ID == serviceID {
			selected = &sandbox.Services[i]
			break
		}
	}
	if selected == nil {
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("sandbox service %q not found", serviceID)
	}
	if blockers := service.SandboxAppServicePublishBlockers(*selected); len(blockers) > 0 {
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, fmt.Errorf("service is not publishable: %s", strings.Join(blockers, ", "))
	}
	mounts := make([]functions.FunctionRevisionMount, 0, len(sandbox.Mounts))
	for _, mount := range sandbox.Mounts {
		snapshot, err := s.createFunctionSourceSnapshot(ctx, authCtx, mount.SandboxVolumeID, snapshotNameForFunction(slug, serviceID, mount.MountPoint))
		if err != nil {
			return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, err
		}
		mounts = append(mounts, functions.FunctionRevisionMount{
			SnapshotID: snapshot.Id,
			MountPath:  mount.MountPoint,
			ReadOnly:   true,
		})
	}
	specValue, err := functions.NormalizeRevisionSpec(functions.FunctionRevisionSpec{
		Template: sandbox.TemplateID,
		Service:  *selected,
		Mounts:   mounts,
	})
	if err != nil {
		return functions.FunctionRevisionSpec{}, functions.FunctionSource{}, err
	}
	return specValue, functions.FunctionSource{
		Type: functions.RevisionSourceSandboxService,
		SandboxService: &functions.SandboxServiceSource{
			SandboxID: sandboxID,
			ServiceID: serviceID,
		},
	}, nil
}

func (s *Server) getFunctionSourceSandbox(ctx context.Context, authCtx *authn.AuthContext, sandboxID string) (*service.Sandbox, error) {
	clusterURL, err := s.clusterGatewayURLForSandbox(ctx, sandboxID, authCtx)
	if err != nil {
		return nil, err
	}
	var sandbox service.Sandbox
	if err := s.doFunctionJSON(ctx, clusterURL, internalauth.ServiceClusterGateway, http.MethodGet, "/api/v1/sandboxes/"+url.PathEscape(sandboxID), authCtx.TeamID, authCtx.UserID, authCtx.Permissions, nil, &sandbox); err != nil {
		return nil, err
	}
	return &sandbox, nil
}

func (s *Server) createFunctionSourceSnapshot(ctx context.Context, authCtx *authn.AuthContext, volumeID, name string) (*apispec.Snapshot, error) {
	var snapshot apispec.Snapshot
	body := map[string]any{"name": name}
	path := "/api/v1/sandboxvolumes/" + url.PathEscape(volumeID) + "/snapshots"
	if err := s.doFunctionJSON(ctx, s.cfg.DefaultClusterGatewayURL, internalauth.ServiceClusterGateway, http.MethodPost, path, authCtx.TeamID, authCtx.UserID, append(authCtx.Permissions, authn.PermSandboxVolumeWrite), body, &snapshot); err != nil {
		return nil, fmt.Errorf("snapshot volume %s: %w", volumeID, err)
	}
	return &snapshot, nil
}

func (s *Server) materializeFunctionVolume(ctx context.Context, teamID, userID string, snapshotID string) (string, error) {
	if userID == "" {
		userID = teamID
	}
	var volume apispec.SandboxVolume
	body := map[string]any{
		"snapshot_id": snapshotID,
		"access_mode": "ROX",
	}
	if err := s.doFunctionJSON(ctx, s.cfg.DefaultClusterGatewayURL, internalauth.ServiceClusterGateway, http.MethodPost, "/api/v1/sandboxvolumes", teamID, userID, []string{authn.PermSandboxVolumeCreate}, body, &volume); err != nil {
		return "", fmt.Errorf("materialize snapshot %s: %w", snapshotID, err)
	}
	return volume.Id, nil
}

func (s *Server) doFunctionJSON(ctx context.Context, baseURL, target, method, path, teamID, userID string, permissions []string, body any, out any) error {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return fmt.Errorf("upstream URL is not configured")
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	token, err := s.internalAuthGen.Generate(target, teamID, userID, internalauth.GenerateOptions{Permissions: permissions})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Team-ID", teamID)
	if userID != "" {
		req.Header.Set("X-User-ID", userID)
	}
	resp, err := s.outboundHTTPClient().Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		if msg, ok := spec.DecodeErrorMessage(data); ok {
			return fmt.Errorf("%s", msg)
		}
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	data, apiErr, err := spec.DecodeResponse[json.RawMessage](resp.Body)
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if apiErr != nil {
		return fmt.Errorf("%s", apiErr.Message)
	}
	if data == nil || len(*data) == 0 {
		return nil
	}
	if err := json.Unmarshal(*data, out); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	return nil
}

func (s *Server) clusterGatewayURLForSandbox(ctx context.Context, sandboxID string, authCtx *authn.AuthContext) (string, error) {
	if s.schedulerRouter == nil {
		return s.cfg.DefaultClusterGatewayURL, nil
	}
	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		return "", fmt.Errorf("invalid sandbox_id")
	}
	clusterURL, err := s.getClusterGatewayURLForCluster(ctx, parsed.ClusterID, authCtx)
	if err != nil {
		return "", err
	}
	if clusterURL == "" {
		return "", fmt.Errorf("cluster not found")
	}
	return clusterURL, nil
}

func (s *Server) decorateDeployResult(result *functions.FunctionDeployResult) {
	if result == nil {
		return
	}
	s.decorateFunction(&result.Function)
}

func (s *Server) decorateFunction(fn *functions.Function) {
	if fn == nil {
		return
	}
	fn.URL = functions.PublicURL(fn.DomainLabel, s.cfg.PublicRegionID, s.cfg.PublicRootDomain)
}

func snapshotNameForFunction(slug, serviceID, mountPath string) string {
	slug, _ = functions.NormalizeSlug(slug)
	if slug == "" {
		slug = "function"
	}
	mountPath = strings.NewReplacer("/", "-", "_", "-").Replace(strings.Trim(mountPath, "/"))
	mountPath = strings.Trim(mountPath, "-")
	if mountPath == "" {
		mountPath = "root"
	}
	name := slug + "-" + serviceID + "-" + mountPath + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	if len(name) > 63 {
		return name[:63]
	}
	return name
}

var errFunctionUnauthorized = errors.New("missing authentication")

func (s *Server) writeDeployError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errFunctionUnauthorized):
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, err.Error())
	case errors.Is(err, functions.ErrNotFound):
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, err.Error())
	default:
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
	}
}

func writeFunctionError(c *gin.Context, err error) {
	if errors.Is(err, functions.ErrNotFound) {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "function not found")
		return
	}
	spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, err.Error())
}
