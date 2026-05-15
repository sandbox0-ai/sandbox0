package functionapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type ClusterGatewayURLResolver func(ctx context.Context, sandboxID string) (string, error)

type HTTPVolumeSnapshotter struct {
	resolveClusterGatewayURL ClusterGatewayURLResolver
	internalAuthGen          *internalauth.Generator
	httpClient               *http.Client
	targetService            string
	pathPrefix               string
	logger                   *zap.Logger
}

type volumeSnapshotResponse struct {
	ID       string `json:"id"`
	VolumeID string `json:"volume_id"`
}

type sandboxVolumeResponse struct {
	ID string `json:"id"`
}

func NewHTTPVolumeSnapshotter(resolve ClusterGatewayURLResolver, internalAuthGen *internalauth.Generator, httpClient *http.Client, logger *zap.Logger) *HTTPVolumeSnapshotter {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HTTPVolumeSnapshotter{
		resolveClusterGatewayURL: resolve,
		internalAuthGen:          internalAuthGen,
		httpClient:               httpClient,
		targetService:            internalauth.ServiceClusterGateway,
		pathPrefix:               "/api/v1",
		logger:                   logger,
	}
}

func NewHTTPStorageProxyVolumeSnapshotter(storageProxyURL string, internalAuthGen *internalauth.Generator, httpClient *http.Client, logger *zap.Logger) *HTTPVolumeSnapshotter {
	s := NewHTTPVolumeSnapshotter(StaticClusterGatewayURLResolver(storageProxyURL), internalAuthGen, httpClient, logger)
	s.targetService = internalauth.ServiceStorageProxy
	s.pathPrefix = ""
	return s
}

func StaticClusterGatewayURLResolver(clusterGatewayURL string) ClusterGatewayURLResolver {
	return func(context.Context, string) (string, error) {
		clusterGatewayURL := strings.TrimRight(strings.TrimSpace(clusterGatewayURL), "/")
		if clusterGatewayURL == "" {
			return "", fmt.Errorf("cluster gateway is not configured")
		}
		return clusterGatewayURL, nil
	}
}

func (s *HTTPVolumeSnapshotter) PrepareRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox) ([]functions.RestoreMount, error) {
	if sandbox == nil || len(sandbox.Mounts) == 0 {
		return nil, nil
	}
	if s == nil || s.resolveClusterGatewayURL == nil {
		return nil, fmt.Errorf("volume snapshotter is not configured")
	}
	clusterGatewayURL, err := s.resolveClusterGatewayURL(ctx, sandbox.ID)
	if err != nil {
		return nil, err
	}
	out := make([]functions.RestoreMount, 0, len(sandbox.Mounts))
	for _, mount := range sandbox.Mounts {
		volumeID := strings.TrimSpace(mount.SandboxVolumeID)
		mountPoint := strings.TrimSpace(mount.MountPoint)
		if volumeID == "" || mountPoint == "" {
			continue
		}
		snapshotID, err := s.createSnapshot(ctx, clusterGatewayURL, authCtx, volumeID, sandbox.ID)
		if err != nil {
			_ = s.DeleteRestoreMounts(context.Background(), authCtx, sandbox, out)
			return nil, err
		}
		revisionVolumeID, err := s.createVolumeFromSnapshot(ctx, clusterGatewayURL, authCtx, snapshotID)
		if err != nil {
			_ = s.DeleteRestoreMounts(context.Background(), authCtx, sandbox, out)
			return nil, err
		}
		out = append(out, functions.RestoreMount{
			SandboxVolumeID:       revisionVolumeID,
			SourceSandboxVolumeID: volumeID,
			SnapshotID:            snapshotID,
			MountPoint:            mountPoint,
		})
	}
	return out, nil
}

func (s *HTTPVolumeSnapshotter) DeleteRestoreMounts(ctx context.Context, authCtx *authn.AuthContext, sandbox *mgr.Sandbox, mounts []functions.RestoreMount) error {
	if len(mounts) == 0 {
		return nil
	}
	if s == nil || s.resolveClusterGatewayURL == nil {
		return fmt.Errorf("volume snapshotter is not configured")
	}
	sandboxID := ""
	if sandbox != nil {
		sandboxID = sandbox.ID
	}
	clusterGatewayURL, err := s.resolveClusterGatewayURL(ctx, sandboxID)
	if err != nil {
		return err
	}
	var firstErr error
	for _, mount := range mounts {
		volumeID := strings.TrimSpace(mount.SandboxVolumeID)
		if volumeID == "" {
			continue
		}
		if err := s.deleteVolume(ctx, clusterGatewayURL, authCtx, volumeID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			s.logger.Warn("Function revision volume cleanup failed",
				zap.String("volume_id", volumeID),
				zap.Error(err),
			)
		}
	}
	return firstErr
}

func (s *HTTPVolumeSnapshotter) createSnapshot(ctx context.Context, clusterGatewayURL string, authCtx *authn.AuthContext, volumeID, sandboxID string) (string, error) {
	if s.internalAuthGen == nil {
		return "", fmt.Errorf("internal auth generator is not configured")
	}
	teamID := ""
	userID := ""
	permissions := []string{authn.PermSandboxVolumeCreate, authn.PermSandboxVolumeRead, authn.PermSandboxVolumeWrite}
	if authCtx != nil {
		teamID = authCtx.TeamID
		userID = principalID(authCtx)
	}
	payload, err := json.Marshal(map[string]string{
		"name":        "function-" + strings.TrimSpace(sandboxID),
		"description": "Function revision volume snapshot",
	})
	if err != nil {
		return "", err
	}
	targetService := s.targetService
	if targetService == "" {
		targetService = internalauth.ServiceClusterGateway
	}
	token, err := s.internalAuthGen.Generate(targetService, teamID, userID, internalauth.GenerateOptions{Permissions: permissions})
	if err != nil {
		return "", fmt.Errorf("generate internal token: %w", err)
	}
	pathPrefix := strings.TrimRight(s.pathPrefix, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(clusterGatewayURL, "/")+pathPrefix+"/sandboxvolumes/"+url.PathEscape(volumeID)+"/snapshots", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warn("Function volume snapshot failed",
			zap.String("volume_id", volumeID),
			zap.Int("status", resp.StatusCode),
			zap.String("body", strings.TrimSpace(string(body))),
		)
		return "", fmt.Errorf("snapshot volume %s: %s", volumeID, resp.Status)
	}
	snapshot, apiErr, err := spec.DecodeResponse[volumeSnapshotResponse](resp.Body)
	if err != nil {
		return "", err
	}
	if apiErr != nil {
		return "", fmt.Errorf("snapshot volume %s: %s", volumeID, apiErr.Message)
	}
	if snapshot == nil || strings.TrimSpace(snapshot.ID) == "" {
		return "", fmt.Errorf("snapshot volume %s: empty snapshot id", volumeID)
	}
	return strings.TrimSpace(snapshot.ID), nil
}

func (s *HTTPVolumeSnapshotter) createVolumeFromSnapshot(ctx context.Context, clusterGatewayURL string, authCtx *authn.AuthContext, snapshotID string) (string, error) {
	if s.internalAuthGen == nil {
		return "", fmt.Errorf("internal auth generator is not configured")
	}
	teamID := ""
	userID := ""
	if authCtx != nil {
		teamID = authCtx.TeamID
		userID = principalID(authCtx)
	}
	payload, err := json.Marshal(map[string]string{
		"snapshot_id": strings.TrimSpace(snapshotID),
	})
	if err != nil {
		return "", err
	}
	targetService := s.targetService
	if targetService == "" {
		targetService = internalauth.ServiceClusterGateway
	}
	token, err := s.internalAuthGen.Generate(targetService, teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxVolumeCreate, authn.PermSandboxVolumeRead, authn.PermSandboxVolumeWrite},
	})
	if err != nil {
		return "", fmt.Errorf("generate internal token: %w", err)
	}
	pathPrefix := strings.TrimRight(s.pathPrefix, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(clusterGatewayURL, "/")+pathPrefix+"/sandboxvolumes", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warn("Function revision volume materialization failed",
			zap.String("snapshot_id", snapshotID),
			zap.Int("status", resp.StatusCode),
			zap.String("body", strings.TrimSpace(string(body))),
		)
		return "", fmt.Errorf("create volume from snapshot %s: %s", snapshotID, resp.Status)
	}
	volume, apiErr, err := spec.DecodeResponse[sandboxVolumeResponse](resp.Body)
	if err != nil {
		return "", err
	}
	if apiErr != nil {
		return "", fmt.Errorf("create volume from snapshot %s: %s", snapshotID, apiErr.Message)
	}
	if volume == nil || strings.TrimSpace(volume.ID) == "" {
		return "", fmt.Errorf("create volume from snapshot %s: empty volume id", snapshotID)
	}
	return strings.TrimSpace(volume.ID), nil
}

func (s *HTTPVolumeSnapshotter) deleteVolume(ctx context.Context, clusterGatewayURL string, authCtx *authn.AuthContext, volumeID string) error {
	if s.internalAuthGen == nil {
		return fmt.Errorf("internal auth generator is not configured")
	}
	teamID := ""
	userID := ""
	if authCtx != nil {
		teamID = authCtx.TeamID
		userID = principalID(authCtx)
	}
	targetService := s.targetService
	if targetService == "" {
		targetService = internalauth.ServiceClusterGateway
	}
	token, err := s.internalAuthGen.Generate(targetService, teamID, userID, internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxVolumeDelete, authn.PermSandboxVolumeRead},
	})
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}
	pathPrefix := strings.TrimRight(s.pathPrefix, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(clusterGatewayURL, "/")+pathPrefix+"/sandboxvolumes/"+url.PathEscape(volumeID), nil)
	if err != nil {
		return err
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("delete volume %s: %s: %s", volumeID, resp.Status, strings.TrimSpace(string(body)))
}
