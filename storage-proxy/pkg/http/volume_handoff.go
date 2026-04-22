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
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const volumeHandoffPollInterval = 100 * time.Millisecond

type preparePortalBindRequest struct {
	TeamID          string `json:"team_id"`
	UserID          string `json:"user_id"`
	VolumeID        string `json:"volume_id"`
	TargetClusterID string `json:"target_cluster_id"`
	TargetCtldAddr  string `json:"target_ctld_addr"`
	Namespace       string `json:"namespace"`
	PodName         string `json:"pod_name"`
	PodUID          string `json:"pod_uid"`
	PortalName      string `json:"portal_name"`
	MountPath       string `json:"mount_path"`
	SandboxID       string `json:"sandbox_id"`
	OwnerTeamID     string `json:"owner_team_id"`
}

func (s *Server) waitForVolumeHandoff(ctx context.Context, volumeID string) error {
	if s == nil || s.repo == nil || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	for {
		handoff, err := s.repo.GetVolumeHandoff(ctx, volumeID)
		if err == nil && handoff != nil {
			if !handoff.ExpiresAt.IsZero() && time.Now().After(handoff.ExpiresAt) {
				_ = s.repo.DeleteVolumeHandoff(context.Background(), volumeID)
				return nil
			}
			timer := time.NewTimer(volumeHandoffPollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if errors.Is(err, db.ErrNotFound) {
			return nil
		}
		return err
	}
}

func (s *Server) executePortalBindHandoff(ctx context.Context, volumeRecord *db.SandboxVolume, sourceMount *db.VolumeMount, req preparePortalBindRequest) error {
	if s == nil || s.repo == nil || s.podResolver == nil || volumeRecord == nil || sourceMount == nil {
		return fmt.Errorf("volume handoff unavailable")
	}
	sourceURL, err := s.resolveVolumeMountURL(ctx, sourceMount)
	if err != nil {
		return err
	}
	if sourceURL == nil {
		return fmt.Errorf("resolve source ctld owner")
	}
	if sameCTLDAddress(sourceURL.String(), req.TargetCtldAddr) {
		return nil
	}
	targetPodID := strings.TrimSpace(req.Namespace + "/" + req.PodName)
	if req.PodName == "" {
		targetPodID = strings.TrimSpace(req.PodUID)
	}
	expiresAt := time.Now().UTC().Add(30 * time.Second)
	if err := s.repo.UpsertVolumeHandoff(ctx, &db.VolumeHandoff{
		VolumeID:        volumeRecord.ID,
		SourceClusterID: sourceMount.ClusterID,
		SourcePodID:     sourceMount.PodID,
		TargetClusterID: strings.TrimSpace(req.TargetClusterID),
		TargetPodID:     targetPodID,
		TargetCtldAddr:  strings.TrimSpace(req.TargetCtldAddr),
		Status:          "preparing",
		ExpiresAt:       expiresAt,
	}); err != nil {
		return err
	}
	cleanup := func() {
		_ = s.repo.DeleteVolumeHandoff(context.Background(), volumeRecord.ID)
	}

	if _, err := postCtldJSON[ctldapi.PrepareVolumePortalHandoffResponse](ctx, sourceURL.String(), "/api/v1/volume-portals/handoffs/prepare", ctldapi.PrepareVolumePortalHandoffRequest{
		SandboxVolumeID: volumeRecord.ID,
	}); err != nil {
		cleanup()
		return err
	}

	if err := s.repo.UpsertVolumeHandoff(ctx, &db.VolumeHandoff{
		VolumeID:        volumeRecord.ID,
		SourceClusterID: sourceMount.ClusterID,
		SourcePodID:     sourceMount.PodID,
		TargetClusterID: strings.TrimSpace(req.TargetClusterID),
		TargetPodID:     targetPodID,
		TargetCtldAddr:  strings.TrimSpace(req.TargetCtldAddr),
		Status:          "binding",
		ExpiresAt:       expiresAt,
	}); err != nil {
		_, _ = postCtldJSON[ctldapi.AbortVolumePortalHandoffResponse](context.Background(), sourceURL.String(), "/api/v1/volume-portals/handoffs/abort", ctldapi.AbortVolumePortalHandoffRequest{
			SandboxVolumeID: volumeRecord.ID,
		})
		cleanup()
		return err
	}

	_, err = postCtldJSON[ctldapi.BindVolumePortalResponse](ctx, req.TargetCtldAddr, "/api/v1/volume-portals/bind", ctldapi.BindVolumePortalRequest{
		Namespace:               req.Namespace,
		PodName:                 req.PodName,
		PodUID:                  req.PodUID,
		PortalName:              req.PortalName,
		MountPath:               req.MountPath,
		SandboxID:               req.SandboxID,
		TeamID:                  req.OwnerTeamID,
		SandboxVolumeID:         volumeRecord.ID,
		TransferSourceClusterID: sourceMount.ClusterID,
		TransferSourcePodID:     sourceMount.PodID,
	})
	if err != nil {
		_, _ = postCtldJSON[ctldapi.AbortVolumePortalHandoffResponse](context.Background(), sourceURL.String(), "/api/v1/volume-portals/handoffs/abort", ctldapi.AbortVolumePortalHandoffRequest{
			SandboxVolumeID: volumeRecord.ID,
		})
		cleanup()
		return err
	}

	if _, err := postCtldJSON[ctldapi.CompleteVolumePortalHandoffResponse](ctx, sourceURL.String(), "/api/v1/volume-portals/handoffs/complete", ctldapi.CompleteVolumePortalHandoffRequest{
		SandboxVolumeID: volumeRecord.ID,
	}); err != nil {
		cleanup()
		return err
	}
	cleanup()
	return nil
}

func (s *Server) resolveVolumeMountURL(ctx context.Context, mount *db.VolumeMount) (*url.URL, error) {
	if s == nil || s.podResolver == nil || mount == nil || mount.PodID == "" {
		return nil, nil
	}
	targetURL, err := s.podResolver.ResolvePodURL(ctx, mount.PodID)
	if err != nil {
		return nil, err
	}
	if targetURL == nil {
		return nil, nil
	}
	targetURL = cloneURL(targetURL)
	opts := volume.DecodeMountOptions(mount.MountOptions)
	ownerPort := opts.OwnerPort
	if ownerPort == 0 && opts.OwnerKind == volume.OwnerKindCtld {
		ownerPort = 8095
	}
	if ownerPort > 0 {
		targetURL.Host = hostWithPort(targetURL.Host, ownerPort)
	}
	return targetURL, nil
}

func sameCTLDAddress(left, right string) bool {
	return strings.TrimRight(strings.TrimSpace(left), "/") == strings.TrimRight(strings.TrimSpace(right), "/")
}

func postCtldJSON[T any](ctx context.Context, baseURL, path string, request any) (*T, error) {
	var reader io.Reader
	if request != nil {
		payload, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out T
	if len(body) > 0 {
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, err
		}
	}
	if resp.StatusCode != http.StatusOK {
		return &out, fmt.Errorf("ctld request failed with status %d", resp.StatusCode)
	}
	return &out, nil
}
