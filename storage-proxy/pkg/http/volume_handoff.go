package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const volumeHandoffPollInterval = 100 * time.Millisecond

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

func (s *Server) executePortalBindHandoff(ctx context.Context, volumeRecord *db.SandboxVolume, sourceMount *db.VolumeMount, req volumeportal.PrepareBindRequest) error {
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
	ctldClient := ctldapi.NewClient(s.ctldHTTPClientOrDefault())
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

	prepareResp, err := ctldClient.PrepareVolumePortalHandoff(ctx, sourceURL.String(), ctldapi.PrepareVolumePortalHandoffRequest{
		SandboxVolumeID: volumeRecord.ID,
	})
	if err != nil {
		cleanup()
		if ctldapi.IsConflictError(err) {
			return fmt.Errorf("%w: %v", db.ErrConflict, err)
		}
		return err
	}
	if prepareResp == nil || !prepareResp.Prepared {
		_, _ = ctldClient.AbortVolumePortalHandoff(context.Background(), sourceURL.String(), ctldapi.AbortVolumePortalHandoffRequest{
			SandboxVolumeID: volumeRecord.ID,
		})
		cleanup()
		if prepareResp != nil && strings.TrimSpace(prepareResp.Error) != "" {
			return fmt.Errorf("ctld handoff preparation did not complete: %s", prepareResp.Error)
		}
		return fmt.Errorf("ctld handoff preparation did not complete")
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
		_, _ = ctldClient.AbortVolumePortalHandoff(context.Background(), sourceURL.String(), ctldapi.AbortVolumePortalHandoffRequest{
			SandboxVolumeID: volumeRecord.ID,
		})
		cleanup()
		return err
	}

	_, err = ctldClient.BindVolumePortal(ctx, req.TargetCtldAddr, ctldapi.BindVolumePortalRequest{
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
		_, _ = ctldClient.AbortVolumePortalHandoff(context.Background(), sourceURL.String(), ctldapi.AbortVolumePortalHandoffRequest{
			SandboxVolumeID: volumeRecord.ID,
		})
		cleanup()
		if ctldapi.IsConflictError(err) {
			return fmt.Errorf("%w: %v", db.ErrConflict, err)
		}
		return err
	}

	if _, err := ctldClient.CompleteVolumePortalHandoff(ctx, sourceURL.String(), ctldapi.CompleteVolumePortalHandoffRequest{
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

func (s *Server) ctldHTTPClientOrDefault() *http.Client {
	if s != nil && s.ctldHTTPClient != nil {
		return s.ctldHTTPClient
	}
	return &http.Client{Timeout: ctldapi.DefaultRequestTimeout}
}
