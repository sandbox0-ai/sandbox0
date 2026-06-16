package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
)

const sandboxRootFSContainerName = "procd"

const sandboxRootFSOperationTimeout = 5 * time.Minute

func (s *SandboxService) saveSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, tx SandboxStoreTx) error {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil {
		return nil
	}
	if record == nil {
		return nil
	}
	sandboxID := sandboxIDFromPod(pod)
	if sandboxID == "" {
		sandboxID = record.ID
	}
	teamID := record.TeamID
	if sandboxID == "" {
		sandboxID = pod.Name
	}
	if teamID == "" && pod.Annotations != nil {
		teamID = pod.Annotations[controller.AnnotationTeamID]
	}
	if strings.TrimSpace(teamID) == "" {
		return fmt.Errorf("team_id is required to save sandbox rootfs checkpoint")
	}

	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	generation := runtimeGenerationFromPod(pod)
	parentLayerID := ""
	if parentState, err := s.latestRootFSState(ctx, sandboxID); err != nil {
		return fmt.Errorf("load current rootfs head: %w", err)
	} else if parentState != nil {
		parentLayerID = strings.TrimSpace(parentState.LayerID)
	}
	saveReq := ctldapi.SaveRootFSRequest{
		Target:                    rootFSTargetForPod(pod),
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		ExpectedRuntimeGeneration: generation,
		ParentLayerID:             parentLayerID,
	}
	resp, err := s.ctldClient.SaveRootFSWithTimeout(ctx, ctldAddress, saveReq, sandboxRootFSOperationTimeout)
	if err != nil && parentLayerID != "" && rootFSBaselineMissing(err, resp) {
		parentLayerID = ""
		saveReq.ParentLayerID = ""
		resp, err = s.ctldClient.SaveRootFSWithTimeout(ctx, ctldAddress, saveReq, sandboxRootFSOperationTimeout)
	}
	if err != nil {
		return fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, saveRootFSError(resp)))
	}
	state, err := rootFSStateFromSaveResponse(sandboxID, teamID, generation, resp)
	if err != nil {
		return err
	}
	state.LayerID = uuid.NewString()
	state.ParentLayerID = parentLayerID
	if tx != nil {
		return tx.SaveRootFSState(ctx, state)
	}
	if s.sandboxStore != nil {
		return s.sandboxStore.SaveRootFSState(ctx, state)
	}
	return nil
}

func (s *SandboxService) applySandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, state *SandboxRootFSState) error {
	if state == nil {
		return nil
	}
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil {
		return fmt.Errorf("ctld is required to restore sandbox rootfs checkpoint")
	}
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	req := ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTargetForPod(pod),
		ExpectedRuntime:             state.Runtime,
		ExpectedRuntimeHandler:      state.RuntimeHandler,
		ExpectedSnapshotter:         state.Snapshotter,
		ExpectedBaseImageDigest:     state.BaseImageDigest,
		ExpectedSnapshotParent:      state.SnapshotParent,
		ExpectedSnapshotParentChain: append([]string(nil), state.SnapshotParentChain...),
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: state.DiffMediaType,
			Digest:    state.DiffDigest,
			Size:      state.DiffSize,
			ObjectKey: state.DiffObjectKey,
		},
	}
	if layers := rootFSLayerDescriptors(state); len(layers) > 0 {
		req.Layers = layers
		req.BaselineLayerID = state.LayerID
		req.Descriptor = ctldapi.RootFSDiffDescriptor{}
	}
	resp, err := s.ctldClient.ApplyRootFSWithTimeout(ctx, ctldAddress, req, sandboxRootFSOperationTimeout)
	if err != nil {
		return fmt.Errorf("apply sandbox rootfs checkpoint: %w", rootFSResponseError(err, applyRootFSError(resp)))
	}
	if resp == nil || !resp.Applied {
		return fmt.Errorf("apply sandbox rootfs checkpoint: ctld did not report applied")
	}
	return nil
}

func rootFSLayerDescriptors(state *SandboxRootFSState) []ctldapi.RootFSLayerDescriptor {
	if state == nil {
		return nil
	}
	if len(state.LayerChain) > 0 {
		out := make([]ctldapi.RootFSLayerDescriptor, 0, len(state.LayerChain))
		for _, layer := range state.LayerChain {
			if layer == nil || strings.TrimSpace(layer.ID) == "" {
				continue
			}
			out = append(out, ctldapi.RootFSLayerDescriptor{
				LayerID:       layer.ID,
				ParentLayerID: layer.ParentLayerID,
				Descriptor: ctldapi.RootFSDiffDescriptor{
					MediaType: layer.DiffMediaType,
					Digest:    layer.DiffDigest,
					Size:      layer.DiffSize,
					ObjectKey: layer.DiffObjectKey,
				},
			})
		}
		if len(out) > 0 {
			return out
		}
	}
	if strings.TrimSpace(state.LayerID) == "" {
		return nil
	}
	return []ctldapi.RootFSLayerDescriptor{{
		LayerID:       state.LayerID,
		ParentLayerID: state.ParentLayerID,
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: state.DiffMediaType,
			Digest:    state.DiffDigest,
			Size:      state.DiffSize,
			ObjectKey: state.DiffObjectKey,
		},
	}}
}

func (s *SandboxService) latestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, nil
	}
	return s.sandboxStore.GetLatestRootFSState(ctx, sandboxID)
}

func rootFSTargetForPod(pod *corev1.Pod) ctldapi.RootFSContainerRef {
	if pod == nil {
		return ctldapi.RootFSContainerRef{ContainerName: sandboxRootFSContainerName}
	}
	return ctldapi.RootFSContainerRef{
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		PodUID:        string(pod.UID),
		ContainerName: sandboxRootFSContainerName,
	}
}

func rootFSStateFromSaveResponse(sandboxID, teamID string, generation int64, resp *ctldapi.SaveRootFSResponse) (*SandboxRootFSState, error) {
	if resp == nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: empty ctld response")
	}
	if strings.TrimSpace(resp.Descriptor.Digest) == "" {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: diff digest is empty")
	}
	if strings.TrimSpace(resp.Descriptor.ObjectKey) == "" {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: diff object key is empty")
	}
	return &SandboxRootFSState{
		SandboxID:           sandboxID,
		TeamID:              teamID,
		RuntimeGeneration:   generation,
		Runtime:             resp.Info.Runtime,
		RuntimeHandler:      resp.Info.RuntimeHandler,
		BaseImageRef:        resp.Info.BaseImageRef,
		BaseImageDigest:     resp.Info.BaseImageDigest,
		Snapshotter:         resp.Info.Snapshotter,
		SnapshotParent:      resp.Info.SnapshotParent,
		SnapshotParentChain: append([]string(nil), resp.Info.SnapshotParentChain...),
		DiffDigest:          resp.Descriptor.Digest,
		DiffMediaType:       resp.Descriptor.MediaType,
		DiffSize:            resp.Descriptor.Size,
		DiffObjectKey:       resp.Descriptor.ObjectKey,
	}, nil
}

func saveRootFSError(resp *ctldapi.SaveRootFSResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func applyRootFSError(resp *ctldapi.ApplyRootFSResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func rootFSBaselineMissing(err error, resp *ctldapi.SaveRootFSResponse) bool {
	var reqErr *ctldapi.RequestError
	if !errors.As(err, &reqErr) || reqErr == nil || reqErr.StatusCode != http.StatusNotFound {
		return false
	}
	message := strings.ToLower(saveRootFSError(resp))
	return strings.Contains(message, "baseline") && strings.Contains(message, "not captured")
}

func rootFSResponseError(err error, message string) error {
	if err == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}
