package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
)

const sandboxRootFSContainerName = "procd"

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
	resp, err := s.ctldClient.SaveRootFS(ctx, ctldAddress, ctldapi.SaveRootFSRequest{
		Target:                    rootFSTargetForPod(pod),
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		ExpectedRuntimeGeneration: generation,
		Freeze:                    true,
	})
	if err != nil {
		return fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, saveRootFSError(resp)))
	}
	state, err := rootFSStateFromSaveResponse(sandboxID, teamID, generation, resp)
	if err != nil {
		return err
	}
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
	resp, err := s.ctldClient.ApplyRootFS(ctx, ctldAddress, ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTargetForPod(pod),
		ExpectedBaseImageDigest:     state.BaseImageDigest,
		ExpectedSnapshotParent:      state.SnapshotParent,
		ExpectedSnapshotParentChain: append([]string(nil), state.SnapshotParentChain...),
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: state.DiffMediaType,
			Digest:    state.DiffDigest,
			Size:      state.DiffSize,
			ObjectKey: state.DiffObjectKey,
		},
		Freeze: true,
	})
	if err != nil {
		return fmt.Errorf("apply sandbox rootfs checkpoint: %w", rootFSResponseError(err, applyRootFSError(resp)))
	}
	if resp == nil || !resp.Applied {
		return fmt.Errorf("apply sandbox rootfs checkpoint: ctld did not report applied")
	}
	return nil
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

func rootFSResponseError(err error, message string) error {
	if err == nil {
		return nil
	}
	if strings.TrimSpace(message) == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}
