package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

const sandboxRootFSContainerName = "procd"

const sandboxRootFSOperationTimeout = 5 * time.Minute
const sandboxRootFSSourceCheckpointLifecycleStaleAfter = sandboxRootFSOperationTimeout + time.Minute
const sandboxRootFSUncommittedObjectDeleteDelay = 15 * time.Minute
const sandboxRootFSUncommittedObjectDeleteTimeout = 30 * time.Second

func (s *SandboxService) saveSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *SandboxRecord, tx SandboxStoreTx) error {
	state, err := s.prepareSandboxRootFSCheckpoint(ctx, pod, record)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if tx != nil {
		return tx.SaveRootFSState(ctx, state)
	}
	if s.sandboxStore != nil {
		return s.sandboxStore.SaveRootFSState(ctx, state)
	}
	return nil
}

func (s *SandboxService) prepareSandboxRootFSCheckpoint(ctx context.Context, pod *corev1.Pod, record *SandboxRecord) (*SandboxRootFSState, error) {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil {
		return nil, nil
	}
	if record == nil {
		return nil, nil
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
		return nil, fmt.Errorf("team_id is required to save sandbox rootfs checkpoint")
	}

	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return nil, err
	}
	generation := runtimeGenerationFromPod(pod)
	layerID := uuid.NewString()
	parentLayerID := ""
	expectedHeadLayerID := ""
	var parentState *SandboxRootFSState
	if parentState, err = s.latestRootFSState(ctx, sandboxID); err != nil {
		return nil, fmt.Errorf("load current rootfs head: %w", err)
	} else if parentState != nil {
		expectedHeadLayerID = strings.TrimSpace(parentState.LayerID)
	}
	filesystemID, err := s.rootFSFilesystemID(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("load rootfs filesystem: %w", err)
	}
	var parent *rootfshead.HeadReference
	if parentState != nil {
		reference, err := optionalRootFSHeadReference(parentState)
		if err != nil {
			return nil, fmt.Errorf("load parent rootfs head: %w", err)
		}
		parent = reference
		if parent != nil {
			parentLayerID = expectedHeadLayerID
		}
	}
	prepareReq := ctldapi.PrepareRootFSSnapshotRequest{
		Target:        rootFSTargetForPod(pod),
		HeadID:        layerID,
		SandboxID:     sandboxID,
		TeamID:        teamID,
		FilesystemID:  filesystemID,
		Parent:        parent,
		ExcludedPaths: rootFSExcludedPathsForPod(pod),
	}
	resp, err := s.prepareAndPublishSandboxRootFSSnapshot(ctx, ctldAddress, prepareReq, sandboxID, teamID, generation, layerID)
	if err != nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: %w", rootFSResponseError(err, saveRootFSError(resp)))
	}
	state, err := rootFSStateFromSaveResponse(sandboxID, teamID, generation, resp)
	if err != nil {
		return nil, err
	}
	inheritCanonicalRootFSBase(state, parentState)
	state.LayerID = layerID
	state.ParentLayerID = parentLayerID
	state.ExpectedHeadLayerID = expectedHeadLayerID
	platform := s.rootFSPlatformForPod(pod)
	if platform.OS == "" {
		platform.OS = state.PlatformOS
	}
	if platform.Architecture == "" {
		platform.Architecture = state.PlatformArchitecture
	}
	if platform.Variant == "" {
		platform.Variant = state.PlatformVariant
	}
	if platform.OS == "" && parentState != nil {
		platform.OS = parentState.PlatformOS
	}
	if platform.Architecture == "" && parentState != nil {
		platform.Architecture = parentState.PlatformArchitecture
	}
	if platform.Variant == "" && parentState != nil {
		platform.Variant = parentState.PlatformVariant
	}
	state.PlatformOS = platform.OS
	state.PlatformArchitecture = platform.Architecture
	state.PlatformVariant = platform.Variant
	state.LayerChain = appendRootFSCheckpointLayer(parentState, state)
	if err := validatePersistedRootFSHeadImage(state); err != nil {
		s.deleteUncommittedRootFSObject(state, "rootfs head image validation failed")
		return nil, err
	}
	return state, nil
}

// inheritCanonicalRootFSBase prevents metadata heads from recursively using a
// previous metadata head as their OCI base. Every generation remains the
// original digest-pinned template image plus one metadata marker layer.
func inheritCanonicalRootFSBase(state, parent *SandboxRootFSState) {
	if state == nil || parent == nil {
		return
	}
	if value := strings.TrimSpace(parent.BaseImageRef); value != "" {
		state.BaseImageRef = value
	}
	if value := strings.TrimSpace(parent.BaseImageDigest); value != "" {
		state.BaseImageDigest = value
	}
}

func appendRootFSCheckpointLayer(parent, state *SandboxRootFSState) []*SandboxRootFSLayer {
	chain := make([]*SandboxRootFSLayer, 0, 1)
	if parent != nil && state != nil && strings.TrimSpace(state.ParentLayerID) != "" {
		chain = cloneSandboxRootFSLayers(parent.LayerChain)
		if len(chain) == 0 && strings.TrimSpace(parent.LayerID) != "" {
			chain = append(chain, rootFSLayerFromState(parent))
		}
	}
	chain = append(chain, rootFSLayerFromState(state))
	return chain
}

func rootFSLayerFromState(state *SandboxRootFSState) *SandboxRootFSLayer {
	if state == nil {
		return nil
	}
	return &SandboxRootFSLayer{
		ID:                   state.LayerID,
		ParentLayerID:        state.ParentLayerID,
		SourceSandboxID:      state.SandboxID,
		TeamID:               state.TeamID,
		RuntimeGeneration:    state.RuntimeGeneration,
		Runtime:              state.Runtime,
		RuntimeHandler:       state.RuntimeHandler,
		BaseImageRef:         state.BaseImageRef,
		BaseImageDigest:      state.BaseImageDigest,
		PlatformOS:           state.PlatformOS,
		PlatformArchitecture: state.PlatformArchitecture,
		PlatformVariant:      state.PlatformVariant,
		Snapshotter:          state.Snapshotter,
		SnapshotParent:       state.SnapshotParent,
		SnapshotParentChain:  append([]string(nil), state.SnapshotParentChain...),
		DiffDigest:           state.DiffDigest,
		DiffID:               state.DiffID,
		DiffMediaType:        state.DiffMediaType,
		DiffSize:             state.DiffSize,
		DiffObjectKey:        state.DiffObjectKey,
		HeadObjectDigest:     state.HeadObjectDigest,
		HeadObjectMediaType:  state.HeadObjectMediaType,
		HeadObjectSize:       state.HeadObjectSize,
		HeadObjectKey:        state.HeadObjectKey,
		HeadImageRef:         state.HeadImageRef,
		HeadImageDigest:      state.HeadImageDigest,
		CreatedAt:            state.CreatedAt,
	}
}

func validatePersistedRootFSHeadImage(state *SandboxRootFSState) error {
	if state == nil {
		return nil
	}
	if _, err := rootFSHeadReferenceFromState(state); err != nil {
		return err
	}
	image := rootfshead.ImageReference{
		Name:           strings.TrimSpace(state.HeadImageRef),
		ManifestDigest: strings.TrimSpace(state.HeadImageDigest),
		Platform:       rootFSPlatformFromState(state),
	}
	if err := image.Validate(); err != nil {
		return fmt.Errorf("validate node-local rootfs head image: %w", err)
	}
	return nil
}

func rootFSPlatformFromState(state *SandboxRootFSState) ocispec.Platform {
	if state == nil {
		return ocispec.Platform{}
	}
	return ocispec.Platform{OS: state.PlatformOS, Architecture: state.PlatformArchitecture, Variant: state.PlatformVariant}
}

func rootFSHeadReferenceFromState(state *SandboxRootFSState) (rootfshead.HeadReference, error) {
	if state == nil {
		return rootfshead.HeadReference{}, fmt.Errorf("rootfs state is required")
	}
	reference := rootfshead.HeadReference{
		Version: rootfshead.Version,
		HeadID:  state.LayerID,
		Manifest: rootfshead.Object{
			Key:       state.HeadObjectKey,
			Digest:    state.HeadObjectDigest,
			Size:      state.HeadObjectSize,
			MediaType: state.HeadObjectMediaType,
		},
	}
	if err := reference.Validate(); err != nil {
		return rootfshead.HeadReference{}, err
	}
	return reference, nil
}

func optionalRootFSHeadReference(state *SandboxRootFSState) (*rootfshead.HeadReference, error) {
	if state == nil {
		return nil, nil
	}
	fields := []string{
		strings.TrimSpace(state.HeadObjectKey),
		strings.TrimSpace(state.HeadObjectDigest),
		strings.TrimSpace(state.HeadObjectMediaType),
	}
	if fields[0] == "" && fields[1] == "" && fields[2] == "" && state.HeadObjectSize == 0 {
		return nil, nil
	}
	reference, err := rootFSHeadReferenceFromState(state)
	if err != nil {
		return nil, err
	}
	return &reference, nil
}

func rootFSHeadMarkerObjectFromState(state *SandboxRootFSState) (rootfshead.Object, error) {
	reference, err := rootFSHeadReferenceFromState(state)
	if err != nil {
		return rootfshead.Object{}, err
	}
	object, _, err := rootfshead.MarkerObject(reference)
	return object, err
}

const rootFSPlatformVariantLabel = "sandbox0.ai/platform-variant"

// rootFSPlatformForPod captures the platform of the node that actually ran the
// sandbox. It deliberately does not fall back to manager's own GOOS/GOARCH.
func (s *SandboxService) rootFSPlatformForPod(pod *corev1.Pod) ocispec.Platform {
	if pod == nil {
		return ocispec.Platform{}
	}
	platform := ocispec.Platform{
		OS:           strings.TrimSpace(pod.Spec.NodeSelector[corev1.LabelOSStable]),
		Architecture: strings.TrimSpace(pod.Spec.NodeSelector[corev1.LabelArchStable]),
		Variant:      strings.TrimSpace(pod.Spec.NodeSelector[rootFSPlatformVariantLabel]),
	}
	if s == nil || s.nodeLister == nil || strings.TrimSpace(pod.Spec.NodeName) == "" {
		return platform
	}
	node, err := s.nodeLister.Get(pod.Spec.NodeName)
	if err != nil || node == nil {
		return platform
	}
	if value := strings.TrimSpace(node.Labels[corev1.LabelOSStable]); value != "" {
		platform.OS = value
	} else if value := strings.TrimSpace(node.Status.NodeInfo.OperatingSystem); value != "" {
		platform.OS = value
	}
	if value := strings.TrimSpace(node.Labels[corev1.LabelArchStable]); value != "" {
		platform.Architecture = value
	} else if value := strings.TrimSpace(node.Status.NodeInfo.Architecture); value != "" {
		platform.Architecture = value
	}
	if value := strings.TrimSpace(node.Labels[rootFSPlatformVariantLabel]); value != "" {
		platform.Variant = value
	}
	return platform
}

func (s *SandboxService) prepareAndPublishSandboxRootFSSnapshot(ctx context.Context, ctldAddress string, prepareReq ctldapi.PrepareRootFSSnapshotRequest, sandboxID, teamID string, generation int64, layerID string) (*ctldapi.SaveRootFSResponse, error) {
	prepared, err := s.ctldClient.PrepareRootFSSnapshotWithTimeout(ctx, ctldAddress, prepareReq, sandboxRootFSOperationTimeout)
	if err != nil {
		resp := &ctldapi.SaveRootFSResponse{}
		if prepared != nil {
			resp.Info = prepared.Info
			resp.Checkpoint = prepared.Checkpoint
			resp.Error = prepared.Error
		}
		return resp, err
	}
	if prepared == nil || prepared.Checkpoint.Reference.HeadID != layerID {
		actualHeadID := ""
		if prepared != nil {
			actualHeadID = prepared.Checkpoint.Reference.HeadID
			_, _ = s.ctldClient.AbortRootFSSnapshotWithTimeout(context.Background(), ctldAddress, ctldapi.AbortRootFSSnapshotRequest{Handle: prepared.Handle}, sandboxRootFSOperationTimeout)
		}
		return &ctldapi.SaveRootFSResponse{}, fmt.Errorf("ctld sealed unexpected rootfs head %q", actualHeadID)
	}
	pendingState := rootFSStateFromPreparedSnapshot(sandboxID, teamID, generation, layerID, prepared)
	if err := s.queueUncommittedRootFSObjectDeletion(ctx, pendingState, s.now().Add(sandboxRootFSUncommittedObjectDeleteDelay)); err != nil {
		_, _ = s.ctldClient.AbortRootFSSnapshotWithTimeout(context.Background(), ctldAddress, ctldapi.AbortRootFSSnapshotRequest{Handle: prepared.Handle}, sandboxRootFSOperationTimeout)
		return &ctldapi.SaveRootFSResponse{Info: prepared.Info, Checkpoint: prepared.Checkpoint}, err
	}
	published, err := s.ctldClient.PublishRootFSSnapshotWithTimeout(ctx, ctldAddress, ctldapi.PublishRootFSSnapshotRequest{
		Handle: prepared.Handle,
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		_, _ = s.ctldClient.AbortRootFSSnapshotWithTimeout(context.Background(), ctldAddress, ctldapi.AbortRootFSSnapshotRequest{Handle: prepared.Handle}, sandboxRootFSOperationTimeout)
		s.deleteUncommittedRootFSObject(pendingState, "rootfs snapshot publish failed")
		resp := &ctldapi.SaveRootFSResponse{Info: prepared.Info, Checkpoint: prepared.Checkpoint}
		if published != nil {
			resp.Info = published.Info
			resp.Checkpoint = published.Checkpoint
			resp.Error = published.Error
		}
		return resp, err
	}
	return &ctldapi.SaveRootFSResponse{
		Info:       published.Info,
		Checkpoint: published.Checkpoint,
		Error:      published.Error,
	}, nil
}

func rootFSExcludedPathsForPod(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	var mounts []ClaimMount
	if pod.Annotations != nil {
		mounts = parseClaimMounts(pod.Annotations[controller.AnnotationMounts])
	}
	seen := make(map[string]struct{}, len(mounts)+1)
	out := make([]string, 0, len(mounts)+1)
	add := func(raw string) {
		if raw == "" || !strings.HasPrefix(raw, "/") {
			return
		}
		mountPath := path.Clean(raw)
		if mountPath == "/" {
			return
		}
		if _, ok := seen[mountPath]; ok {
			return
		}
		seen[mountPath] = struct{}{}
		out = append(out, mountPath)
	}
	for _, mount := range mounts {
		add(strings.TrimSpace(mount.MountPoint))
	}
	if pod.Annotations != nil && strings.TrimSpace(pod.Annotations[controller.AnnotationWebhookStateVolumeID]) != "" {
		add(webhookStateMountPoint)
	}
	return out
}

func (s *SandboxService) latestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, nil
	}
	return s.sandboxStore.GetLatestRootFSState(ctx, sandboxID)
}

func (s *SandboxService) rootFSFilesystemID(ctx context.Context, sandboxID string) (string, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	reader, ok := s.sandboxStore.(interface {
		GetRootFSFilesystem(context.Context, string) (*RootFSFilesystem, error)
	})
	if !ok || reader == nil {
		return sandboxID, nil
	}
	filesystem, err := reader.GetRootFSFilesystem(ctx, sandboxID)
	if err != nil {
		return "", err
	}
	if filesystem == nil || strings.TrimSpace(filesystem.ID) == "" {
		return sandboxID, nil
	}
	return strings.TrimSpace(filesystem.ID), nil
}

func (s *SandboxService) bindSandboxRootFSSync(ctx context.Context, pod *corev1.Pod, req *ClaimRequest) error {
	if s == nil || !s.config.CtldEnabled || s.ctldClient == nil || pod == nil || req == nil {
		return nil
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		sandboxID = sandboxIDFromPod(pod)
	}
	filesystemID, err := s.rootFSFilesystemID(ctx, sandboxID)
	if err != nil {
		return err
	}
	state, err := s.latestRootFSState(ctx, sandboxID)
	if err != nil {
		return err
	}
	var parent *rootfshead.HeadReference
	if state != nil {
		reference, err := optionalRootFSHeadReference(state)
		if err != nil {
			return err
		}
		parent = reference
	}
	ctldAddress, err := s.ctldAddressForPod(ctx, pod)
	if err != nil {
		return err
	}
	response, err := s.ctldClient.BindRootFSSyncWithTimeout(ctx, ctldAddress, ctldapi.BindRootFSSyncRequest{
		Target:        rootFSTargetForPod(pod),
		SandboxID:     sandboxID,
		TeamID:        req.TeamID,
		FilesystemID:  filesystemID,
		Parent:        parent,
		ExcludedPaths: rootFSExcludedPathsForPod(pod),
	}, 30*time.Second)
	if err != nil {
		return rootFSResponseError(err, bindRootFSSyncError(response))
	}
	if response == nil || !response.Bound {
		return fmt.Errorf("ctld did not bind rootfs sync")
	}
	return nil
}

func bindRootFSSyncError(response *ctldapi.BindRootFSSyncResponse) string {
	if response == nil {
		return ""
	}
	return strings.TrimSpace(response.Error)
}

func rootFSTargetForPod(pod *corev1.Pod) ctldapi.RootFSContainerRef {
	if pod == nil {
		return ctldapi.RootFSContainerRef{ContainerName: sandboxRootFSContainerName}
	}
	containerID := ""
	if status := procdContainerStatus(pod); status != nil {
		containerID = status.ContainerID
	}
	return ctldapi.RootFSContainerRef{
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		PodUID:        string(pod.UID),
		ContainerName: sandboxRootFSContainerName,
		ContainerID:   containerID,
	}
}

func rootFSStateFromSaveResponse(sandboxID, teamID string, generation int64, resp *ctldapi.SaveRootFSResponse) (*SandboxRootFSState, error) {
	if resp == nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: empty ctld response")
	}
	if err := resp.Checkpoint.Reference.Validate(); err != nil {
		return nil, fmt.Errorf("save sandbox rootfs checkpoint: %w", err)
	}
	manifest := resp.Checkpoint.Reference.Manifest
	return &SandboxRootFSState{
		SandboxID:            sandboxID,
		TeamID:               teamID,
		RuntimeGeneration:    generation,
		Runtime:              resp.Info.Runtime,
		RuntimeHandler:       resp.Info.RuntimeHandler,
		BaseImageRef:         resp.Info.BaseImageRef,
		BaseImageDigest:      resp.Info.BaseImageDigest,
		PlatformOS:           resp.Checkpoint.Image.Platform.OS,
		PlatformArchitecture: resp.Checkpoint.Image.Platform.Architecture,
		PlatformVariant:      resp.Checkpoint.Image.Platform.Variant,
		Snapshotter:          resp.Info.Snapshotter,
		SnapshotParent:       resp.Info.SnapshotParent,
		SnapshotParentChain:  append([]string(nil), resp.Info.SnapshotParentChain...),
		HeadObjectDigest:     manifest.Digest,
		HeadObjectMediaType:  manifest.MediaType,
		HeadObjectSize:       manifest.Size,
		HeadObjectKey:        manifest.Key,
		HeadImageRef:         resp.Checkpoint.Image.Name,
		HeadImageDigest:      resp.Checkpoint.Image.ManifestDigest,
		Objects:              append([]rootfshead.Object(nil), resp.Checkpoint.Objects...),
	}, nil
}

func rootFSStateFromPreparedSnapshot(sandboxID, teamID string, generation int64, layerID string, prepared *ctldapi.PrepareRootFSSnapshotResponse) *SandboxRootFSState {
	if prepared == nil {
		return nil
	}
	manifest := prepared.Checkpoint.Reference.Manifest
	return &SandboxRootFSState{
		LayerID:              layerID,
		SandboxID:            sandboxID,
		TeamID:               teamID,
		RuntimeGeneration:    generation,
		Runtime:              prepared.Info.Runtime,
		RuntimeHandler:       prepared.Info.RuntimeHandler,
		BaseImageRef:         prepared.Info.BaseImageRef,
		BaseImageDigest:      prepared.Info.BaseImageDigest,
		PlatformOS:           prepared.Checkpoint.Image.Platform.OS,
		PlatformArchitecture: prepared.Checkpoint.Image.Platform.Architecture,
		PlatformVariant:      prepared.Checkpoint.Image.Platform.Variant,
		Snapshotter:          prepared.Info.Snapshotter,
		SnapshotParent:       prepared.Info.SnapshotParent,
		SnapshotParentChain:  append([]string(nil), prepared.Info.SnapshotParentChain...),
		HeadObjectDigest:     manifest.Digest,
		HeadObjectMediaType:  manifest.MediaType,
		HeadObjectSize:       manifest.Size,
		HeadObjectKey:        manifest.Key,
		HeadImageRef:         prepared.Checkpoint.Image.Name,
		HeadImageDigest:      prepared.Checkpoint.Image.ManifestDigest,
		Objects:              append([]rootfshead.Object(nil), prepared.Checkpoint.Objects...),
	}
}

func (s *SandboxService) queueUncommittedRootFSObjectDeletion(ctx context.Context, state *SandboxRootFSState, notBefore time.Time) error {
	if state == nil || (strings.TrimSpace(state.HeadObjectKey) == "" && len(state.Objects) == 0) {
		return nil
	}
	store, ok := s.sandboxStore.(interface {
		QueueUncommittedRootFSObjectDeletion(context.Context, *SandboxRootFSState, time.Time) error
	})
	if !ok || store == nil {
		return nil
	}
	return store.QueueUncommittedRootFSObjectDeletion(ctx, state, notBefore)
}

func (s *SandboxService) deleteUncommittedRootFSObject(state *SandboxRootFSState, reason string) {
	if state == nil || (strings.TrimSpace(state.HeadObjectKey) == "" && len(state.Objects) == 0) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sandboxRootFSUncommittedObjectDeleteTimeout)
	defer cancel()
	// A running sync session may reuse these immutable objects in its next seal.
	// Delayed deletion gives a later committed layer time to cancel the queue;
	// deleting immediately could corrupt that still-running generation.
	if err := s.queueUncommittedRootFSObjectDeletion(ctx, state, time.Now().Add(sandboxRootFSUncommittedObjectDeleteDelay)); err != nil && s.logger != nil {
		s.logger.Warn("Failed to queue uncommitted rootfs object deletion",
			zap.String("sandboxID", state.SandboxID),
			zap.String("objectKey", state.DiffObjectKey),
			zap.String("reason", reason),
			zap.Error(err),
		)
	}
}

func saveRootFSError(resp *ctldapi.SaveRootFSResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Error)
}

func rootFSTerminatedSnapshotMissing(err error) bool {
	var reqErr *ctldapi.RequestError
	return errors.As(err, &reqErr) && reqErr != nil && reqErr.StatusCode == http.StatusNotFound
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
