package snapshotter

import (
	"context"
	"fmt"
	"strings"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type ContainerStore interface {
	Get(ctx context.Context, id string) (containers.Container, error)
	List(ctx context.Context, filters ...string) ([]containers.Container, error)
}

type RuntimeService interface {
	PodSandboxStatus(ctx context.Context, in *runtime.PodSandboxStatusRequest, opts ...grpc.CallOption) (*runtime.PodSandboxStatusResponse, error)
	ListContainers(ctx context.Context, in *runtime.ListContainersRequest, opts ...grpc.CallOption) (*runtime.ListContainersResponse, error)
}

// CRIMetadataResolver uses containerd container metadata to find the owning CRI
// sandbox and reads Sandbox0 rootfs annotations from PodSandboxStatus.
type CRIMetadataResolver struct {
	Containers ContainerStore
	Runtime    RuntimeService
	Namespace  string
}

func (r CRIMetadataResolver) ResolveRootFSMetadata(ctx context.Context, snapshotKey string) (rootfs.Metadata, bool, error) {
	snapshotKey = strings.TrimSpace(snapshotKey)
	if snapshotKey == "" {
		return rootfs.Metadata{}, false, nil
	}
	if r.Containers == nil {
		return rootfs.Metadata{}, false, fmt.Errorf("container store is required")
	}
	if r.Runtime == nil {
		return rootfs.Metadata{}, false, fmt.Errorf("CRI runtime service is required")
	}

	ctx = r.ensureNamespace(ctx)
	candidates := snapshotKeyCandidates(snapshotKey)
	container, ok, err := r.resolveContainer(ctx, candidates)
	if err != nil {
		return rootfs.Metadata{}, false, err
	}
	if ok {
		sandboxID := strings.TrimSpace(container.SandboxID)
		if sandboxID == "" {
			sandboxID = strings.TrimSpace(container.ID)
		}
		if sandboxID != "" {
			return r.resolvePodSandboxMetadata(ctx, sandboxID)
		}
	}

	sandboxID, ok, err := r.resolveSandboxIDFromCRIContainer(ctx, candidates)
	if err != nil {
		return rootfs.Metadata{}, false, err
	}
	if ok {
		return r.resolvePodSandboxMetadata(ctx, sandboxID)
	}

	for _, candidate := range candidates {
		meta, ok, err := r.resolvePodSandboxMetadata(ctx, candidate)
		if err != nil || ok {
			return meta, ok, err
		}
	}
	return rootfs.Metadata{}, false, nil
}

func (r CRIMetadataResolver) resolvePodSandboxMetadata(ctx context.Context, sandboxID string) (rootfs.Metadata, bool, error) {
	resp, err := r.Runtime.PodSandboxStatus(ctx, &runtime.PodSandboxStatusRequest{PodSandboxId: sandboxID})
	if err != nil {
		if isNotFound(err) {
			return rootfs.Metadata{}, false, nil
		}
		return rootfs.Metadata{}, false, fmt.Errorf("get pod sandbox status %s: %w", sandboxID, err)
	}
	status := resp.GetStatus()
	if status == nil {
		return rootfs.Metadata{}, false, fmt.Errorf("pod sandbox status %s is empty", sandboxID)
	}
	return rootfs.MetadataFromAnnotations(status.GetAnnotations()), true, nil
}

func (r CRIMetadataResolver) resolveSandboxIDFromCRIContainer(ctx context.Context, snapshotKeys []string) (string, bool, error) {
	for _, snapshotKey := range snapshotKeys {
		resp, err := r.Runtime.ListContainers(ctx, &runtime.ListContainersRequest{
			Filter: &runtime.ContainerFilter{Id: snapshotKey},
		})
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return "", false, fmt.Errorf("list containers for CRI id %s: %w", snapshotKey, err)
		}
		for _, container := range resp.GetContainers() {
			if strings.TrimSpace(container.GetId()) != snapshotKey {
				continue
			}
			sandboxID := strings.TrimSpace(container.GetPodSandboxId())
			if sandboxID == "" {
				return "", false, nil
			}
			return sandboxID, true, nil
		}
	}
	return "", false, nil
}

func (r CRIMetadataResolver) resolveContainer(ctx context.Context, snapshotKeys []string) (containers.Container, bool, error) {
	for _, snapshotKey := range snapshotKeys {
		container, err := r.Containers.Get(ctx, snapshotKey)
		if err == nil {
			return container, true, nil
		}
		if !isNotFound(err) {
			return containers.Container{}, false, fmt.Errorf("get container %s: %w", snapshotKey, err)
		}
	}

	all, err := r.Containers.List(ctx)
	if err != nil {
		return containers.Container{}, false, fmt.Errorf("list containers for snapshot key %s: %w", snapshotKeys[0], err)
	}
	for _, snapshotKey := range snapshotKeys {
		for _, container := range all {
			if strings.TrimSpace(container.SnapshotKey) == snapshotKey {
				return container, true, nil
			}
		}
	}
	return containers.Container{}, false, nil
}

func (r CRIMetadataResolver) ensureNamespace(ctx context.Context) context.Context {
	return ensureContainerdNamespace(ctx, r.Namespace)
}

func isNotFound(err error) bool {
	return errdefs.IsNotFound(err) || status.Code(err) == codes.NotFound
}

func snapshotKeyCandidates(snapshotKey string) []string {
	snapshotKey = strings.TrimSpace(snapshotKey)
	if snapshotKey == "" {
		return nil
	}
	candidates := []string{snapshotKey}
	if idx := strings.LastIndex(snapshotKey, "/"); idx >= 0 && idx+1 < len(snapshotKey) {
		candidates = appendUniqueString(candidates, strings.TrimSpace(snapshotKey[idx+1:]))
	}
	return candidates
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
