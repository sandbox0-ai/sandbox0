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
	container, ok, err := r.resolveContainer(ctx, snapshotKey)
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

	sandboxID, ok, err := r.resolveSandboxIDFromCRIContainer(ctx, snapshotKey)
	if err != nil {
		return rootfs.Metadata{}, false, err
	}
	if ok {
		return r.resolvePodSandboxMetadata(ctx, sandboxID)
	}

	return r.resolvePodSandboxMetadata(ctx, snapshotKey)
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

func (r CRIMetadataResolver) resolveSandboxIDFromCRIContainer(ctx context.Context, snapshotKey string) (string, bool, error) {
	resp, err := r.Runtime.ListContainers(ctx, &runtime.ListContainersRequest{
		Filter: &runtime.ContainerFilter{Id: snapshotKey},
	})
	if err != nil {
		if isNotFound(err) {
			return "", false, nil
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
	return "", false, nil
}

func (r CRIMetadataResolver) resolveContainer(ctx context.Context, snapshotKey string) (containers.Container, bool, error) {
	container, err := r.Containers.Get(ctx, snapshotKey)
	if err != nil {
		if isNotFound(err) {
			all, err := r.Containers.List(ctx)
			if err != nil {
				return containers.Container{}, false, fmt.Errorf("list containers for snapshot key %s: %w", snapshotKey, err)
			}
			for _, container := range all {
				if strings.TrimSpace(container.SnapshotKey) == snapshotKey {
					return container, true, nil
				}
			}
			return containers.Container{}, false, nil
		}
		return containers.Container{}, false, fmt.Errorf("get container %s: %w", snapshotKey, err)
	}
	return container, true, nil
}

func (r CRIMetadataResolver) ensureNamespace(ctx context.Context) context.Context {
	return ensureContainerdNamespace(ctx, r.Namespace)
}

func isNotFound(err error) bool {
	return errdefs.IsNotFound(err) || status.Code(err) == codes.NotFound
}
