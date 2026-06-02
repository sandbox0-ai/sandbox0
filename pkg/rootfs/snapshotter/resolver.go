package snapshotter

import (
	"context"
	"fmt"
	"strings"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"google.golang.org/grpc"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const defaultContainerdNamespace = "k8s.io"

type ContainerStore interface {
	Get(ctx context.Context, id string) (containers.Container, error)
}

type RuntimeService interface {
	PodSandboxStatus(ctx context.Context, in *runtime.PodSandboxStatusRequest, opts ...grpc.CallOption) (*runtime.PodSandboxStatusResponse, error)
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
	container, err := r.Containers.Get(ctx, snapshotKey)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return rootfs.Metadata{}, false, nil
		}
		return rootfs.Metadata{}, false, fmt.Errorf("get container %s: %w", snapshotKey, err)
	}
	sandboxID := strings.TrimSpace(container.SandboxID)
	if sandboxID == "" {
		return rootfs.Metadata{}, false, nil
	}

	resp, err := r.Runtime.PodSandboxStatus(ctx, &runtime.PodSandboxStatusRequest{PodSandboxId: sandboxID})
	if err != nil {
		return rootfs.Metadata{}, false, fmt.Errorf("get pod sandbox status %s: %w", sandboxID, err)
	}
	status := resp.GetStatus()
	if status == nil {
		return rootfs.Metadata{}, false, fmt.Errorf("pod sandbox status %s is empty", sandboxID)
	}
	return rootfs.MetadataFromAnnotations(status.GetAnnotations()), true, nil
}

func (r CRIMetadataResolver) ensureNamespace(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := namespaces.Namespace(ctx); ok {
		return ctx
	}
	namespace := strings.TrimSpace(r.Namespace)
	if namespace == "" {
		namespace = defaultContainerdNamespace
	}
	return namespaces.WithNamespace(ctx, namespace)
}
