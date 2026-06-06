package portal

import (
	"context"
	"fmt"
	"os"
	"strings"

	containerd "github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
	cdmount "github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/platforms"
	"github.com/opencontainers/image-spec/identity"
	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

const containerdK8sNamespace = "k8s.io"

type rootFSBase struct {
	rootPath       string
	snapshotter    string
	snapshotKey    string
	mountPath      string
	snapshotActive bool
}

func (m *Manager) prepareRootFSBase(ctx context.Context, req ctldapi.BindSandboxRootFSRequest, paths rootFSPaths) (rootFSBase, error) {
	if strings.TrimSpace(req.BaseRootPath) != "" {
		return rootFSBase{rootPath: paths.baseRootPath}, nil
	}
	if !shouldPrepareImageBase(req) {
		if strings.TrimSpace(paths.baseRootPath) == "" {
			return rootFSBase{}, fmt.Errorf("container_id or base_root_path is required to bind sandbox rootfs")
		}
		return rootFSBase{rootPath: paths.baseRootPath}, nil
	}
	return m.prepareContainerdImageBase(ctx, req, paths)
}

func shouldPrepareImageBase(req ctldapi.BindSandboxRootFSRequest) bool {
	expected := normalizeRootFSImageDigest(req.BaseImageDigest)
	if expected == "" {
		return false
	}
	carrier := normalizeRootFSImageDigest(req.CarrierImageDigest)
	return carrier == "" || carrier != expected
}

func (m *Manager) prepareContainerdImageBase(ctx context.Context, req ctldapi.BindSandboxRootFSRequest, paths rootFSPaths) (rootFSBase, error) {
	ref, err := rootFSBaseImageReference(req.BaseImageRef, req.BaseImageDigest)
	if err != nil {
		return rootFSBase{}, err
	}
	address := normalizeContainerdAddress(m.containerdAddress)
	if address == "" {
		return rootFSBase{}, fmt.Errorf("containerd address is required to prepare sandbox rootfs base image")
	}
	snapshotter := containerd.DefaultSnapshotter
	snapshotKey := rootFSBaseSnapshotKey(req)
	if snapshotKey == "" {
		return rootFSBase{}, fmt.Errorf("sandbox rootfs base snapshot key is required")
	}
	if err := resetRootFSBaseMountDir(paths.baseMountDir); err != nil {
		return rootFSBase{}, err
	}

	client, err := containerd.New(address,
		containerd.WithDefaultNamespace(containerdK8sNamespace),
		containerd.WithDefaultPlatform(platforms.Default()),
	)
	if err != nil {
		return rootFSBase{}, fmt.Errorf("connect containerd for sandbox rootfs base image: %w", err)
	}
	defer client.Close()

	namespacedCtx := namespaces.WithNamespace(ctx, containerdK8sNamespace)
	image, err := client.GetImage(namespacedCtx, ref)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return rootFSBase{}, fmt.Errorf("get sandbox rootfs base image %s: %w", ref, err)
		}
		image, err = client.Pull(namespacedCtx, ref,
			containerd.WithPullUnpack,
			containerd.WithPullSnapshotter(snapshotter),
		)
		if err != nil {
			return rootFSBase{}, fmt.Errorf("pull sandbox rootfs base image %s: %w", ref, err)
		}
	} else if err := image.Unpack(namespacedCtx, snapshotter); err != nil && !errdefs.IsAlreadyExists(err) {
		return rootFSBase{}, fmt.Errorf("unpack sandbox rootfs base image %s: %w", ref, err)
	}

	diffIDs, err := image.RootFS(namespacedCtx)
	if err != nil {
		return rootFSBase{}, fmt.Errorf("resolve sandbox rootfs base chain for %s: %w", ref, err)
	}
	parent := identity.ChainID(diffIDs).String()
	snapshotService := client.SnapshotService(snapshotter)
	mounts, err := snapshotService.View(namespacedCtx, snapshotKey, parent)
	if err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return rootFSBase{}, fmt.Errorf("create sandbox rootfs base snapshot view: %w", err)
		}
		mounts, err = snapshotService.Mounts(namespacedCtx, snapshotKey)
		if err != nil {
			return rootFSBase{}, fmt.Errorf("read sandbox rootfs base snapshot mounts: %w", err)
		}
	}
	if err := cdmount.All(mounts, paths.baseMountDir); err != nil {
		_ = snapshotService.Remove(namespacedCtx, snapshotKey)
		return rootFSBase{}, fmt.Errorf("mount sandbox rootfs base image: %w", err)
	}
	return rootFSBase{
		rootPath:       paths.baseMountDir,
		snapshotter:    snapshotter,
		snapshotKey:    snapshotKey,
		mountPath:      paths.baseMountDir,
		snapshotActive: true,
	}, nil
}

func releaseRootFSBase(ctx context.Context, m *Manager, base rootFSBase) error {
	if !base.snapshotActive {
		return nil
	}
	if strings.TrimSpace(base.mountPath) != "" {
		if err := cdmount.UnmountAll(base.mountPath, unix.MNT_DETACH); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("unmount sandbox rootfs base image: %w", err)
		}
		_ = os.RemoveAll(base.mountPath)
	}
	address := normalizeContainerdAddress(m.containerdAddress)
	if address == "" || strings.TrimSpace(base.snapshotKey) == "" {
		return nil
	}
	client, err := containerd.New(address, containerd.WithDefaultNamespace(containerdK8sNamespace))
	if err != nil {
		return fmt.Errorf("connect containerd to release sandbox rootfs base image: %w", err)
	}
	defer client.Close()
	namespacedCtx := namespaces.WithNamespace(ctx, containerdK8sNamespace)
	snapshotter := strings.TrimSpace(base.snapshotter)
	if snapshotter == "" {
		snapshotter = containerd.DefaultSnapshotter
	}
	if err := client.SnapshotService(snapshotter).Remove(namespacedCtx, base.snapshotKey); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove sandbox rootfs base snapshot view: %w", err)
	}
	return nil
}

func resetRootFSBaseMountDir(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("sandbox rootfs base mount dir is required")
	}
	_ = cdmount.UnmountAll(path, unix.MNT_DETACH)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("reset sandbox rootfs base mount dir: %w", err)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create sandbox rootfs base mount dir: %w", err)
	}
	return nil
}

func rootFSBaseSnapshotKey(req ctldapi.BindSandboxRootFSRequest) string {
	parts := []string{
		"sandbox0-rootfs-base",
		safePath(req.TeamID),
		safePath(req.FilesystemID),
		fmt.Sprintf("%d", req.RuntimeGeneration),
	}
	return strings.Join(parts, "-")
}

func rootFSBaseImageReference(ref, digest string) (string, error) {
	ref = strings.TrimSpace(ref)
	digest = normalizeRootFSImageDigest(digest)
	if ref == "" {
		return "", fmt.Errorf("base_image_ref is required to prepare sandbox rootfs base digest %s", digest)
	}
	if digest == "" {
		return ref, nil
	}
	if at := strings.LastIndex(ref, "@"); at >= 0 {
		ref = ref[:at]
	}
	lastSlash := strings.LastIndex(ref, "/")
	lastColon := strings.LastIndex(ref, ":")
	if lastColon > lastSlash {
		ref = ref[:lastColon]
	}
	if ref == "" {
		return "", fmt.Errorf("base_image_ref is invalid")
	}
	return ref + "@" + digest, nil
}

func normalizeContainerdAddress(address string) string {
	address = strings.TrimSpace(address)
	address = strings.TrimPrefix(address, "unix://")
	return strings.TrimPrefix(address, "unix:")
}

func normalizeRootFSImageDigest(digest string) string {
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return ""
	}
	if idx := strings.LastIndex(digest, "sha256:"); idx >= 0 {
		digest = digest[idx:]
	}
	return strings.ToLower(digest)
}
