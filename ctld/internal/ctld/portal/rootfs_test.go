package portal

import (
	"context"
	"path/filepath"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

func TestManagerSandboxRootFSLifecycle(t *testing.T) {
	var mountedTarget string
	var unmountedTarget string
	previousMount := mountOverlayRootFSForBind
	previousUnmount := unmountRootFSForRelease
	mountOverlayRootFSForBind = func(lowerDir, upperDir, workDir, targetPath string) error {
		if lowerDir == "" || upperDir == "" || workDir == "" || targetPath == "" {
			t.Fatalf("mount args = lower:%q upper:%q work:%q target:%q", lowerDir, upperDir, workDir, targetPath)
		}
		mountedTarget = targetPath
		return nil
	}
	unmountRootFSForRelease = func(targetPath string) error {
		unmountedTarget = targetPath
		return nil
	}
	t.Cleanup(func() {
		mountOverlayRootFSForBind = previousMount
		unmountRootFSForRelease = previousUnmount
	})

	baseRoot := t.TempDir()
	targetHostPath := t.TempDir()
	mgr := NewManager(Config{
		RootDir:       t.TempDir(),
		StorageConfig: &apiconfig.StorageProxyConfig{},
	})

	req := ctldapi.BindSandboxRootFSRequest{
		TeamID:            "team-a",
		SandboxID:         "sandbox-a",
		PodUID:            "pod-a",
		FilesystemID:      "fs-a",
		RuntimeGeneration: 1,
		BaseImageRef:      "ubuntu:24.04",
		TargetPath:        "/sandbox0/rootfs",
		TargetHostPath:    targetHostPath,
		BaseRootPath:      baseRoot,
	}
	resp, err := mgr.BindSandboxRootFS(context.Background(), req)
	if err != nil {
		t.Fatalf("BindSandboxRootFS() error = %v", err)
	}
	if resp.FilesystemID != "fs-a" || resp.MountPoint != req.TargetPath {
		t.Fatalf("BindSandboxRootFS() response = %+v", resp)
	}
	if mountedTarget != targetHostPath {
		t.Fatalf("mounted target = %q, want %q", mountedTarget, targetHostPath)
	}
	if _, err := filepath.Abs(resp.MountPoint); err != nil {
		t.Fatalf("mount point is not a valid path: %v", err)
	}

	second, err := mgr.BindSandboxRootFS(context.Background(), req)
	if err != nil {
		t.Fatalf("BindSandboxRootFS(idempotent) error = %v", err)
	}
	if second.FilesystemID != resp.FilesystemID || second.MountPoint != resp.MountPoint {
		t.Fatalf("idempotent response = %+v, want %+v", second, resp)
	}

	conflictReq := req
	conflictReq.SandboxID = "sandbox-b"
	conflictReq.PodUID = "pod-b"
	if _, err := mgr.BindSandboxRootFS(context.Background(), conflictReq); err == nil {
		t.Fatal("BindSandboxRootFS(conflict) returned nil error")
	}

	flushResp, err := mgr.FlushSandboxRootFS(context.Background(), ctldapi.FlushSandboxRootFSRequest{
		TeamID:            req.TeamID,
		SandboxID:         req.SandboxID,
		PodUID:            req.PodUID,
		FilesystemID:      req.FilesystemID,
		RuntimeGeneration: req.RuntimeGeneration,
	})
	if err != nil {
		t.Fatalf("FlushSandboxRootFS() error = %v", err)
	}
	if !flushResp.Flushed || flushResp.FilesystemID != req.FilesystemID {
		t.Fatalf("FlushSandboxRootFS() response = %+v", flushResp)
	}

	releaseResp, err := mgr.ReleaseSandboxRootFS(context.Background(), ctldapi.ReleaseSandboxRootFSRequest{
		TeamID:            req.TeamID,
		SandboxID:         req.SandboxID,
		PodUID:            req.PodUID,
		FilesystemID:      req.FilesystemID,
		RuntimeGeneration: req.RuntimeGeneration,
	})
	if err != nil {
		t.Fatalf("ReleaseSandboxRootFS() error = %v", err)
	}
	if !releaseResp.Released || releaseResp.FilesystemID != req.FilesystemID {
		t.Fatalf("ReleaseSandboxRootFS() response = %+v", releaseResp)
	}
	if unmountedTarget != targetHostPath {
		t.Fatalf("unmounted target = %q, want %q", unmountedTarget, targetHostPath)
	}

	nextReq := req
	nextReq.SandboxID = "sandbox-b"
	nextReq.PodUID = "pod-b"
	nextReq.RuntimeGeneration = 2
	nextReq.TargetHostPath = t.TempDir()
	if _, err := mgr.BindSandboxRootFS(context.Background(), nextReq); err != nil {
		t.Fatalf("BindSandboxRootFS(after release) error = %v", err)
	}
}

func TestSandboxRootFSPathsDerivesHostPathFromPodUID(t *testing.T) {
	paths, err := sandboxRootFSPaths(ctldapi.BindSandboxRootFSRequest{
		PodUID:            "pod/../uid",
		ContainerID:       "containerd://container-a",
		TargetPath:        "/sandbox0/rootfs",
		RootFSVolumeName:  "sandbox-rootfs",
		RuntimeGeneration: 7,
	}, "/cache")
	if err != nil {
		t.Fatalf("sandboxRootFSPaths() error = %v", err)
	}
	if paths.mountPoint != "/sandbox0/rootfs" {
		t.Fatalf("mountPoint = %q, want /sandbox0/rootfs", paths.mountPoint)
	}
	wantTarget := filepath.Join(kubeletEmptyDirVolumeRoot, "pod_.._uid", "volumes", "kubernetes.io~empty-dir", "sandbox-rootfs")
	if paths.targetHostPath != wantTarget {
		t.Fatalf("targetHostPath = %q, want %q", paths.targetHostPath, wantTarget)
	}
	wantBase := filepath.Join("/run/containerd/io.containerd.runtime.v2.task/k8s.io", "container-a", "rootfs")
	if paths.baseRootPath != wantBase {
		t.Fatalf("baseRootPath = %q, want %q", paths.baseRootPath, wantBase)
	}
	if paths.upperDir != filepath.Join("/cache", "runtime", "7", "upper") {
		t.Fatalf("upperDir = %q", paths.upperDir)
	}
}
