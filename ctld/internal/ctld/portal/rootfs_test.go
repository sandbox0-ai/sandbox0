package portal

import (
	"context"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

func TestManagerSandboxRootFSLifecycle(t *testing.T) {
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
		TargetPath:        t.TempDir(),
	}
	resp, err := mgr.BindSandboxRootFS(context.Background(), req)
	if err != nil {
		t.Fatalf("BindSandboxRootFS() error = %v", err)
	}
	if resp.FilesystemID != "fs-a" || resp.MountPoint != req.TargetPath {
		t.Fatalf("BindSandboxRootFS() response = %+v", resp)
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

	nextReq := req
	nextReq.SandboxID = "sandbox-b"
	nextReq.PodUID = "pod-b"
	nextReq.RuntimeGeneration = 2
	nextReq.TargetPath = t.TempDir()
	if _, err := mgr.BindSandboxRootFS(context.Background(), nextReq); err != nil {
		t.Fatalf("BindSandboxRootFS(after release) error = %v", err)
	}
}
