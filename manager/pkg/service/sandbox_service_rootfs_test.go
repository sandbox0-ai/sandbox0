package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBindSandboxRootFSSendsCtldRequest(t *testing.T) {
	var got ctldapi.BindSandboxRootFSRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sandbox-rootfs/bind" {
			t.Fatalf("path = %q, want /api/v1/sandbox-rootfs/bind", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode bind request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.BindSandboxRootFSResponse{
			FilesystemID: got.FilesystemID,
			MountPoint:   got.TargetPath,
		})
	}))
	defer ctld.Close()
	host, port := splitTestServerHostPort(t, ctld.URL)

	service := &SandboxService{
		ctldClient: NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldEnabled: true,
			CtldPort:    port,
		},
	}
	pod := &corev1.Pod{}
	pod.Namespace = "ns-a"
	pod.Name = "pod-a"
	pod.UID = types.UID("pod-uid")
	pod.Status.HostIP = host
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:        "procd",
		ContainerID: "containerd://container-a",
		ImageID:     "docker-pullable://ubuntu@sha256:abc",
	}}

	err := service.bindSandboxRootFS(context.Background(), pod, &ClaimRequest{
		TeamID:                    "team-a",
		SandboxID:                 "sandbox-a",
		FilesystemID:              "fs-a",
		RuntimeGeneration:         3,
		FilesystemBaseImageRef:    "ubuntu:24.04",
		FilesystemBaseImageDigest: "sha256:abc",
	})
	if err != nil {
		t.Fatalf("bindSandboxRootFS() error = %v", err)
	}
	if got.TeamID != "team-a" || got.SandboxID != "sandbox-a" || got.FilesystemID != "fs-a" || got.RuntimeGeneration != 3 {
		t.Fatalf("unexpected bind request: %+v", got)
	}
	if got.TargetPath != sandboxRootFSMountPath {
		t.Fatalf("target_path = %q, want %q", got.TargetPath, sandboxRootFSMountPath)
	}
	if got.ContainerID != "containerd://container-a" {
		t.Fatalf("container_id = %q, want containerd://container-a", got.ContainerID)
	}
	if got.RootFSVolumeName != "sandbox-rootfs" {
		t.Fatalf("rootfs_volume_name = %q, want sandbox-rootfs", got.RootFSVolumeName)
	}
	if got.BaseImageDigest != "sha256:abc" {
		t.Fatalf("base_image_digest = %q, want sha256:abc", got.BaseImageDigest)
	}
	if got.CarrierImageDigest != "sha256:abc" {
		t.Fatalf("carrier_image_digest = %q, want sha256:abc", got.CarrierImageDigest)
	}
}

func TestBindSandboxRootFSSendsBaseDigestMismatchToCtld(t *testing.T) {
	var got ctldapi.BindSandboxRootFSRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode bind request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.BindSandboxRootFSResponse{
			FilesystemID: got.FilesystemID,
			MountPoint:   got.TargetPath,
		})
	}))
	defer ctld.Close()
	host, port := splitTestServerHostPort(t, ctld.URL)

	service := &SandboxService{
		ctldClient: NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldEnabled: true,
			CtldPort:    port,
		},
	}
	pod := &corev1.Pod{}
	pod.Namespace = "ns-a"
	pod.Name = "pod-a"
	pod.UID = types.UID("pod-uid")
	pod.Status.HostIP = host
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:        "procd",
		ContainerID: "containerd://container-a",
		ImageID:     "docker-pullable://ubuntu@sha256:def",
	}}

	err := service.bindSandboxRootFS(context.Background(), pod, &ClaimRequest{
		TeamID:                    "team-a",
		SandboxID:                 "sandbox-a",
		FilesystemID:              "fs-a",
		RuntimeGeneration:         3,
		FilesystemBaseImageDigest: "sha256:abc",
	})
	if err != nil {
		t.Fatalf("bindSandboxRootFS() error = %v", err)
	}
	if got.BaseImageDigest != "sha256:abc" || got.CarrierImageDigest != "sha256:def" {
		t.Fatalf("digest request = base:%q carrier:%q, want base sha256:abc carrier sha256:def", got.BaseImageDigest, got.CarrierImageDigest)
	}
}

func TestBindSandboxRootFSBackfillsBaseDigestFromCarrierImage(t *testing.T) {
	var got ctldapi.BindSandboxRootFSRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode bind request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.BindSandboxRootFSResponse{
			FilesystemID: got.FilesystemID,
			MountPoint:   got.TargetPath,
		})
	}))
	defer ctld.Close()
	host, port := splitTestServerHostPort(t, ctld.URL)

	store := &recordingSandboxFilesystemStore{
		acquireResp: &SandboxFilesystemRecord{
			FilesystemID: "fs-a",
			TeamID:       "team-a",
			UserID:       "user-a",
			BaseImageRef: "ubuntu:24.04",
		},
	}
	service := &SandboxService{
		ctldClient:             NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		sandboxFilesystemStore: store,
		config: SandboxServiceConfig{
			CtldEnabled: true,
			CtldPort:    port,
		},
	}
	pod := &corev1.Pod{}
	pod.Namespace = "ns-a"
	pod.Name = "pod-a"
	pod.UID = types.UID("pod-uid")
	pod.Status.HostIP = host
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:        "procd",
		ContainerID: "containerd://container-a",
		ImageID:     "docker-pullable://ubuntu@sha256:def",
	}}

	req := &ClaimRequest{
		TeamID:                 "team-a",
		UserID:                 "user-a",
		SandboxID:              "sandbox-a",
		FilesystemID:           "fs-a",
		RuntimeGeneration:      3,
		FilesystemBaseImageRef: "ubuntu:24.04",
	}
	if err := service.bindSandboxRootFS(context.Background(), pod, req); err != nil {
		t.Fatalf("bindSandboxRootFS() error = %v", err)
	}
	if req.FilesystemBaseImageDigest != "sha256:def" {
		t.Fatalf("request base digest = %q, want carrier digest", req.FilesystemBaseImageDigest)
	}
	if got.BaseImageDigest != "sha256:def" || got.CarrierImageDigest != "sha256:def" {
		t.Fatalf("ctld digest request = base:%q carrier:%q, want sha256:def", got.BaseImageDigest, got.CarrierImageDigest)
	}
	if len(store.acquireReqs) != 1 {
		t.Fatalf("acquire requests = %d, want 1", len(store.acquireReqs))
	}
	acquire := store.acquireReqs[0]
	if acquire.BaseImageDigest != "sha256:def" || acquire.OwnerSandboxID != "sandbox-a" || acquire.OwnerRuntimeGeneration != 3 {
		t.Fatalf("filesystem acquire request = %+v, want digest backfill for sandbox-a generation 3", acquire)
	}
}

func TestFlushSandboxRootFSForPodRequiresCtldFlush(t *testing.T) {
	var got ctldapi.FlushSandboxRootFSRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sandbox-rootfs/flush" {
			t.Fatalf("path = %q, want /api/v1/sandbox-rootfs/flush", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode flush request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(ctldapi.FlushSandboxRootFSResponse{
			Flushed:      true,
			FilesystemID: got.FilesystemID,
		})
	}))
	defer ctld.Close()
	host, port := splitTestServerHostPort(t, ctld.URL)

	service := &SandboxService{
		ctldClient: NewCtldClient(CtldClientConfig{Timeout: time.Second}),
		config: SandboxServiceConfig{
			CtldEnabled: true,
			CtldPort:    port,
		},
	}
	pod := &corev1.Pod{}
	pod.Namespace = "ns-a"
	pod.Name = "pod-a"
	pod.UID = types.UID("pod-uid")
	pod.Status.HostIP = host

	if err := service.flushSandboxRootFSForPod(context.Background(), pod, "fs-a", "team-a", "sandbox-a", 4); err != nil {
		t.Fatalf("flushSandboxRootFSForPod() error = %v", err)
	}
	if got.TeamID != "team-a" || got.SandboxID != "sandbox-a" || got.FilesystemID != "fs-a" || got.RuntimeGeneration != 4 {
		t.Fatalf("unexpected flush request: %+v", got)
	}
}

func splitTestServerHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatalf("parse server port: %v", err)
	}
	return parsed.Hostname(), port
}
