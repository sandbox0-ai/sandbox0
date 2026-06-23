package rootfs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestRuntimeFamily(t *testing.T) {
	tests := []struct {
		handler string
		want    string
	}{
		{handler: "io.containerd.runc.v2", want: "runc"},
		{handler: "runsc", want: "gvisor"},
		{handler: "gvisor-rootfs", want: "gvisor"},
		{handler: "containerd-shim-kata-v2", want: "kata"},
		{handler: "custom-runtime", want: "custom-runtime"},
	}

	for _, tt := range tests {
		t.Run(tt.handler, func(t *testing.T) {
			assert.Equal(t, tt.want, runtimeFamily(tt.handler))
		})
	}
}

func TestResolveContainerIDUsesCRILabels(t *testing.T) {
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{
		CRIClient: fakeCRIClient{containers: []*runtimeapi.Container{
			{
				Id:       "wrong-container",
				Metadata: &runtimeapi.ContainerMetadata{Name: "sandbox"},
				Labels: map[string]string{
					"io.kubernetes.pod.namespace": "default",
					"io.kubernetes.pod.name":      "other-pod",
					"io.kubernetes.pod.uid":       "other-uid",
				},
			},
			{
				Id:       "container-1",
				State:    runtimeapi.ContainerState_CONTAINER_RUNNING,
				Metadata: &runtimeapi.ContainerMetadata{Name: "sandbox"},
				Labels: map[string]string{
					"io.kubernetes.pod.namespace": "default",
					"io.kubernetes.pod.name":      "pod-1",
					"io.kubernetes.pod.uid":       "uid-1",
				},
			},
		}},
	})

	containerID, podUID, err := runtime.resolveContainerID(context.Background(), ctldapi.RootFSContainerRef{
		Namespace:     "default",
		PodName:       "pod-1",
		PodUID:        "uid-1",
		ContainerName: "sandbox",
	})

	require.NoError(t, err)
	assert.Equal(t, "container-1", containerID)
	assert.Equal(t, "uid-1", podUID)
}

func TestResolveContainerIDPrefersRunningAttempt(t *testing.T) {
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{
		CRIClient: fakeCRIClient{containers: []*runtimeapi.Container{
			{
				Id:       "exited-container",
				State:    runtimeapi.ContainerState_CONTAINER_EXITED,
				Metadata: &runtimeapi.ContainerMetadata{Name: "sandbox"},
				Labels: map[string]string{
					"io.kubernetes.pod.namespace": "default",
					"io.kubernetes.pod.name":      "pod-1",
					"io.kubernetes.pod.uid":       "uid-1",
				},
			},
			{
				Id:       "running-container",
				State:    runtimeapi.ContainerState_CONTAINER_RUNNING,
				Metadata: &runtimeapi.ContainerMetadata{Name: "sandbox"},
				Labels: map[string]string{
					"io.kubernetes.pod.namespace": "default",
					"io.kubernetes.pod.name":      "pod-1",
					"io.kubernetes.pod.uid":       "uid-1",
				},
			},
		}},
	})

	containerID, podUID, err := runtime.resolveContainerID(context.Background(), ctldapi.RootFSContainerRef{
		Namespace:     "default",
		PodName:       "pod-1",
		PodUID:        "uid-1",
		ContainerName: "sandbox",
	})

	require.NoError(t, err)
	assert.Equal(t, "running-container", containerID)
	assert.Equal(t, "uid-1", podUID)
}

func TestResolveContainerIDReturnsNotFound(t *testing.T) {
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{CRIClient: fakeCRIClient{}})

	_, _, err := runtime.resolveContainerID(context.Background(), ctldapi.RootFSContainerRef{
		Namespace:     "default",
		PodName:       "pod-1",
		ContainerName: "sandbox",
	})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestFindLiveRootFSByTaskAnnotations(t *testing.T) {
	taskRoot := t.TempDir()
	hostTaskRoot := filepath.Join(string(filepath.Separator), "run", "containerd", "io.containerd.runtime.v2.task", "k8s.io")
	writeTaskConfig(t, filepath.Join(taskRoot, "wrong"), map[string]string{
		"io.kubernetes.cri.container-type":    "container",
		"io.kubernetes.cri.container-name":    "procd",
		"io.kubernetes.cri.sandbox-namespace": "tpl-default",
		"io.kubernetes.cri.sandbox-name":      "other-pod",
		"io.kubernetes.cri.sandbox-uid":       "other-uid",
	})
	wantTask := filepath.Join(taskRoot, "task-1")
	writeTaskConfig(t, wantTask, map[string]string{
		"io.kubernetes.cri.container-type":    "container",
		"io.kubernetes.cri.container-name":    "procd",
		"io.kubernetes.cri.sandbox-namespace": "tpl-default",
		"io.kubernetes.cri.sandbox-name":      "pod-1",
		"io.kubernetes.cri.sandbox-uid":       "uid-1",
	})
	require.NoError(t, os.Mkdir(filepath.Join(wantTask, "rootfs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(wantTask, "init.pid"), []byte("1234\n"), 0o644))

	got, err := findLiveRootFSByTaskAnnotations(taskRoot, hostTaskRoot, ctldapi.RootFSInfo{
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
		PodUID:        "uid-1",
	})

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "1234", "root"), got.mountedPath)
	assert.Equal(t, filepath.Join(hostTaskRoot, "task-1", "rootfs"), got.hostPath)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "1234", "ns", "mnt"), got.mountNamespacePath)
}

func TestLiveRootFSPathsExposeMountedAndHostRoots(t *testing.T) {
	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	taskDir := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID)
	require.NoError(t, os.MkdirAll(filepath.Join(taskDir, "rootfs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(taskDir, "init.pid"), []byte("5678"), 0o644))

	got, err := liveRootFSPathsForContainer(containerdRoot, containerdHostRoot, "k8s.io", ctldapi.RootFSInfo{
		ContainerID:   containerID,
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
	})

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "5678", "root"), got.mountedPath)
	assert.Equal(t, filepath.Join(containerdHostRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID, "rootfs"), got.hostPath)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "5678", "ns", "mnt"), got.mountNamespacePath)
}

func TestLiveRootFSPathMapsMountedRootToHostRoot(t *testing.T) {
	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	require.NoError(t, os.MkdirAll(filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID, "rootfs"), 0o755))

	got, err := liveRootFSPath(containerdRoot, containerdHostRoot, "k8s.io", ctldapi.RootFSInfo{
		ContainerID:   containerID,
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
	})

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(containerdHostRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID, "rootfs"), got)
}

func TestMountedLiveRootFSPathReturnsContainerMountedRoot(t *testing.T) {
	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	mountedRoot := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID, "rootfs")
	require.NoError(t, os.MkdirAll(mountedRoot, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(mountedRoot), "init.pid"), []byte("3456"), 0o644))

	got, err := mountedLiveRootFSPath(containerdRoot, containerdHostRoot, "k8s.io", ctldapi.RootFSInfo{
		ContainerID:   containerID,
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
	})

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "3456", "root"), got)
}

func TestMountedLiveRootFSPathFallsBackToTaskRootWithoutInitPID(t *testing.T) {
	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	mountedRoot := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID, "rootfs")
	require.NoError(t, os.MkdirAll(mountedRoot, 0o755))

	got, err := mountedLiveRootFSPath(containerdRoot, containerdHostRoot, "k8s.io", ctldapi.RootFSInfo{
		ContainerID:   containerID,
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
	})

	require.NoError(t, err)
	assert.Equal(t, mountedRoot, got)
}

func TestLiveRootFSPathsFallBackToProcessRoot(t *testing.T) {
	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	taskDir := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID)
	require.NoError(t, os.MkdirAll(taskDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(taskDir, "init.pid"), []byte("9012\n"), 0o644))

	got, err := liveRootFSPathsForContainer(containerdRoot, containerdHostRoot, "k8s.io", ctldapi.RootFSInfo{
		ContainerID:   containerID,
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
	})

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "9012", "root"), got.mountedPath)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "9012", "root"), got.hostPath)
	assert.Equal(t, filepath.Join(string(filepath.Separator), "proc", "9012", "ns", "mnt"), got.mountNamespacePath)
}

func TestDigestFromReference(t *testing.T) {
	assert.Equal(t, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", digestFromReference("busybox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	assert.Empty(t, digestFromReference("busybox:1.36"))
}

func TestNormalizeCRIEndpoint(t *testing.T) {
	assert.Equal(t, "unix:///run/containerd/containerd.sock", normalizeCRIEndpoint("/run/containerd/containerd.sock"))
	assert.Equal(t, "unix:///run/containerd/containerd.sock", normalizeCRIEndpoint("unix:///run/containerd/containerd.sock"))
	assert.Equal(t, "127.0.0.1:1234", normalizeCRIEndpoint("127.0.0.1:1234"))
}

type fakeCRIClient struct {
	containers []*runtimeapi.Container
	err        error
}

func (c fakeCRIClient) ListContainers(_ context.Context, _ *runtimeapi.ListContainersRequest, _ ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &runtimeapi.ListContainersResponse{Containers: c.containers}, nil
}

func writeTaskConfig(t *testing.T, taskDir string, annotations map[string]string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(taskDir, 0o755))
	raw, err := json.Marshal(struct {
		Annotations map[string]string `json:"annotations"`
	}{Annotations: annotations})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(taskDir, "config.json"), raw, 0o644))
}
