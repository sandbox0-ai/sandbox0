package rootfs

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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
		CRIClient: &fakeCRIClient{containers: []*runtimeapi.Container{
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
		CRIClient: &fakeCRIClient{containers: []*runtimeapi.Container{
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
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{CRIClient: &fakeCRIClient{}})

	_, _, err := runtime.resolveContainerID(context.Background(), ctldapi.RootFSContainerRef{
		Namespace:     "default",
		PodName:       "pod-1",
		ContainerName: "sandbox",
	})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestListPodSandboxesUsesReadyCRIRequest(t *testing.T) {
	want := []*runtimeapi.PodSandbox{{Id: "sandbox-1"}}
	client := &fakeCRIClient{sandboxes: want}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{CRIClient: client})

	got, err := runtime.ListPodSandboxes(context.Background())

	require.NoError(t, err)
	assert.Equal(t, want, got)
	require.NotNil(t, client.listSandboxFilter)
	require.NotNil(t, client.listSandboxFilter.State)
	assert.Equal(t, runtimeapi.PodSandboxState_SANDBOX_READY, client.listSandboxFilter.State.State)
}

func TestPodSandboxStatsUsesSingleCRIRequest(t *testing.T) {
	want := &runtimeapi.PodSandboxStats{Attributes: &runtimeapi.PodSandboxAttributes{Id: "sandbox-1"}}
	client := &fakeCRIClient{statsByID: map[string]*runtimeapi.PodSandboxStats{"sandbox-1": want}}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{CRIClient: client})

	got, err := runtime.PodSandboxStats(context.Background(), "sandbox-1")

	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, []string{"sandbox-1"}, client.statsRequests)
}

func TestListPodSandboxesWrapsCRIError(t *testing.T) {
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{CRIClient: &fakeCRIClient{err: errors.New("unavailable")}})

	_, err := runtime.ListPodSandboxes(context.Background())

	require.ErrorContains(t, err, "list CRI pod sandboxes")
}

func TestContainerdRuntimeReusesAndClosesCRIConnection(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(server, &fakeRuntimeServiceServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	var dialCount atomic.Int32
	var connMu sync.Mutex
	var dialedConn *grpc.ClientConn
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{
		CRIEndpoint: "bufnet",
		CRIDialContext: func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
			dialCount.Add(1)
			conn, err := grpc.DialContext(ctx, "bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err == nil {
				connMu.Lock()
				dialedConn = conn
				connMu.Unlock()
			}
			return conn, err
		},
	})

	_, err := runtime.ListPodSandboxes(context.Background())
	require.NoError(t, err)
	_, err = runtime.PodSandboxStats(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, int32(1), dialCount.Load())

	require.NoError(t, runtime.Close())
	connMu.Lock()
	closedConn := dialedConn
	connMu.Unlock()
	require.NotNil(t, closedConn)
	assert.Equal(t, connectivity.Shutdown, closedConn.GetState())
	require.NoError(t, runtime.Close())
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

	got, err := findLiveRootFSByTaskAnnotations(taskRoot, hostTaskRoot, ctldapi.RootFSInfo{
		ContainerName: "procd",
		PodNamespace:  "tpl-default",
		PodName:       "pod-1",
		PodUID:        "uid-1",
	})

	require.NoError(t, err)
	assert.Equal(t, filepath.Join(hostTaskRoot, "task-1", "rootfs"), got)
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
	containers        []*runtimeapi.Container
	sandboxes         []*runtimeapi.PodSandbox
	statsByID         map[string]*runtimeapi.PodSandboxStats
	listSandboxFilter *runtimeapi.PodSandboxFilter
	statsRequests     []string
	err               error
}

type fakeRuntimeServiceServer struct {
	runtimeapi.UnimplementedRuntimeServiceServer
}

func (*fakeRuntimeServiceServer) ListPodSandbox(context.Context, *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	return &runtimeapi.ListPodSandboxResponse{Items: []*runtimeapi.PodSandbox{{Id: "sandbox-1"}}}, nil
}

func (*fakeRuntimeServiceServer) PodSandboxStats(context.Context, *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	return &runtimeapi.PodSandboxStatsResponse{Stats: &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{Id: "sandbox-1"},
	}}, nil
}

func (c *fakeCRIClient) ListContainers(_ context.Context, _ *runtimeapi.ListContainersRequest, _ ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &runtimeapi.ListContainersResponse{Containers: c.containers}, nil
}

func (c *fakeCRIClient) ListPodSandbox(_ context.Context, req *runtimeapi.ListPodSandboxRequest, _ ...grpc.CallOption) (*runtimeapi.ListPodSandboxResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.listSandboxFilter = req.Filter
	return &runtimeapi.ListPodSandboxResponse{Items: c.sandboxes}, nil
}

func (c *fakeCRIClient) PodSandboxStats(_ context.Context, req *runtimeapi.PodSandboxStatsRequest, _ ...grpc.CallOption) (*runtimeapi.PodSandboxStatsResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.statsRequests = append(c.statsRequests, req.PodSandboxId)
	return &runtimeapi.PodSandboxStatsResponse{Stats: c.statsByID[req.PodSandboxId]}, nil
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
