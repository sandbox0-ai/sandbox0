package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
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

func TestRootFSS0FSMaterializerReadsInheritedSegments(t *testing.T) {
	baseStore := objectstore.NewMemoryStore("rootfs-s0fs-inherited-segments-" + t.Name())
	sourceStore := rootFSS0FSObjectStore(baseStore, "team-1", "source")
	require.NoError(t, sourceStore.Put("segments/source-1.bin", bytes.NewReader([]byte("parent-data"))))

	materializer := rootFSS0FSMaterializer(baseStore, "team-1", "fork")
	got, err := materializer.ReadSegmentRange(&s0fs.Segment{
		ID:       "source-1",
		VolumeID: "source",
		Key:      "segments/source-1.bin",
		Length:   uint64(len("parent-data")),
	}, 0, int64(len("parent-data")))

	require.NoError(t, err)
	assert.Equal(t, "parent-data", string(got))
}

func TestCommitS0FSRootFSUsesActiveMountEngine(t *testing.T) {
	ctx := context.Background()
	baseStore := objectstore.NewMemoryStore("rootfs-s0fs-active-mount-" + t.Name())
	teamID := "team-1"
	sourceID := "source"
	childID := "child"

	source, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:             sourceID,
		WALPath:              filepath.Join(t.TempDir(), "source.wal"),
		ObjectStore:          rootFSS0FSObjectStore(baseStore, teamID, sourceID),
		ObjectStoreForVolume: rootFSS0FSObjectStoreResolver(baseStore, teamID),
	})
	require.NoError(t, err)
	node, err := source.CreateFile(s0fs.RootInode, "state.txt", 0o644)
	require.NoError(t, err)
	_, err = source.Write(node.Inode, 0, []byte("parent-data"))
	require.NoError(t, err)
	sourceManifest, err := source.EnsureMaterialized(ctx)
	require.NoError(t, err)
	require.NoError(t, source.Close())

	sourceHead, err := rootFSS0FSHeadFromManifest(teamID, sourceID, sourceManifest)
	require.NoError(t, err)
	sourceState, _, err := loadRootFSS0FSHead(ctx, baseStore, sourceHead)
	require.NoError(t, err)

	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{RootFSCacheDir: t.TempDir()})
	child, err := runtime.openRootFSS0FSEngine(ctx, baseStore, teamID, childID)
	require.NoError(t, err)
	require.NoError(t, child.ReplaceState(sourceState))
	childNode, err := child.Lookup(s0fs.RootInode, "state.txt")
	require.NoError(t, err)
	require.NoError(t, child.Truncate(childNode.Inode, 0))
	_, err = child.Write(childNode.Inode, 0, []byte("child-data"))
	require.NoError(t, err)
	childNode, err = child.Lookup(s0fs.RootInode, "state.txt")
	require.NoError(t, err)

	runtime.s0fsMounts[rootFSS0FSMountKey(ctldapi.RootFSInfo{ContainerID: "container-1"})] = &s0fsRootFSMount{
		volumeID: childID,
		teamID:   teamID,
		engine:   child,
	}
	head, err := runtime.CommitS0FSRootFS(ctx, S0FSCommitRequest{
		Store:        baseStore,
		TeamID:       teamID,
		FilesystemID: childID,
		Info:         ctldapi.RootFSInfo{ContainerID: "container-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, childID, head.FilesystemID)
	assert.Equal(t, childID, head.VolumeID)
	assert.NotEmpty(t, head.ManifestKey)

	childState, _, err := loadRootFSS0FSHead(ctx, baseStore, head)
	require.NoError(t, err)
	reader := s0fs.NewSnapshotReader(childState, rootFSS0FSMaterializer(baseStore, teamID, childID))
	payload, err := reader.Read(childNode.Inode, 0, childNode.Size)
	require.NoError(t, err)
	assert.Equal(t, "child-data", string(payload))
}

func TestAttachS0FSRootFSGVisorUsesRootFSPortal(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore("rootfs-gvisor-attach-" + t.Name())
	teamID := "team-1"
	sourceID := "source"
	targetID := "target"
	sourceHead := writeTestRootFSHead(t, ctx, store, teamID, sourceID)

	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	taskRoot := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID)
	require.NoError(t, os.MkdirAll(filepath.Join(taskRoot, "rootfs", "bin"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "rootfs", "bin", "sh"), []byte("#!/bin/sh\n"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "init.pid"), []byte("123456"), 0o644))

	attacher := &recordingRootFSPortalAttacher{
		response: portal.RootFSPortalSessionResponse{
			MountPath:  volumeportal.RootFSMountPath,
			TargetPath: filepath.Join(t.TempDir(), "rootfs-target"),
		},
	}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{
		ContainerdRoot:     containerdRoot,
		ContainerdHostRoot: containerdHostRoot,
		RootFSCacheDir:     t.TempDir(),
	})
	runtime.SetRootFSPortalAttacher(attacher)

	head, mountPath, err := runtime.AttachS0FSRootFS(ctx, S0FSAttachRequest{
		Info: ctldapi.RootFSInfo{
			Runtime:       "gvisor",
			ContainerID:   containerID,
			ContainerName: "procd",
			PodNamespace:  "tpl-default",
			PodName:       "pod-1",
			PodUID:        "pod-uid",
		},
		Store:        store,
		FilesystemID: targetID,
		Head:         sourceHead,
	})
	require.NoError(t, err)
	assert.Equal(t, targetID, head.FilesystemID)
	assert.Equal(t, volumeportal.RootFSMountPath, mountPath)
	assert.Equal(t, "pod-uid", attacher.attach.PodUID)
	assert.NotNil(t, attacher.attach.Session)
	binNode, err := attacher.attach.Session.Lookup(ctx, &pb.LookupRequest{
		VolumeId: targetID,
		Parent:   s0fs.RootInode,
		Name:     "bin",
	})
	require.NoError(t, err)
	_, err = attacher.attach.Session.Lookup(ctx, &pb.LookupRequest{
		VolumeId: targetID,
		Parent:   binNode.Inode,
		Name:     "sh",
	})
	require.NoError(t, err)

	active := runtime.s0fsMountByPodUID("pod-uid")
	require.NotNil(t, active)
	assert.True(t, active.rootFSPortalAttach)
	assert.Equal(t, filepath.Join(taskRoot, "rootfs"), active.mountRootPath)
	assert.Equal(t, attacher.response.TargetPath, active.hostMountPath)
}

func TestAttachS0FSRootFSGVisorRejectsEmptyBaseRoot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore("rootfs-gvisor-empty-base-" + t.Name())
	teamID := "team-1"
	sourceID := "source"
	targetID := "target"
	sourceHead := writeTestRootFSHead(t, ctx, store, teamID, sourceID)

	containerdRoot := t.TempDir()
	containerdHostRoot := filepath.Join(string(filepath.Separator), "run", "containerd")
	containerID := "container-1"
	taskRoot := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", "k8s.io", containerID)
	require.NoError(t, os.MkdirAll(filepath.Join(taskRoot, "rootfs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(taskRoot, "init.pid"), []byte("123456"), 0o644))

	attacher := &recordingRootFSPortalAttacher{
		response: portal.RootFSPortalSessionResponse{
			MountPath:  volumeportal.RootFSMountPath,
			TargetPath: filepath.Join(t.TempDir(), "rootfs-target"),
		},
	}
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{
		ContainerdRoot:     containerdRoot,
		ContainerdHostRoot: containerdHostRoot,
		RootFSCacheDir:     t.TempDir(),
	})
	runtime.SetRootFSPortalAttacher(attacher)

	_, _, err := runtime.AttachS0FSRootFS(ctx, S0FSAttachRequest{
		Info: ctldapi.RootFSInfo{
			Runtime:       "gvisor",
			ContainerID:   containerID,
			ContainerName: "procd",
			PodNamespace:  "tpl-default",
			PodName:       "pod-1",
			PodUID:        "pod-uid",
		},
		Store:        store,
		FilesystemID: targetID,
		Head:         sourceHead,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
	assert.Contains(t, err.Error(), "is empty")
	assert.Nil(t, attacher.attach.Session)
	assert.Nil(t, runtime.s0fsMountByPodUID("pod-uid"))
}

func writeTestRootFSHead(t *testing.T, ctx context.Context, store objectstore.Store, teamID, sourceID string) ctldapi.RootFSHeadDescriptor {
	t.Helper()

	source, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:             sourceID,
		WALPath:              filepath.Join(t.TempDir(), "source.wal"),
		ObjectStore:          rootFSS0FSObjectStore(store, teamID, sourceID),
		ObjectStoreForVolume: rootFSS0FSObjectStoreResolver(store, teamID),
	})
	require.NoError(t, err)
	node, err := source.CreateFile(s0fs.RootInode, "restored.txt", 0o644)
	require.NoError(t, err)
	_, err = source.Write(node.Inode, 0, []byte("restored"))
	require.NoError(t, err)
	sourceManifest, err := source.EnsureMaterialized(ctx)
	require.NoError(t, err)
	require.NoError(t, source.Close())
	sourceHead, err := rootFSS0FSHeadFromManifest(teamID, sourceID, sourceManifest)
	require.NoError(t, err)
	return sourceHead
}

func TestTakeS0FSMountFallsBackToPodUID(t *testing.T) {
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{})
	active := &s0fsRootFSMount{
		key:    "old-container",
		podUID: "pod-1",
	}
	runtime.s0fsMounts[active.key] = active
	runtime.s0fsMountsByPodUID[active.podUID] = active

	got := runtime.takeS0FSMount(ctldapi.RootFSInfo{
		ContainerID: "new-container",
		PodUID:      "pod-1",
	})

	require.Same(t, active, got)
	assert.Empty(t, runtime.s0fsMounts)
	assert.Empty(t, runtime.s0fsMountsByPodUID)
}

type recordingRootFSPortalAttacher struct {
	response portal.RootFSPortalSessionResponse
	attach   portal.RootFSPortalSessionRequest
	detach   string
}

func (a *recordingRootFSPortalAttacher) AttachRootFSPortalSession(_ context.Context, req portal.RootFSPortalSessionRequest) (portal.RootFSPortalSessionResponse, error) {
	a.attach = req
	return a.response, nil
}

func (a *recordingRootFSPortalAttacher) DetachRootFSPortalSession(_ context.Context, podUID string) error {
	a.detach = podUID
	return nil
}

func TestS0FSRootFSVolumePortalPaths(t *testing.T) {
	active := &s0fsRootFSMount{
		mountRootPath:      "/proc/123/root",
		containerMountPath: "/sandbox0/rootfs",
	}

	source, target := active.rootFSVolumePortalPaths("/workspace/data")

	assert.Equal(t, "/proc/123/root/workspace/data", source)
	assert.Equal(t, "/proc/123/root/sandbox0/rootfs/workspace/data", target)
	assert.Equal(t, "/sandbox0/rootfs/workspace/data", rootFSNestedMountPath("/sandbox0/rootfs", "/workspace/data"))
}

func TestRootFSRuntimeBindMountPathsCollapseRecursiveMounts(t *testing.T) {
	got := rootFSRuntimeBindMountPaths([]string{
		"/proc",
		"/proc/bus",
		"/dev",
		"/dev/pts",
		"/dev/shm",
		"/sys",
		"/sys/fs/cgroup",
		"/etc/hosts",
		"/etc/resolv.conf",
		"/workspace/data",
		"/sandbox0/rootfs",
		"/sandbox0/rootfs/dev",
		"/sandbox0/rootfs/proc",
		"/dev",
	}, "/sandbox0/rootfs")

	assert.Equal(t, []string{
		"/dev",
		"/proc",
		"/sys",
		"/etc/hosts",
		"/etc/resolv.conf",
		"/workspace/data",
	}, got)
}

func TestFilterRootFSRuntimeBindMountPathsSkipsUnboundPortals(t *testing.T) {
	got := filterRootFSRuntimeBindMountPaths([]string{
		"/dev",
		"/workspace",
		"/tmp/cache",
		"/var/run",
	}, []ctldapi.RootFSPortalPath{
		{MountPath: "/workspace/data", BackingPath: "/portal/workspace-data"},
		{MountPath: "/tmp/cache", BackingPath: "/portal/cache"},
	})

	assert.Equal(t, []string{"/dev", "/var/run"}, got)
}

func TestMergeS0FSRootFSPortalPathsImportsBackingDir(t *testing.T) {
	baseRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(baseRoot, "workspace", "data"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseRoot, "workspace", "data", "old.txt"), []byte("old"), 0o644))
	state, err := s0fs.ImportHostTree(context.Background(), baseRoot, s0fs.HostImportOptions{})
	require.NoError(t, err)

	backing := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(backing, "state.txt"), []byte("portal"), 0o644))

	merged, err := mergeS0FSRootFSPortalPaths(context.Background(), state, []ctldapi.RootFSPortalPath{{
		MountPath:   "/workspace/data",
		BackingPath: backing,
	}})
	require.NoError(t, err)

	reader := s0fs.NewSnapshotReader(merged, nil)
	file := requireSnapshotPath(t, merged, "/workspace/data/state.txt")
	payload, err := reader.Read(file, 0, uint64(len("portal")))
	require.NoError(t, err)
	assert.Equal(t, "portal", string(payload))
	assert.Zero(t, snapshotPath(merged, "/workspace/data/old.txt"))
}

func TestRootFSRuntimeBindPathsTargetNestedRootFS(t *testing.T) {
	source, target := rootFSRuntimeBindPaths("/sandbox0/rootfs", "/dev")

	assert.Equal(t, "/dev", source)
	assert.Equal(t, "/sandbox0/rootfs/dev", target)
}

func TestPrepareRuntimeRootFSBindTargetCreatesDirectoryAndFileTargets(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source-dir")
	sourceFile := filepath.Join(root, "source-file")
	targetDir := filepath.Join(root, "target-dir")
	targetFile := filepath.Join(root, "nested", "target-file")
	require.NoError(t, os.Mkdir(sourceDir, 0o755))
	require.NoError(t, os.WriteFile(sourceFile, []byte("source"), 0o644))

	require.NoError(t, prepareRuntimeRootFSBindTarget(sourceDir, targetDir))
	require.NoError(t, prepareRuntimeRootFSBindTarget(sourceFile, targetFile))

	dirInfo, err := os.Stat(targetDir)
	require.NoError(t, err)
	assert.True(t, dirInfo.IsDir())
	fileInfo, err := os.Stat(targetFile)
	require.NoError(t, err)
	assert.False(t, fileInfo.IsDir())
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

func TestRootFSImportExcludedPathsIncludesProcessMounts(t *testing.T) {
	mountInfo := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(mountInfo, []byte(strings.Join([]string{
		"36 25 0:32 / / rw,relatime - overlay overlay rw",
		"37 36 0:33 / /proc rw,nosuid,nodev,noexec - proc proc rw",
		"38 36 0:34 / /workspace rw,relatime - tmpfs tmpfs rw",
		"39 36 0:35 / /etc/resolv.conf rw,relatime - tmpfs tmpfs rw",
		"40 36 0:36 / /path\\040with\\040space rw,relatime - tmpfs tmpfs rw",
		"",
	}, "\n")), 0o644))

	got := rootFSImportExcludedPaths([]string{"/workspace/data", "/proc"}, mountInfo)

	assert.ElementsMatch(t, []string{
		"/.wh..wh..opq",
		"/procd",
		"/workspace/data",
		"/proc",
		"/workspace",
		"/etc/resolv.conf",
		"/path with space",
	}, got)
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

func requireSnapshotPath(t *testing.T, state *s0fs.SnapshotState, target string) uint64 {
	t.Helper()
	inode := snapshotPath(state, target)
	require.NotZero(t, inode, "missing snapshot path %s", target)
	return inode
}

func snapshotPath(state *s0fs.SnapshotState, target string) uint64 {
	if state == nil {
		return 0
	}
	current := s0fs.RootInode
	for _, part := range strings.Split(strings.Trim(target, "/"), "/") {
		if part == "" {
			continue
		}
		current = state.Children[current][part]
		if current == 0 {
			return 0
		}
	}
	return current
}
