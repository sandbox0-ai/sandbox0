package processrecovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/procdstate"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeRuntimeService struct {
	mu         sync.Mutex
	containers []*runtimeapi.Container
	stopIDs    []string
	stopErr    error
	listCalls  int
}

func (f *fakeRuntimeService) ListContainers(context.Context, *runtimeapi.ListContainersRequest, ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	return &runtimeapi.ListContainersResponse{Containers: append([]*runtimeapi.Container(nil), f.containers...)}, nil
}

func (f *fakeRuntimeService) StopContainer(_ context.Context, req *runtimeapi.StopContainerRequest, _ ...grpc.CallOption) (*runtimeapi.StopContainerResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopIDs = append(f.stopIDs, req.GetContainerId())
	return &runtimeapi.StopContainerResponse{}, f.stopErr
}

func TestRecoverSkipsHealthyPodWithoutPendingRecovery(t *testing.T) {
	fake := &fakeRuntimeService{}
	recoverer := New(Config{
		KubeletPodsRoot: t.TempDir(),
		PendingDir:      filepath.Join(t.TempDir(), "pending"),
		RuntimeService:  fake,
	})
	if err := recoverer.Recover(context.Background(), testTarget(), false); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if fake.listCalls != 0 {
		t.Fatalf("ListContainers calls = %d, want 0", fake.listCalls)
	}
}

func TestRecoverMarksStateAndStopsCurrentProcd(t *testing.T) {
	recoverer, fake, markerPath, pendingPath := newRecovererFixture(t, "container-old")
	if err := recoverer.Recover(context.Background(), testTarget(), true); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(fake.stopIDs) != 1 || fake.stopIDs[0] != "container-old" {
		t.Fatalf("stopped containers = %v, want [container-old]", fake.stopIDs)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("recovery marker does not exist: %v", err)
	}
	if _, err := os.Stat(pendingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending recovery still exists: %v", err)
	}
}

func TestRecoverRetriesPendingStop(t *testing.T) {
	recoverer, fake, _, pendingPath := newRecovererFixture(t, "container-old")
	fake.stopErr = errors.New("temporary CRI failure")
	if err := recoverer.Recover(context.Background(), testTarget(), true); err == nil {
		t.Fatal("Recover() error = nil, want CRI failure")
	}
	if _, err := os.Stat(pendingPath); err != nil {
		t.Fatalf("pending recovery does not exist: %v", err)
	}

	fake.stopErr = nil
	if err := recoverer.Recover(context.Background(), testTarget(), false); err != nil {
		t.Fatalf("Recover() retry error = %v", err)
	}
	if len(fake.stopIDs) != 2 {
		t.Fatalf("stop calls = %v, want two attempts", fake.stopIDs)
	}
	if fake.listCalls != 1 {
		t.Fatalf("list calls = %d, want one shared CRI snapshot", fake.listCalls)
	}
	if _, err := os.Stat(pendingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending recovery still exists after retry: %v", err)
	}
}

func TestRecoverDoesNotStopReplacementContainerForOldPendingRecord(t *testing.T) {
	recoverer, fake, _, pendingPath := newRecovererFixture(t, "container-old")
	fake.stopErr = errors.New("stop interrupted")
	if err := recoverer.Recover(context.Background(), testTarget(), true); err == nil {
		t.Fatal("Recover() error = nil, want interrupted stop")
	}
	fake.stopErr = nil
	fake.containers = []*runtimeapi.Container{testContainer("container-new")}
	replacementRecoverer := New(Config{
		KubeletPodsRoot: recoverer.kubeletPodsRoot,
		PendingDir:      recoverer.pendingDir,
		RuntimeService:  fake,
	})
	if err := replacementRecoverer.Recover(context.Background(), testTarget(), false); err != nil {
		t.Fatalf("Recover() replacement check error = %v", err)
	}
	if len(fake.stopIDs) != 1 {
		t.Fatalf("stop calls = %v, replacement container must not be stopped", fake.stopIDs)
	}
	if _, err := os.Stat(pendingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending recovery still exists: %v", err)
	}
}

func TestRecoverWritesMarkerWhenContainerAlreadyStopped(t *testing.T) {
	recoverer, fake, markerPath, _ := newRecovererFixture(t, "")
	if err := recoverer.Recover(context.Background(), testTarget(), true); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(fake.stopIDs) != 0 {
		t.Fatalf("stop calls = %v, want none", fake.stopIDs)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("recovery marker does not exist: %v", err)
	}
}

func TestRecoverReplacesIdleProcdWithoutRequestingProcessReplay(t *testing.T) {
	recoverer, fake, markerPath, _ := newRecovererFixture(t, "container-idle")
	target := testTarget()
	target.ReplayProcesses = false
	if err := recoverer.Recover(context.Background(), target, true); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(fake.stopIDs) != 1 || fake.stopIDs[0] != "container-idle" {
		t.Fatalf("stopped containers = %v, want [container-idle]", fake.stopIDs)
	}
	if _, err := os.Stat(markerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("idle recovery marker exists: %v", err)
	}
}

func TestRecoverSharesCRIContainerSnapshotAcrossConcurrentPods(t *testing.T) {
	kubeletRoot := t.TempDir()
	pendingDir := filepath.Join(t.TempDir(), "pending")
	fake := &fakeRuntimeService{}
	targets := make([]Target, 32)
	for i := range targets {
		target := testTarget()
		target.PodName = fmt.Sprintf("sandbox-%d", i)
		target.PodUID = fmt.Sprintf("pod-%d", i)
		targets[i] = target
		fake.containers = append(fake.containers, testContainerForTarget(fmt.Sprintf("container-%d", i), target))
		stateMount := filepath.Join(kubeletRoot, target.PodUID, "volumes", kubeletCSIVolumeDir, target.StateVolumeName, "mount")
		if err := os.MkdirAll(stateMount, 0o700); err != nil {
			t.Fatalf("create state mount: %v", err)
		}
	}
	recoverer := New(Config{KubeletPodsRoot: kubeletRoot, PendingDir: pendingDir, RuntimeService: fake})
	errs := make(chan error, len(targets))
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(target Target) {
			defer wg.Done()
			errs <- recoverer.Recover(context.Background(), target, true)
		}(target)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Recover() error = %v", err)
		}
	}
	if fake.listCalls != 1 {
		t.Fatalf("list calls = %d, want 1", fake.listCalls)
	}
	if len(fake.stopIDs) != len(targets) {
		t.Fatalf("stop calls = %d, want %d", len(fake.stopIDs), len(targets))
	}
}

func newRecovererFixture(t *testing.T, containerID string) (*Recoverer, *fakeRuntimeService, string, string) {
	t.Helper()
	kubeletRoot := t.TempDir()
	pendingDir := filepath.Join(t.TempDir(), "pending")
	target := testTarget()
	stateMount := filepath.Join(kubeletRoot, target.PodUID, "volumes", kubeletCSIVolumeDir, target.StateVolumeName, "mount")
	if err := os.MkdirAll(stateMount, 0o700); err != nil {
		t.Fatalf("create state mount: %v", err)
	}
	fake := &fakeRuntimeService{}
	if containerID != "" {
		fake.containers = []*runtimeapi.Container{testContainer(containerID)}
	}
	return New(Config{
		KubeletPodsRoot: kubeletRoot,
		PendingDir:      pendingDir,
		RuntimeService:  fake,
	}), fake, filepath.Join(stateMount, procdstate.RecoveryRequestFilename), filepath.Join(pendingDir, target.PodUID+".json")
}

func testTarget() Target {
	return Target{
		Namespace:       "sandbox0",
		PodName:         "sandbox-pod",
		PodUID:          "pod-uid",
		ContainerName:   "procd",
		StateVolumeName: "sandbox0-volume-0-sandbox0-webhook-state",
		ReplayProcesses: true,
	}
}

func testContainer(id string) *runtimeapi.Container {
	return testContainerForTarget(id, testTarget())
}

func testContainerForTarget(id string, target Target) *runtimeapi.Container {
	return &runtimeapi.Container{
		Id:       id,
		Metadata: &runtimeapi.ContainerMetadata{Name: target.ContainerName},
		State:    runtimeapi.ContainerState_CONTAINER_RUNNING,
		Labels: map[string]string{
			"io.kubernetes.pod.namespace": target.Namespace,
			"io.kubernetes.pod.name":      target.PodName,
			"io.kubernetes.pod.uid":       target.PodUID,
		},
	}
}
