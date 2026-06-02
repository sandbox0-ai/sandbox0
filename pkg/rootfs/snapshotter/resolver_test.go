package snapshotter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"google.golang.org/grpc"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestCRIMetadataResolverReadsRootFSAnnotationsFromPodSandboxStatus(t *testing.T) {
	store := &fakeContainerStore{
		containers: map[string]containers.Container{
			"container-a": {ID: "container-a", SandboxID: "cri-sandbox-a"},
		},
	}
	runtimeService := &fakeRuntimeService{
		status: &runtime.PodSandboxStatusResponse{
			Status: &runtime.PodSandboxStatus{
				Annotations: map[string]string{
					rootfs.AnnotationSandboxID: "sandbox-a",
					rootfs.AnnotationTeamID:    "team-a",
					rootfs.AnnotationMode:      rootfs.ModeS0FSUpperdir,
					rootfs.AnnotationVolumeID:  "rootfs-a",
					rootfs.AnnotationCtldPort:  "8095",
				},
			},
		},
	}
	resolver := CRIMetadataResolver{Containers: store, Runtime: runtimeService}

	meta, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "container-a")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveRootFSMetadata() ok = false, want true")
	}
	if runtimeService.request.PodSandboxId != "cri-sandbox-a" {
		t.Fatalf("CRI sandbox id = %q, want cri-sandbox-a", runtimeService.request.PodSandboxId)
	}
	if meta.SandboxID != "sandbox-a" || meta.TeamID != "team-a" || meta.VolumeID != "rootfs-a" || meta.CtldPort != 8095 {
		t.Fatalf("metadata = %+v, want sandbox-a team-a rootfs-a 8095", meta)
	}
	if store.namespace != defaultContainerdNamespace {
		t.Fatalf("container store namespace = %q, want %s", store.namespace, defaultContainerdNamespace)
	}
}

func TestCRIMetadataResolverFindsContainerBySnapshotKey(t *testing.T) {
	store := &fakeContainerStore{
		containers: map[string]containers.Container{
			"container-a": {ID: "container-a", SnapshotKey: "snapshot-key-a", SandboxID: "cri-sandbox-a"},
			"container-b": {ID: "container-b", SnapshotKey: "snapshot-key-b", SandboxID: "cri-sandbox-b"},
		},
	}
	runtimeService := &fakeRuntimeService{
		status: &runtime.PodSandboxStatusResponse{
			Status: &runtime.PodSandboxStatus{
				Annotations: map[string]string{
					rootfs.AnnotationSandboxID: "sandbox-a",
					rootfs.AnnotationTeamID:    "team-a",
					rootfs.AnnotationMode:      rootfs.ModeS0FSUpperdir,
					rootfs.AnnotationVolumeID:  "rootfs-a",
					rootfs.AnnotationCtldPort:  "8095",
				},
			},
		},
	}
	resolver := CRIMetadataResolver{Containers: store, Runtime: runtimeService}

	meta, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "snapshot-key-a")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveRootFSMetadata() ok = false, want true")
	}
	if !store.listCalled {
		t.Fatal("container store List() was not called")
	}
	if runtimeService.request.PodSandboxId != "cri-sandbox-a" {
		t.Fatalf("CRI sandbox id = %q, want cri-sandbox-a", runtimeService.request.PodSandboxId)
	}
	if meta.SandboxID != "sandbox-a" || meta.TeamID != "team-a" || meta.VolumeID != "rootfs-a" || meta.CtldPort != 8095 {
		t.Fatalf("metadata = %+v, want sandbox-a team-a rootfs-a 8095", meta)
	}
}

func TestCRIMetadataResolverNormalizesProxySnapshotKey(t *testing.T) {
	store := &fakeContainerStore{
		containers: map[string]containers.Container{
			"container-a": {ID: "container-a", SandboxID: "cri-sandbox-a"},
		},
	}
	runtimeService := &fakeRuntimeService{
		status: &runtime.PodSandboxStatusResponse{
			Status: &runtime.PodSandboxStatus{
				Annotations: map[string]string{
					rootfs.AnnotationSandboxID: "sandbox-a",
					rootfs.AnnotationTeamID:    "team-a",
					rootfs.AnnotationMode:      rootfs.ModeS0FSUpperdir,
					rootfs.AnnotationVolumeID:  "rootfs-a",
					rootfs.AnnotationCtldPort:  "8095",
				},
			},
		},
	}
	resolver := CRIMetadataResolver{Containers: store, Runtime: runtimeService}

	meta, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "k8s.io/42/container-a")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveRootFSMetadata() ok = false, want true")
	}
	if runtimeService.request.PodSandboxId != "cri-sandbox-a" {
		t.Fatalf("CRI sandbox id = %q, want cri-sandbox-a", runtimeService.request.PodSandboxId)
	}
	if meta.SandboxID != "sandbox-a" || meta.TeamID != "team-a" || meta.VolumeID != "rootfs-a" || meta.CtldPort != 8095 {
		t.Fatalf("metadata = %+v, want sandbox-a team-a rootfs-a 8095", meta)
	}
}

func TestCRIMetadataResolverFallsBackToCRIListContainers(t *testing.T) {
	store := &fakeContainerStore{err: errdefs.ErrNotFound}
	runtimeService := &fakeRuntimeService{
		status: &runtime.PodSandboxStatusResponse{
			Status: &runtime.PodSandboxStatus{
				Annotations: map[string]string{
					rootfs.AnnotationSandboxID: "sandbox-a",
					rootfs.AnnotationTeamID:    "team-a",
					rootfs.AnnotationMode:      rootfs.ModeS0FSUpperdir,
					rootfs.AnnotationVolumeID:  "rootfs-a",
					rootfs.AnnotationCtldPort:  "8095",
				},
			},
		},
		containers: &runtime.ListContainersResponse{
			Containers: []*runtime.Container{
				{
					Id:           "container-a",
					PodSandboxId: "cri-sandbox-a",
				},
			},
		},
	}
	resolver := CRIMetadataResolver{Containers: store, Runtime: runtimeService}

	meta, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "container-a")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveRootFSMetadata() ok = false, want true")
	}
	if runtimeService.containerRequest.GetFilter().GetId() != "container-a" {
		t.Fatalf("CRI container id = %q, want container-a", runtimeService.containerRequest.GetFilter().GetId())
	}
	if runtimeService.request.PodSandboxId != "cri-sandbox-a" {
		t.Fatalf("CRI sandbox id = %q, want cri-sandbox-a", runtimeService.request.PodSandboxId)
	}
	if meta.SandboxID != "sandbox-a" || meta.TeamID != "team-a" || meta.VolumeID != "rootfs-a" || meta.CtldPort != 8095 {
		t.Fatalf("metadata = %+v, want sandbox-a team-a rootfs-a 8095", meta)
	}
}

func TestSnapshotKeyCandidatesDeduplicatesPlainKey(t *testing.T) {
	got := snapshotKeyCandidates("container-a")
	want := []string{"container-a"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("snapshotKeyCandidates() = %#v, want %#v", got, want)
	}
}

func TestSnapshotKeyCandidatesIncludesProxySuffix(t *testing.T) {
	got := snapshotKeyCandidates("k8s.io/42/container-a")
	want := []string{"k8s.io/42/container-a", "container-a"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("snapshotKeyCandidates() = %#v, want %#v", got, want)
	}
}

func TestCRIMetadataResolverReadsSandboxContainerAnnotations(t *testing.T) {
	store := &fakeContainerStore{
		containers: map[string]containers.Container{
			"cri-sandbox-a": {ID: "cri-sandbox-a"},
		},
	}
	runtimeService := &fakeRuntimeService{
		status: &runtime.PodSandboxStatusResponse{
			Status: &runtime.PodSandboxStatus{
				Annotations: map[string]string{
					rootfs.AnnotationSandboxID: "sandbox-a",
					rootfs.AnnotationTeamID:    "team-a",
					rootfs.AnnotationMode:      rootfs.ModeS0FSUpperdir,
					rootfs.AnnotationVolumeID:  "rootfs-a",
					rootfs.AnnotationCtldPort:  "8095",
				},
			},
		},
	}
	resolver := CRIMetadataResolver{Containers: store, Runtime: runtimeService}

	meta, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "cri-sandbox-a")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveRootFSMetadata() ok = false, want true")
	}
	if runtimeService.request.PodSandboxId != "cri-sandbox-a" {
		t.Fatalf("CRI sandbox id = %q, want cri-sandbox-a", runtimeService.request.PodSandboxId)
	}
	if meta.SandboxID != "sandbox-a" || meta.TeamID != "team-a" || meta.VolumeID != "rootfs-a" || meta.CtldPort != 8095 {
		t.Fatalf("metadata = %+v, want sandbox-a team-a rootfs-a 8095", meta)
	}
}

func TestCRIMetadataResolverNoopsWhenContainerIsNotCreatedYet(t *testing.T) {
	resolver := CRIMetadataResolver{
		Containers: &fakeContainerStore{err: errdefs.ErrNotFound},
		Runtime:    &fakeRuntimeService{},
	}

	meta, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "container-a")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if ok || meta != (rootfs.Metadata{}) {
		t.Fatalf("ResolveRootFSMetadata() = %+v, %v; want empty false", meta, ok)
	}
}

func TestCRIMetadataResolverNoopsForContainerWithoutSandbox(t *testing.T) {
	resolver := CRIMetadataResolver{
		Containers: &fakeContainerStore{containers: map[string]containers.Container{
			"sandbox-container": {ID: "sandbox-container"},
		}},
		Runtime: &fakeRuntimeService{},
	}

	_, ok, err := resolver.ResolveRootFSMetadata(context.Background(), "sandbox-container")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if ok {
		t.Fatal("ResolveRootFSMetadata() ok = true, want false")
	}
}

func TestCRIMetadataResolverReturnsCRIError(t *testing.T) {
	resolver := CRIMetadataResolver{
		Containers: &fakeContainerStore{containers: map[string]containers.Container{
			"container-a": {ID: "container-a", SandboxID: "cri-sandbox-a"},
		}},
		Runtime: &fakeRuntimeService{err: errors.New("cri unavailable")},
	}

	_, _, err := resolver.ResolveRootFSMetadata(context.Background(), "container-a")
	if err == nil || !strings.Contains(err.Error(), "cri unavailable") {
		t.Fatalf("ResolveRootFSMetadata() error = %v, want cri unavailable", err)
	}
}

func TestCRIMetadataResolverReturnsContainerListError(t *testing.T) {
	resolver := CRIMetadataResolver{
		Containers: &fakeContainerStore{err: errdefs.ErrNotFound, listErr: errors.New("container list unavailable")},
		Runtime:    &fakeRuntimeService{},
	}

	_, _, err := resolver.ResolveRootFSMetadata(context.Background(), "snapshot-key-a")
	if err == nil || !strings.Contains(err.Error(), "container list unavailable") {
		t.Fatalf("ResolveRootFSMetadata() error = %v, want container list unavailable", err)
	}
}

func TestCRIMetadataResolverUsesExistingNamespace(t *testing.T) {
	store := &fakeContainerStore{err: errdefs.ErrNotFound}
	resolver := CRIMetadataResolver{Containers: store, Runtime: &fakeRuntimeService{}, Namespace: "fallback"}

	_, ok, err := resolver.ResolveRootFSMetadata(namespaces.WithNamespace(context.Background(), "custom"), "extract-key")
	if err != nil {
		t.Fatalf("ResolveRootFSMetadata() error = %v", err)
	}
	if ok {
		t.Fatal("ResolveRootFSMetadata() ok = true, want false")
	}
	if store.namespace != "custom" {
		t.Fatalf("container store namespace = %q, want custom", store.namespace)
	}
}

type fakeContainerStore struct {
	containers map[string]containers.Container
	err        error
	listErr    error
	namespace  string
	listCalled bool
}

func (s *fakeContainerStore) Get(ctx context.Context, id string) (containers.Container, error) {
	s.namespace, _ = namespaces.Namespace(ctx)
	if s.err != nil {
		return containers.Container{}, s.err
	}
	container, ok := s.containers[id]
	if !ok {
		return containers.Container{}, errdefs.ErrNotFound
	}
	return container, nil
}

func (s *fakeContainerStore) List(ctx context.Context, _ ...string) ([]containers.Container, error) {
	s.namespace, _ = namespaces.Namespace(ctx)
	s.listCalled = true
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]containers.Container, 0, len(s.containers))
	for _, container := range s.containers {
		out = append(out, container)
	}
	return out, nil
}

type fakeRuntimeService struct {
	request          *runtime.PodSandboxStatusRequest
	status           *runtime.PodSandboxStatusResponse
	err              error
	containerRequest *runtime.ListContainersRequest
	containers       *runtime.ListContainersResponse
	containerErr     error
}

func (s *fakeRuntimeService) PodSandboxStatus(_ context.Context, req *runtime.PodSandboxStatusRequest, _ ...grpc.CallOption) (*runtime.PodSandboxStatusResponse, error) {
	s.request = req
	if s.err != nil {
		return nil, s.err
	}
	if s.status == nil {
		return nil, errdefs.ErrNotFound
	}
	return s.status, nil
}

func (s *fakeRuntimeService) ListContainers(_ context.Context, req *runtime.ListContainersRequest, _ ...grpc.CallOption) (*runtime.ListContainersResponse, error) {
	s.containerRequest = req
	if s.containerErr != nil {
		return nil, s.containerErr
	}
	if s.containers == nil {
		return nil, errdefs.ErrNotFound
	}
	return s.containers, nil
}
