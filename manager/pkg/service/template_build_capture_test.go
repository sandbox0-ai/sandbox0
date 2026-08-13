package service

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnsureTemplateBuildCaptureExportsPinnedV3Head(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	objects := objectstore.NewMemoryStore(t.Name())
	pinned := templateCaptureHeadFixture(t, objects, "team-1", "sandbox-1", "head-pinned", "arm64", "v8")
	newer := templateCaptureHeadFixture(t, objects, "team-1", "sandbox-1", "head-newer", "amd64", "")
	store := &templateCaptureMemoryStore{
		memorySandboxStore: &memorySandboxStore{
			records: map[string]*sandboxstore.SandboxRecord{
				"sandbox-1": {ID: "sandbox-1", TeamID: "team-1", DesiredState: sandboxstore.SandboxDesiredStatePaused},
			},
			rootFSSnapshots: map[string]*sandboxstore.RootFSSnapshot{
				"template-build-1": {
					ID: "template-build-1", FilesystemID: "filesystem-1", TeamID: "team-1",
					SourceSandboxID: "sandbox-1", HeadID: pinned.Reference.HeadID, CreatedAt: now,
				},
			},
		},
		heads: map[string]*sandboxstore.SandboxRootFSHead{
			pinned.Reference.HeadID: pinned,
			newer.Reference.HeadID:  newer,
		},
		exports: make(map[string]*sandboxstore.RootFSExport),
	}
	service := &SandboxService{sandboxStore: store, rootFSObjectStore: objects, clock: systemTime{}}

	capture, err := service.EnsureTemplateBuildCapture(
		context.Background(),
		"sandbox-1",
		"team-1",
		"template-build-1",
		v1alpha1.SandboxTemplateSpec{},
	)
	if err != nil {
		t.Fatalf("EnsureTemplateBuildCapture() error = %v", err)
	}
	if capture.HeadID != pinned.Reference.HeadID || len(capture.Layers) != 1 || capture.Layers[0].ID != pinned.Reference.HeadID {
		t.Fatalf("capture followed mutable sandbox Head: %#v", capture)
	}
	if capture.Platform.Architecture != "arm64" || capture.Platform.Variant != "v8" {
		t.Fatalf("capture platform = %#v, want pinned arm64/v8", capture.Platform)
	}
	if capture.Layers[0].MediaType != rootfshead.ExportLayerMediaType || capture.Layers[0].DiffID == "" {
		t.Fatalf("capture layer = %#v, want v3 Head OCI export", capture.Layers[0])
	}
	if !capture.CapturedAt.Equal(now) {
		t.Fatalf("capture time = %v, want %v", capture.CapturedAt, now)
	}
	if store.exports[pinned.Reference.HeadID] == nil {
		t.Fatal("v3 Head export was not persisted")
	}

	second, err := service.EnsureTemplateBuildCapture(
		context.Background(), "sandbox-1", "team-1", "template-build-1", v1alpha1.SandboxTemplateSpec{},
	)
	if err != nil {
		t.Fatalf("second EnsureTemplateBuildCapture() error = %v", err)
	}
	if second.Layers[0] != capture.Layers[0] {
		t.Fatalf("durable export was not reused: first=%#v second=%#v", capture.Layers[0], second.Layers[0])
	}
}

func TestEnsureTemplateBuildCaptureRejectsMissingV3Head(t *testing.T) {
	t.Parallel()

	objects := objectstore.NewMemoryStore(t.Name())
	store := &templateCaptureMemoryStore{
		memorySandboxStore: &memorySandboxStore{
			records: map[string]*sandboxstore.SandboxRecord{
				"sandbox-1": {ID: "sandbox-1", TeamID: "team-1", DesiredState: sandboxstore.SandboxDesiredStatePaused},
			},
			rootFSSnapshots: map[string]*sandboxstore.RootFSSnapshot{
				"template-build-1": {
					ID: "template-build-1", TeamID: "team-1", SourceSandboxID: "sandbox-1",
					HeadID: "missing-head", CreatedAt: time.Unix(100, 0).UTC(),
				},
			},
		},
		heads:   make(map[string]*sandboxstore.SandboxRootFSHead),
		exports: make(map[string]*sandboxstore.RootFSExport),
	}
	service := &SandboxService{sandboxStore: store, rootFSObjectStore: objects, clock: systemTime{}}

	_, err := service.EnsureTemplateBuildCapture(
		context.Background(), "sandbox-1", "team-1", "template-build-1", v1alpha1.SandboxTemplateSpec{},
	)
	if !errors.Is(err, errTemplateBuildCaptureInvalid) {
		t.Fatalf("EnsureTemplateBuildCapture() error = %v, want invalid capture", err)
	}
}

func TestRootFSPlatformForPodUsesActualNodeLabels(t *testing.T) {
	t.Parallel()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-a",
			Labels: map[string]string{
				corev1.LabelOSStable:       "linux",
				corev1.LabelArchStable:     "arm64",
				rootFSPlatformVariantLabel: "v8",
			},
		},
	}
	service := &SandboxService{nodeLister: newClaimTestNodeLister(t, node)}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			NodeSelector: map[string]string{
				corev1.LabelArchStable: "amd64",
			},
		},
	}
	platform := service.rootFSPlatformForPod(pod)
	if platform.OS != "linux" || platform.Architecture != "arm64" || platform.Variant != "v8" {
		t.Fatalf("rootFSPlatformForPod() = %#v, want actual node linux/arm64/v8", platform)
	}
}

type templateCaptureMemoryStore struct {
	*memorySandboxStore
	heads   map[string]*sandboxstore.SandboxRootFSHead
	exports map[string]*sandboxstore.RootFSExport
}

func (s *templateCaptureMemoryStore) GetRootFSHeadByID(_ context.Context, headID, teamID string) (*sandboxstore.SandboxRootFSHead, error) {
	head := s.heads[headID]
	if head == nil || head.TeamID != teamID {
		return nil, nil
	}
	copy := *head
	return &copy, nil
}

func (s *templateCaptureMemoryStore) GetRootFSExport(_ context.Context, headID, teamID string) (*sandboxstore.RootFSExport, error) {
	export := s.exports[headID]
	if export == nil || export.TeamID != teamID {
		return nil, nil
	}
	copy := *export
	return &copy, nil
}

func (s *templateCaptureMemoryStore) SaveRootFSExport(_ context.Context, export *sandboxstore.RootFSExport) error {
	if existing := s.exports[export.HeadID]; existing != nil {
		if *existing == *export {
			return nil
		}
		return sandboxstore.ErrRootFSHeadConflict
	}
	copy := *export
	s.exports[export.HeadID] = &copy
	return nil
}

func (*templateCaptureMemoryStore) AcquireRootFSWriteLease(context.Context, string, string, time.Duration) error {
	return nil
}

func (*templateCaptureMemoryStore) ReleaseRootFSWriteLease(context.Context, string, string) error {
	return nil
}

func templateCaptureHeadFixture(
	t *testing.T,
	store objectstore.Store,
	teamID, sandboxID, headID, architecture, variant string,
) *sandboxstore.SandboxRootFSHead {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		t.Fatalf("TeamObjectPrefix() error = %v", err)
	}
	put := func(mediaType string, payload []byte) rootfshead.Object {
		digestValue := digest.FromBytes(payload)
		key, err := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		if err != nil {
			t.Fatalf("ObjectKey() error = %v", err)
		}
		if err := store.Put(key, bytes.NewReader(payload)); err != nil {
			t.Fatalf("store fixture object: %v", err)
		}
		return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	}
	indexPayload, err := rootfshead.EncodeDirectoryIndex(rootfshead.DirectoryIndex{Version: rootfshead.Version})
	if err != nil {
		t.Fatalf("EncodeDirectoryIndex() error = %v", err)
	}
	indexObject := put(rootfshead.DirectoryIndexMediaType, indexPayload)
	base := rootfshead.BaseIdentity{
		ImageReference: "docker.io/library/busybox:1.36",
		ManifestDigest: digest.FromString("base manifest").String(),
		ChainID:        digest.FromString("base chain").String(),
		OS:             "linux",
		Architecture:   architecture,
		Variant:        variant,
	}
	headPayload, err := rootfshead.EncodeHead(rootfshead.Head{
		Version: rootfshead.Version,
		HeadID:  headID,
		Base:    base,
		Root: rootfshead.Entry{
			Inode: "root", Kind: rootfshead.EntryDirectory, Mode: 0o040755, Nlink: 2, Directory: &indexObject,
		},
	})
	if err != nil {
		t.Fatalf("EncodeHead() error = %v", err)
	}
	manifest := put(rootfshead.HeadMediaType, headPayload)
	return &sandboxstore.SandboxRootFSHead{
		SandboxID: sandboxID, SourceSandboxID: sandboxID, TeamID: teamID, RuntimeGeneration: 1,
		Reference: rootfshead.HeadReference{Version: rootfshead.Version, HeadID: headID, Manifest: manifest},
		Base:      base,
	}
}
