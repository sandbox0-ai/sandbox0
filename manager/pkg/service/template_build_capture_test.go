package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnsureTemplateBuildCaptureReadsPinnedHeadAfterSourceAdvances(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	base := &memorySandboxStore{
		records: map[string]*SandboxRecord{
			"sandbox-1": {
				ID:     "sandbox-1",
				TeamID: "team-1",
				Status: SandboxStatusPaused,
			},
		},
		rootFSSnapshots: map[string]*RootFSSnapshot{
			"template-build-1": {
				ID:              "template-build-1",
				FilesystemID:    "filesystem-1",
				TeamID:          "team-1",
				SourceSandboxID: "sandbox-1",
				HeadLayerID:     "layer-pinned",
				CreatedAt:       now,
			},
		},
		rootFSStates: map[string]*SandboxRootFSState{
			"sandbox-1": {
				LayerID: "layer-newer",
				TeamID:  "team-1",
			},
		},
	}
	pinned := &SandboxRootFSLayer{
		ID:                   "layer-pinned",
		TeamID:               "team-1",
		BaseImageRef:         "docker.io/library/busybox:1.36",
		BaseImageDigest:      digest.FromString("base-index").String(),
		PlatformOS:           "linux",
		PlatformArchitecture: "arm64",
		PlatformVariant:      "v8",
		DiffDigest:           digest.FromString("pinned layer").String(),
		DiffID:               digest.FromString("pinned diff").String(),
		DiffMediaType:        ocispec.MediaTypeImageLayerGzip,
		DiffSize:             42,
		DiffObjectKey:        "rootfs/pinned.tar.gz",
	}
	store := &templateCaptureMemoryStore{
		memorySandboxStore: base,
		chains: map[string][]*SandboxRootFSLayer{
			"layer-pinned": {pinned},
			"layer-newer": {{
				ID:                   "layer-newer",
				TeamID:               "team-1",
				PlatformOS:           "linux",
				PlatformArchitecture: "amd64",
			}},
		},
	}
	service := &SandboxService{sandboxStore: store, clock: systemTime{}}

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
	if capture.HeadLayerID != "layer-pinned" || capture.Layers[0].ID != "layer-pinned" {
		t.Fatalf("capture followed mutable sandbox head: %#v", capture)
	}
	if capture.Platform.Architecture != "arm64" || capture.Platform.Variant != "v8" {
		t.Fatalf("capture platform = %#v, want pinned arm64/v8", capture.Platform)
	}
	if !capture.CapturedAt.Equal(now) {
		t.Fatalf("capture time = %v, want %v", capture.CapturedAt, now)
	}
}

func TestEnsureTemplateBuildCaptureRejectsMixedRootFSChain(t *testing.T) {
	t.Parallel()

	baseDigest := digest.FromString("base-index").String()
	tests := []struct {
		name       string
		mutateRoot func(*SandboxRootFSLayer)
		wantError  string
	}{
		{
			name: "base digest mismatch",
			mutateRoot: func(layer *SandboxRootFSLayer) {
				layer.BaseImageDigest = digest.FromString("different-base").String()
			},
			wantError: "base image digest",
		},
		{
			name: "base repository mismatch",
			mutateRoot: func(layer *SandboxRootFSLayer) {
				layer.BaseImageRef = "registry.example.com/other/image:1"
			},
			wantError: "base image reference",
		},
		{
			name: "platform mismatch",
			mutateRoot: func(layer *SandboxRootFSLayer) {
				layer.PlatformArchitecture = "arm64"
			},
			wantError: "platform",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := &SandboxRootFSLayer{
				ID:                   "layer-root",
				TeamID:               "team-1",
				BaseImageRef:         "busybox:1.36",
				BaseImageDigest:      baseDigest,
				PlatformOS:           "linux",
				PlatformArchitecture: "amd64",
				DiffDigest:           digest.FromString("root").String(),
				DiffID:               digest.FromString("root").String(),
				DiffMediaType:        ocispec.MediaTypeImageLayer,
				DiffSize:             4,
				DiffObjectKey:        "rootfs/root",
			}
			tt.mutateRoot(root)
			head := &SandboxRootFSLayer{
				ID:                   "layer-head",
				ParentLayerID:        root.ID,
				TeamID:               "team-1",
				BaseImageRef:         "docker.io/library/busybox:1.36",
				BaseImageDigest:      baseDigest,
				PlatformOS:           "linux",
				PlatformArchitecture: "amd64",
				DiffDigest:           digest.FromString("head").String(),
				DiffID:               digest.FromString("head").String(),
				DiffMediaType:        ocispec.MediaTypeImageLayer,
				DiffSize:             4,
				DiffObjectKey:        "rootfs/head",
			}
			store := &templateCaptureMemoryStore{
				memorySandboxStore: &memorySandboxStore{
					records: map[string]*SandboxRecord{
						"sandbox-1": {
							ID:     "sandbox-1",
							TeamID: "team-1",
							Status: SandboxStatusPaused,
						},
					},
					rootFSSnapshots: map[string]*RootFSSnapshot{
						"template-build-1": {
							ID:              "template-build-1",
							TeamID:          "team-1",
							SourceSandboxID: "sandbox-1",
							HeadLayerID:     head.ID,
							CreatedAt:       time.Unix(100, 0).UTC(),
						},
					},
				},
				chains: map[string][]*SandboxRootFSLayer{
					head.ID: {root, head},
				},
			}
			service := &SandboxService{sandboxStore: store, clock: systemTime{}}

			_, err := service.EnsureTemplateBuildCapture(
				context.Background(),
				"sandbox-1",
				"team-1",
				"template-build-1",
				v1alpha1.SandboxTemplateSpec{},
			)
			if !errors.Is(err, errTemplateBuildCaptureInvalid) || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("EnsureTemplateBuildCapture() error = %v, want terminal %q mismatch", err, tt.wantError)
			}
		})
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
	chains map[string][]*SandboxRootFSLayer
}

func (s *templateCaptureMemoryStore) GetRootFSLayerChainByHead(_ context.Context, teamID, headLayerID string) ([]*SandboxRootFSLayer, error) {
	chain := s.chains[headLayerID]
	out := make([]*SandboxRootFSLayer, 0, len(chain))
	for _, layer := range chain {
		if layer == nil || (teamID != "" && layer.TeamID != teamID) {
			continue
		}
		copy := *layer
		out = append(out, &copy)
	}
	return out, nil
}
