package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/platforms"
	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
)

const templateBuildCaptureMetadataVersion = 1

var errTemplateBuildCaptureInvalid = errors.New("invalid template build capture")

// TemplateBuildCaptureMetadata is the durable handoff between checkpointing
// and publishing. It pins both the immutable rootfs head and the source
// platform selected when the sandbox actually ran.
type TemplateBuildCaptureMetadata struct {
	Version         int                   `json:"version"`
	SnapshotID      string                `json:"snapshot_id"`
	HeadLayerID     string                `json:"head_layer_id"`
	BaseImageRef    string                `json:"base_image_ref"`
	BaseImageDigest string                `json:"base_image_digest"`
	Platform        ocispec.Platform      `json:"platform"`
	Layers          []templateimage.Layer `json:"layers"`
	CapturedAt      time.Time             `json:"captured_at"`
}

type templateBuildRootFSStore interface {
	SandboxRootFSProductStore
	GetRootFSLayerChainByHead(ctx context.Context, teamID, headLayerID string) ([]*SandboxRootFSLayer, error)
}

// EnsureTemplateBuildCapture creates or recovers a deterministic internal
// snapshot and reads its immutable layer chain.
func (s *SandboxService) EnsureTemplateBuildCapture(
	ctx context.Context,
	sandboxID, teamID, snapshotID string,
	desiredSpec v1alpha1.SandboxTemplateSpec,
) (*TemplateBuildCaptureMetadata, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, ErrSandboxRootFSStoreUnavailable
	}
	store, ok := s.sandboxStore.(templateBuildRootFSStore)
	if !ok {
		return nil, fmt.Errorf("sandbox rootfs store does not support template image capture")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	snapshotID = strings.TrimSpace(snapshotID)
	if sandboxID == "" || teamID == "" || snapshotID == "" {
		return nil, fmt.Errorf("sandbox_id, team_id, and snapshot_id are required")
	}

	snapshot, err := store.GetRootFSSnapshot(ctx, snapshotID, teamID)
	if err != nil && !errors.Is(err, ErrRootFSSnapshotNotFound) {
		return nil, err
	}
	if errors.Is(err, ErrRootFSSnapshotNotFound) || snapshot == nil {
		_, createErr := s.createSandboxRootFSSnapshotWithID(
			ctx,
			sandboxID,
			teamID,
			snapshotID,
			"Template image build",
			"Temporary snapshot pinned while publishing a template image.",
			time.Time{},
		)
		if createErr != nil {
			// Recover the transaction-committed snapshot when a prior worker
			// lost its response or crashed before recording capture metadata.
			snapshot, err = store.GetRootFSSnapshot(ctx, snapshotID, teamID)
			if err != nil {
				return nil, createErr
			}
		} else {
			snapshot, err = store.GetRootFSSnapshot(ctx, snapshotID, teamID)
			if err != nil {
				return nil, err
			}
		}
	}
	if snapshot.SourceSandboxID != sandboxID || snapshot.TeamID != teamID {
		return nil, fmt.Errorf("template build snapshot %q belongs to a different sandbox or team", snapshotID)
	}

	chain, err := store.GetRootFSLayerChainByHead(ctx, teamID, snapshot.HeadLayerID)
	if err != nil {
		return nil, err
	}
	if len(chain) == 0 || chain[len(chain)-1] == nil || chain[len(chain)-1].ID != snapshot.HeadLayerID {
		return nil, fmt.Errorf("%w: template build snapshot %q has no immutable rootfs chain", errTemplateBuildCaptureInvalid, snapshotID)
	}
	head := chain[len(chain)-1]
	platform, err := s.templateBuildSourcePlatform(ctx, sandboxID, head, desiredSpec)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(head.BaseImageRef) == "" || strings.TrimSpace(head.BaseImageDigest) == "" {
		return nil, fmt.Errorf("%w: captured rootfs head has no base image identity", errTemplateBuildCaptureInvalid)
	}
	if err := validateTemplateBuildRootFSChain(chain, teamID, head.BaseImageRef, head.BaseImageDigest, platform); err != nil {
		return nil, err
	}

	layers := make([]templateimage.Layer, 0, len(chain))
	for _, layer := range chain {
		if layer == nil {
			return nil, fmt.Errorf("captured rootfs chain contains an empty layer")
		}
		layers = append(layers, templateimage.Layer{
			ID:        layer.ID,
			ObjectKey: layer.DiffObjectKey,
			MediaType: layer.DiffMediaType,
			Digest:    layer.DiffDigest,
			DiffID:    layer.DiffID,
			Size:      layer.DiffSize,
		})
	}
	return &TemplateBuildCaptureMetadata{
		Version:         templateBuildCaptureMetadataVersion,
		SnapshotID:      snapshot.ID,
		HeadLayerID:     snapshot.HeadLayerID,
		BaseImageRef:    head.BaseImageRef,
		BaseImageDigest: head.BaseImageDigest,
		Platform:        platform,
		Layers:          layers,
		CapturedAt:      snapshot.CreatedAt.UTC(),
	}, nil
}

func validateTemplateBuildRootFSChain(
	chain []*SandboxRootFSLayer,
	teamID, baseImageRef, baseImageDigest string,
	platform ocispec.Platform,
) error {
	expectedDigest, err := digest.Parse(strings.TrimSpace(baseImageDigest))
	if err != nil {
		return fmt.Errorf("%w: parse captured base image digest: %v", errTemplateBuildCaptureInvalid, err)
	}
	expectedRepository, err := normalizedTemplateBuildBaseRepository(baseImageRef)
	if err != nil {
		return fmt.Errorf("%w: parse captured base image reference: %v", errTemplateBuildCaptureInvalid, err)
	}
	expectedPlatform := platforms.Normalize(platform)

	for i, layer := range chain {
		if layer == nil {
			return fmt.Errorf("%w: captured rootfs chain contains an empty layer", errTemplateBuildCaptureInvalid)
		}
		expectedParentID := ""
		if i > 0 {
			expectedParentID = strings.TrimSpace(chain[i-1].ID)
		}
		if strings.TrimSpace(layer.ParentLayerID) != expectedParentID {
			return fmt.Errorf(
				"%w: rootfs layer %q parent %q does not match chain parent %q",
				errTemplateBuildCaptureInvalid,
				layer.ID,
				layer.ParentLayerID,
				expectedParentID,
			)
		}
		if strings.TrimSpace(layer.TeamID) != strings.TrimSpace(teamID) {
			return fmt.Errorf("%w: rootfs layer %q belongs to a different team", errTemplateBuildCaptureInvalid, layer.ID)
		}
		if value := strings.TrimSpace(layer.BaseImageDigest); value != "" {
			layerDigest, parseErr := digest.Parse(value)
			if parseErr != nil || layerDigest != expectedDigest {
				return fmt.Errorf(
					"%w: rootfs layer %q base image digest %q does not match head digest %q",
					errTemplateBuildCaptureInvalid,
					layer.ID,
					value,
					expectedDigest,
				)
			}
		}
		if value := strings.TrimSpace(layer.BaseImageRef); value != "" {
			layerRepository, parseErr := normalizedTemplateBuildBaseRepository(value)
			if parseErr != nil || layerRepository != expectedRepository {
				return fmt.Errorf(
					"%w: rootfs layer %q base image reference %q is incompatible with head reference %q",
					errTemplateBuildCaptureInvalid,
					layer.ID,
					value,
					baseImageRef,
				)
			}
			if named, parseErr := distref.ParseNormalizedNamed(value); parseErr == nil {
				if digested, ok := named.(distref.Digested); ok && digest.Digest(digested.Digest()) != expectedDigest {
					return fmt.Errorf(
						"%w: rootfs layer %q base image reference digest does not match head digest %q",
						errTemplateBuildCaptureInvalid,
						layer.ID,
						expectedDigest,
					)
				}
			}
		}

		layerPlatform := expectedPlatform
		if value := strings.TrimSpace(layer.PlatformOS); value != "" {
			layerPlatform.OS = value
		}
		if value := strings.TrimSpace(layer.PlatformArchitecture); value != "" {
			layerPlatform.Architecture = value
		}
		if value := strings.TrimSpace(layer.PlatformVariant); value != "" {
			layerPlatform.Variant = value
		}
		layerPlatform = platforms.Normalize(layerPlatform)
		if value := strings.TrimSpace(layer.PlatformOS); value != "" && layerPlatform.OS != expectedPlatform.OS {
			return templateBuildLayerPlatformMismatch(layer, expectedPlatform)
		}
		if value := strings.TrimSpace(layer.PlatformArchitecture); value != "" && layerPlatform.Architecture != expectedPlatform.Architecture {
			return templateBuildLayerPlatformMismatch(layer, expectedPlatform)
		}
		if value := strings.TrimSpace(layer.PlatformVariant); value != "" && layerPlatform.Variant != expectedPlatform.Variant {
			return templateBuildLayerPlatformMismatch(layer, expectedPlatform)
		}
	}
	return nil
}

func normalizedTemplateBuildBaseRepository(raw string) (string, error) {
	named, err := distref.ParseNormalizedNamed(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	return distref.TrimNamed(named).Name(), nil
}

func templateBuildLayerPlatformMismatch(layer *SandboxRootFSLayer, expected ocispec.Platform) error {
	return fmt.Errorf(
		"%w: rootfs layer %q platform %s/%s/%s does not match captured platform %s",
		errTemplateBuildCaptureInvalid,
		layer.ID,
		layer.PlatformOS,
		layer.PlatformArchitecture,
		layer.PlatformVariant,
		platforms.Format(expected),
	)
}

// DeleteTemplateBuildCapture releases the temporary snapshot GC pin.
func (s *SandboxService) DeleteTemplateBuildCapture(ctx context.Context, snapshotID, teamID string) error {
	store, err := s.rootFSProductStore()
	if err != nil {
		return err
	}
	err = s.deleteRootFSSnapshotWithQuota(
		ctx,
		store,
		strings.TrimSpace(snapshotID),
		strings.TrimSpace(teamID),
	)
	if errors.Is(err, ErrRootFSSnapshotNotFound) {
		return nil
	}
	return err
}

func (s *SandboxService) templateBuildSourcePlatform(
	ctx context.Context,
	sandboxID string,
	head *SandboxRootFSLayer,
	desiredSpec v1alpha1.SandboxTemplateSpec,
) (ocispec.Platform, error) {
	platform := ocispec.Platform{
		OS:           strings.TrimSpace(head.PlatformOS),
		Architecture: strings.TrimSpace(head.PlatformArchitecture),
		Variant:      strings.TrimSpace(head.PlatformVariant),
	}
	if platform.OS != "" && platform.Architecture != "" {
		return platform, nil
	}

	if s.podLister != nil {
		if pod, err := s.getSandboxPod(ctx, sandboxID); err == nil && pod != nil {
			platform = s.rootFSPlatformForPod(pod)
			if platform.OS != "" && platform.Architecture != "" {
				return platform, nil
			}
		} else if err != nil && !apierrors.IsNotFound(err) {
			return ocispec.Platform{}, fmt.Errorf("resolve source sandbox pod platform: %w", err)
		}
	}

	if desiredSpec.Pod != nil {
		platform = ocispec.Platform{
			OS:           strings.TrimSpace(desiredSpec.Pod.NodeSelector["kubernetes.io/os"]),
			Architecture: strings.TrimSpace(desiredSpec.Pod.NodeSelector["kubernetes.io/arch"]),
			Variant:      strings.TrimSpace(desiredSpec.Pod.NodeSelector[rootFSPlatformVariantLabel]),
		}
		if platform.OS != "" && platform.Architecture != "" {
			return platform, nil
		}
	}

	// Upgrade compatibility for old rootfs rows: inferring from the cluster is
	// safe only when every cached node reports the same platform.
	if s.nodeLister != nil {
		nodes, err := s.nodeLister.List(labels.Everything())
		if err != nil {
			return ocispec.Platform{}, fmt.Errorf("list nodes for source platform: %w", err)
		}
		var unique *ocispec.Platform
		for _, node := range nodes {
			candidate := ocispec.Platform{
				OS:           firstTemplateBuildValue(node.Labels["kubernetes.io/os"], node.Status.NodeInfo.OperatingSystem),
				Architecture: firstTemplateBuildValue(node.Labels["kubernetes.io/arch"], node.Status.NodeInfo.Architecture),
				Variant:      node.Labels[rootFSPlatformVariantLabel],
			}
			if candidate.OS == "" || candidate.Architecture == "" {
				continue
			}
			if unique == nil {
				copy := candidate
				unique = &copy
				continue
			}
			if unique.OS != candidate.OS || unique.Architecture != candidate.Architecture || unique.Variant != candidate.Variant {
				return ocispec.Platform{}, fmt.Errorf("source platform is absent from legacy rootfs metadata and cluster nodes are multi-platform")
			}
		}
		if unique != nil {
			return *unique, nil
		}
	}
	return ocispec.Platform{}, fmt.Errorf("source platform is unavailable; resume the sandbox on a known node and retry")
}

func firstTemplateBuildValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
