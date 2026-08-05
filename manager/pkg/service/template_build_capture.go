package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsexport"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templatebuild"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/templateimage"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const templateBuildCaptureMetadataVersion = templatebuild.CaptureMetadataVersion

const templateBuildRootFSWriteLeaseTTL = 6 * time.Hour

var errTemplateBuildCaptureInvalid = templatebuild.ErrCaptureInvalid

type templateBuildRootFSStore interface {
	SandboxRootFSProductStore
	GetRootFSHeadByID(ctx context.Context, headID, teamID string) (*sandboxstore.SandboxRootFSHead, error)
	GetRootFSExport(ctx context.Context, headID, teamID string) (*sandboxstore.RootFSExport, error)
	SaveRootFSExport(ctx context.Context, export *sandboxstore.RootFSExport) error
	AcquireRootFSWriteLease(ctx context.Context, leaseID, teamID string, ttl time.Duration) error
	ReleaseRootFSWriteLease(ctx context.Context, leaseID, teamID string) error
}

// EnsureTemplateBuildCapture creates or recovers a deterministic internal
// snapshot and asynchronously exports its complete v3 Head as one OCI layer.
func (s *SandboxService) EnsureTemplateBuildCapture(
	ctx context.Context,
	sandboxID, teamID, snapshotID string,
	_ v1alpha1.SandboxTemplateSpec,
) (*templatebuild.CaptureMetadata, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, ErrSandboxRootFSStoreUnavailable
	}
	if s.rootFSObjectStore == nil {
		return nil, fmt.Errorf("rootfs object store is unavailable for template image capture")
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
	if err != nil && !errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		return nil, err
	}
	if errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) || snapshot == nil {
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
	if snapshot == nil || snapshot.SourceSandboxID != sandboxID || snapshot.TeamID != teamID {
		return nil, fmt.Errorf("template build snapshot %q belongs to a different sandbox or team", snapshotID)
	}
	headID := strings.TrimSpace(snapshot.HeadID)
	if headID == "" {
		return nil, fmt.Errorf("%w: template build snapshot %q has no v3 Head", errTemplateBuildCaptureInvalid, snapshotID)
	}
	head, err := store.GetRootFSHeadByID(ctx, headID, teamID)
	if err != nil {
		return nil, err
	}
	if head == nil || head.Reference.HeadID != headID || head.TeamID != teamID {
		return nil, fmt.Errorf("%w: template build snapshot %q references an unavailable v3 Head", errTemplateBuildCaptureInvalid, snapshotID)
	}
	if head.Base.ImageReference == "" || head.Base.ManifestDigest == "" {
		return nil, fmt.Errorf("%w: captured rootfs Head has no base image identity", errTemplateBuildCaptureInvalid)
	}

	export, err := store.GetRootFSExport(ctx, headID, teamID)
	if err != nil {
		return nil, err
	}
	if export == nil {
		leaseID := "rootfs-export:" + headID
		if err := store.AcquireRootFSWriteLease(ctx, leaseID, teamID, templateBuildRootFSWriteLeaseTTL); err != nil {
			return nil, err
		}
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = store.ReleaseRootFSWriteLease(releaseCtx, leaseID, teamID)
		}()
		result, exportErr := rootfsexport.Export(ctx, s.rootFSObjectStore, teamID, head.Reference)
		if exportErr != nil {
			return nil, exportErr
		}
		candidate := &sandboxstore.RootFSExport{
			HeadID: headID,
			TeamID: teamID,
			Object: result.Object,
			DiffID: result.DiffID,
		}
		if saveErr := store.SaveRootFSExport(ctx, candidate); saveErr != nil {
			// Another durable worker may have won the deterministic export race.
			export, err = store.GetRootFSExport(ctx, headID, teamID)
			if err != nil || export == nil {
				return nil, saveErr
			}
		} else {
			export = candidate
		}
	}
	if err := validateTemplateBuildRootFSExport(export, head, teamID); err != nil {
		return nil, err
	}

	return &templatebuild.CaptureMetadata{
		Version:         templateBuildCaptureMetadataVersion,
		SnapshotID:      snapshot.ID,
		HeadID:          headID,
		BaseImageRef:    head.Base.ImageReference,
		BaseImageDigest: head.Base.ManifestDigest,
		Platform: ocispec.Platform{
			OS:           head.Base.OS,
			Architecture: head.Base.Architecture,
			Variant:      head.Base.Variant,
		},
		Layers: []templateimage.Layer{{
			ID:        headID,
			ObjectKey: export.Object.Key,
			MediaType: export.Object.MediaType,
			Digest:    export.Object.Digest,
			DiffID:    export.DiffID,
			Size:      export.Object.Size,
		}},
		CapturedAt: snapshot.CreatedAt.UTC(),
	}, nil
}

func validateTemplateBuildRootFSExport(export *sandboxstore.RootFSExport, head *sandboxstore.SandboxRootFSHead, teamID string) error {
	if export == nil || head == nil || export.HeadID != head.Reference.HeadID || export.TeamID != teamID {
		return fmt.Errorf("%w: rootfs Head export ownership is inconsistent", errTemplateBuildCaptureInvalid)
	}
	if err := export.Object.Validate(rootfshead.ExportLayerMediaType); err != nil {
		return fmt.Errorf("%w: %v", errTemplateBuildCaptureInvalid, err)
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	if err := rootfshead.ValidateObjectScope(prefix, export.Object); err != nil {
		return fmt.Errorf("%w: %v", errTemplateBuildCaptureInvalid, err)
	}
	parsedDiffID, err := digest.Parse(strings.TrimSpace(export.DiffID))
	if err != nil || parsedDiffID.Algorithm() != digest.Canonical {
		return fmt.Errorf("%w: rootfs Head export diff_id is invalid", errTemplateBuildCaptureInvalid)
	}
	return nil
}

// DeleteTemplateBuildCapture releases the temporary snapshot GC pin.
func (s *SandboxService) DeleteTemplateBuildCapture(ctx context.Context, snapshotID, teamID string) error {
	store, err := s.rootFSProductStore()
	if err != nil {
		return err
	}
	err = store.DeleteRootFSSnapshot(ctx, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID))
	if errors.Is(err, sandboxstore.ErrRootFSSnapshotNotFound) {
		return nil
	}
	return err
}
