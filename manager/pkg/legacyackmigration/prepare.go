package legacyackmigration

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

// TargetBaseImportStore is the normal regional Base-artifact importer. The
// migration reuses it instead of creating a second OCI artifact authority.
type TargetBaseImportStore interface {
	GetReadyRootFSBaseArtifact(
		context.Context,
		string,
		sandboxstore.RootFSArtifactPlatform,
		sandboxstore.ReadyRootFSArtifactRequirements,
	) (*sandboxstore.RootFSBaseArtifact, error)
	BeginRootFSImport(
		context.Context,
		*sandboxstore.BeginRootFSImportRequest,
	) (*sandboxstore.RootFSImportOperation, error)
}

type TargetPreparationResult struct {
	Builds             int
	BaseRequirements   int
	ReadyBaseArtifacts int
	PendingBaseImports int
}

type targetBaseRequirement struct {
	SourceOCIRef     string
	SourceOCIDigest  string
	Platform         sandboxstore.RootFSArtifactPlatform
	LogicalSizeBytes int64
}

// PrepareCatalog creates the session, exact materialized-build operations,
// and normal Base-artifact import operations without performing any object
// write itself. Repeating it is safe and reports when all Base artifacts are
// ready for the privileged generation builder.
func (s *TargetStore) PrepareCatalog(
	ctx context.Context,
	sessionID, sourceCatalogDigest, targetClusterID string,
	catalog *NormalizedCatalog,
	contract TargetContract,
	baseImports TargetBaseImportStore,
) (*TargetPreparationResult, error) {
	if s == nil || s.pool == nil || catalog == nil || baseImports == nil {
		return nil, fmt.Errorf("target store, normalized catalog, and Base importer are required")
	}
	if err := s.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	if err := s.EnsureSession(ctx, sessionID, sourceCatalogDigest, targetClusterID); err != nil {
		return nil, err
	}
	result := &TargetPreparationResult{Builds: len(catalog.MaterializedBuilds)}
	requirements := make(map[string]targetBaseRequirement)
	for _, build := range catalog.MaterializedBuilds {
		operation, err := s.BeginBuild(ctx, sessionID, build, contract)
		if err != nil {
			return nil, err
		}
		if operation.SessionID != strings.TrimSpace(sessionID) {
			return nil, fmt.Errorf("%w: build %s belongs to another session", ErrTargetMigrationConflict, build.ID)
		}
		key := targetBaseRequirementKey(build)
		if existing, ok := requirements[key]; ok {
			if existing.SourceOCIRef != build.PinnedOCIRef {
				return nil, fmt.Errorf("%w: Base requirement has conflicting pinned references", ErrTargetMigrationConflict)
			}
			continue
		}
		requirements[key] = targetBaseRequirement{
			SourceOCIRef: build.PinnedOCIRef, SourceOCIDigest: build.SourceOCIDigest,
			Platform: sandboxstore.RootFSArtifactPlatform{
				OS: build.Platform.OS, Architecture: build.Platform.Architecture, Variant: build.Platform.Variant,
			},
			LogicalSizeBytes: build.LogicalSizeBytes,
		}
	}
	result.BaseRequirements = len(requirements)
	baseOptions := contract.BlockOptions
	baseOptions.ObjectPrefix = "rootfs/v1"
	baseOptions, err := rootfsblock.NormalizeBuildOptions(baseOptions)
	if err != nil {
		return nil, fmt.Errorf("normalize target Base block options: %w", err)
	}
	for _, requirement := range requirements {
		requirements := sandboxstore.ReadyRootFSArtifactRequirements{
			FormatGeneration: contract.FormatGeneration,
			LogicalSizeBytes: requirement.LogicalSizeBytes,
			ProcdProtocol:    contract.ProcdProtocol, ProcdDigest: contract.ProcdDigest,
		}
		artifact, readyErr := baseImports.GetReadyRootFSBaseArtifact(
			ctx, requirement.SourceOCIDigest, requirement.Platform, requirements,
		)
		if readyErr == nil && artifact != nil {
			result.ReadyBaseArtifacts++
			continue
		}
		if readyErr != nil && !errors.Is(readyErr, sandboxstore.ErrRootFSBaseArtifactNotFound) {
			return nil, fmt.Errorf("inspect target Base artifact %s: %w", requirement.SourceOCIDigest, readyErr)
		}
		spec := rootfsimporter.OperationSpec{
			SourceOCIRef: requirement.SourceOCIRef,
			Platform: rootfsimporter.ReadyArtifactPlatform{
				OS: requirement.Platform.OS, Architecture: requirement.Platform.Architecture,
				Variant: requirement.Platform.Variant,
			},
			FormatGeneration: contract.FormatGeneration,
			ProcdProtocol:    contract.ProcdProtocol, ProcdDigest: contract.ProcdDigest,
			LogicalSizeBytes: requirement.LogicalSizeBytes, BlockOptions: baseOptions,
		}
		operationID, normalized, err := rootfsimporter.DeterministicOperation(spec)
		if err != nil {
			return nil, fmt.Errorf("construct target Base import: %w", err)
		}
		operation, err := baseImports.BeginRootFSImport(ctx, &sandboxstore.BeginRootFSImportRequest{
			OperationID: operationID, Spec: normalized,
		})
		if err != nil {
			return nil, fmt.Errorf("ensure target Base import %s: %w", operationID, err)
		}
		switch operation.State {
		case sandboxstore.RootFSImportStatePending, sandboxstore.RootFSImportStateBuilding:
			result.PendingBaseImports++
		case sandboxstore.RootFSImportStateReady:
			// Publication and operation state commit atomically. The artifact
			// becomes visible on the next preparation pass.
			result.PendingBaseImports++
		case sandboxstore.RootFSImportStateAbandoned:
			return nil, fmt.Errorf("target Base import %s is abandoned: %s", operation.ID, operation.AbandonReason)
		default:
			return nil, fmt.Errorf("target Base import %s has invalid state %q", operation.ID, operation.State)
		}
	}
	return result, nil
}

func targetBaseRequirementKey(build MaterializedBuild) string {
	return strings.Join([]string{
		build.SourceOCIDigest, build.Platform.OS, build.Platform.Architecture,
		build.Platform.Variant, fmt.Sprintf("%d", build.LogicalSizeBytes),
	}, "\x00")
}
