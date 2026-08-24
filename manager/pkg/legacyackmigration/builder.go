package legacyackmigration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

type MaterializedBuildExecutorConfig struct {
	Store         *TargetStore
	SourceObjects objectstore.Store
	TargetObjects objectstore.Store
	Unpacker      rootfsimporter.OCIUnpacker
	Filesystem    rootfsimporter.FilesystemImageBuilder
	WorkRoot      string
	ProcdPath     string
	WorkerID      string
	LeaseTTL      time.Duration
	LeaseRenewal  time.Duration
	LayerLimits   LayerApplyLimits
}

// MaterializedBuildExecutor performs the privileged OCI unpack, legacy layer
// application, XFS construction, and journaled immutable publication for one
// durable build operation.
type MaterializedBuildExecutor struct {
	store         *TargetStore
	sourceObjects objectstore.Store
	targetObjects objectstore.ContextConditionalStore
	unpacker      rootfsimporter.OCIUnpacker
	filesystem    rootfsimporter.FilesystemImageBuilder
	workRoot      string
	procdPath     string
	workerID      string
	leaseTTL      time.Duration
	leaseRenewal  time.Duration
	layerLimits   LayerApplyLimits
}

func NewMaterializedBuildExecutor(config MaterializedBuildExecutorConfig) (*MaterializedBuildExecutor, error) {
	conditional, ok := config.TargetObjects.(objectstore.ContextConditionalStore)
	if config.Store == nil || config.SourceObjects == nil || !ok ||
		!objectstore.SupportsContextConditionalCreate(config.TargetObjects) ||
		config.Unpacker == nil || config.Filesystem == nil {
		return nil, fmt.Errorf("target store, source objects, contextual target objects, unpacker, and filesystem builder are required")
	}
	if strings.TrimSpace(config.WorkRoot) == "" || strings.TrimSpace(config.ProcdPath) == "" ||
		strings.TrimSpace(config.WorkerID) == "" || len(config.WorkerID) > 256 {
		return nil, fmt.Errorf("materialized build local paths and worker identity are required")
	}
	if config.LeaseTTL < MinTargetBuildLeaseTTL || config.LeaseTTL > MaxTargetBuildLeaseTTL ||
		config.LeaseTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("materialized build lease TTL is outside supported bounds")
	}
	if config.LeaseRenewal <= 0 || config.LeaseRenewal >= config.LeaseTTL ||
		config.LeaseRenewal%time.Millisecond != 0 {
		return nil, fmt.Errorf("materialized build lease renewal must be positive and shorter than its TTL")
	}
	return &MaterializedBuildExecutor{
		store: config.Store, sourceObjects: config.SourceObjects, targetObjects: conditional,
		unpacker: config.Unpacker, filesystem: config.Filesystem,
		workRoot: strings.TrimSpace(config.WorkRoot), procdPath: strings.TrimSpace(config.ProcdPath),
		workerID: strings.TrimSpace(config.WorkerID), leaseTTL: config.LeaseTTL,
		leaseRenewal: config.LeaseRenewal, layerLimits: config.LayerLimits,
	}, nil
}

func (e *MaterializedBuildExecutor) Build(
	ctx context.Context,
	sessionID string,
	build MaterializedBuild,
	contract TargetContract,
	baseArtifactDigest string,
) (*TargetBuildOperation, error) {
	if e == nil || e.store == nil {
		return nil, fmt.Errorf("materialized build executor is not configured")
	}
	if err := validateMaterializedBuildIdentity(build); err != nil {
		return nil, err
	}
	operation, err := e.store.BeginBuild(ctx, sessionID, build, contract)
	if err != nil {
		return nil, err
	}
	if operation.State == targetBuildStateReady {
		return operation, nil
	}
	operation, err = e.store.LeaseBuild(ctx, build.ID, e.workerID, e.leaseTTL)
	if err != nil {
		return nil, err
	}
	if operation.State == targetBuildStateReady {
		return operation, nil
	}
	operation.Build.Layers = append([]Layer(nil), build.Layers...)
	lease, err := operation.Lease()
	if err != nil {
		return nil, err
	}

	procdDigest, err := digest.Parse(operation.Contract.ProcdDigest)
	if err != nil {
		return nil, e.releaseAfterFailure(lease, fmt.Errorf("parse materialized build procd digest: %w", err))
	}
	mutationDigest, err := digest.Parse(operation.Build.MutationDigest)
	if err != nil {
		return nil, e.releaseAfterFailure(lease, fmt.Errorf("parse materialized build mutation digest: %w", err))
	}
	if err := ocirootfs.ValidateLocalImportEnvironment(e.workRoot, e.procdPath, procdDigest); err != nil {
		return nil, e.releaseAfterFailure(lease, fmt.Errorf("validate materialized build environment: %w", err))
	}

	buildCtx, cancelBuild := context.WithCancelCause(ctx)
	renewStop := make(chan struct{})
	renewDone := make(chan error, 1)
	go e.renewLease(buildCtx, cancelBuild, lease, renewStop, renewDone)

	applier := LayerApplier{Store: e.sourceObjects, Limits: e.layerLimits}
	publisher := rootfsimporter.JournaledPublisher{
		OperationID: operation.Build.ID, Journal: e.store.Journal(lease),
		Publisher: rootfsblock.ObjectStorePublisher{Store: e.targetObjects},
	}
	blockBuilder := rootfsimporter.BlockBuilder{
		Unpacker: e.unpacker, Filesystem: e.filesystem, Publisher: publisher,
	}
	result, buildErr := blockBuilder.BuildMaterializedGeneration(buildCtx,
		rootfsimporter.MaterializedGenerationBuildRequest{
			BuildRequest: rootfsimporter.BuildRequest{
				Image: ocirootfs.Request{
					Reference: operation.Build.PinnedOCIRef, Platform: operation.Build.Platform,
					WorkRoot: e.workRoot, ProcdPath: e.procdPath, ExpectedProcdDigest: procdDigest,
				},
				LogicalSizeBytes: operation.Build.LogicalSizeBytes,
				BlockOptions:     operation.Contract.BlockOptions,
			},
			MutationDigest: mutationDigest,
			Mutator: rootfsimporter.RootMutatorFunc(func(mutateCtx context.Context, root string) error {
				return applier.Apply(mutateCtx, root, operation.Build.Layers)
			}),
		})
	var ready *TargetBuildOperation
	if buildErr == nil {
		ready, buildErr = e.store.PublishReadyBuild(buildCtx, lease, baseArtifactDigest, result)
	}
	close(renewStop)
	renewErr := <-renewDone
	cancelBuild(nil)
	if renewErr != nil {
		buildErr = errors.Join(buildErr, renewErr)
	}
	if buildErr != nil {
		return nil, e.releaseAfterFailure(lease, buildErr)
	}
	return ready, nil
}

func (e *MaterializedBuildExecutor) renewLease(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	lease TargetBuildLease,
	stop <-chan struct{},
	done chan<- error,
) {
	ticker := time.NewTicker(e.leaseRenewal)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			done <- nil
			return
		case <-ctx.Done():
			done <- context.Cause(ctx)
			return
		case <-ticker.C:
			if _, err := e.store.RenewBuildLease(ctx, lease, e.leaseTTL); err != nil {
				err = fmt.Errorf("renew materialized build lease: %w", err)
				cancel(err)
				done <- err
				return
			}
		}
	}
}

func (e *MaterializedBuildExecutor) releaseAfterFailure(lease TargetBuildLease, buildErr error) error {
	releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	releaseErr := e.store.ReleaseBuildLease(releaseCtx, lease)
	if errors.Is(releaseErr, ErrTargetBuildLeaseLost) {
		releaseErr = nil
	}
	return errors.Join(buildErr, releaseErr)
}
