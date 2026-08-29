package legacyackmigration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

func TestTargetStoreDurablePublicationAndExactReadyRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newTargetStoreIntegrationPool(t, ctx)
	store, err := NewTargetStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	build, contract, result, baseArtifactDigest, baseDescriptor := targetStoreIntegrationFixture(t)
	normalized := targetStoreIntegrationCatalog(build)
	sourceCatalog := validCatalog(t)
	sourceCatalogDigest, err := sourceCatalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	captures, err := NewCaptureStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := captures.CaptureCatalog(ctx, "migration-session", "ali-ue1-nomad", &sourceCatalog); err != nil {
		t.Fatal(err)
	}
	markTargetStoreCaptureRetired(t, ctx, pool, "migration-session")
	baseImports := &targetStoreIntegrationBaseImports{}
	prepared, err := store.PrepareCatalog(
		ctx, "migration-session", sourceCatalogDigest,
		"ali-ue1-nomad", normalized, contract, baseImports,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Builds != 1 || prepared.BaseRequirements != 1 || prepared.PendingBaseImports != 1 ||
		prepared.ReadyBaseArtifacts != 0 || baseImports.operation == nil {
		t.Fatalf("preparation = %#v, operation = %#v", prepared, baseImports.operation)
	}
	operation, err := store.GetBuild(ctx, build.ID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != targetBuildStatePending || operation.Contract.BlockOptions.ObjectPrefix != build.ObjectPrefix {
		t.Fatalf("pending operation = %#v", operation)
	}
	if _, err := store.BeginBuild(ctx, "migration-session", build, contract); err != nil {
		t.Fatalf("exact BeginBuild() retry: %v", err)
	}

	if err := insertTargetStoreReadyBaseArtifact(ctx, pool, build, contract, baseArtifactDigest, baseDescriptor); err != nil {
		t.Fatal(err)
	}
	operation, err = store.LeaseBuild(ctx, build.ID, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := operation.Lease()
	if err != nil {
		t.Fatal(err)
	}
	for _, reference := range result.References {
		if err := store.PrepareBuildObject(ctx, lease, reference); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkBuildObjectPublished(ctx, lease, reference); err != nil {
			t.Fatal(err)
		}
	}

	ready, err := store.PublishReadyBuild(ctx, lease, baseArtifactDigest, result)
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != targetBuildStateReady || ready.Result == nil || ready.BaseArtifactDigest != baseArtifactDigest {
		t.Fatalf("ready operation = %#v", ready)
	}
	// This uses the pre-commit lease deliberately: a lost commit response must
	// converge on the exact ready result instead of requiring a new lease.
	retried, err := store.PublishReadyBuild(ctx, lease, baseArtifactDigest, result)
	if err != nil {
		t.Fatalf("exact PublishReadyBuild() retry: %v", err)
	}
	if retried.State != targetBuildStateReady || retried.Result == nil {
		t.Fatalf("retried operation = %#v", retried)
	}
	committed, err := store.CommitCatalog(ctx, "migration-session", normalized)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Sandboxes != 1 || committed.Filesystems != 1 || committed.Generations != 2 ||
		committed.Snapshots != 1 || committed.CommitDigest == "" {
		t.Fatalf("commit result = %#v", committed)
	}
	committedRetry, err := store.CommitCatalog(ctx, "migration-session", normalized)
	if err != nil {
		t.Fatalf("exact CommitCatalog() retry: %v", err)
	}
	if *committedRetry != *committed {
		t.Fatalf("commit retry = %#v, want %#v", committedRetry, committed)
	}
	var runtimeID, runtimeNamespace, durability string
	var writerEpoch int64
	if err := pool.QueryRow(ctx, `
		SELECT sandbox.runtime_id, sandbox.runtime_namespace,
			filesystem.writer_epoch, generation.durability_state
		FROM manager.sandboxes sandbox
		JOIN manager.sandbox_rootfs_bindings binding USING (sandbox_id)
		JOIN manager.rootfs_filesystems filesystem USING (filesystem_id)
		JOIN manager.rootfs_generations generation
			ON generation.generation_id = filesystem.head_generation_id
		WHERE sandbox.sandbox_id = 'sandbox-1'
	`).Scan(&runtimeID, &runtimeNamespace, &writerEpoch, &durability); err != nil {
		t.Fatal(err)
	}
	if runtimeID != "" || runtimeNamespace != "" || writerEpoch != 1 || durability != "s3_materialized" {
		t.Fatalf("committed paused graph = runtime %q/%q epoch %d durability %q",
			runtimeNamespace, runtimeID, writerEpoch, durability)
	}
	assertTargetStoreMigratedClaim(t, ctx, pool, store, "migration-session", "sandbox-1")
	if _, err := pool.Exec(ctx, `
		DELETE FROM manager.sandbox_runtime_claims WHERE sandbox_id = 'sandbox-1'
	`); err != nil {
		t.Fatal(err)
	}
	repaired, err := store.CommitCatalog(ctx, "migration-session", normalized)
	if err != nil {
		t.Fatalf("CommitCatalog() missing-claim repair: %v", err)
	}
	if *repaired != *committed {
		t.Fatalf("claim repair commit = %#v, want %#v", repaired, committed)
	}
	assertTargetStoreMigratedClaim(t, ctx, pool, store, "migration-session", "sandbox-1")
	if _, err := pool.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET credential_binding_digest = 'sha256:' || repeat('0', 64)
		WHERE sandbox_id = 'sandbox-1'
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitCatalog(ctx, "migration-session", normalized); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("CommitCatalog() after claim tamper error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims SET credential_binding_digest = $2
		WHERE sandbox_id = $1
	`, "sandbox-1", credentialbinding.EmptyDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE manager.sandboxes SET template_name = 'tampered' WHERE sandbox_id = 'sandbox-1'
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitCatalog(ctx, "migration-session", normalized); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("CommitCatalog() after product tamper error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE manager.sandboxes SET template_name = 'template-1' WHERE sandbox_id = 'sandbox-1'
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE legacy_ack_migration.builds SET input_digest = $2 WHERE build_id = $1
	`, build.ID, digest.FromString("tampered-input").String()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetBuild(ctx, build.ID); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("GetBuild() after durable input tamper error = %v", err)
	}
}

func assertTargetStoreMigratedClaim(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *TargetStore,
	sessionID, sandboxID string,
) {
	t.Helper()
	var operationID, phase, bindingDigest string
	var completedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT operation_id, phase, credential_binding_digest, completed_at
		FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, sandboxID).Scan(&operationID, &phase, &bindingDigest, &completedAt); err != nil {
		t.Fatal(err)
	}
	if operationID != targetRuntimeClaimOperationID(sessionID, sandboxID) ||
		phase != sandboxstore.SandboxRuntimeClaimPhaseReady ||
		bindingDigest != credentialbinding.EmptyDigest || completedAt.IsZero() {
		t.Fatalf("migrated claim = operation %q phase %q digest %q completed %s",
			operationID, phase, bindingDigest, completedAt)
	}
	bindings, err := sandboxstore.NewPGSandboxStore(pool).GetNomadSandboxCredentialBindings(
		ctx, "team-1", sandboxID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if bindings == nil || bindings.Digest != credentialbinding.EmptyDigest || len(bindings.Bindings) != 0 {
		t.Fatalf("migrated credential authority = %#v", bindings)
	}
}

func TestTargetRuntimeClaimOperationIDIsDeterministicAndSessionBound(t *testing.T) {
	first := targetRuntimeClaimOperationID("session-1", "sandbox-1")
	if first != targetRuntimeClaimOperationID("session-1", "sandbox-1") {
		t.Fatal("target runtime claim operation ID is not deterministic")
	}
	if first == targetRuntimeClaimOperationID("session-2", "sandbox-1") ||
		first == targetRuntimeClaimOperationID("session-1", "sandbox-2") {
		t.Fatal("target runtime claim operation ID is not bound to both identities")
	}
	if len(first) > 512 || !strings.HasPrefix(first, "legacy-ack-claim-") {
		t.Fatalf("target runtime claim operation ID = %q", first)
	}
}

func TestTargetCommitClaimBindsSurvivingCredentialProjectionIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newTargetStoreIntegrationPool(t, ctx)
	unscopedPool := newTargetStoreUnscopedIntegrationPool(t, ctx)
	var sourceID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO credential_sources (
			team_id, name, resolver_kind, current_version, status
		) VALUES ('team-1', 'source-1', 'static_headers', 1, 'active')
		RETURNING id
	`).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO credential_source_versions (
			source_id, version, spec_json, resolver_kind
		) VALUES ($1, 1, '{}'::jsonb, 'static_headers')
	`, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO sandbox_egress_credential_bindings (
			team_id, sandbox_id, ref, source_ref, source_id, source_version,
			projection, cache_policy
		) VALUES (
			'team-1', 'sandbox-1', 'binding-1', 'source-1', $1, 1,
			'{"type":"http_headers","httpHeaders":{"headers":[{"name":"Authorization","valueTemplate":"Bearer {{.token}}"}]}}'::jsonb,
			'null'::jsonb
		)
	`, sourceID); err != nil {
		t.Fatal(err)
	}
	plan := &targetCommitPlan{
		SessionID: "migration-session",
		Sandboxes: []targetCommitSandbox{{Record: sandboxstore.SandboxRecord{
			ID: "sandbox-1", TeamID: "team-1",
		}}},
	}
	tx, err := unscopedPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := bindTargetCommitClaims(ctx, tx, plan); err != nil {
		t.Fatal(err)
	}
	expected := credentialbinding.DigestStore([]egressauthstore.CredentialBinding{{
		Ref: "binding-1", SourceRef: "source-1", SourceID: sourceID, SourceVersion: 1,
		Projection: egressauthstore.ProjectionSpec{
			Type: egressauthstore.CredentialProjectionTypeHTTPHeaders,
			HTTPHeaders: &egressauthstore.HTTPHeadersProjection{Headers: []egressauthstore.ProjectedHeader{{
				Name: "Authorization", ValueTemplate: "Bearer {{.token}}",
			}}},
		},
	}})
	if plan.Sandboxes[0].CredentialBindingDigest != expected {
		t.Fatalf("credential binding digest = %q, want %q",
			plan.Sandboxes[0].CredentialBindingDigest, expected)
	}
	if plan.Sandboxes[0].ClaimOperationID != targetRuntimeClaimOperationID("migration-session", "sandbox-1") {
		t.Fatalf("claim operation ID = %q", plan.Sandboxes[0].ClaimOperationID)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var foreignSourceID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO credential_sources (
			team_id, name, resolver_kind, current_version, status
		) VALUES ('team-2', 'source-2', 'static_headers', 1, 'active')
		RETURNING id
	`).Scan(&foreignSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO credential_source_versions (
			source_id, version, spec_json, resolver_kind
		) VALUES ($1, 1, '{}'::jsonb, 'static_headers')
	`, foreignSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO sandbox_egress_credential_bindings (
			team_id, sandbox_id, ref, source_ref, source_id, source_version,
			projection, cache_policy
		) VALUES (
			'team-2', 'sandbox-1', 'binding-2', 'source-2', $1, 1,
			'{"type":"http_headers","httpHeaders":{"headers":[]}}'::jsonb,
			'null'::jsonb
		)
	`, foreignSourceID); err != nil {
		t.Fatal(err)
	}
	foreignTx, err := unscopedPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreignTx.Rollback(ctx) }()
	if err := bindTargetCommitClaims(ctx, foreignTx, plan); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("bindTargetCommitClaims() cross-team error = %v", err)
	}
}

func TestCaptureStoreExactRetryConflictAndTamperDetection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newTargetStoreIntegrationPool(t, ctx)
	store, err := NewCaptureStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	catalog := validCatalog(t)
	first, err := store.CaptureCatalog(ctx, "capture-session", "ali-ue1-nomad", &catalog)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.CaptureCatalog(ctx, "capture-session", "ali-ue1-nomad", &catalog)
	if err != nil {
		t.Fatalf("exact CaptureCatalog() retry: %v", err)
	}
	if retry.SourceCatalogDigest != first.SourceCatalogDigest || !retry.CapturedAt.Equal(first.CapturedAt) {
		t.Fatalf("capture retry = %#v, want original %#v", retry, first)
	}
	loaded, err := store.LoadCapturedCatalog(ctx, "capture-session")
	if err != nil {
		t.Fatal(err)
	}
	loadedDigest, err := loaded.Catalog.Digest()
	if err != nil || loadedDigest != first.SourceCatalogDigest {
		t.Fatalf("loaded catalog digest = %q, %v; want %q", loadedDigest, err, first.SourceCatalogDigest)
	}

	divergent := catalog
	divergent.Layers = append([]Layer(nil), catalog.Layers...)
	divergent.Layers[0].DiffSize++
	if _, err := store.CaptureCatalog(ctx, "capture-session", "ali-ue1-nomad", &divergent); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("divergent CaptureCatalog() error = %v", err)
	}
	if _, err := store.CaptureCatalog(ctx, "capture-session", "other-cluster", &catalog); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("retargeted CaptureCatalog() error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE legacy_ack_migration.source_catalogs
		SET catalog = jsonb_set(catalog, '{ActiveLifecycleTxns}', '1'::jsonb)
		WHERE session_id = 'capture-session'
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadCapturedCatalog(ctx, "capture-session"); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("LoadCapturedCatalog() after tamper error = %v", err)
	}
}

func TestTargetStoreRejectsSessionWithoutMatchingCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newTargetStoreIntegrationPool(t, ctx)
	store, err := NewTargetStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	digestValue := digest.FromString("source-catalog").String()
	if err := store.EnsureSession(ctx, "missing", digestValue, "ali-ue1-nomad"); !errors.Is(err, ErrCapturedCatalogNotFound) {
		t.Fatalf("EnsureSession() without capture error = %v", err)
	}
	captures, err := NewCaptureStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	catalog := validCatalog(t)
	captured, err := captures.CaptureCatalog(ctx, "captured", "ali-ue1-nomad", &catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSession(ctx, "captured", captured.SourceCatalogDigest, "other-cluster"); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("EnsureSession() with mismatched capture error = %v", err)
	}
	if err := store.EnsureSession(ctx, "captured", captured.SourceCatalogDigest, "ali-ue1-nomad"); !errors.Is(err, ErrLegacyCatalogNotRetired) {
		t.Fatalf("EnsureSession() with unretired capture error = %v", err)
	}
}

func markTargetStoreCaptureRetired(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	sessionID string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		UPDATE legacy_ack_migration.source_catalogs SET retired_at = NOW() WHERE session_id = $1
	`, sessionID); err != nil {
		t.Fatal(err)
	}
}

type targetStoreIntegrationBaseImports struct {
	operation *sandboxstore.RootFSImportOperation
}

func (s *targetStoreIntegrationBaseImports) GetReadyRootFSBaseArtifact(
	context.Context,
	string,
	sandboxstore.RootFSArtifactPlatform,
	sandboxstore.ReadyRootFSArtifactRequirements,
) (*sandboxstore.RootFSBaseArtifact, error) {
	return nil, sandboxstore.ErrRootFSBaseArtifactNotFound
}

func (s *targetStoreIntegrationBaseImports) BeginRootFSImport(
	_ context.Context,
	request *sandboxstore.BeginRootFSImportRequest,
) (*sandboxstore.RootFSImportOperation, error) {
	s.operation = &sandboxstore.RootFSImportOperation{
		ID: request.OperationID, Spec: request.Spec, State: sandboxstore.RootFSImportStatePending,
	}
	return s.operation, nil
}

func targetStoreIntegrationCatalog(build MaterializedBuild) *NormalizedCatalog {
	createdAt := time.Date(2026, 8, 25, 0, 0, 0, 123000000, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	return &NormalizedCatalog{
		MaterializedBuilds: []MaterializedBuild{build},
		Sandboxes: []NormalizedSandbox{{
			FilesystemID: "filesystem-1",
			Record: sandboxstore.SandboxRecord{
				ID: "sandbox-1", TeamID: "team-1", UserID: "user-1",
				TemplateID: "template-1", TemplateName: "template-1", TemplateNamespace: "default",
				ClusterID: "ali-ue1-nomad", DesiredState: sandboxstore.SandboxDesiredStatePaused,
				RuntimeGeneration: 7, LifecycleEpoch: 3, OwnerKind: "claimed",
				ResourceMillicpu: 1000, ResourceMemoryMiB: 2048,
				CreatedAt: createdAt, UpdatedAt: updatedAt,
			},
		}},
		Filesystems: []NormalizedFilesystem{{
			Record: Filesystem{
				ID: "filesystem-1", TeamID: "team-1", HeadLayerID: build.HeadLayerID,
				BaseImageDigest: build.SourceOCIDigest, CreatedAt: createdAt, UpdatedAt: updatedAt,
			},
			LogicalSizeBytes: build.LogicalSizeBytes, HeadBuildID: build.ID,
			BuildIDByLayer: map[string]string{build.HeadLayerID: build.ID},
		}},
		Snapshots: []NormalizedSnapshot{{
			Record: Snapshot{
				ID: "snapshot-1", TeamID: "team-1", SourceSandboxID: "sandbox-1",
				HeadLayerID: build.HeadLayerID, FilesystemID: "filesystem-1",
				Name: "snapshot", Description: "migrated", CreatedAt: updatedAt,
			},
			BuildID: build.ID,
		}},
	}
}

func targetStoreIntegrationFixture(
	t *testing.T,
) (MaterializedBuild, TargetContract, rootfsimporter.MaterializedGenerationBuildResult, string, []byte) {
	t.Helper()
	sourceDigest := digest.FromString("source-image")
	procdDigest := digest.FromString("procd")
	mapDigest := digest.FromString("migrated-map-page")
	logicalSize := int64(1 << 30)
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	build := MaterializedBuild{
		TeamID: "team-1", HeadLayerID: "layer-1",
		PinnedOCIRef: "registry.example/sandbox@" + sourceDigest.String(), SourceOCIDigest: sourceDigest.String(),
		LogicalSizeBytes: logicalSize, Platform: platform,
		Layers: []Layer{{
			ID: "layer-1", SourceSandboxID: "sandbox-1", TeamID: "team-1",
			BaseImageRef: "registry.example/sandbox", BaseImageDigest: sourceDigest.String(),
			DiffDigest: digest.FromString("legacy-layer").String(),
			DiffID:     digest.FromString("legacy-layer").String(), DiffMediaType: ocispec.MediaTypeImageLayer,
			DiffSize: 4096, DiffObjectKey: "rootfs/layers/layer-1.tar",
			PlatformOS: "linux", PlatformArchitecture: "amd64",
		}},
	}
	build, err := normalizeMaterializedBuildIdentity(build)
	if err != nil {
		t.Fatal(err)
	}
	mutationDigest, err := digest.Parse(build.MutationDigest)
	if err != nil {
		t.Fatal(err)
	}
	mapKey := build.ObjectPrefix + "/maps/sha256/" + mapDigest.Encoded()
	descriptor := rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: logicalSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: mapDigest.String(),
			Object: rootfsblock.ObjectRange{
				Key: mapKey, Length: 4096, Checksum: mapDigest.String(),
			},
		},
	}
	descriptorBytes, err := rootfsblock.EncodeDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	contract := TargetContract{
		FormatGeneration: 1, ProcdProtocol: "sandbox0.procd.test.v1", ProcdDigest: procdDigest.String(),
	}
	result := rootfsimporter.MaterializedGenerationBuildResult{
		SourceOCIRef: build.PinnedOCIRef, SourceOCIDigest: sourceDigest, Platform: platform,
		ProcdDigest: procdDigest, LogicalSizeBytes: logicalSize, MutationDigest: mutationDigest,
		DescriptorDigest: digest.FromBytes(descriptorBytes), CurrentBlockHead: mapDigest,
		Descriptor: descriptor, DescriptorBytes: descriptorBytes, Objects: 1, Bytes: 4096,
		References: []rootfsblock.ObjectReference{{
			Key: mapKey, Kind: rootfsblock.ObjectKindMappingPage, Size: 4096, Checksum: mapDigest.String(),
		}},
	}

	baseMapDigest := digest.FromString("base-map-page")
	baseDescriptorValue := descriptor
	baseDescriptorValue.MappingRoot.RootDigest = baseMapDigest.String()
	baseDescriptorValue.MappingRoot.Object = rootfsblock.ObjectRange{
		Key: "rootfs/base/maps/sha256/" + baseMapDigest.Encoded(), Length: 4096,
		Checksum: baseMapDigest.String(),
	}
	baseDescriptor, err := rootfsblock.EncodeDescriptor(baseDescriptorValue)
	if err != nil {
		t.Fatal(err)
	}
	return build, contract, result, digest.FromString("base-artifact").String(), baseDescriptor
}

func insertTargetStoreReadyBaseArtifact(
	ctx context.Context,
	pool *pgxpool.Pool,
	build MaterializedBuild,
	contract TargetContract,
	artifactDigest string,
	descriptor []byte,
) error {
	decoded, err := rootfsblock.DecodeDescriptor(descriptor)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_base_artifacts (
			artifact_digest, source_oci_ref, source_oci_digest, base_block_root,
			format_generation, state, descriptor, oci_os, oci_architecture, oci_variant,
			procd_protocol, procd_digest, logical_size_bytes, descriptor_digest
		) VALUES ($1, $2, $3, $4, $5, 'ready', $6, $7, $8, $9, $10, $11, $12, $13)
	`, artifactDigest, build.PinnedOCIRef, build.SourceOCIDigest, decoded.MappingRoot.RootDigest,
		contract.FormatGeneration, descriptor, build.Platform.OS, build.Platform.Architecture,
		build.Platform.Variant, contract.ProcdProtocol, contract.ProcdDigest, build.LogicalSizeBytes,
		digest.FromBytes(descriptor).String())
	return err
}

func newTargetStoreIntegrationPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: databaseURL, Schema: "scheduler", MaxConns: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, schema := range []string{"legacy_ack_migration", "manager"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS legacy_ack_migration CASCADE")
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS manager CASCADE")
	})
	if _, err := pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		t.Fatal(err)
	}
	logger := targetStoreIntegrationLogger{}
	if err := egressauthstore.RunMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	if err := sandboxstore.RunSandboxStoreMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	return pool
}

func newTargetStoreUnscopedIntegrationPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = "public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type targetStoreIntegrationLogger struct{}

func (targetStoreIntegrationLogger) Printf(string, ...any) {}
func (targetStoreIntegrationLogger) Fatalf(format string, args ...any) {
	panic(strings.TrimSpace(fmt.Sprintf(format, args...)))
}
