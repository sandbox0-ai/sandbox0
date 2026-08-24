package legacyackmigration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

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
	if err := store.EnsureSession(ctx, "migration-session", digest.FromString("source-catalog").String(), "ali-ue1-nomad"); err != nil {
		t.Fatal(err)
	}
	operation, err := store.BeginBuild(ctx, "migration-session", build, contract)
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

	if _, err := pool.Exec(ctx, `
		UPDATE legacy_ack_migration.builds SET input_digest = $2 WHERE build_id = $1
	`, build.ID, digest.FromString("tampered-input").String()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetBuild(ctx, build.ID); !errors.Is(err, ErrTargetMigrationConflict) {
		t.Fatalf("GetBuild() after durable input tamper error = %v", err)
	}
}

func targetStoreIntegrationFixture(
	t *testing.T,
) (MaterializedBuild, TargetContract, rootfsimporter.MaterializedGenerationBuildResult, string, []byte) {
	t.Helper()
	sourceDigest := digest.FromString("source-image")
	mutationDigest := digest.FromString("legacy-layer-manifest")
	procdDigest := digest.FromString("procd")
	objectPrefix := "rootfs/legacy-ack-v1/team/build"
	mapDigest := digest.FromString("migrated-map-page")
	mapKey := objectPrefix + "/maps/sha256/" + mapDigest.Encoded()
	logicalSize := int64(1 << 30)
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
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	build := MaterializedBuild{
		ID: "legacy-ack-generation-v1-integration", TeamID: "team-1", HeadLayerID: "layer-1",
		PinnedOCIRef: "registry.example/sandbox@" + sourceDigest.String(), SourceOCIDigest: sourceDigest.String(),
		LogicalSizeBytes: logicalSize, Platform: platform, MutationDigest: mutationDigest.String(),
		ObjectPrefix: objectPrefix,
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

type targetStoreIntegrationLogger struct{}

func (targetStoreIntegrationLogger) Printf(string, ...any) {}
func (targetStoreIntegrationLogger) Fatalf(format string, args ...any) {
	panic(strings.TrimSpace(fmt.Sprintf(format, args...)))
}
