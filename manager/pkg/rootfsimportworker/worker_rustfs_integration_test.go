//go:build linux

// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootfsimportworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

// TestDurableWorkerReplaysPublishedRustFSObjectsBeforeReadyCAS exercises the
// production PostgreSQL journal and a real RustFS conditional object store.
// The sparse filesystem fixture is intentionally not privileged XFS coverage;
// the privileged importer tests own that separate contract.
func TestDurableWorkerReplaysPublishedRustFSObjectsBeforeReadyCAS(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("SANDBOX0_ROOTFS_IMPORT_DATABASE_URL"))
	endpoint := strings.TrimSpace(os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"))
	if databaseURL == "" || endpoint == "" {
		t.Skip("set SANDBOX0_ROOTFS_IMPORT_DATABASE_URL and SANDBOX0_RUSTFS_ENDPOINT to isolated services")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	pool := newRootFSImportIntegrationPool(t, ctx, databaseURL)
	store := sandboxstore.NewPGSandboxStore(pool)

	bucket := strings.TrimSpace(os.Getenv("SANDBOX0_RUSTFS_BUCKET"))
	if bucket == "" {
		bucket = "sandbox0-rootfs-importer-test"
	}
	objects, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"),
		SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") &&
		!strings.Contains(strings.ToLower(err.Error()), "exist") {
		t.Fatalf("create RustFS bucket: %v", err)
	}
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	if !ok || !objectstore.SupportsContextConditionalCreate(objects) {
		t.Fatal("RustFS store does not provide cancellable conditional create")
	}

	runID := digest.FromString(fmt.Sprintf("durable-rustfs-replay-%d", time.Now().UnixNano())).Encoded()
	operationID := "rootfs.import." + runID
	prefix := "rootfs/integration/durable-replay/" + runID
	t.Cleanup(func() {
		for _, key := range listRootFSImportIntegrationKeys(t, objects, prefix) {
			if err := objects.Delete(key); err != nil {
				t.Errorf("delete RustFS integration object %s: %v", key, err)
			}
		}
	})

	source := digest.FromString("source-" + runID)
	procd := digest.FromString("procd-" + runID)
	spec, err := rootfsimporter.NormalizeOperationSpec(rootfsimporter.OperationSpec{
		SourceOCIRef: "docker.io/library/alpine@" + source.String(),
		Platform: rootfsimporter.ReadyArtifactPlatform{
			OS: "linux", Architecture: "amd64",
		},
		FormatGeneration: 1,
		ProcdProtocol:    "sandbox0.procd.v1",
		ProcdDigest:      procd.String(),
		LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
		BlockOptions:     rootfsblock.BuildOptions{ObjectPrefix: prefix},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupRootFSImportIntegrationRows(t, pool, operationID, spec.SourceOCIRef, prefix)
	})
	if _, err := store.BeginRootFSImport(ctx, &sandboxstore.BeginRootFSImportRequest{
		OperationID: operationID, Spec: spec,
	}); err != nil {
		t.Fatal(err)
	}

	marker := []byte("durable-import-replayed-before-ready-cas\n")
	const markerOffset = int64(4 << 20)
	durable, err := NewDurableBuilder(DurableBuilderConfig{
		Store: store,
		Unpacker: rootFSImportIntegrationUnpacker{
			manifest: digest.FromString("manifest-" + runID),
			config:   digest.FromString("config-" + runID),
			layer:    digest.FromString("layer-" + runID),
			diffID:   digest.FromString("diff-id-" + runID),
		},
		Filesystem: rootFSImportIntegrationSparseFilesystem{marker: marker, offset: markerOffset},
		Publisher:  rootfsblock.ObjectStorePublisher{Store: conditional},
		WorkRoot:   t.TempDir(),
		ProcdPath:  "/opt/sandbox0/bin/procd",
	})
	if err != nil {
		t.Fatal(err)
	}

	var firstResult rootfsimporter.BuildResult
	var firstLease sandboxstore.RootFSImportLease
	firstBuilder := OperationBuilderFunc(func(
		buildCtx context.Context,
		operation *sandboxstore.RootFSImportOperation,
		lease sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		built, buildErr := durable.Build(buildCtx, operation, lease)
		if buildErr != nil {
			return rootfsimporter.BuildResult{}, buildErr
		}
		firstResult = built
		firstLease = lease
		return built, errors.New("simulated result loss before ready CAS")
	})
	firstWorker := newRootFSImportIntegrationWorker(t, store, firstBuilder, "manager.integration.first", spec)
	firstPass, err := firstWorker.RunOnce(ctx)
	var attemptErr *AttemptError
	if err == nil || !errors.As(err, &attemptErr) || attemptErr.Category != failureBuild {
		t.Fatalf("first pass error = %v, want sanitized build failure", err)
	}
	if firstPass.Leased != 1 || firstPass.Released != 1 || firstPass.Failed != 1 ||
		firstPass.Ready != 0 || firstPass.FailureCategory != failureBuild {
		t.Fatalf("unexpected first pass result: %#v", firstPass)
	}
	if err := firstResult.Validate(); err != nil {
		t.Fatalf("validate first durable build: %v", err)
	}
	pending, err := store.GetRootFSImportOperation(ctx, operationID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.State != sandboxstore.RootFSImportStatePending || pending.AttemptCount != 1 ||
		pending.LeaseOwner != "" || pending.LeaseToken != "" {
		t.Fatalf("operation was not durably released: %#v", pending)
	}

	keysAfterFirst := listRootFSImportIntegrationKeys(t, objects, prefix)
	if len(keysAfterFirst) == 0 || len(keysAfterFirst) != len(firstResult.References) {
		t.Fatalf("first publication keys=%d references=%d", len(keysAfterFirst), len(firstResult.References))
	}
	journalKeys := queryRootFSImportIntegrationKeys(t, ctx, pool, `
		SELECT object_key FROM manager.rootfs_import_operation_objects
		WHERE operation_id = $1 AND upload_state = 'published' ORDER BY object_key
	`, operationID)
	if !reflect.DeepEqual(keysAfterFirst, journalKeys) {
		t.Fatalf("RustFS and published journal keys differ:\nRustFS=%v\njournal=%v", keysAfterFirst, journalKeys)
	}
	if _, err := store.PublishReadyRootFSImport(ctx, &sandboxstore.PublishReadyRootFSImportRequest{
		Lease: firstLease, Result: firstResult,
	}); !errors.Is(err, sandboxstore.ErrRootFSImportLeaseLost) {
		t.Fatalf("stale ready CAS error = %v, want lease lost", err)
	}

	var secondResult rootfsimporter.BuildResult
	secondBuilder := OperationBuilderFunc(func(
		buildCtx context.Context,
		operation *sandboxstore.RootFSImportOperation,
		lease sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		built, buildErr := durable.Build(buildCtx, operation, lease)
		if buildErr == nil {
			secondResult = built
		}
		return built, buildErr
	})
	secondWorker := newRootFSImportIntegrationWorker(t, store, secondBuilder, "manager.integration.second", spec)
	secondPass, err := secondWorker.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if secondPass.Leased != 1 || secondPass.Ready != 1 || secondPass.Failed != 0 {
		t.Fatalf("unexpected replay pass result: %#v", secondPass)
	}
	if !reflect.DeepEqual(firstResult, secondResult) {
		t.Fatal("replayed build result differs from the first published result")
	}
	keysAfterReplay := listRootFSImportIntegrationKeys(t, objects, prefix)
	if !reflect.DeepEqual(keysAfterFirst, keysAfterReplay) {
		t.Fatalf("replay changed immutable RustFS objects:\nfirst=%v\nreplay=%v", keysAfterFirst, keysAfterReplay)
	}

	ready, err := store.GetRootFSImportOperation(ctx, operationID)
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != sandboxstore.RootFSImportStateReady || ready.AttemptCount != 2 || ready.ArtifactDigest == "" {
		t.Fatalf("operation did not reach Ready on attempt two: %#v", ready)
	}
	artifact, err := store.GetReadyRootFSBaseArtifactByDigest(ctx, ready.ArtifactDigest,
		sandboxstore.RootFSArtifactPlatform{OS: spec.Platform.OS, Architecture: spec.Platform.Architecture},
		sandboxstore.ReadyRootFSArtifactRequirements{
			FormatGeneration: spec.FormatGeneration,
			LogicalSizeBytes: spec.LogicalSizeBytes,
			ProcdProtocol:    spec.ProcdProtocol,
			ProcdDigest:      spec.ProcdDigest,
		})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := rootfsblock.DecodeDescriptor(artifact.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := rootfsblock.NewReader(conditional, descriptor, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	actual := make([]byte, len(marker))
	n, err := reader.ReadAt(actual, markerOffset)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if n != len(marker) || !reflect.DeepEqual(marker, actual) {
		t.Fatalf("reconstructed marker = %q, want %q", actual, marker)
	}

	catalogKeys := queryRootFSImportIntegrationKeys(t, ctx, pool, `
		SELECT object_key FROM manager.rootfs_materialization_objects
		WHERE object_key LIKE $1 ORDER BY object_key
	`, prefix+"/%")
	artifactKeys := queryRootFSImportIntegrationKeys(t, ctx, pool, `
		SELECT object_key FROM manager.rootfs_base_artifact_objects
		WHERE artifact_digest = $1 ORDER BY object_key
	`, artifact.ArtifactDigest)
	if !reflect.DeepEqual(keysAfterReplay, catalogKeys) || !reflect.DeepEqual(catalogKeys, artifactKeys) {
		t.Fatalf("object reachability differs:\nRustFS=%v\ncatalog=%v\nartifact=%v",
			keysAfterReplay, catalogKeys, artifactKeys)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET updated_at = NOW() - INTERVAL '2 hours'
		WHERE operation_id = $1
	`, operationID); err != nil {
		t.Fatal(err)
	}
	garbage, err := store.ReconcileRootFSImportGarbage(ctx, time.Minute, 100)
	if err != nil {
		t.Fatal(err)
	}
	if garbage.PurgedReady != 1 || garbage.EnqueuedObjects != 0 {
		t.Fatalf("unexpected terminal reconciliation: %#v", garbage)
	}
	if _, err := store.GetRootFSImportOperation(ctx, operationID); !errors.Is(err, sandboxstore.ErrRootFSImportNotFound) {
		t.Fatalf("terminal operation lookup error = %v, want not found", err)
	}
	keysAfterGarbage := listRootFSImportIntegrationKeys(t, objects, prefix)
	artifactKeysAfterGarbage := queryRootFSImportIntegrationKeys(t, ctx, pool, `
		SELECT object_key FROM manager.rootfs_base_artifact_objects
		WHERE artifact_digest = $1 ORDER BY object_key
	`, artifact.ArtifactDigest)
	if !reflect.DeepEqual(keysAfterReplay, keysAfterGarbage) ||
		!reflect.DeepEqual(artifactKeys, artifactKeysAfterGarbage) {
		t.Fatal("terminal journal garbage collection removed live artifact objects")
	}
	var deletionQueue int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM manager.rootfs_object_deletions WHERE object_key LIKE $1
	`, prefix+"/%").Scan(&deletionQueue); err != nil {
		t.Fatal(err)
	}
	if deletionQueue != 0 {
		t.Fatalf("live ready artifact has %d queued object deletions", deletionQueue)
	}
}

type rootFSImportIntegrationUnpacker struct {
	manifest digest.Digest
	config   digest.Digest
	layer    digest.Digest
	diffID   digest.Digest
}

func (u rootFSImportIntegrationUnpacker) Import(
	_ context.Context,
	request ocirootfs.Request,
) (ocirootfs.Result, error) {
	root, err := os.MkdirTemp(request.WorkRoot, "oci-rootfs-")
	if err != nil {
		return ocirootfs.Result{}, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return ocirootfs.Result{}, err
	}
	if err := os.Mkdir(filepath.Join(root, "etc"), 0o755); err != nil {
		return ocirootfs.Result{}, err
	}
	if err := os.WriteFile(filepath.Join(root, "etc", "source"), []byte("verified-import\n"), 0o640); err != nil {
		return ocirootfs.Result{}, err
	}
	source, err := rootfsimporter.PinnedSourceDigest(request.Reference)
	if err != nil {
		return ocirootfs.Result{}, err
	}
	return ocirootfs.Result{
		Reference: request.Reference, SourceDigest: source,
		ManifestDigest: u.manifest, ConfigDigest: u.config, Platform: request.Platform,
		LayerDigests: []digest.Digest{u.layer}, DiffIDs: []digest.Digest{u.diffID},
		ProcdDigest: request.ExpectedProcdDigest, RootPath: root, UnpackedBytes: 16, Files: 2,
	}, nil
}

type rootFSImportIntegrationSparseFilesystem struct {
	marker []byte
	offset int64
}

func (b rootFSImportIntegrationSparseFilesystem) Build(
	_ context.Context,
	_ string,
	destination string,
	logicalSize int64,
) error {
	image, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer image.Close()
	if err := image.Truncate(logicalSize); err != nil {
		return err
	}
	if _, err := image.WriteAt(b.marker, b.offset); err != nil {
		return err
	}
	return image.Sync()
}

type rootFSImportIntegrationMigrateLogger struct{ t *testing.T }

func (l rootFSImportIntegrationMigrateLogger) Printf(string, ...any) {}

func (l rootFSImportIntegrationMigrateLogger) Fatalf(format string, args ...any) {
	l.t.Fatalf(format, args...)
}

func newRootFSImportIntegrationPool(
	t *testing.T,
	ctx context.Context,
	databaseURL string,
) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS manager CASCADE"); err != nil {
		t.Fatalf("reset dedicated manager schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS manager CASCADE"); err != nil {
			t.Errorf("drop integration manager schema: %v", err)
		}
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
	logger := rootFSImportIntegrationMigrateLogger{t: t}
	if err := egressauthstore.RunMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	if err := sandboxstore.RunSandboxStoreMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	return pool
}

func newRootFSImportIntegrationWorker(
	t *testing.T,
	store Store,
	builder OperationBuilder,
	workerID string,
	spec rootfsimporter.OperationSpec,
) *Worker {
	t.Helper()
	worker, err := New(Config{
		Store: store, Builder: builder, WorkerID: workerID,
		Interval: time.Second, BuildTimeout: 2 * time.Minute,
		LeaseTTL: 30 * time.Second, LeaseRenewal: 5 * time.Second,
		MaxAttempts: 3, GarbageInterval: time.Hour, TerminalRetention: time.Minute,
		GarbageLimit: 100, ProcdProtocol: spec.ProcdProtocol, ProcdDigest: spec.ProcdDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func listRootFSImportIntegrationKeys(t *testing.T, store objectstore.Store, prefix string) []string {
	t.Helper()
	var result []string
	var token string
	for {
		items, truncated, next, err := store.List(prefix, "", token, "", 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if !item.IsPrefix {
				result = append(result, item.Key)
			}
		}
		if !truncated {
			break
		}
		if next == "" {
			t.Fatal("truncated RustFS listing has no continuation token")
		}
		token = next
	}
	sort.Strings(result)
	return result
}

func queryRootFSImportIntegrationKeys(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	query string,
	args ...any,
) []string {
	t.Helper()
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatal(err)
		}
		result = append(result, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func cleanupRootFSImportIntegrationRows(
	t *testing.T,
	pool *pgxpool.Pool,
	operationID string,
	sourceOCIRef string,
	prefix string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`DELETE FROM manager.rootfs_import_operations WHERE operation_id = $1`, []any{operationID}},
		{`DELETE FROM manager.rootfs_base_artifacts WHERE source_oci_ref = $1`, []any{sourceOCIRef}},
		{`DELETE FROM manager.rootfs_object_deletions WHERE object_key LIKE $1`, []any{prefix + "/%"}},
		{`DELETE FROM manager.rootfs_materialization_objects WHERE object_key LIKE $1`, []any{prefix + "/%"}},
	} {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Errorf("clean RootFS import integration rows: %v", err)
		}
	}
}
