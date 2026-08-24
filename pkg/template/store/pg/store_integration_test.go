package pg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
)

func TestListImageSourcesForRootFSImportUsesBoundedKeysetAndSkipsCapturedTemplates(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	for _, fixture := range []struct {
		id        string
		ephemeral string
	}{
		{id: "a", ephemeral: "1Gi"},
		{id: "b", ephemeral: "2Gi"},
	} {
		spec := integrationTemplateSpec("registry.example/" + fixture.id + "@" + digest.FromString(fixture.id).String())
		spec.MainContainer.Resources.EphemeralStorage = fixture.ephemeral
		if err := store.CreateTemplate(ctx, &template.Template{
			TemplateID: fixture.id, Scope: naming.ScopeTeam, TeamID: "team-1", Spec: spec,
		}); err != nil {
			t.Fatal(err)
		}
	}
	publishRootFSTemplateForIntegration(t, store, "captured")

	first, err := store.ListImageSourcesForRootFSImport(ctx, templatestore.ImageSourceCursor{}, 1)
	if err != nil || len(first) != 1 || first[0].Cursor.TemplateID != "a" || first[0].EphemeralStorage != "1Gi" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := store.ListImageSourcesForRootFSImport(ctx, first[0].Cursor, 10)
	if err != nil || len(second) != 1 || second[0].Cursor.TemplateID != "b" || second[0].EphemeralStorage != "2Gi" {
		t.Fatalf("second page = %#v, %v", second, err)
	}
}

func TestPublishRootFSTemplateBuildRetainsAttestationAndQueuesDeletion(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	tpl, build := newTemplateBuildFixture("block-derived", "cluster-a", "9")
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "capture-worker", time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	capturedAt := time.Now().UTC().Add(-time.Second)
	source := templateBuildSource(build, tpl.Spec.MainContainer.Image, "generation-1", 4)
	if err := store.MarkTemplateBuildCaptured(
		ctx, build.BuildID, "capture-worker", build.SnapshotID, templateBuildCaptureJSON(source, capturedAt), capturedAt,
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.ReleaseTemplateBuild(ctx, build.BuildID, "capture-worker", time.Now().UTC(), "regional handoff"); err != nil {
		t.Fatalf("ReleaseTemplateBuild() error = %v", err)
	}
	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-b", "publish-worker", time.Minute)
	if err != nil {
		t.Fatalf("publishing ClaimTemplateBuild() error = %v", err)
	}
	if claimed == nil || claimed.BuildID != build.BuildID || claimed.Stage != v1alpha1.TemplateCreationStagePublishing {
		t.Fatalf("publishing claim = %#v", claimed)
	}
	mismatched := source
	mismatched.GenerationID = "substituted-generation"
	if err := store.PublishRootFSTemplateBuild(
		ctx, build.BuildID, "publish-worker", mismatched, capturedAt,
	); !errors.Is(err, template.ErrTemplateBuildLeaseLost) {
		t.Fatalf("mismatched publication error = %v, want lease lost", err)
	}
	if err := store.PublishRootFSTemplateBuild(ctx, build.BuildID, "publish-worker", source, capturedAt); err != nil {
		t.Fatalf("PublishRootFSTemplateBuild() error = %v", err)
	}

	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if loaded == nil || !loaded.ReadyForClaim() || loaded.RootFS == nil || !reflect.DeepEqual(*loaded.RootFS, source) {
		t.Fatalf("published template = %#v", loaded)
	}
	if loaded.Status == nil || loaded.Status.Creation == nil ||
		loaded.Status.Creation.Stage != v1alpha1.TemplateCreationStagePublishing {
		t.Fatalf("published creation status = %#v", loaded.Status)
	}
	var builds int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduler_template_builds WHERE build_id = $1::uuid`, build.BuildID).Scan(&builds); err != nil {
		t.Fatalf("count published build rows: %v", err)
	}
	if builds != 0 {
		t.Fatalf("published build rows = %d, want 0", builds)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO scheduler_template_rootfs_deletions (snapshot_id, team_id)
		VALUES ($1, $2)
	`, source.SnapshotID, tpl.TeamID); err != nil {
		t.Fatalf("insert stale cleanup tombstone: %v", err)
	}
	cleanup, err := store.ClaimTemplateRootFSDeletion(ctx, "premature-cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("premature ClaimTemplateRootFSDeletion() error = %v", err)
	}
	if cleanup != nil {
		t.Fatalf("cleanup claimed referenced snapshot = %#v", cleanup)
	}
	deleted, err := store.CancelTemplateBuildAndDeleteTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil || !deleted {
		t.Fatalf("CancelTemplateBuildAndDeleteTemplate() = deleted %v, error %v", deleted, err)
	}
	cleanup, err = store.ClaimTemplateRootFSDeletion(ctx, "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateRootFSDeletion() error = %v", err)
	}
	if cleanup == nil || cleanup.SnapshotID != source.SnapshotID || cleanup.TeamID != tpl.TeamID {
		t.Fatalf("cleanup tombstone = %#v", cleanup)
	}
}

func TestTemplateRootFSDeletionWaitsForCanceledBuildCleanup(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	tpl, build := newTemplateBuildFixture("canceled-before-capture", "cluster-a", "8")
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.CancelTemplateBuildAndDeleteTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		t.Fatalf("CancelTemplateBuildAndDeleteTemplate() error = %v", err)
	}
	deletion, err := store.ClaimTemplateRootFSDeletion(ctx, "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("premature ClaimTemplateRootFSDeletion() error = %v", err)
	}
	if deletion != nil {
		t.Fatalf("cleanup claimed snapshot still owned by canceled build = %#v", deletion)
	}
	canceled, err := store.ClaimTemplateBuild(ctx, "cluster-b", "canceled-build-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if canceled == nil || canceled.BuildID != build.BuildID || canceled.CancelRequestedAt.IsZero() {
		t.Fatalf("canceled build claim = %#v", canceled)
	}
	if err := store.FinishTemplateBuild(ctx, build.BuildID, "canceled-build-worker"); err != nil {
		t.Fatalf("FinishTemplateBuild() error = %v", err)
	}
	deletion, err = store.ClaimTemplateRootFSDeletion(ctx, "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateRootFSDeletion() error = %v", err)
	}
	if deletion == nil || deletion.SnapshotID != build.SnapshotID {
		t.Fatalf("cleanup tombstone = %#v", deletion)
	}
}

func TestClaimTemplateBuildKeepsCaptureSourceBoundAndAllowsPublishingTakeover(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	tpl, build := newTemplateBuildFixture("publishing-takeover", "cluster-a", "f")
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if claimed, err := store.ClaimTemplateBuild(ctx, "cluster-b", "wrong-cluster-worker", time.Minute); err != nil {
		t.Fatalf("cross-cluster capturing claim error = %v", err)
	} else if claimed != nil {
		t.Fatalf("cross-cluster capturing claim = %#v, want nil", claimed)
	}
	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "capture-worker", time.Minute)
	if err != nil || claimed == nil || claimed.Stage != v1alpha1.TemplateCreationStageCapturing {
		t.Fatalf("source-cluster claim = %#v, error %v", claimed, err)
	}
	source := templateBuildSource(build, tpl.Spec.MainContainer.Image, "generation-takeover", 1)
	capturedAt := time.Now().UTC()
	if err := store.MarkTemplateBuildCaptured(
		ctx, build.BuildID, "capture-worker", build.SnapshotID, templateBuildCaptureJSON(source, capturedAt), capturedAt,
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.ReleaseTemplateBuild(ctx, build.BuildID, "capture-worker", time.Now().UTC(), "durable handoff"); err != nil {
		t.Fatalf("ReleaseTemplateBuild() error = %v", err)
	}
	takenOver, err := store.ClaimTemplateBuild(ctx, "cluster-b", "publish-worker", time.Minute)
	if err != nil {
		t.Fatalf("publishing takeover claim error = %v", err)
	}
	if takenOver == nil || takenOver.Stage != v1alpha1.TemplateCreationStagePublishing ||
		takenOver.TargetClusterID != "cluster-a" {
		t.Fatalf("publishing takeover claim = %#v", takenOver)
	}
}

func TestUpdateTemplatePreservesOrReleasesCapturedRootFSByImageIdentity(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	tpl, source := publishRootFSTemplateForIntegration(t, store, "block-update")

	sameImage := *tpl
	sameImage.Spec = *tpl.Spec.DeepCopy()
	sameImage.Spec.Description = "metadata-only update"
	sameImage.UserID = "user-2"
	if err := store.UpdateTemplate(ctx, &sameImage); err != nil {
		t.Fatalf("same-image UpdateTemplate() error = %v", err)
	}
	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if loaded.RootFS == nil || !reflect.DeepEqual(*loaded.RootFS, source) {
		t.Fatalf("same-image update changed RootFS source = %#v", loaded.RootFS)
	}
	deletion, err := store.ClaimTemplateRootFSDeletion(ctx, "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateRootFSDeletion() error = %v", err)
	}
	if deletion != nil {
		t.Fatalf("same-image update queued cleanup = %#v", deletion)
	}

	changedImage := *loaded
	changedImage.Spec = *loaded.Spec.DeepCopy()
	changedImage.Spec.MainContainer.Image = "registry.example/replacement@" + digest.FromString("replacement").String()
	if err := store.UpdateTemplate(ctx, &changedImage); err != nil {
		t.Fatalf("changed-image UpdateTemplate() error = %v", err)
	}
	loaded, err = store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() after image update error = %v", err)
	}
	if loaded.RootFS != nil {
		t.Fatalf("changed-image update retained stale RootFS = %#v", loaded.RootFS)
	}
	deletion, err = store.ClaimTemplateRootFSDeletion(ctx, "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateRootFSDeletion() after image update error = %v", err)
	}
	if deletion == nil || deletion.SnapshotID != source.SnapshotID {
		t.Fatalf("changed-image cleanup tombstone = %#v", deletion)
	}
}

func publishRootFSTemplateForIntegration(t *testing.T, store *Store, templateID string) (*template.Template, template.RootFSTemplateSource) {
	t.Helper()
	ctx := context.Background()
	tpl, build := newTemplateBuildFixture(templateID, "cluster-a", "7")
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker", time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	capturedAt := time.Now().UTC()
	source := templateBuildSource(build, tpl.Spec.MainContainer.Image, "generation-"+templateID, 1)
	if err := store.MarkTemplateBuildCaptured(
		ctx, build.BuildID, "worker", build.SnapshotID, templateBuildCaptureJSON(source, capturedAt), capturedAt,
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.PublishRootFSTemplateBuild(ctx, build.BuildID, "worker", source, capturedAt); err != nil {
		t.Fatalf("PublishRootFSTemplateBuild() error = %v", err)
	}
	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	return loaded, source
}

func newTemplateBuildFixture(templateID, clusterID, hashCharacter string) (*template.Template, *template.TemplateBuild) {
	buildID := uuid.NewString()
	sourceDigest := digest.FromString("source-" + templateID).String()
	tpl := &template.Template{
		TemplateID: templateID, Scope: naming.ScopeTeam, TeamID: "team-1", UserID: "user-1",
		Spec: integrationTemplateSpec("registry.example/source@" + sourceDigest), CreatedAt: time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID: buildID, TemplateID: tpl.TemplateID, Scope: tpl.Scope, TeamID: tpl.TeamID,
		UserID: tpl.UserID, SourceSandboxID: "source-sandbox", TargetClusterID: clusterID,
		RequestHash: strings.Repeat(hashCharacter, 64), SnapshotID: template.BuildSnapshotID(buildID),
	}
	return tpl, build
}

func templateBuildSource(build *template.TemplateBuild, image, generationID string, formatGeneration int) template.RootFSTemplateSource {
	parts := strings.Split(image, "@")
	return template.RootFSTemplateSource{
		StorageFormat: template.RootFSTemplateStorageFormatBlockCOWV1,
		SnapshotID:    build.SnapshotID, GenerationID: generationID,
		SourceOCIDigest: parts[len(parts)-1], BaseArtifactDigest: digest.FromString("artifact-" + build.TemplateID).String(),
		FormatGeneration: formatGeneration, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
}

func templateBuildCaptureJSON(source template.RootFSTemplateSource, capturedAt time.Time) []byte {
	return []byte(fmt.Sprintf(
		`{"version":%d,"snapshot_id":%q,"platform":{"os":%q,"architecture":%q,"variant":%q},"captured_at":%q,"storage_format":%q,"head_generation_id":%q,"source_oci_digest":%q,"base_artifact_digest":%q,"format_generation":%d}`,
		template.TemplateBuildCaptureVersion, source.SnapshotID, source.Platform.OS, source.Platform.Architecture,
		source.Platform.Variant, capturedAt.UTC().Format(time.RFC3339Nano), source.StorageFormat, source.GenerationID,
		source.SourceOCIDigest, source.BaseArtifactDigest, source.FormatGeneration,
	))
}

func newTemplateStoreIntegrationTest(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	schema := "template_store_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: databaseURL, Schema: schema, DefaultMaxConns: 4})
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() { _, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)) })
	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(migrations.FS), migrate.WithSchema(schema), migrate.WithTableName("goose_template_store_test")); err != nil {
		t.Fatalf("migrate template store schema: %v", err)
	}
	return NewStore(pool), pool
}

func integrationTemplateSpec(image string) v1alpha1.SandboxTemplateSpec {
	return v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{Image: image},
	}
}
