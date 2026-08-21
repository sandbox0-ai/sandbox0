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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestPublishRootFSTemplateBuildRetainsAttestationAndQueuesDeletion(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	buildID := uuid.NewString()
	tpl := &template.Template{
		TemplateID: "block-derived", Scope: naming.ScopeTeam, TeamID: "team-1", UserID: "user-1",
		Spec:      integrationTemplateSpec("registry.example/source@" + digest.FromString("source").String()),
		CreatedAt: time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID: buildID, TemplateID: tpl.TemplateID, Scope: tpl.Scope, TeamID: tpl.TeamID,
		UserID: tpl.UserID, SourceSandboxID: "source-sandbox", TargetClusterID: "cluster-a",
		RequestHash: strings.Repeat("9", 64), SnapshotID: "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "block-worker", template.TemplateBuildCaptureVersionBlockCOW, time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	capturedAt := time.Now().UTC().Add(-time.Second)
	source := template.RootFSTemplateSource{
		StorageFormat: template.RootFSTemplateStorageFormatBlockCOWV1,
		SnapshotID:    build.SnapshotID, GenerationID: "generation-1",
		SourceOCIDigest:    digest.FromString("source").String(),
		BaseArtifactDigest: digest.FromString("artifact").String(), FormatGeneration: 4,
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64", Variant: "v3"},
	}
	captureMetadata := fmt.Sprintf(
		`{"version":2,"snapshot_id":%q,"storage_format":%q,"head_generation_id":%q,"source_oci_digest":%q,"base_artifact_digest":%q,"format_generation":%d,"platform":{"os":%q,"architecture":%q,"variant":%q}}`,
		source.SnapshotID, source.StorageFormat, source.GenerationID, source.SourceOCIDigest,
		source.BaseArtifactDigest, source.FormatGeneration, source.Platform.OS,
		source.Platform.Architecture, source.Platform.Variant,
	)
	if err := store.MarkTemplateBuildCaptured(
		ctx, buildID, "block-worker", build.SnapshotID, []byte(captureMetadata), capturedAt,
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.ReleaseTemplateBuild(ctx, buildID, "block-worker", time.Now().UTC(), "regional handoff"); err != nil {
		t.Fatalf("ReleaseTemplateBuild() error = %v", err)
	}
	wrongMode, err := store.ClaimTemplateBuild(
		ctx, "cluster-b", "oci-worker", template.TemplateBuildCaptureVersionOCI, time.Minute,
	)
	if err != nil {
		t.Fatalf("wrong-mode ClaimTemplateBuild() error = %v", err)
	}
	if wrongMode != nil {
		t.Fatalf("OCI worker claimed block-COW publication = %#v", wrongMode)
	}
	claimed, err := store.ClaimTemplateBuild(
		ctx, "cluster-b", "block-publish-worker",
		template.TemplateBuildCaptureVersionBlockCOW, time.Minute,
	)
	if err != nil {
		t.Fatalf("block publishing ClaimTemplateBuild() error = %v", err)
	}
	if claimed == nil || claimed.BuildID != buildID {
		t.Fatalf("block publishing claim = %#v", claimed)
	}
	if err := store.PublishTemplateBuild(
		ctx, buildID, "block-publish-worker", tpl.Spec, tpl.Spec.MainContainer.Image,
	); !errors.Is(err, template.ErrTemplateBuildLeaseLost) {
		t.Fatalf("OCI publication of block capture error = %v, want lease lost", err)
	}
	mismatchedSource := source
	mismatchedSource.GenerationID = "substituted-generation"
	if err := store.PublishRootFSTemplateBuild(
		ctx, buildID, "block-publish-worker", mismatchedSource, capturedAt,
	); !errors.Is(err, template.ErrTemplateBuildLeaseLost) {
		t.Fatalf("mismatched RootFS publication error = %v, want lease lost", err)
	}
	if err := store.PublishRootFSTemplateBuild(ctx, buildID, "block-publish-worker", source, capturedAt); err != nil {
		t.Fatalf("PublishRootFSTemplateBuild() error = %v", err)
	}

	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if loaded == nil || !loaded.ReadyForClaim() || loaded.RootFS == nil || !reflect.DeepEqual(*loaded.RootFS, source) {
		t.Fatalf("published template = %#v", loaded)
	}
	var builds int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduler_template_builds WHERE build_id = $1::uuid`, buildID).Scan(&builds); err != nil {
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
		t.Fatalf("cleanup claimed snapshot still referenced by template = %#v", cleanup)
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
	if err := store.FinishTemplateRootFSDeletion(ctx, cleanup.SnapshotID, "cleanup-worker"); err != nil {
		t.Fatalf("FinishTemplateRootFSDeletion() error = %v", err)
	}
}

func TestTemplateRootFSDeletionWaitsForCanceledBuildCleanup(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	buildID := uuid.NewString()
	tpl := &template.Template{
		TemplateID: "canceled-before-capture", Scope: naming.ScopeTeam,
		TeamID: "team-1", UserID: "user-1",
		Spec: integrationTemplateSpec("registry.example/source@" + digest.FromString("source").String()),
	}
	build := &template.TemplateBuild{
		BuildID: buildID, TemplateID: tpl.TemplateID, Scope: tpl.Scope, TeamID: tpl.TeamID,
		UserID: tpl.UserID, SourceSandboxID: "source-sandbox", TargetClusterID: "cluster-a",
		RequestHash: strings.Repeat("8", 64), SnapshotID: template.BuildSnapshotID(buildID),
	}
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
	canceled, err := store.ClaimTemplateBuild(
		ctx, "cluster-b", "canceled-build-worker",
		template.TemplateBuildCaptureVersionBlockCOW, time.Minute,
	)
	if err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if canceled == nil || canceled.BuildID != buildID || canceled.CancelRequestedAt.IsZero() {
		t.Fatalf("canceled build claim = %#v", canceled)
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "canceled-build-worker"); err != nil {
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

func TestClaimTemplateBuildRecoversReconcilingCleanupAfterWorkerCrash(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	buildID := uuid.NewString()
	initialSpec := integrationTemplateSpec("ubuntu:22.04")
	tpl := &template.Template{
		TemplateID: "derived",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       initialSpec,
		CreatedAt:  time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID:         buildID,
		TemplateID:      tpl.TemplateID,
		Scope:           tpl.Scope,
		TeamID:          tpl.TeamID,
		UserID:          tpl.UserID,
		SourceSandboxID: "source-sandbox",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("a", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, created, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	} else if !created {
		t.Fatal("CreateTemplateBuild() created = false, want true")
	}

	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker-before-crash", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if claimed == nil || claimed.Stage != v1alpha1.TemplateCreationStageCapturing {
		t.Fatalf("initial claimed build = %#v, want capturing", claimed)
	}
	if err := store.MarkTemplateBuildCaptured(ctx, buildID, "worker-before-crash", build.SnapshotID, []byte(`{"version":1}`), time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	finalSpec := integrationTemplateSpec("registry.internal/team-1/derived@sha256:" + strings.Repeat("b", 64))
	if err := store.PublishTemplateBuild(ctx, buildID, "worker-before-crash", finalSpec, finalSpec.MainContainer.Image); err != nil {
		t.Fatalf("PublishTemplateBuild() error = %v", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE build_id = $1::uuid
	`, buildID); err != nil {
		t.Fatalf("expire crashed worker lease: %v", err)
	}

	recovered, err := store.ClaimTemplateBuild(ctx, "cluster-b", "cleanup-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("recovery ClaimTemplateBuild() error = %v", err)
	}
	if recovered == nil {
		t.Fatal("recovery ClaimTemplateBuild() = nil, want reconciling cleanup job")
	}
	if recovered.Stage != v1alpha1.TemplateCreationStageReconciling {
		t.Fatalf("recovered stage = %q, want %q", recovered.Stage, v1alpha1.TemplateCreationStageReconciling)
	}
	if recovered.OutputImage != finalSpec.MainContainer.Image {
		t.Fatalf("recovered output image = %q, want %q", recovered.OutputImage, finalSpec.MainContainer.Image)
	}
	if recovered.DesiredSpec.MainContainer.Image != finalSpec.MainContainer.Image {
		t.Fatalf("recovered spec image = %q, want %q", recovered.DesiredSpec.MainContainer.Image, finalSpec.MainContainer.Image)
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "cleanup-worker"); err != nil {
		t.Fatalf("FinishTemplateBuild() error = %v", err)
	}

	var remaining int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduler_template_builds WHERE build_id = $1::uuid`, buildID).Scan(&remaining); err != nil {
		t.Fatalf("count finished builds: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining build rows = %d, want 0", remaining)
	}
}

func TestClaimTemplateBuildRecoversFailedBuildCleanupAfterWorkerCrash(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	buildID := uuid.NewString()
	tpl := &template.Template{
		TemplateID: "failed-derived",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
		CreatedAt:  time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID:         buildID,
		TemplateID:      tpl.TemplateID,
		Scope:           tpl.Scope,
		TeamID:          tpl.TeamID,
		UserID:          tpl.UserID,
		SourceSandboxID: "source-sandbox",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("e", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker-before-crash", template.TemplateBuildCaptureVersionOCI, time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if err := store.MarkTemplateBuildCaptured(ctx, buildID, "worker-before-crash", build.SnapshotID, []byte(`{"version":1}`), time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.FailTemplateBuild(ctx, buildID, "worker-before-crash", "publish_failed", "registry unavailable"); err != nil {
		t.Fatalf("FailTemplateBuild() error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE build_id = $1::uuid
	`, buildID); err != nil {
		t.Fatalf("expire failed worker lease: %v", err)
	}

	recovered, err := store.ClaimTemplateBuild(ctx, "cluster-b", "cleanup-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("cleanup ClaimTemplateBuild() error = %v", err)
	}
	if recovered == nil || recovered.CancelRequestedAt.IsZero() {
		t.Fatalf("recovered failed build = %#v, want cancellation cleanup job", recovered)
	}
	if recovered.SnapshotID != build.SnapshotID {
		t.Fatalf("recovered snapshot = %q, want %q", recovered.SnapshotID, build.SnapshotID)
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "cleanup-worker"); err != nil {
		t.Fatalf("FinishTemplateBuild() error = %v", err)
	}
	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if loaded.Status == nil || loaded.Status.Creation == nil ||
		loaded.Status.Creation.State != v1alpha1.TemplateCreationStateFailed {
		t.Fatalf("template creation status = %#v, want failed", loaded.Status)
	}
}

func TestClaimTemplateBuildKeepsCaptureSourceBoundAndAllowsPublishingTakeover(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	buildID := uuid.NewString()
	tpl := &template.Template{
		TemplateID: "publishing-takeover",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
		CreatedAt:  time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID:         buildID,
		TemplateID:      tpl.TemplateID,
		Scope:           tpl.Scope,
		TeamID:          tpl.TeamID,
		UserID:          tpl.UserID,
		SourceSandboxID: "source-sandbox",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("f", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}

	if claimed, err := store.ClaimTemplateBuild(ctx, "cluster-b", "wrong-cluster-worker", template.TemplateBuildCaptureVersionOCI, time.Minute); err != nil {
		t.Fatalf("cross-cluster capturing claim error = %v", err)
	} else if claimed != nil {
		t.Fatalf("cross-cluster capturing claim = %#v, want nil", claimed)
	}

	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "capture-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("source-cluster capturing claim error = %v", err)
	}
	if claimed == nil || claimed.Stage != v1alpha1.TemplateCreationStageCapturing {
		t.Fatalf("source-cluster claim = %#v, want capturing", claimed)
	}
	if err := store.MarkTemplateBuildCaptured(
		ctx,
		buildID,
		"capture-worker",
		build.SnapshotID,
		[]byte(`{"version":1,"durable":true}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.ReleaseTemplateBuild(ctx, buildID, "capture-worker", time.Now().UTC(), "handoff after durable capture"); err != nil {
		t.Fatalf("ReleaseTemplateBuild() error = %v", err)
	}

	wrongMode, err := store.ClaimTemplateBuild(
		ctx, "cluster-b", "block-publish-worker",
		template.TemplateBuildCaptureVersionBlockCOW, time.Minute,
	)
	if err != nil {
		t.Fatalf("wrong-mode publishing claim error = %v", err)
	}
	if wrongMode != nil {
		t.Fatalf("block worker claimed OCI publication = %#v", wrongMode)
	}
	takenOver, err := store.ClaimTemplateBuild(ctx, "cluster-b", "publish-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("publishing takeover claim error = %v", err)
	}
	if takenOver == nil || takenOver.Stage != v1alpha1.TemplateCreationStagePublishing {
		t.Fatalf("publishing takeover claim = %#v, want publishing", takenOver)
	}
	if takenOver.TargetClusterID != "cluster-a" {
		t.Fatalf("target cluster provenance = %q, want cluster-a", takenOver.TargetClusterID)
	}
}

func TestFailCapturingTemplateBuildsForClusterLeavesCapturedBuildsTakeoverEligible(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	capturingBuildID := uuid.NewString()
	capturingTemplate := &template.Template{
		TemplateID: "uncaptured-before-disable",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
		CreatedAt:  time.Now().UTC(),
	}
	capturingBuild := &template.TemplateBuild{
		BuildID:         capturingBuildID,
		TemplateID:      capturingTemplate.TemplateID,
		Scope:           capturingTemplate.Scope,
		TeamID:          capturingTemplate.TeamID,
		UserID:          capturingTemplate.UserID,
		SourceSandboxID: "source-uncaptured",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("1", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(capturingBuildID, "-", ""),
		NextAttemptAt:   time.Now().Add(time.Hour),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, capturingTemplate, capturingBuild); err != nil {
		t.Fatalf("create capturing build: %v", err)
	}

	capturedBuildID := uuid.NewString()
	capturedTemplate := &template.Template{
		TemplateID: "captured-before-disable",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
		CreatedAt:  time.Now().UTC(),
	}
	capturedBuild := &template.TemplateBuild{
		BuildID:         capturedBuildID,
		TemplateID:      capturedTemplate.TemplateID,
		Scope:           capturedTemplate.Scope,
		TeamID:          capturedTemplate.TeamID,
		UserID:          capturedTemplate.UserID,
		SourceSandboxID: "source-captured",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("2", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(capturedBuildID, "-", ""),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, capturedTemplate, capturedBuild); err != nil {
		t.Fatalf("create captured build: %v", err)
	}
	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "capture-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("claim build to capture: %v", err)
	}
	if claimed == nil || claimed.BuildID != capturedBuildID {
		t.Fatalf("claimed build = %#v, want captured build %s", claimed, capturedBuildID)
	}
	if err := store.MarkTemplateBuildCaptured(
		ctx,
		capturedBuildID,
		"capture-worker",
		capturedBuild.SnapshotID,
		[]byte(`{"version":1,"durable":true}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("mark second build captured: %v", err)
	}
	if err := store.ReleaseTemplateBuild(ctx, capturedBuildID, "capture-worker", time.Now().UTC(), "ready for takeover"); err != nil {
		t.Fatalf("release captured build: %v", err)
	}

	failed, err := store.FailCapturingTemplateBuildsForCluster(
		ctx,
		"cluster-a",
		"source_cluster_unavailable",
		`source cluster "cluster-a" was disabled before rootfs capture completed`,
	)
	if err != nil {
		t.Fatalf("FailCapturingTemplateBuildsForCluster() error = %v", err)
	}
	if failed != 1 {
		t.Fatalf("failed build count = %d, want 1", failed)
	}

	loaded, err := store.GetTemplate(ctx, capturingTemplate.Scope, capturingTemplate.TeamID, capturingTemplate.TemplateID)
	if err != nil {
		t.Fatalf("load failed template: %v", err)
	}
	if loaded.Status == nil || loaded.Status.Creation == nil ||
		loaded.Status.Creation.State != v1alpha1.TemplateCreationStateFailed ||
		loaded.Status.Creation.Reason != "source_cluster_unavailable" {
		t.Fatalf("uncaptured template creation = %#v, want source-cluster failure", loaded.Status)
	}

	cleanup, err := store.ClaimTemplateBuild(ctx, "cluster-b", "cleanup-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("claim cancellation cleanup from another cluster: %v", err)
	}
	if cleanup == nil || cleanup.BuildID != capturingBuildID || cleanup.CancelRequestedAt.IsZero() {
		t.Fatalf("cancellation cleanup claim = %#v, want build %s", cleanup, capturingBuildID)
	}
	if err := store.FinishTemplateBuild(ctx, capturingBuildID, "cleanup-worker"); err != nil {
		t.Fatalf("finish cancellation cleanup: %v", err)
	}

	publishing, err := store.ClaimTemplateBuild(ctx, "cluster-b", "publish-worker", template.TemplateBuildCaptureVersionOCI, time.Minute)
	if err != nil {
		t.Fatalf("claim captured build from another cluster: %v", err)
	}
	if publishing == nil || publishing.BuildID != capturedBuildID ||
		publishing.Stage != v1alpha1.TemplateCreationStagePublishing {
		t.Fatalf("publishing takeover = %#v, want captured build %s", publishing, capturedBuildID)
	}
}

func TestUpdateTemplatePreservesBuildIdempotencyBindingAndProvenance(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	buildID := uuid.NewString()
	idempotencyKey := "create-derived-1"
	tpl := &template.Template{
		TemplateID: "derived",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
		CreatedAt:  time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID:         buildID,
		TemplateID:      tpl.TemplateID,
		Scope:           tpl.Scope,
		TeamID:          tpl.TeamID,
		UserID:          tpl.UserID,
		SourceSandboxID: "source-sandbox",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("c", 64),
		IdempotencyKey:  idempotencyKey,
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker", template.TemplateBuildCaptureVersionOCI, time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if err := store.MarkTemplateBuildCaptured(ctx, buildID, "worker", build.SnapshotID, []byte(`{"version":1}`), time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	outputImage := "registry.internal/team-1/derived@sha256:" + strings.Repeat("d", 64)
	publishedSpec := integrationTemplateSpec(outputImage)
	if err := store.PublishTemplateBuild(ctx, buildID, "worker", publishedSpec, outputImage); err != nil {
		t.Fatalf("PublishTemplateBuild() error = %v", err)
	}
	if updated, err := store.MarkTemplateCreationReady(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID, buildID, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateCreationReady() error = %v", err)
	} else if updated {
		t.Fatal("MarkTemplateCreationReady() updated = true before cleanup, want false")
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "worker"); err != nil {
		t.Fatalf("FinishTemplateBuild() error = %v", err)
	}
	if updated, err := store.MarkTemplateCreationReady(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID, buildID, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateCreationReady() after cleanup error = %v", err)
	} else if !updated {
		t.Fatal("MarkTemplateCreationReady() after cleanup updated = false, want true")
	}

	manualImage := "ubuntu:24.04"
	if err := store.UpdateTemplate(ctx, &template.Template{
		TemplateID: tpl.TemplateID,
		Scope:      tpl.Scope,
		TeamID:     tpl.TeamID,
		UserID:     "user-2",
		Spec:       integrationTemplateSpec(manualImage),
	}); err != nil {
		t.Fatalf("UpdateTemplate() error = %v", err)
	}

	replayed, err := store.GetTemplateByIdempotencyKey(ctx, tpl.Scope, tpl.TeamID, idempotencyKey)
	if err != nil {
		t.Fatalf("GetTemplateByIdempotencyKey() error = %v", err)
	}
	if replayed == nil {
		t.Fatal("GetTemplateByIdempotencyKey() = nil, want current template after manual update")
	}
	if replayed.Spec.MainContainer.Image != manualImage {
		t.Fatalf("replayed current image = %q, want %q", replayed.Spec.MainContainer.Image, manualImage)
	}
	if replayed.CreationRequestHash != build.RequestHash {
		t.Fatalf("replayed request hash = %q, want original %q", replayed.CreationRequestHash, build.RequestHash)
	}
	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	if loaded.Spec.MainContainer.Image != manualImage {
		t.Fatalf("manual image = %q, want %q", loaded.Spec.MainContainer.Image, manualImage)
	}
	if loaded.Status == nil || loaded.Status.Creation == nil || loaded.Status.Creation.OutputImage != outputImage {
		t.Fatalf("creation provenance = %#v, want original output image %q", loaded.Status, outputImage)
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

func publishRootFSTemplateForIntegration(
	t *testing.T,
	store *Store,
	templateID string,
) (*template.Template, template.RootFSTemplateSource) {
	t.Helper()
	ctx := context.Background()
	buildID := uuid.NewString()
	sourceDigest := digest.FromString("source-" + templateID).String()
	tpl := &template.Template{
		TemplateID: templateID, Scope: naming.ScopeTeam, TeamID: "team-1", UserID: "user-1",
		Spec: integrationTemplateSpec("registry.example/source@" + sourceDigest),
	}
	build := &template.TemplateBuild{
		BuildID: buildID, TemplateID: tpl.TemplateID, Scope: tpl.Scope, TeamID: tpl.TeamID,
		UserID: tpl.UserID, SourceSandboxID: "source-sandbox", TargetClusterID: "cluster-a",
		RequestHash: strings.Repeat("7", 64), SnapshotID: template.BuildSnapshotID(buildID),
	}
	if _, _, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	}
	if _, err := store.ClaimTemplateBuild(
		ctx, "cluster-a", "block-worker", template.TemplateBuildCaptureVersionBlockCOW, time.Minute,
	); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	capturedAt := time.Now().UTC()
	if err := store.MarkTemplateBuildCaptured(
		ctx, buildID, "block-worker", build.SnapshotID, []byte(`{"version":2}`), capturedAt,
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	source := template.RootFSTemplateSource{
		StorageFormat: template.RootFSTemplateStorageFormatBlockCOWV1,
		SnapshotID:    build.SnapshotID, GenerationID: "generation-" + templateID,
		SourceOCIDigest: sourceDigest, BaseArtifactDigest: digest.FromString("artifact-" + templateID).String(),
		FormatGeneration: 1, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	if err := store.PublishRootFSTemplateBuild(ctx, buildID, "block-worker", source, capturedAt); err != nil {
		t.Fatalf("PublishRootFSTemplateBuild() error = %v", err)
	}
	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() error = %v", err)
	}
	return loaded, source
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
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	})

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithSchema(schema),
		migrate.WithTableName("goose_template_store_test"),
	); err != nil {
		t.Fatalf("migrate template store schema: %v", err)
	}
	return NewStore(pool), pool
}

func integrationTemplateSpec(image string) v1alpha1.SandboxTemplateSpec {
	return v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: image,
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
	}
}
