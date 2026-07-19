package pg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotatestutil "github.com/sandbox0-ai/sandbox0/pkg/teamquota/testutil"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	"k8s.io/apimachinery/pkg/api/resource"
)

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

	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker-before-crash", time.Minute)
	if err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if claimed == nil || claimed.Stage != v1alpha1.TemplateCreationStageCapturing {
		t.Fatalf("initial claimed build = %#v, want capturing", claimed)
	}
	if err := store.MarkTemplateBuildCaptured(ctx, buildID, "worker-before-crash", build.SnapshotID, nil, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	finalSpec := integrationTemplateSpec("registry.internal/team-1/derived@sha256:" + strings.Repeat("b", 64))
	publishTemplateBuildForIntegration(t, ctx, store, buildID, "worker-before-crash", finalSpec, finalSpec.MainContainer.Image)

	if _, err := pool.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE build_id = $1::uuid
	`, buildID); err != nil {
		t.Fatalf("expire crashed worker lease: %v", err)
	}

	recovered, err := store.ClaimTemplateBuild(ctx, "cluster-b", "cleanup-worker", time.Minute)
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
	if err := store.FinishTemplateBuild(ctx, buildID, "cleanup-worker", false); err != nil {
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

func TestTemplateControlPlaneQuotaCountsActiveBuildUntilFinish(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	quotaRepo := teamquota.NewRepository(pool)
	store.teamQuotaStore = quotaRepo
	teamID := "team-template-quota-" + uuid.NewString()
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyControlPlaneObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 2,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}

	buildID := uuid.NewString()
	tpl := &template.Template{
		TemplateID: "derived-quota",
		Scope:      naming.ScopeTeam,
		TeamID:     teamID,
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
		RequestHash:     strings.Repeat("d", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, created, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	} else if !created {
		t.Fatal("CreateTemplateBuild() created = false, want true")
	}

	blocked := &template.Template{
		TemplateID: "blocked-template",
		Scope:      naming.ScopeTeam,
		TeamID:     teamID,
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
	}
	if err := store.CreateTemplate(ctx, blocked); !teamquota.IsExceeded(err) {
		t.Fatalf("CreateTemplate() error = %v, want quota exceeded", err)
	}
	if claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "cleanup-worker", time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	} else if claimed == nil || claimed.BuildID != buildID {
		t.Fatalf("claimed build = %#v, want %s", claimed, buildID)
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "cleanup-worker", false); err != nil {
		t.Fatalf("FinishTemplateBuild() error = %v", err)
	}
	if err := store.CreateTemplate(ctx, blocked); err != nil {
		t.Fatalf("CreateTemplate() after build finish error = %v", err)
	}
}

func TestTemplateImageReservationRequiresRegistryDeleteProofBeforeAbort(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	quotaRepo := teamquota.NewRepository(pool)
	store.teamQuotaStore = quotaRepo
	teamID := "team-template-image-reservation-" + uuid.NewString()
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyTemplateImageStorageBytes,
		Kind:  teamquota.KindCapacity,
		Limit: 64,
	}); err != nil {
		t.Fatalf("set template image quota: %v", err)
	}

	buildID := uuid.NewString()
	tpl := &template.Template{
		TemplateID: "reserved-image",
		Scope:      naming.ScopeTeam,
		TeamID:     teamID,
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
		RequestHash:     strings.Repeat("a", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, created, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil || !created {
		t.Fatalf("CreateTemplateBuild() = created %v, error %v", created, err)
	}
	if claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker", time.Minute); err != nil || claimed == nil {
		t.Fatalf("ClaimTemplateBuild() = %#v, error %v", claimed, err)
	}
	if err := store.MarkTemplateBuildCaptured(
		ctx,
		buildID,
		"worker",
		build.SnapshotID,
		nil,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	manifestDigest := digest.FromString("reserved-image").String()
	if err := store.ReserveTemplateImageBuild(ctx, buildID, "worker", manifestDigest, 65); !teamquota.IsExceeded(err) {
		t.Fatalf("ReserveTemplateImageBuild() over limit error = %v, want quota exceeded", err)
	}
	var rejectedPlanPersisted bool
	if err := pool.QueryRow(ctx, `
		SELECT image_manifest_digest IS NOT NULL
			OR image_logical_size_bytes IS NOT NULL
			OR image_quota_reserved_at IS NOT NULL
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid
	`, buildID).Scan(&rejectedPlanPersisted); err != nil {
		t.Fatalf("inspect rejected image plan: %v", err)
	}
	if rejectedPlanPersisted {
		t.Fatal("quota-rejected image plan was persisted")
	}
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 0, 0)
	if err := store.ReserveTemplateImageBuild(ctx, buildID, "worker", manifestDigest, 64); err != nil {
		t.Fatalf("ReserveTemplateImageBuild() error = %v", err)
	}
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 0, 64)
	if err := store.ReserveTemplateImageBuild(ctx, buildID, "worker", manifestDigest, 64); err != nil {
		t.Fatalf("idempotent ReserveTemplateImageBuild() error = %v", err)
	}
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 0, 64)
	if err := store.MarkTemplateImagePushStarted(ctx, buildID, "worker"); err != nil {
		t.Fatalf("MarkTemplateImagePushStarted() error = %v", err)
	}
	if err := store.MarkTemplateImagePushStarted(ctx, buildID, "worker"); err != nil {
		t.Fatalf("idempotent MarkTemplateImagePushStarted() error = %v", err)
	}
	if err := store.FailTemplateBuild(ctx, buildID, "worker", "publish_failed", "registry unavailable"); err != nil {
		t.Fatalf("FailTemplateBuild() error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE build_id = $1::uuid
	`, buildID); err != nil {
		t.Fatalf("expire failed image build lease: %v", err)
	}
	const recoveryWorker = "recovery-worker"
	recovered, err := store.ClaimTemplateBuild(ctx, "cluster-a", recoveryWorker, time.Minute)
	if err != nil {
		t.Fatalf("recover push-started template build: %v", err)
	}
	if recovered == nil || recovered.ImagePushStartedAt.IsZero() ||
		recovered.ImageManifestDigest != manifestDigest ||
		recovered.ImageLogicalSizeBytes != 64 {
		t.Fatalf("recovered push-started build = %#v", recovered)
	}

	err = store.FinishTemplateBuild(ctx, buildID, recoveryWorker, false)
	if err == nil || !strings.Contains(err.Error(), "registry deletion must be confirmed") {
		t.Fatalf("FinishTemplateBuild() without delete proof error = %v", err)
	}
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 0, 64)

	if err := store.FinishTemplateBuild(ctx, buildID, recoveryWorker, true); err != nil {
		t.Fatalf("FinishTemplateBuild() with delete proof error = %v", err)
	}
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 0, 0)
	requireTemplateBuildCount(t, ctx, pool, buildID, 0)
}

func TestTemplateImageCleanupHoldsQuotaAcrossRetryConcurrencyAndRestart(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	quotaRepo := teamquota.NewRepository(pool)
	store.teamQuotaStore = quotaRepo
	teamID := "team-template-cleanup-" + uuid.NewString()
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyControlPlaneObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 2,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}

	buildID := uuid.NewString()
	templateID := "cleanup-fenced-template"
	tpl := &template.Template{
		TemplateID: templateID,
		Scope:      naming.ScopeTeam,
		TeamID:     teamID,
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:22.04"),
		CreatedAt:  time.Now().UTC(),
	}
	build := &template.TemplateBuild{
		BuildID:         buildID,
		TemplateID:      templateID,
		Scope:           naming.ScopeTeam,
		TeamID:          teamID,
		UserID:          "user-1",
		SourceSandboxID: "source-sandbox",
		TargetClusterID: "cluster-a",
		RequestHash:     strings.Repeat("f", 64),
		SnapshotID:      "template-build-" + strings.ReplaceAll(buildID, "-", ""),
	}
	if _, created, err := store.CreateTemplateBuild(ctx, tpl, build); err != nil {
		t.Fatalf("CreateTemplateBuild() error = %v", err)
	} else if !created {
		t.Fatal("CreateTemplateBuild() created = false, want true")
	}
	if claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "publish-worker", time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	} else if claimed == nil || claimed.BuildID != buildID {
		t.Fatalf("claimed build = %#v, want %s", claimed, buildID)
	}
	if err := store.MarkTemplateBuildCaptured(
		ctx,
		buildID,
		"publish-worker",
		build.SnapshotID,
		nil,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	outputImage := "registry.internal/" + naming.TeamImageRepositoryPrefix(teamID) +
		"/template@sha256:" + strings.Repeat("a", 64)
	publishTemplateBuildForIntegration(
		t, ctx, store, buildID, "publish-worker",
		integrationTemplateSpec(outputImage), outputImage,
	)
	if err := store.FinishTemplateBuild(ctx, buildID, "publish-worker", false); err != nil {
		t.Fatalf("FinishTemplateBuild() error = %v", err)
	}
	assertTemplateControlPlaneCommitted(t, ctx, quotaRepo, teamID, 1)
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 4096, 0)

	if err := store.DeleteTemplate(ctx, naming.ScopeTeam, teamID, templateID); err != nil {
		t.Fatalf("DeleteTemplate() error = %v", err)
	}
	if loaded, err := store.GetTemplate(ctx, naming.ScopeTeam, teamID, templateID); err != nil {
		t.Fatalf("GetTemplate() after delete error = %v", err)
	} else if loaded != nil {
		t.Fatalf("GetTemplate() after delete = %#v, want nil", loaded)
	}
	assertTemplateControlPlaneCommitted(t, ctx, quotaRepo, teamID, 1)
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 4096, 0)

	replacement := &template.Template{
		TemplateID: templateID,
		Scope:      naming.ScopeTeam,
		TeamID:     teamID,
		UserID:     "user-2",
		Spec:       integrationTemplateSpec("ubuntu:24.04"),
	}
	if err := store.CreateTemplate(ctx, replacement); !errors.Is(err, template.ErrTemplateImageCleanupPending) {
		t.Fatalf("CreateTemplate() while cleanup pending error = %v, want cleanup pending", err)
	}

	const workers = 8
	start := make(chan struct{})
	results := make(chan *template.TemplateImageCleanup, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			cleanup, err := store.ClaimTemplateImageCleanup(
				ctx,
				"cluster-a",
				fmt.Sprintf("cleanup-worker-%d", worker),
				time.Minute,
			)
			results <- cleanup
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ClaimTemplateImageCleanup() error = %v", err)
		}
	}
	var first *template.TemplateImageCleanup
	for cleanup := range results {
		if cleanup == nil {
			continue
		}
		if first != nil {
			t.Fatalf("multiple workers claimed one cleanup: %#v and %#v", first, cleanup)
		}
		first = cleanup
	}
	if first == nil || first.CleanupID != buildID || first.AttemptCount != 1 {
		t.Fatalf("first cleanup claim = %#v, want build %s attempt 1", first, buildID)
	}
	if err := store.ReleaseTemplateImageCleanup(
		ctx,
		first.CleanupID,
		first.LeaseOwner,
		time.Now().UTC().Add(time.Hour),
		"registry unavailable",
	); err != nil {
		t.Fatalf("ReleaseTemplateImageCleanup() error = %v", err)
	}
	assertTemplateControlPlaneCommitted(t, ctx, quotaRepo, teamID, 1)
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 4096, 0)
	if cleanup, err := store.ClaimTemplateImageCleanup(
		ctx,
		"cluster-b",
		"wrong-cluster-worker",
		time.Minute,
	); err != nil {
		t.Fatalf("wrong cluster claim error = %v", err)
	} else if cleanup != nil {
		t.Fatalf("wrong cluster claim = %#v, want nil", cleanup)
	}
	if cleanup, err := store.ClaimTemplateImageCleanup(ctx, "cluster-a", "early-retry-worker", time.Minute); err != nil {
		t.Fatalf("early retry claim error = %v", err)
	} else if cleanup != nil {
		t.Fatalf("early retry claim = %#v, want nil before next_attempt_at", cleanup)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE scheduler_template_image_cleanups
		SET next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE cleanup_id = $1::uuid
	`, buildID); err != nil {
		t.Fatalf("make cleanup retry eligible: %v", err)
	}
	crashed, err := store.ClaimTemplateImageCleanup(ctx, "cluster-a", "crashed-cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("retry ClaimTemplateImageCleanup() error = %v", err)
	}
	if crashed == nil || crashed.AttemptCount != 2 {
		t.Fatalf("retry cleanup claim = %#v, want attempt 2", crashed)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE scheduler_template_image_cleanups
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE cleanup_id = $1::uuid
	`, buildID); err != nil {
		t.Fatalf("expire crashed cleanup worker lease: %v", err)
	}
	recovered, err := store.ClaimTemplateImageCleanup(ctx, "cluster-a", "recovery-cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("recovery ClaimTemplateImageCleanup() error = %v", err)
	}
	if recovered == nil || recovered.AttemptCount != 3 {
		t.Fatalf("recovered cleanup claim = %#v, want attempt 3", recovered)
	}
	if err := store.FinishTemplateImageCleanup(ctx, buildID, "recovery-cleanup-worker"); err != nil {
		t.Fatalf("FinishTemplateImageCleanup() error = %v", err)
	}
	assertTemplateControlPlaneCommitted(t, ctx, quotaRepo, teamID, 0)
	assertTemplateImageStorageStatus(t, ctx, quotaRepo, teamID, 0, 0)

	if err := store.CreateTemplate(ctx, replacement); err != nil {
		t.Fatalf("CreateTemplate() after cleanup error = %v", err)
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
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker-before-crash", time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if err := store.MarkTemplateBuildCaptured(ctx, buildID, "worker-before-crash", build.SnapshotID, nil, time.Now().UTC()); err != nil {
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

	recovered, err := store.ClaimTemplateBuild(ctx, "cluster-a", "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("cleanup ClaimTemplateBuild() error = %v", err)
	}
	if recovered == nil || recovered.CancelRequestedAt.IsZero() {
		t.Fatalf("recovered failed build = %#v, want cancellation cleanup job", recovered)
	}
	if recovered.SnapshotID != build.SnapshotID {
		t.Fatalf("recovered snapshot = %q, want %q", recovered.SnapshotID, build.SnapshotID)
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "cleanup-worker", false); err != nil {
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

	if claimed, err := store.ClaimTemplateBuild(ctx, "cluster-b", "wrong-cluster-worker", time.Minute); err != nil {
		t.Fatalf("cross-cluster capturing claim error = %v", err)
	} else if claimed != nil {
		t.Fatalf("cross-cluster capturing claim = %#v, want nil", claimed)
	}

	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "capture-worker", time.Minute)
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
		[]byte(`{"durable":true}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	if err := store.ReleaseTemplateBuild(ctx, buildID, "capture-worker", time.Now().UTC(), "handoff after durable capture"); err != nil {
		t.Fatalf("ReleaseTemplateBuild() error = %v", err)
	}

	takenOver, err := store.ClaimTemplateBuild(ctx, "cluster-b", "publish-worker", time.Minute)
	if err != nil {
		t.Fatalf("publishing takeover claim error = %v", err)
	}
	if takenOver == nil || takenOver.Stage != v1alpha1.TemplateCreationStagePublishing {
		t.Fatalf("publishing takeover claim = %#v, want publishing", takenOver)
	}
	if takenOver.TargetClusterID != "cluster-b" {
		t.Fatalf("publishing cluster = %q, want cluster-b", takenOver.TargetClusterID)
	}
	outputImage := "registry.internal/team/template@sha256:" + strings.Repeat("b", 64)
	publishTemplateBuildForIntegration(
		t, ctx, store, buildID, "publish-worker",
		integrationTemplateSpec(outputImage), outputImage,
	)
	if err := store.FinishTemplateBuild(ctx, buildID, "publish-worker", false); err != nil {
		t.Fatalf("FinishTemplateBuild() after takeover error = %v", err)
	}
	if err := store.DeleteTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		t.Fatalf("DeleteTemplate() after takeover error = %v", err)
	}
	if cleanup, err := store.ClaimTemplateImageCleanup(
		ctx,
		"cluster-a",
		"wrong-cleanup-worker",
		time.Minute,
	); err != nil {
		t.Fatalf("original cluster cleanup claim error = %v", err)
	} else if cleanup != nil {
		t.Fatalf("original cluster cleanup claim = %#v, want nil", cleanup)
	}
	cleanup, err := store.ClaimTemplateImageCleanup(
		ctx,
		"cluster-b",
		"publishing-cluster-cleanup-worker",
		time.Minute,
	)
	if err != nil {
		t.Fatalf("publishing cluster cleanup claim error = %v", err)
	}
	if cleanup == nil || cleanup.TargetClusterID != "cluster-b" {
		t.Fatalf("publishing cluster cleanup claim = %#v, want cluster-b", cleanup)
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
	claimed, err := store.ClaimTemplateBuild(ctx, "cluster-a", "capture-worker", time.Minute)
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
		[]byte(`{"durable":true}`),
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

	cleanup, err := store.ClaimTemplateBuild(ctx, "cluster-b", "cleanup-worker", time.Minute)
	if err != nil {
		t.Fatalf("claim cancellation cleanup from another cluster: %v", err)
	}
	if cleanup == nil || cleanup.BuildID != capturingBuildID || cleanup.CancelRequestedAt.IsZero() {
		t.Fatalf("cancellation cleanup claim = %#v, want build %s", cleanup, capturingBuildID)
	}
	if err := store.FinishTemplateBuild(ctx, capturingBuildID, "cleanup-worker", false); err != nil {
		t.Fatalf("finish cancellation cleanup: %v", err)
	}

	publishing, err := store.ClaimTemplateBuild(ctx, "cluster-b", "publish-worker", time.Minute)
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
	if _, err := store.ClaimTemplateBuild(ctx, "cluster-a", "worker", time.Minute); err != nil {
		t.Fatalf("ClaimTemplateBuild() error = %v", err)
	}
	if err := store.MarkTemplateBuildCaptured(ctx, buildID, "worker", build.SnapshotID, nil, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateBuildCaptured() error = %v", err)
	}
	outputImage := "registry.internal/team-1/derived@sha256:" + strings.Repeat("d", 64)
	publishedSpec := integrationTemplateSpec(outputImage)
	publishTemplateBuildForIntegration(t, ctx, store, buildID, "worker", publishedSpec, outputImage)
	if updated, err := store.MarkTemplateCreationReady(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID, buildID, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTemplateCreationReady() error = %v", err)
	} else if updated {
		t.Fatal("MarkTemplateCreationReady() updated = true before cleanup, want false")
	}
	if err := store.FinishTemplateBuild(ctx, buildID, "worker", false); err != nil {
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

func TestCreateTemplateRejectsOversizedSpecBeforePostgresWrite(t *testing.T) {
	store, pool := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()

	var before int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduler_templates`).Scan(&before); err != nil {
		t.Fatalf("count templates before rejected create: %v", err)
	}
	tpl := &template.Template{
		TemplateID: "oversized",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec: v1alpha1.SandboxTemplateSpec{
			Description: strings.Repeat("d", int(template.MaxDescriptionBytes)+1),
		},
	}
	err := store.CreateTemplate(ctx, tpl)
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("CreateTemplate() error = %v, want TooLargeError", err)
	}

	var after int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduler_templates`).Scan(&after); err != nil {
		t.Fatalf("count templates after rejected create: %v", err)
	}
	if after != before {
		t.Fatalf("template count = %d, want unchanged %d", after, before)
	}
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
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     databaseURL,
		Schema:          schema,
		DefaultMaxConns: 10,
		DefaultMinConns: 1,
	})
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
	if err := teamquota.RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("migrate team quota schema: %v", err)
	}
	if err := teamquota.NewRepository(pool).UnsafeReplaceDefaultPoliciesForTest(
		ctx,
		teamquotatestutil.CompleteDefaultPolicies(),
	); err != nil {
		t.Fatalf("configure team quota defaults: %v", err)
	}
	return NewStore(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore())), pool
}

func integrationTemplateSpec(image string) v1alpha1.SandboxTemplateSpec {
	return v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: image,
			Resources: v1alpha1.SandboxResourceLimits{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
	}
}

func publishTemplateBuildForIntegration(
	t *testing.T,
	ctx context.Context,
	store *Store,
	buildID, workerID string,
	spec v1alpha1.SandboxTemplateSpec,
	outputImage string,
) {
	t.Helper()
	parts := strings.SplitN(outputImage, "@", 2)
	if len(parts) != 2 {
		t.Fatalf("test output image %q is not digest-pinned", outputImage)
	}
	manifestDigest, err := digest.Parse(parts[1])
	if err != nil {
		t.Fatalf("parse test output image digest: %v", err)
	}
	const logicalSizeBytes = int64(4096)
	if err := store.ReserveTemplateImageBuild(
		ctx,
		buildID,
		workerID,
		manifestDigest.String(),
		logicalSizeBytes,
	); err != nil {
		t.Fatalf("ReserveTemplateImageBuild() error = %v", err)
	}
	if err := store.MarkTemplateImagePushStarted(ctx, buildID, workerID); err != nil {
		t.Fatalf("MarkTemplateImagePushStarted() error = %v", err)
	}
	if err := store.PublishTemplateBuild(
		ctx,
		buildID,
		workerID,
		spec,
		outputImage,
		manifestDigest.String(),
		logicalSizeBytes,
	); err != nil {
		t.Fatalf("PublishTemplateBuild() error = %v", err)
	}
}

func assertTemplateControlPlaneCommitted(
	t *testing.T,
	ctx context.Context,
	repository *teamquota.Repository,
	teamID string,
	want int64,
) {
	t.Helper()
	statuses, err := repository.ListStatus(ctx, teamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	for _, status := range statuses {
		if status.Key == teamquota.KeyControlPlaneObjectCount {
			if status.Committed != want || status.Reserved != 0 {
				t.Fatalf(
					"control-plane quota committed/reserved = %d/%d, want %d/0",
					status.Committed,
					status.Reserved,
					want,
				)
			}
			return
		}
	}
	t.Fatalf("control-plane quota status not found in %#v", statuses)
}

func assertTemplateImageStorageStatus(
	t *testing.T,
	ctx context.Context,
	repository *teamquota.Repository,
	teamID string,
	wantCommitted, wantReserved int64,
) {
	t.Helper()
	statuses, err := repository.ListStatus(ctx, teamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	for _, status := range statuses {
		if status.Key != teamquota.KeyTemplateImageStorageBytes {
			continue
		}
		if status.Committed != wantCommitted || status.Reserved != wantReserved {
			t.Fatalf(
				"template image quota committed/reserved = %d/%d, want %d/%d",
				status.Committed,
				status.Reserved,
				wantCommitted,
				wantReserved,
			)
		}
		return
	}
	t.Fatalf("template image quota status not found in %#v", statuses)
}

func requireTemplateBuildCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	buildID string,
	want int,
) {
	t.Helper()
	var count int
	if err := pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM scheduler_template_builds WHERE build_id = $1::uuid`,
		buildID,
	).Scan(&count); err != nil {
		t.Fatalf("count template builds: %v", err)
	}
	if count != want {
		t.Fatalf("template build count = %d, want %d", count, want)
	}
}
