package pg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestEnsureTemplateImageRevisionStaysShadowUntilSelected(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	tpl := &template.Template{
		TemplateID: "shadow-imagefs",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:24.04"),
		CreatedAt:  time.Now().UTC(),
	}
	if err := store.CreateTemplate(ctx, tpl); err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	revision, created, err := store.EnsureTemplateImageRevision(ctx, tpl)
	if err != nil {
		t.Fatalf("EnsureTemplateImageRevision() error = %v", err)
	}
	if !created || revision == nil {
		t.Fatalf("EnsureTemplateImageRevision() = (%#v,%v), want new revision", revision, created)
	}
	loaded, err := store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() after shadow ensure error = %v", err)
	}
	if loaded.Status != nil && loaded.Status.ImageRevision != nil {
		t.Fatalf("shadow ensure selected revision %#v", loaded.Status.ImageRevision)
	}

	if err := store.SelectCurrentTemplateImageRevision(ctx, revision); err != nil {
		t.Fatalf("SelectCurrentTemplateImageRevision() error = %v", err)
	}
	loaded, err = store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() after selection error = %v", err)
	}
	if loaded.Status == nil || loaded.Status.ImageRevision == nil || loaded.Status.ImageRevision.RevisionID != revision.RevisionID {
		t.Fatalf("selected revision = %#v, want %q", loaded.Status, revision.RevisionID)
	}

	if err := store.ClearCurrentTemplateImageRevision(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		t.Fatalf("ClearCurrentTemplateImageRevision() error = %v", err)
	}
	loaded, err = store.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		t.Fatalf("GetTemplate() after clear error = %v", err)
	}
	if loaded.Status != nil && loaded.Status.ImageRevision != nil {
		t.Fatalf("cleared template still selected revision %#v", loaded.Status.ImageRevision)
	}
}

func TestRecreatedTemplateGetsNewImageRevisionIncarnation(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	tpl := &template.Template{
		TemplateID: "recreated-imagefs",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("alpine:3.20"),
	}
	recreate := func() *template.TemplateImageRevision {
		t.Helper()
		require.NoError(t, store.CreateTemplate(ctx, tpl))
		revision, created, err := store.EnsureTemplateImageRevision(ctx, tpl)
		require.NoError(t, err)
		require.True(t, created)
		require.NotEmpty(t, revision.IncarnationID)
		return revision
	}

	first := recreate()
	require.NoError(t, store.DeleteTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID))
	second := recreate()

	assert.Equal(t, first.RevisionID, second.RevisionID)
	assert.NotEqual(t, first.IncarnationID, second.IncarnationID)
}

func TestEnsureTemplateImageRevisionRevivesOnlySupersededSpec(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	tpl := &template.Template{
		TemplateID: "revived-imagefs",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		UserID:     "user-1",
		Spec:       integrationTemplateSpec("ubuntu:24.04"),
		CreatedAt:  time.Now().UTC(),
	}
	if err := store.CreateTemplate(ctx, tpl); err != nil {
		t.Fatalf("CreateTemplate() error = %v", err)
	}
	oldRevision, _, err := store.EnsureTemplateImageRevision(ctx, tpl)
	if err != nil {
		t.Fatalf("EnsureTemplateImageRevision() error = %v", err)
	}
	claimed, err := store.ClaimTemplateImageRevision(ctx, "worker-a", time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("ClaimTemplateImageRevision() error = %v", err)
	}
	if claimed == nil || claimed.RevisionID != oldRevision.RevisionID {
		t.Fatalf("claimed revision = %#v, want %q", claimed, oldRevision.RevisionID)
	}
	if err := store.FailTemplateImageRevision(
		ctx,
		claimed.RevisionID,
		"worker-a",
		template.TemplateImageRevisionReasonSuperseded,
		"template changed",
	); err != nil {
		t.Fatalf("FailTemplateImageRevision() error = %v", err)
	}

	tpl.Spec = integrationTemplateSpec("ubuntu:24.10")
	if err := store.UpdateTemplate(ctx, tpl); err != nil {
		t.Fatalf("UpdateTemplate(new spec) error = %v", err)
	}
	if _, created, err := store.EnsureTemplateImageRevision(ctx, tpl); err != nil {
		t.Fatalf("EnsureTemplateImageRevision(new spec) error = %v", err)
	} else if !created {
		t.Fatal("EnsureTemplateImageRevision(new spec) created = false, want true")
	}

	tpl.Spec = integrationTemplateSpec("ubuntu:24.04")
	if err := store.UpdateTemplate(ctx, tpl); err != nil {
		t.Fatalf("UpdateTemplate(old spec) error = %v", err)
	}
	revived, created, err := store.EnsureTemplateImageRevision(ctx, tpl)
	if err != nil {
		t.Fatalf("EnsureTemplateImageRevision(revived spec) error = %v", err)
	}
	if created {
		t.Fatal("EnsureTemplateImageRevision(revived spec) created = true, want reused revision")
	}
	if revived.RevisionID != oldRevision.RevisionID {
		t.Fatalf("revived revision = %q, want %q", revived.RevisionID, oldRevision.RevisionID)
	}
	if revived.State != template.TemplateImageRevisionStateResolving || revived.AttemptCount != 0 {
		t.Fatalf("revived revision state = %q attempts = %d, want resolving/0", revived.State, revived.AttemptCount)
	}
	if revived.Reason != "" || revived.Message != "" || !revived.CompletedAt.IsZero() {
		t.Fatalf("revived revision retained terminal status: %#v", revived)
	}
}

func TestClaimTemplateImageRevisionFiltersShadowImportCohort(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	templates := []*template.Template{
		{
			TemplateID: "private-a", Scope: naming.ScopeTeam, TeamID: "team-a", UserID: "user-1",
			Spec: integrationTemplateSpec("ubuntu:24.04"), CreatedAt: time.Now().UTC(),
		},
		{
			TemplateID: "public-a", Scope: naming.ScopePublic, UserID: "user-1",
			Spec: integrationTemplateSpec("alpine:3.20"), CreatedAt: time.Now().UTC(),
		},
		{
			TemplateID: "private-b", Scope: naming.ScopeTeam, TeamID: "team-b", UserID: "user-1",
			Spec: integrationTemplateSpec("debian:12"), CreatedAt: time.Now().UTC(),
		},
	}
	for _, tpl := range templates {
		if err := store.CreateTemplate(ctx, tpl); err != nil {
			t.Fatalf("CreateTemplate(%s) error = %v", tpl.TemplateID, err)
		}
		if _, created, err := store.EnsureTemplateImageRevision(ctx, tpl); err != nil {
			t.Fatalf("EnsureTemplateImageRevision(%s) error = %v", tpl.TemplateID, err)
		} else if !created {
			t.Fatalf("EnsureTemplateImageRevision(%s) created = false, want true", tpl.TemplateID)
		}
	}

	claimed, err := store.ClaimTemplateImageRevision(ctx, "team-worker", time.Minute, []string{"team-a"}, nil)
	if err != nil {
		t.Fatalf("team cohort claim error = %v", err)
	}
	if claimed == nil || claimed.TemplateID != "private-a" {
		t.Fatalf("team cohort claim = %#v, want private-a", claimed)
	}

	claimed, err = store.ClaimTemplateImageRevision(ctx, "template-worker", time.Minute, nil, []string{"public-a"})
	if err != nil {
		t.Fatalf("template cohort claim error = %v", err)
	}
	if claimed == nil || claimed.TemplateID != "public-a" {
		t.Fatalf("template cohort claim = %#v, want public-a", claimed)
	}

	claimed, err = store.ClaimTemplateImageRevision(ctx, "empty-worker", time.Minute, []string{"team-missing"}, []string{"missing"})
	if err != nil {
		t.Fatalf("unmatched cohort claim error = %v", err)
	}
	if claimed != nil {
		t.Fatalf("unmatched cohort claim = %#v, want nil", claimed)
	}

	claimed, err = store.ClaimTemplateImageRevision(ctx, "all-worker", time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("import-all claim error = %v", err)
	}
	if claimed == nil || claimed.TemplateID != "private-b" {
		t.Fatalf("import-all claim = %#v, want private-b", claimed)
	}
}

func TestClaimTemplateImageRevisionRequiresExactPrivateCohort(t *testing.T) {
	store, _ := newTemplateStoreIntegrationTest(t)
	ctx := context.Background()
	templates := []*template.Template{
		{
			TemplateID: "canary", Scope: naming.ScopeTeam, TeamID: "team-a", UserID: "user-1",
			Spec: integrationTemplateSpec("ubuntu:24.04"), CreatedAt: time.Now().UTC(),
		},
		{
			TemplateID: "canary", Scope: naming.ScopeTeam, TeamID: "team-b", UserID: "user-1",
			Spec: integrationTemplateSpec("alpine:3.20"), CreatedAt: time.Now().UTC(),
		},
		{
			TemplateID: "other", Scope: naming.ScopeTeam, TeamID: "team-a", UserID: "user-1",
			Spec: integrationTemplateSpec("debian:12"), CreatedAt: time.Now().UTC(),
		},
		{
			TemplateID: "canary", Scope: naming.ScopePublic, UserID: "user-1",
			Spec: integrationTemplateSpec("busybox:1.37"), CreatedAt: time.Now().UTC(),
		},
	}
	for _, tpl := range templates {
		if err := store.CreateTemplate(ctx, tpl); err != nil {
			t.Fatalf("CreateTemplate(%s/%s) error = %v", tpl.TeamID, tpl.TemplateID, err)
		}
		if _, created, err := store.EnsureTemplateImageRevision(ctx, tpl); err != nil {
			t.Fatalf("EnsureTemplateImageRevision(%s/%s) error = %v", tpl.TeamID, tpl.TemplateID, err)
		} else if !created {
			t.Fatalf("EnsureTemplateImageRevision(%s/%s) created = false, want true", tpl.TeamID, tpl.TemplateID)
		}
	}

	claimedKeys := make(map[string]bool)
	for range 2 {
		claimed, err := store.ClaimTemplateImageRevision(ctx, "exact-worker", time.Minute, []string{"team-a"}, []string{"canary"})
		if err != nil {
			t.Fatalf("exact cohort claim error = %v", err)
		}
		if claimed == nil {
			t.Fatal("exact cohort claim = nil, want private exact or public template match")
		}
		claimedKeys[claimed.Scope+"/"+claimed.TeamID+"/"+claimed.TemplateID] = true
	}
	if !claimedKeys[naming.ScopeTeam+"/team-a/canary"] || !claimedKeys[naming.ScopePublic+"//canary"] {
		t.Fatalf("exact cohort claims = %#v, want team/team-a/canary and public//canary", claimedKeys)
	}
	claimed, err := store.ClaimTemplateImageRevision(ctx, "exact-worker", time.Minute, []string{"team-a"}, []string{"canary"})
	if err != nil {
		t.Fatalf("exhausted exact cohort claim error = %v", err)
	}
	if claimed != nil {
		t.Fatalf("exhausted exact cohort claim = %#v, want nil", claimed)
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

	recovered, err := store.ClaimTemplateBuild(ctx, "cluster-b", "cleanup-worker", time.Minute)
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
	if err := store.FinishTemplateBuild(ctx, capturingBuildID, "cleanup-worker"); err != nil {
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
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: databaseURL, Schema: schema, MaxConns: 4})
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
