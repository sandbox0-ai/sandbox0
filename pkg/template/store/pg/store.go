package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	templateImageQuotaOwnerKind       = "template_image"
	templateImagePublishOperationKind = "template_image_publish"
	templateImageDeleteOperationKind  = "template_image_delete"
)

func templateImageQuotaOwner(teamID, templateID string) teamquota.Owner {
	return teamquota.Owner{
		TeamID: strings.TrimSpace(teamID),
		Kind:   templateImageQuotaOwnerKind,
		ID:     strings.TrimSpace(templateID),
	}
}

func templateImagePublishOperation(buildID string) teamquota.Operation {
	return teamquota.Operation{
		ID:   strings.TrimSpace(buildID) + ":image-publish",
		Kind: templateImagePublishOperationKind,
	}
}

func templateImageDeleteOperation(cleanupID string) teamquota.Operation {
	return teamquota.Operation{
		ID:   strings.TrimSpace(cleanupID) + ":image-delete",
		Kind: templateImageDeleteOperationKind,
	}
}

// Store implements template and allocation storage in PostgreSQL.
type Store struct {
	pool           *pgxpool.Pool
	teamQuotaStore teamquota.CapacityTxStore
}

// StoreOption customizes a template store.
type StoreOption func(*Store)

// WithTeamQuotaStore overrides the team quota store.
func WithTeamQuotaStore(store teamquota.CapacityTxStore) StoreOption {
	return func(templateStore *Store) {
		templateStore.teamQuotaStore = store
	}
}

// NewStore creates a new Store.
func NewStore(pool *pgxpool.Pool, opts ...StoreOption) *Store {
	templateStore := &Store{
		pool:           pool,
		teamQuotaStore: teamquota.NewRepository(pool),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(templateStore)
		}
	}
	return templateStore
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const templateSelectColumns = `
	template_id, scope, team_id, user_id, spec, created_at, updated_at,
	creation_build_id::text, creation_idempotency_key, creation_request_hash,
	creation_state, creation_stage, creation_started_at, creation_captured_at,
	creation_completed_at, creation_output_image, creation_reason, creation_message,
	creation_image_cluster_id, creation_image_logical_size_bytes
`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTemplate(row rowScanner) (*template.Template, error) {
	var tpl template.Template
	var specJSON []byte
	var buildID, idempotencyKey, requestHash *string
	var creationState string
	var creationStage, outputImage, reason, message, imageClusterID *string
	var startedAt, capturedAt, completedAt *time.Time
	var imageLogicalSizeBytes *int64
	if err := row.Scan(
		&tpl.TemplateID,
		&tpl.Scope,
		&tpl.TeamID,
		&tpl.UserID,
		&specJSON,
		&tpl.CreatedAt,
		&tpl.UpdatedAt,
		&buildID,
		&idempotencyKey,
		&requestHash,
		&creationState,
		&creationStage,
		&startedAt,
		&capturedAt,
		&completedAt,
		&outputImage,
		&reason,
		&message,
		&imageClusterID,
		&imageLogicalSizeBytes,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(specJSON, &tpl.Spec); err != nil {
		return nil, fmt.Errorf("unmarshal spec: %w", err)
	}

	tpl.CreationBuildID = stringValue(buildID)
	tpl.CreationIdempotencyKey = stringValue(idempotencyKey)
	tpl.CreationRequestHash = stringValue(requestHash)
	tpl.CreationImageClusterID = stringValue(imageClusterID)
	if imageLogicalSizeBytes != nil {
		tpl.CreationImageLogicalSizeBytes = *imageLogicalSizeBytes
	}
	if buildID != nil {
		tpl.Status = &v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State:       v1alpha1.TemplateCreationState(creationState),
				Stage:       v1alpha1.TemplateCreationStage(stringValue(creationStage)),
				StartedAt:   metaTime(startedAt),
				CapturedAt:  metaTime(capturedAt),
				CompletedAt: metaTime(completedAt),
				OutputImage: stringValue(outputImage),
				Reason:      stringValue(reason),
				Message:     stringValue(message),
			},
		}
	}
	return &tpl, nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func metaTime(value *time.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	out := metav1.NewTime(value.UTC())
	return &out
}

func lockTemplateLifecycleTx(ctx context.Context, tx pgx.Tx, scope, teamID, templateID string) error {
	if scope == naming.ScopeTeam {
		if err := teamquota.LockTeamMutationTx(ctx, tx, teamID); err != nil {
			return err
		}
	}
	lockKey := strings.Join([]string{"template-lifecycle", scope, teamID, templateID}, ":")
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return fmt.Errorf("lock template lifecycle: %w", err)
	}
	return nil
}

func ensureNoTemplateImageCleanupTx(
	ctx context.Context,
	tx pgx.Tx,
	scope, teamID, templateID string,
) error {
	var pending bool
	if err := tx.QueryRow(ctx, `
		SELECT
			EXISTS (
				SELECT 1
				FROM scheduler_template_image_cleanups
				WHERE scope = $1 AND team_id = $2 AND template_id = $3
			)
			OR EXISTS (
				SELECT 1
				FROM scheduler_template_builds
				WHERE scope = $1 AND team_id = $2 AND template_id = $3
			)
	`, scope, teamID, templateID).Scan(&pending); err != nil {
		return fmt.Errorf("check template image lifecycle cleanup: %w", err)
	}
	if pending {
		return template.ErrTemplateImageCleanupPending
	}
	return nil
}

func templateImageCleanupPendingTx(
	ctx context.Context,
	tx pgx.Tx,
	scope, teamID, templateID string,
) (bool, error) {
	var pending bool
	if err := tx.QueryRow(ctx, `
		SELECT
			EXISTS (
				SELECT 1
				FROM scheduler_template_image_cleanups
				WHERE scope = $1 AND team_id = $2 AND template_id = $3
			)
			OR EXISTS (
				SELECT 1
				FROM scheduler_template_builds
				WHERE scope = $1 AND team_id = $2 AND template_id = $3
			)
	`, scope, teamID, templateID).Scan(&pending); err != nil {
		return false, fmt.Errorf("check template image lifecycle cleanup: %w", err)
	}
	return pending, nil
}

func enqueueTemplateImageCleanupTx(
	ctx context.Context,
	tx pgx.Tx,
	cleanupID, scope, teamID, templateID, targetClusterID, outputImage string,
	logicalSizeBytes int64,
) error {
	if strings.TrimSpace(cleanupID) == "" ||
		strings.TrimSpace(targetClusterID) == "" ||
		strings.TrimSpace(outputImage) == "" {
		return fmt.Errorf("template image cleanup requires cleanup_id, target_cluster_id, and output_image")
	}
	if logicalSizeBytes < 0 {
		return fmt.Errorf("template image cleanup logical size must not be negative")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO scheduler_template_image_cleanups (
			cleanup_id, template_id, scope, team_id, target_cluster_id,
			output_image, image_logical_size_bytes
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
	`, cleanupID, templateID, scope, teamID, targetClusterID, outputImage, logicalSizeBytes); err != nil {
		return fmt.Errorf("enqueue template image cleanup: %w", err)
	}
	return nil
}

// CreateTemplate creates a new template.
func (s *Store) CreateTemplate(ctx context.Context, tpl *template.Template) error {
	if tpl == nil {
		return fmt.Errorf("template is required")
	}
	if err := template.ValidateTemplateSpecSize(&tpl.Spec); err != nil {
		return err
	}
	specJSON, err := json.Marshal(tpl.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin template transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockTemplateLifecycleTx(ctx, tx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		return err
	}
	if err := ensureNoTemplateImageCleanupTx(ctx, tx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		return err
	}
	var quotaRef teamquota.OperationRef
	if tpl.Scope == naming.ScopeTeam {
		quotaRef, err = teamquota.ReserveControlPlaneObjectTargetTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(tpl.TeamID, teamquota.ControlPlaneOwnerKindTemplate, tpl.TemplateID),
			"create_template",
			1,
		)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO scheduler_templates (template_id, scope, team_id, user_id, spec)
		VALUES ($1, $2, $3, $4, $5)
	`, tpl.TemplateID, tpl.Scope, tpl.TeamID, tpl.UserID, specJSON)
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	if tpl.Scope == naming.ScopeTeam {
		if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, s.teamQuotaStore, tx, quotaRef); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit template: %w", err)
	}
	return nil
}

// CreateTemplateBuild atomically creates a template and its durable build.
func (s *Store) CreateTemplateBuild(ctx context.Context, tpl *template.Template, build *template.TemplateBuild) (*template.Template, bool, error) {
	if tpl == nil || build == nil {
		return nil, false, fmt.Errorf("template and build are required")
	}
	if err := template.ValidateTemplateSpecSize(&tpl.Spec); err != nil {
		return nil, false, err
	}
	specJSON, err := json.Marshal(tpl.Spec)
	if err != nil {
		return nil, false, fmt.Errorf("marshal spec: %w", err)
	}
	if strings.TrimSpace(build.BuildID) == "" {
		return nil, false, fmt.Errorf("build_id is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin template build transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockTemplateLifecycleTx(ctx, tx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		return nil, false, err
	}
	if err := ensureNoTemplateImageCleanupTx(ctx, tx, tpl.Scope, tpl.TeamID, tpl.TemplateID); err != nil {
		return nil, false, err
	}
	var templateQuotaRef, buildQuotaRef teamquota.OperationRef
	if tpl.Scope == naming.ScopeTeam {
		templateQuotaRef, err = teamquota.ReserveControlPlaneObjectTargetTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(tpl.TeamID, teamquota.ControlPlaneOwnerKindTemplate, tpl.TemplateID),
			"create_template_from_sandbox",
			1,
		)
		if err != nil {
			return nil, false, err
		}
	}

	now := time.Now().UTC()
	if !tpl.CreatedAt.IsZero() {
		now = tpl.CreatedAt.UTC()
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO scheduler_templates (
			template_id, scope, team_id, user_id, spec,
			creation_build_id, creation_idempotency_key, creation_request_hash,
			creation_state, creation_stage, creation_started_at,
			creation_image_cluster_id
		)
		VALUES ($1, $2, $3, $4, $5, $6::uuid, NULLIF($7, ''), $8,
			'creating', 'capturing', $9, $10)
	`, tpl.TemplateID, tpl.Scope, tpl.TeamID, tpl.UserID, specJSON,
		build.BuildID, build.IdempotencyKey, build.RequestHash, now, build.TargetClusterID)
	if err != nil {
		if isUniqueViolation(err) {
			_ = tx.Rollback(ctx)
			return s.resolveTemplateBuildConflict(ctx, tpl, build)
		}
		return nil, false, fmt.Errorf("create template for build: %w", err)
	}
	if tpl.Scope == naming.ScopeTeam {
		buildQuotaRef, err = teamquota.ReserveControlPlaneObjectTargetTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(
				tpl.TeamID,
				teamquota.ControlPlaneOwnerKindTemplateBuild,
				build.BuildID,
			),
			"create_template_build",
			1,
		)
		if err != nil {
			return nil, false, err
		}
	}

	nextAttemptAt := build.NextAttemptAt
	if nextAttemptAt.IsZero() {
		nextAttemptAt = now
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO scheduler_template_builds (
			build_id, template_id, scope, team_id, user_id,
			source_sandbox_id, target_cluster_id, request_hash,
			idempotency_key, status, stage, snapshot_id, next_attempt_at
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8,
			NULLIF($9, ''), 'queued', 'capturing', NULLIF($10, ''), $11)
	`, build.BuildID, tpl.TemplateID, tpl.Scope, tpl.TeamID, tpl.UserID,
		build.SourceSandboxID, build.TargetClusterID, build.RequestHash,
		build.IdempotencyKey, build.SnapshotID, nextAttemptAt)
	if err != nil {
		return nil, false, fmt.Errorf("create template build: %w", err)
	}
	if tpl.Scope == naming.ScopeTeam {
		if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, s.teamQuotaStore, tx, templateQuotaRef); err != nil {
			return nil, false, err
		}
		if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, s.teamQuotaStore, tx, buildQuotaRef); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit template build: %w", err)
	}

	created, err := s.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		return nil, false, err
	}
	return created, true, nil
}

func (s *Store) resolveTemplateBuildConflict(ctx context.Context, tpl *template.Template, build *template.TemplateBuild) (*template.Template, bool, error) {
	if key := strings.TrimSpace(build.IdempotencyKey); key != "" {
		existing, err := scanTemplate(s.pool.QueryRow(ctx, `
			SELECT `+templateSelectColumns+`
			FROM scheduler_templates
			WHERE scope = $1 AND team_id = $2 AND creation_idempotency_key = $3
		`, tpl.Scope, tpl.TeamID, key))
		if err == nil {
			if existing.CreationRequestHash == build.RequestHash {
				return existing, false, nil
			}
			return nil, false, template.ErrTemplateIdempotencyConflict
		}
		if err != pgx.ErrNoRows {
			return nil, false, fmt.Errorf("resolve idempotency conflict: %w", err)
		}
	}
	existing, err := s.GetTemplate(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return nil, false, template.ErrTemplateAlreadyExists
	}
	return nil, false, template.ErrTemplateAlreadyExists
}

// GetTemplateByIdempotencyKey resolves a prior from-sandbox request.
func (s *Store) GetTemplateByIdempotencyKey(ctx context.Context, scope, teamID, idempotencyKey string) (*template.Template, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, nil
	}
	tpl, err := scanTemplate(s.pool.QueryRow(ctx, `
		SELECT `+templateSelectColumns+`
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND creation_idempotency_key = $3
	`, scope, teamID, idempotencyKey))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get template by idempotency key: %w", err)
	}
	return tpl, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// GetTemplate gets a template by primary key (scope, team_id, template_id).
func (s *Store) GetTemplate(ctx context.Context, scope, teamID, templateID string) (*template.Template, error) {
	tpl, err := scanTemplate(s.pool.QueryRow(ctx, `
		SELECT `+templateSelectColumns+`
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get template: %w", err)
	}
	return tpl, nil
}

// GetTemplateForTeam gets a template visible to a team.
// It prefers the team's private template (scope=team) and falls back to public.
func (s *Store) GetTemplateForTeam(ctx context.Context, teamID, templateID string) (*template.Template, error) {
	if teamID != "" {
		privateTpl, err := s.GetTemplate(ctx, "team", teamID, templateID)
		if err != nil {
			return nil, err
		}
		if privateTpl != nil {
			return privateTpl, nil
		}
	}
	return s.GetTemplate(ctx, "public", "", templateID)
}

// ListTemplates lists all templates.
func (s *Store) ListTemplates(ctx context.Context) ([]*template.Template, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+templateSelectColumns+`
		FROM scheduler_templates
		ORDER BY scope, team_id, template_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	defer rows.Close()

	var templates []*template.Template
	for rows.Next() {
		tpl, err := scanTemplate(rows)
		if err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		templates = append(templates, tpl)
	}
	return templates, nil
}

// ListVisibleTemplates lists templates visible to a team (public + that team's private).
func (s *Store) ListVisibleTemplates(ctx context.Context, teamID string) ([]*template.Template, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+templateSelectColumns+`
		FROM scheduler_templates
		WHERE scope = 'public' OR (scope = 'team' AND team_id = $1)
		ORDER BY scope, template_id
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("list visible templates: %w", err)
	}
	defer rows.Close()

	var templates []*template.Template
	for rows.Next() {
		tpl, err := scanTemplate(rows)
		if err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		templates = append(templates, tpl)
	}
	return templates, nil
}

// UpdateTemplate updates a template.
func (s *Store) UpdateTemplate(ctx context.Context, tpl *template.Template) error {
	if tpl == nil {
		return fmt.Errorf("template is required")
	}
	if err := template.ValidateTemplateSpecSize(&tpl.Spec); err != nil {
		return err
	}
	specJSON, err := json.Marshal(tpl.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		UPDATE scheduler_templates
		SET spec = $5, user_id = $4
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, tpl.Scope, tpl.TeamID, tpl.TemplateID, tpl.UserID, specJSON)
	if err != nil {
		return fmt.Errorf("update template: %w", err)
	}
	return nil
}

// DeleteTemplate deletes a template.
func (s *Store) DeleteTemplate(ctx context.Context, scope, teamID, templateID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin template deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return err
	}
	var buildID, outputImage, imageClusterID *string
	var imageLogicalSizeBytes *int64
	err = tx.QueryRow(ctx, `
		SELECT creation_build_id::text, creation_output_image,
			creation_image_cluster_id, creation_image_logical_size_bytes
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		FOR UPDATE
	`, scope, teamID, templateID).Scan(&buildID, &outputImage, &imageClusterID, &imageLogicalSizeBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		pending, pendingErr := templateImageCleanupPendingTx(ctx, tx, scope, teamID, templateID)
		if pendingErr != nil {
			return pendingErr
		}
		if pending {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit pending template deletion: %w", err)
			}
			return nil
		}
	} else if err != nil {
		return fmt.Errorf("lock template for deletion: %w", err)
	}

	var quotaRef teamquota.OperationRef
	releaseQuota := scope == naming.ScopeTeam
	if outputImage != nil && strings.TrimSpace(*outputImage) != "" {
		if buildID == nil || strings.TrimSpace(*buildID) == "" {
			return fmt.Errorf("managed template image has no creation build id")
		}
		if imageClusterID == nil || strings.TrimSpace(*imageClusterID) == "" {
			return fmt.Errorf("managed template image has no publishing cluster id")
		}
		if err := enqueueTemplateImageCleanupTx(
			ctx,
			tx,
			*buildID,
			scope,
			teamID,
			templateID,
			*imageClusterID,
			*outputImage,
			int64Value(imageLogicalSizeBytes),
		); err != nil {
			return err
		}
		releaseQuota = false
	}
	if releaseQuota {
		quotaRef, err = teamquota.BeginControlPlaneObjectReleaseTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindTemplate, templateID),
			"delete_template",
			0,
		)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `
		DELETE FROM scheduler_templates WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	if releaseQuota {
		if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, s.teamQuotaStore, tx, quotaRef); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit template deletion: %w", err)
	}
	return nil
}

const templateBuildSelectColumns = `
	build_id::text, template_id, scope, team_id, user_id,
	source_sandbox_id, target_cluster_id, request_hash, idempotency_key,
	status, stage, snapshot_id, capture_metadata, output_image,
	image_manifest_digest, image_logical_size_bytes, image_quota_reserved_at,
	image_push_started_at,
	attempt_count, next_attempt_at, lease_owner, lease_expires_at,
	cancel_requested_at, last_error, created_at, updated_at
`

func scanTemplateBuild(row rowScanner) (*template.TemplateBuild, error) {
	var build template.TemplateBuild
	var idempotencyKey, snapshotID, outputImage, imageManifestDigest, leaseOwner, lastError *string
	var captureMetadata []byte
	var imageLogicalSizeBytes *int64
	var imageQuotaReservedAt, imagePushStartedAt, leaseExpiresAt, cancelRequestedAt *time.Time
	var stage string
	if err := row.Scan(
		&build.BuildID,
		&build.TemplateID,
		&build.Scope,
		&build.TeamID,
		&build.UserID,
		&build.SourceSandboxID,
		&build.TargetClusterID,
		&build.RequestHash,
		&idempotencyKey,
		&build.Status,
		&stage,
		&snapshotID,
		&captureMetadata,
		&outputImage,
		&imageManifestDigest,
		&imageLogicalSizeBytes,
		&imageQuotaReservedAt,
		&imagePushStartedAt,
		&build.AttemptCount,
		&build.NextAttemptAt,
		&leaseOwner,
		&leaseExpiresAt,
		&cancelRequestedAt,
		&lastError,
		&build.CreatedAt,
		&build.UpdatedAt,
	); err != nil {
		return nil, err
	}
	build.IdempotencyKey = stringValue(idempotencyKey)
	build.Stage = v1alpha1.TemplateCreationStage(stage)
	build.SnapshotID = stringValue(snapshotID)
	build.CaptureMetadata = append([]byte(nil), captureMetadata...)
	build.OutputImage = stringValue(outputImage)
	build.ImageManifestDigest = stringValue(imageManifestDigest)
	build.ImageLogicalSizeBytes = int64Value(imageLogicalSizeBytes)
	if imageQuotaReservedAt != nil {
		build.ImageQuotaReservedAt = imageQuotaReservedAt.UTC()
	}
	if imagePushStartedAt != nil {
		build.ImagePushStartedAt = imagePushStartedAt.UTC()
	}
	build.LeaseOwner = stringValue(leaseOwner)
	if leaseExpiresAt != nil {
		build.LeaseExpiresAt = leaseExpiresAt.UTC()
	}
	if cancelRequestedAt != nil {
		build.CancelRequestedAt = cancelRequestedAt.UTC()
	}
	build.LastError = stringValue(lastError)
	return &build, nil
}

// ClaimTemplateBuild leases one build to a manager in the local region.
// Capturing remains bound to the source cluster. Once capture is durable,
// publishing and reconciliation cleanup may be taken over by another regional
// manager. Publishing claims become the image-owning cluster; later image
// deletion remains routed there.
func (s *Store) ClaimTemplateBuild(ctx context.Context, targetClusterID, workerID string, leaseDuration time.Duration) (*template.TemplateBuild, error) {
	if strings.TrimSpace(targetClusterID) == "" || strings.TrimSpace(workerID) == "" {
		return nil, fmt.Errorf("target_cluster_id and worker_id are required")
	}
	if leaseDuration <= 0 {
		leaseDuration = 2 * time.Minute
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	leaseMillis := leaseDuration.Milliseconds()
	if leaseMillis < 1 {
		leaseMillis = 1
	}
	build, err := scanTemplateBuild(tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT b.build_id
			FROM scheduler_template_builds b
			LEFT JOIN scheduler_templates t
			  ON t.scope = b.scope
			 AND t.team_id = b.team_id
			 AND t.template_id = b.template_id
			 AND t.creation_build_id = b.build_id
			WHERE b.next_attempt_at <= NOW()
			  AND (b.lease_expires_at IS NULL OR b.lease_expires_at <= NOW())
			  AND (
				(
					b.cancel_requested_at IS NOT NULL
					AND (
						b.stage = 'capturing'
						OR b.target_cluster_id = $1
					)
				)
				OR (
					b.cancel_requested_at IS NULL
					AND t.creation_build_id IS NOT NULL
					AND (
						(
							b.stage = 'capturing'
							AND b.target_cluster_id = $1
							AND t.creation_state = 'creating'
							AND t.creation_stage = 'capturing'
						)
						OR (
							b.stage = 'publishing'
							AND t.creation_state = 'creating'
							AND t.creation_stage = 'publishing'
							AND (
								b.target_cluster_id = $1
								OR (
									NULLIF(b.snapshot_id, '') IS NOT NULL
									AND b.capture_metadata IS NOT NULL
									AND t.creation_captured_at IS NOT NULL
								)
							)
						)
						OR (
							b.stage = 'reconciling'
							AND (
								(t.creation_state = 'creating' AND t.creation_stage = 'reconciling')
								OR t.creation_state = 'ready'
							)
						)
					)
				)
			  )
			ORDER BY (b.cancel_requested_at IS NOT NULL) DESC, b.created_at
			FOR UPDATE OF b SKIP LOCKED
			LIMIT 1
		)
		UPDATE scheduler_template_builds b
		SET status = 'running',
			lease_owner = $2,
			lease_expires_at = NOW() + ($3 * INTERVAL '1 millisecond'),
			attempt_count = attempt_count + 1,
			target_cluster_id = CASE
				WHEN b.stage = 'publishing' AND b.cancel_requested_at IS NULL
					THEN $1
				ELSE b.target_cluster_id
			END
		FROM candidate
		WHERE b.build_id = candidate.build_id
		RETURNING `+templateBuildSelectColumnsWithAlias("b")+`
	`, targetClusterID, workerID, leaseMillis))
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim template build: %w", err)
	}

	var specJSON []byte
	err = tx.QueryRow(ctx, `
		SELECT spec
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		  AND creation_build_id = $4::uuid
	`, build.Scope, build.TeamID, build.TemplateID, build.BuildID).Scan(&specJSON)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("load claimed template spec: %w", err)
	}
	if len(specJSON) > 0 {
		if err := json.Unmarshal(specJSON, &build.DesiredSpec); err != nil {
			return nil, fmt.Errorf("unmarshal claimed template spec: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit template build claim: %w", err)
	}
	return build, nil
}

// FailCapturingTemplateBuildsForCluster terminally fails builds whose source
// rootfs was not captured before their owning cluster became unavailable.
// Captured builds are deliberately left claimable by another regional manager.
func (s *Store) FailCapturingTemplateBuildsForCluster(ctx context.Context, clusterID, reason, message string) (int64, error) {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return 0, fmt.Errorf("cluster_id is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "source_cluster_unavailable"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "source cluster became unavailable before rootfs capture completed"
	}

	rows, err := s.pool.Query(ctx, `
		WITH affected AS MATERIALIZED (
			SELECT b.build_id
			FROM scheduler_template_builds b
			JOIN scheduler_templates t
			  ON t.scope = b.scope
			 AND t.team_id = b.team_id
			 AND t.template_id = b.template_id
			 AND t.creation_build_id = b.build_id
			WHERE b.target_cluster_id = $1
			  AND b.stage = 'capturing'
			  AND b.cancel_requested_at IS NULL
			  AND t.creation_state = 'creating'
			  AND t.creation_stage = 'capturing'
			FOR UPDATE OF b, t
		),
		cancelled AS (
			UPDATE scheduler_template_builds b
			SET status = 'cancelled',
				cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
				next_attempt_at = NOW(),
				last_error = $3
			FROM affected a
			WHERE b.build_id = a.build_id
			RETURNING b.build_id
		)
		UPDATE scheduler_templates t
		SET creation_state = 'failed',
			creation_completed_at = NOW(),
			creation_reason = $2,
			creation_message = $3
		FROM cancelled c
		WHERE t.creation_build_id = c.build_id
		  AND t.creation_state = 'creating'
		  AND t.creation_stage = 'capturing'
		RETURNING t.creation_build_id::text
	`, clusterID, reason, message)
	if err != nil {
		return 0, fmt.Errorf("fail capturing template builds for cluster: %w", err)
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		var buildID string
		if err := rows.Scan(&buildID); err != nil {
			return 0, fmt.Errorf("scan failed capturing template build: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate failed capturing template builds: %w", err)
	}
	return count, nil
}

func templateBuildSelectColumnsWithAlias(alias string) string {
	columns := strings.Split(templateBuildSelectColumns, ",")
	for i, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		columns[i] = alias + "." + column
	}
	return strings.Join(columns, ", ")
}

// RenewTemplateBuildLease extends a worker's current lease.
func (s *Store) RenewTemplateBuildLease(ctx context.Context, buildID, workerID string, leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		leaseDuration = 2 * time.Minute
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET lease_expires_at = NOW() + ($3 * INTERVAL '1 millisecond')
		WHERE build_id = $1::uuid AND lease_owner = $2
		  AND status = 'running' AND cancel_requested_at IS NULL
	`, buildID, workerID, maxInt64(leaseDuration.Milliseconds(), 1))
	if err != nil {
		return fmt.Errorf("renew template build lease: %w", err)
	}
	return requireBuildRow(result.RowsAffected())
}

// MarkTemplateBuildCaptured records the immutable rootfs snapshot and advances
// both the job and public template status to publishing.
func (s *Store) MarkTemplateBuildCaptured(ctx context.Context, buildID, workerID, snapshotID string, captureMetadata json.RawMessage, capturedAt time.Time) error {
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin mark template build captured: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := tx.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET stage = 'publishing',
			snapshot_id = $3,
			capture_metadata = $4,
			last_error = NULL
		WHERE build_id = $1::uuid AND lease_owner = $2
		  AND status = 'running' AND cancel_requested_at IS NULL
		  AND (
			stage = 'capturing'
			OR (stage = 'publishing' AND snapshot_id = $3)
		  )
	`, buildID, workerID, snapshotID, nullableJSON(captureMetadata))
	if err != nil {
		return fmt.Errorf("mark build captured: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	result, err = tx.Exec(ctx, `
		UPDATE scheduler_templates
		SET creation_stage = 'publishing',
			creation_captured_at = COALESCE(creation_captured_at, $2),
			creation_message = NULL
		WHERE creation_build_id = $1::uuid
		  AND creation_state = 'creating'
		  AND creation_stage IN ('capturing', 'publishing')
	`, buildID, capturedAt)
	if err != nil {
		return fmt.Errorf("update captured template status: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit captured template build: %w", err)
	}
	return nil
}

// ReserveTemplateImageBuild durably records the deterministic image plan and
// reserves its exact logical bytes before the worker may write to a registry.
func (s *Store) ReserveTemplateImageBuild(
	ctx context.Context,
	buildID, workerID, manifestDigest string,
	logicalSizeBytes int64,
) error {
	parsedDigest, err := digest.Parse(strings.TrimSpace(manifestDigest))
	if err != nil {
		return fmt.Errorf("parse planned template image manifest digest: %w", err)
	}
	if logicalSizeBytes < 0 {
		return fmt.Errorf("planned template image logical size must not be negative")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reserve template image build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var scope, teamID, templateID string
	if err := tx.QueryRow(ctx, `
		SELECT scope, team_id, template_id
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid
	`, buildID).Scan(&scope, &teamID, &templateID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return template.ErrTemplateBuildLeaseLost
		}
		return fmt.Errorf("load template build for image reservation: %w", err)
	}
	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET image_manifest_digest = COALESCE(image_manifest_digest, $3),
			image_logical_size_bytes = COALESCE(image_logical_size_bytes, $4),
			image_quota_reserved_at = COALESCE(image_quota_reserved_at, NOW())
		WHERE build_id = $1::uuid
		  AND lease_owner = $2
		  AND status = 'running'
		  AND stage = 'publishing'
		  AND cancel_requested_at IS NULL
		  AND (image_manifest_digest IS NULL OR image_manifest_digest = $3)
		  AND (image_logical_size_bytes IS NULL OR image_logical_size_bytes = $4)
	`, buildID, workerID, parsedDigest.String(), logicalSizeBytes)
	if err != nil {
		return fmt.Errorf("record planned template image: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if scope == naming.ScopeTeam {
		if s.teamQuotaStore == nil {
			return &teamquota.UnavailableError{
				Operation: "reserve template image storage quota",
				Err:       fmt.Errorf("capacity store is not configured"),
			}
		}
		owner := templateImageQuotaOwner(teamID, templateID)
		operation := templateImagePublishOperation(buildID)
		if _, err := s.teamQuotaStore.ReserveTargetTx(ctx, tx, teamquota.ReserveRequest{
			Owner:     owner,
			Operation: operation,
			Target: teamquota.Values{
				teamquota.KeyTemplateImageStorageBytes: logicalSizeBytes,
			},
		}); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit template image reservation: %w", err)
	}
	return nil
}

// MarkTemplateImagePushStarted persists the uncertainty boundary before the
// publisher makes its first registry write.
func (s *Store) MarkTemplateImagePushStarted(ctx context.Context, buildID, workerID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin template image push marker: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var scope, teamID, templateID string
	if err := tx.QueryRow(ctx, `
		SELECT scope, team_id, template_id
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid
	`, buildID).Scan(&scope, &teamID, &templateID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return template.ErrTemplateBuildLeaseLost
		}
		return fmt.Errorf("load template build for push marker: %w", err)
	}
	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET image_push_started_at = COALESCE(image_push_started_at, NOW())
		WHERE build_id = $1::uuid
		  AND lease_owner = $2
		  AND status = 'running'
		  AND stage = 'publishing'
		  AND cancel_requested_at IS NULL
		  AND image_manifest_digest IS NOT NULL
		  AND image_logical_size_bytes IS NOT NULL
		  AND image_quota_reserved_at IS NOT NULL
	`, buildID, workerID)
	if err != nil {
		return fmt.Errorf("mark template image push started: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit template image push marker: %w", err)
	}
	return nil
}

// PublishTemplateBuild atomically installs the digest-pinned image spec and
// advances the public creation stage to reconciliation while committing the
// previously reserved image bytes.
func (s *Store) PublishTemplateBuild(
	ctx context.Context,
	buildID, workerID string,
	finalSpec v1alpha1.SandboxTemplateSpec,
	outputImage, manifestDigest string,
	logicalSizeBytes int64,
) error {
	parsedDigest, err := digest.Parse(strings.TrimSpace(manifestDigest))
	if err != nil {
		return fmt.Errorf("parse published template image manifest digest: %w", err)
	}
	if logicalSizeBytes < 0 {
		return fmt.Errorf("published template image logical size must not be negative")
	}
	if err := template.ValidateTemplateSpecSize(&finalSpec); err != nil {
		return err
	}
	outputParts := strings.SplitN(strings.TrimSpace(outputImage), "@", 2)
	if len(outputParts) != 2 || strings.TrimSpace(outputParts[1]) != parsedDigest.String() {
		return fmt.Errorf("published template image reference does not match planned manifest digest")
	}
	specJSON, err := json.Marshal(finalSpec)
	if err != nil {
		return fmt.Errorf("marshal published template spec: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin publish template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var scope, teamID, templateID string
	if err := tx.QueryRow(ctx, `
		SELECT scope, team_id, template_id
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid
	`, buildID).Scan(&scope, &teamID, &templateID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return template.ErrTemplateBuildLeaseLost
		}
		return fmt.Errorf("load template build for publication: %w", err)
	}
	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET stage = 'reconciling', output_image = $3, last_error = NULL
		WHERE build_id = $1::uuid AND lease_owner = $2
		  AND status = 'running' AND cancel_requested_at IS NULL
		  AND (
			stage = 'publishing'
			OR (stage = 'reconciling' AND output_image = $3)
		  )
		  AND image_manifest_digest = $4
		  AND image_logical_size_bytes = $5
		  AND image_quota_reserved_at IS NOT NULL
		  AND image_push_started_at IS NOT NULL
	`, buildID, workerID, outputImage, parsedDigest.String(), logicalSizeBytes)
	if err != nil {
		return fmt.Errorf("mark template build published: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	result, err = tx.Exec(ctx, `
		UPDATE scheduler_templates t
		SET spec = $3,
			creation_stage = 'reconciling',
			creation_output_image = $4,
			creation_image_cluster_id = b.target_cluster_id,
			creation_image_logical_size_bytes = $5,
			creation_message = NULL
		FROM scheduler_template_builds b
		WHERE b.build_id = $1::uuid
		  AND b.lease_owner = $2
		  AND t.creation_build_id = b.build_id
		  AND t.creation_state = 'creating'
		  AND t.creation_stage IN ('publishing', 'reconciling')
	`, buildID, workerID, specJSON, outputImage, logicalSizeBytes)
	if err != nil {
		return fmt.Errorf("install published template spec: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if scope == naming.ScopeTeam {
		if s.teamQuotaStore == nil {
			return &teamquota.UnavailableError{
				Operation: "commit template image storage quota",
				Err:       fmt.Errorf("capacity store is not configured"),
			}
		}
		owner := templateImageQuotaOwner(teamID, templateID)
		if err := s.teamQuotaStore.CommitTx(
			ctx,
			tx,
			teamquota.Ref(owner, templateImagePublishOperation(buildID)),
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit published template build: %w", err)
	}
	return nil
}

// FailTemplateBuild records a terminal, user-visible build failure.
func (s *Store) FailTemplateBuild(ctx context.Context, buildID, workerID, reason, message string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fail template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := tx.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET status = 'cancelled',
			cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
			next_attempt_at = NOW(),
			last_error = $3
		WHERE build_id = $1::uuid AND lease_owner = $2
		  AND status = 'running' AND cancel_requested_at IS NULL
	`, buildID, workerID, message)
	if err != nil {
		return fmt.Errorf("record template build failure: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	result, err = tx.Exec(ctx, `
		UPDATE scheduler_templates
		SET creation_state = 'failed',
			creation_completed_at = NOW(),
			creation_reason = $2,
			creation_message = $3
		WHERE creation_build_id = $1::uuid AND creation_state = 'creating'
	`, buildID, reason, message)
	if err != nil {
		return fmt.Errorf("mark template creation failed: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit template build failure: %w", err)
	}
	return nil
}

// ReleaseTemplateBuild returns a transiently failed build to the durable queue.
func (s *Store) ReleaseTemplateBuild(ctx context.Context, buildID, workerID string, retryAt time.Time, lastError string) error {
	if retryAt.IsZero() {
		retryAt = time.Now().UTC()
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET status = 'queued',
			next_attempt_at = $3,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error = $4
		WHERE build_id = $1::uuid AND lease_owner = $2
		  AND status = 'running' AND cancel_requested_at IS NULL
	`, buildID, workerID, retryAt, lastError)
	if err != nil {
		return fmt.Errorf("release template build: %w", err)
	}
	return requireBuildRow(result.RowsAffected())
}

// TemplateBuildCancelled checks the durable cancellation tombstone.
func (s *Store) TemplateBuildCancelled(ctx context.Context, buildID string) (bool, error) {
	var cancelled bool
	err := s.pool.QueryRow(ctx, `
		SELECT cancel_requested_at IS NOT NULL
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid
	`, buildID).Scan(&cancelled)
	if err == pgx.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("check template build cancellation: %w", err)
	}
	return cancelled, nil
}

// FinishTemplateBuild removes internal queue state after snapshot cleanup. A
// prepared image reservation can be aborted only when no registry write began
// or the caller has confirmed deletion of the unpublished build tag.
func (s *Store) FinishTemplateBuild(
	ctx context.Context,
	buildID, workerID string,
	unpublishedImageDeleteConfirmed bool,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin finish template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var scope, teamID, templateID string
	if err := tx.QueryRow(ctx, `
		SELECT scope, team_id, template_id
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid
	`, buildID).Scan(&scope, &teamID, &templateID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return template.ErrTemplateBuildLeaseLost
		}
		return fmt.Errorf("load template build for finish: %w", err)
	}
	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return err
	}
	var stage string
	var imageQuotaReservedAt, imagePushStartedAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT stage, image_quota_reserved_at, image_push_started_at
		FROM scheduler_template_builds
		WHERE build_id = $1::uuid AND lease_owner = $2
		FOR UPDATE
	`, buildID, workerID).Scan(
		&stage,
		&imageQuotaReservedAt,
		&imagePushStartedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return template.ErrTemplateBuildLeaseLost
		}
		return fmt.Errorf("lock template build for finish: %w", err)
	}
	if scope == naming.ScopeTeam &&
		imageQuotaReservedAt != nil &&
		stage != string(v1alpha1.TemplateCreationStageReconciling) {
		if imagePushStartedAt != nil && !unpublishedImageDeleteConfirmed {
			return fmt.Errorf(
				"template image registry deletion must be confirmed before releasing reserved bytes",
			)
		}
		if s.teamQuotaStore == nil {
			return &teamquota.UnavailableError{
				Operation: "abort unpublished template image quota",
				Err:       fmt.Errorf("capacity store is not configured"),
			}
		}
		owner := templateImageQuotaOwner(teamID, templateID)
		if err := s.teamQuotaStore.AbortTx(
			ctx,
			tx,
			teamquota.Ref(owner, templateImagePublishOperation(buildID)),
			"template image was not published",
		); err != nil {
			return err
		}
	}
	var quotaRef teamquota.OperationRef
	if scope == naming.ScopeTeam {
		quotaRef, err = teamquota.BeginControlPlaneObjectReleaseTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindTemplateBuild, buildID),
			"finish_template_build",
			0,
		)
		if err != nil {
			return err
		}
	}
	result, err := tx.Exec(ctx, `
		DELETE FROM scheduler_template_builds
		WHERE build_id = $1::uuid AND lease_owner = $2
	`, buildID, workerID)
	if err != nil {
		return fmt.Errorf("finish template build: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if scope == naming.ScopeTeam {
		if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, s.teamQuotaStore, tx, quotaRef); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit finish template build: %w", err)
	}
	return nil
}

const templateImageCleanupReturningColumns = `
	c.cleanup_id::text, c.scope, c.team_id, c.template_id, c.target_cluster_id,
	c.output_image, c.image_logical_size_bytes, c.status,
	c.attempt_count, c.next_attempt_at, c.lease_owner, c.lease_expires_at,
	c.last_error, c.created_at, c.updated_at
`

func scanTemplateImageCleanup(row rowScanner) (*template.TemplateImageCleanup, error) {
	var cleanup template.TemplateImageCleanup
	var leaseOwner, lastError *string
	var leaseExpiresAt *time.Time
	if err := row.Scan(
		&cleanup.CleanupID,
		&cleanup.Scope,
		&cleanup.TeamID,
		&cleanup.TemplateID,
		&cleanup.TargetClusterID,
		&cleanup.OutputImage,
		&cleanup.ImageLogicalSizeBytes,
		&cleanup.Status,
		&cleanup.AttemptCount,
		&cleanup.NextAttemptAt,
		&leaseOwner,
		&leaseExpiresAt,
		&lastError,
		&cleanup.CreatedAt,
		&cleanup.UpdatedAt,
	); err != nil {
		return nil, err
	}
	cleanup.LeaseOwner = stringValue(leaseOwner)
	cleanup.LastError = stringValue(lastError)
	if leaseExpiresAt != nil {
		cleanup.LeaseExpiresAt = leaseExpiresAt.UTC()
	}
	return &cleanup, nil
}

// ClaimTemplateImageCleanup leases one registry artifact cleanup. Expired
// leases are recoverable by another manager after a crash.
func (s *Store) ClaimTemplateImageCleanup(
	ctx context.Context,
	targetClusterID string,
	workerID string,
	leaseDuration time.Duration,
) (*template.TemplateImageCleanup, error) {
	if strings.TrimSpace(targetClusterID) == "" || strings.TrimSpace(workerID) == "" {
		return nil, fmt.Errorf("target_cluster_id and worker_id are required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin template image cleanup claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cleanup, err := scanTemplateImageCleanup(tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT cleanup_id
			FROM scheduler_template_image_cleanups
			WHERE target_cluster_id = $1
			  AND next_attempt_at <= NOW()
			  AND (
				status = 'queued'
				OR (status = 'running' AND lease_expires_at <= NOW())
			  )
			ORDER BY next_attempt_at, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE scheduler_template_image_cleanups c
		SET status = 'running',
			attempt_count = c.attempt_count + 1,
			lease_owner = $2,
			lease_expires_at = NOW() + $3::interval
		FROM candidate
		WHERE c.cleanup_id = candidate.cleanup_id
		RETURNING `+templateImageCleanupReturningColumns,
		targetClusterID,
		workerID,
		leaseDuration.String(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty template image cleanup claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim template image cleanup: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit template image cleanup claim: %w", err)
	}
	return cleanup, nil
}

// ReleaseTemplateImageCleanup schedules a failed registry deletion for retry.
func (s *Store) ReleaseTemplateImageCleanup(
	ctx context.Context,
	cleanupID, workerID string,
	retryAt time.Time,
	lastError string,
) error {
	if retryAt.IsZero() {
		retryAt = time.Now().UTC()
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_image_cleanups
		SET status = 'queued',
			next_attempt_at = $3,
			lease_owner = NULL,
			lease_expires_at = NULL,
			last_error = $4
		WHERE cleanup_id = $1::uuid
		  AND status = 'running'
		  AND lease_owner = $2
	`, cleanupID, workerID, retryAt, lastError)
	if err != nil {
		return fmt.Errorf("release template image cleanup: %w", err)
	}
	return requireBuildRow(result.RowsAffected())
}

// FinishTemplateImageCleanup acknowledges physical registry deletion and only
// then releases both the managed image bytes and the template object.
func (s *Store) FinishTemplateImageCleanup(ctx context.Context, cleanupID, workerID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin finish template image cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var scope, teamID, templateID string
	if err := tx.QueryRow(ctx, `
		SELECT scope, team_id, template_id
		FROM scheduler_template_image_cleanups
		WHERE cleanup_id = $1::uuid
		  AND status = 'running'
		  AND lease_owner = $2
	`, cleanupID, workerID).Scan(&scope, &teamID, &templateID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return template.ErrTemplateBuildLeaseLost
		}
		return fmt.Errorf("load template image cleanup for finish: %w", err)
	}
	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return err
	}
	var imageQuotaRef, templateQuotaRef teamquota.OperationRef
	if scope == naming.ScopeTeam {
		if s.teamQuotaStore == nil {
			return &teamquota.UnavailableError{
				Operation: "release template image storage quota",
				Err:       fmt.Errorf("capacity store is not configured"),
			}
		}
		imageOwner := templateImageQuotaOwner(teamID, templateID)
		imageOperation := templateImageDeleteOperation(cleanupID)
		reservation, err := s.teamQuotaStore.BeginReleaseTx(ctx, tx, teamquota.ReleaseRequest{
			Owner:     imageOwner,
			Operation: imageOperation,
			Target: teamquota.Values{
				teamquota.KeyTemplateImageStorageBytes: 0,
			},
		})
		if err != nil {
			return err
		}
		imageQuotaRef = teamquota.Ref(reservation.Owner, reservation.Operation)
		templateQuotaRef, err = teamquota.BeginControlPlaneObjectReleaseTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindTemplate, templateID),
			"finish_template_image_cleanup",
			0,
		)
		if err != nil {
			return err
		}
	}
	result, err := tx.Exec(ctx, `
		DELETE FROM scheduler_template_image_cleanups
		WHERE cleanup_id = $1::uuid
		  AND status = 'running'
		  AND lease_owner = $2
	`, cleanupID, workerID)
	if err != nil {
		return fmt.Errorf("finish template image cleanup: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	if scope == naming.ScopeTeam {
		if err := s.teamQuotaStore.ConfirmReleaseTx(
			ctx,
			tx,
			imageQuotaRef,
			teamquota.RuntimeRef{},
		); err != nil {
			return err
		}
		if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, s.teamQuotaStore, tx, templateQuotaRef); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit template image cleanup finish: %w", err)
	}
	return nil
}

// CancelTemplateBuildAndDeleteTemplate atomically removes the visible template
// and leaves a cancellation tombstone until a worker cleans captured artifacts.
func (s *Store) CancelTemplateBuildAndDeleteTemplate(ctx context.Context, scope, teamID, templateID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin cancel template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockTemplateLifecycleTx(ctx, tx, scope, teamID, templateID); err != nil {
		return false, err
	}
	var templateQuotaRef teamquota.OperationRef
	var buildID, outputImage, imageClusterID *string
	var imageLogicalSizeBytes *int64
	err = tx.QueryRow(ctx, `
		SELECT creation_build_id::text, creation_output_image,
			creation_image_cluster_id, creation_image_logical_size_bytes
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		FOR UPDATE
	`, scope, teamID, templateID).Scan(&buildID, &outputImage, &imageClusterID, &imageLogicalSizeBytes)
	if err == pgx.ErrNoRows {
		pending, pendingErr := templateImageCleanupPendingTx(ctx, tx, scope, teamID, templateID)
		if pendingErr != nil {
			return false, pendingErr
		}
		if scope == naming.ScopeTeam && !pending {
			templateQuotaRef, err = teamquota.BeginControlPlaneObjectReleaseTx(
				ctx,
				s.teamQuotaStore,
				tx,
				teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindTemplate, templateID),
				"cancel_missing_template_build",
				0,
			)
			if err != nil {
				return false, err
			}
			if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, s.teamQuotaStore, tx, templateQuotaRef); err != nil {
				return false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit missing template build cancellation: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock template for cancellation: %w", err)
	}
	releaseQuota := scope == naming.ScopeTeam
	if outputImage != nil && strings.TrimSpace(*outputImage) != "" {
		if buildID == nil || strings.TrimSpace(*buildID) == "" {
			return false, fmt.Errorf("managed template image has no creation build id")
		}
		if imageClusterID == nil || strings.TrimSpace(*imageClusterID) == "" {
			return false, fmt.Errorf("managed template image has no publishing cluster id")
		}
		if err := enqueueTemplateImageCleanupTx(
			ctx,
			tx,
			*buildID,
			scope,
			teamID,
			templateID,
			*imageClusterID,
			*outputImage,
			int64Value(imageLogicalSizeBytes),
		); err != nil {
			return false, err
		}
		releaseQuota = false
	}
	if releaseQuota {
		templateQuotaRef, err = teamquota.BeginControlPlaneObjectReleaseTx(
			ctx,
			s.teamQuotaStore,
			tx,
			teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindTemplate, templateID),
			"cancel_template_build_and_delete_template",
			0,
		)
		if err != nil {
			return false, err
		}
	}
	if buildID != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE scheduler_template_builds
			SET status = 'cancelled',
				cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
				next_attempt_at = NOW()
			WHERE build_id = $1::uuid
		`, *buildID); err != nil {
			return false, fmt.Errorf("request template build cancellation: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID); err != nil {
		return false, fmt.Errorf("delete template during build cancellation: %w", err)
	}
	if releaseQuota {
		if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, s.teamQuotaStore, tx, templateQuotaRef); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit template build cancellation: %w", err)
	}
	return buildID != nil, nil
}

// MarkTemplateCreationReady finalizes creation after a reconciler verifies
// that at least one data-plane cluster can claim the template.
func (s *Store) MarkTemplateCreationReady(ctx context.Context, scope, teamID, templateID, buildID string, completedAt time.Time) (bool, error) {
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE scheduler_templates
		SET creation_state = 'ready',
			creation_completed_at = COALESCE(creation_completed_at, $5),
			creation_reason = NULL,
			creation_message = NULL
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		  AND creation_build_id = $4::uuid
		  AND creation_state = 'creating'
		  AND creation_stage = 'reconciling'
		  AND NOT EXISTS (
			SELECT 1
			FROM scheduler_template_builds b
			WHERE b.build_id = $4::uuid
		  )
	`, scope, teamID, templateID, buildID, completedAt)
	if err != nil {
		return false, fmt.Errorf("mark template creation ready: %w", err)
	}
	return result.RowsAffected() > 0, nil
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return string(value)
}

func requireBuildRow(rows int64) error {
	if rows == 0 {
		return template.ErrTemplateBuildLeaseLost
	}
	return nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// UpsertAllocation creates or updates a template allocation.
func (s *Store) UpsertAllocation(ctx context.Context, alloc *template.TemplateAllocation) error {
	if alloc == nil {
		return fmt.Errorf("template allocation is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin allocation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if alloc.Scope == naming.ScopeTeam {
		if err := teamquota.AdmitTeamMutationTx(ctx, tx, alloc.TeamID); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO scheduler_template_allocations (template_id, scope, team_id, cluster_id, min_idle, max_idle, sync_status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (scope, team_id, template_id, cluster_id)
		DO UPDATE SET min_idle = $5, max_idle = $6, sync_status = $7
	`, alloc.TemplateID, alloc.Scope, alloc.TeamID, alloc.ClusterID, alloc.MinIdle, alloc.MaxIdle, alloc.SyncStatus)
	if err != nil {
		return fmt.Errorf("upsert allocation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit allocation: %w", err)
	}
	return nil
}

// ListAllocationsByTemplate lists all allocations for a template.
func (s *Store) ListAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) ([]*template.TemplateAllocation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_id, scope, team_id, cluster_id, min_idle, max_idle, last_synced_at, sync_status, sync_error, created_at, updated_at
		FROM scheduler_template_allocations
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		ORDER BY cluster_id
	`, scope, teamID, templateID)
	if err != nil {
		return nil, fmt.Errorf("list allocations by template: %w", err)
	}
	defer rows.Close()

	var allocations []*template.TemplateAllocation
	for rows.Next() {
		var alloc template.TemplateAllocation
		if err := rows.Scan(
			&alloc.TemplateID,
			&alloc.Scope,
			&alloc.TeamID,
			&alloc.ClusterID,
			&alloc.MinIdle,
			&alloc.MaxIdle,
			&alloc.LastSyncedAt,
			&alloc.SyncStatus,
			&alloc.SyncError,
			&alloc.CreatedAt,
			&alloc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan allocation: %w", err)
		}
		allocations = append(allocations, &alloc)
	}
	return allocations, nil
}

// UpdateAllocationSyncStatus updates the sync status of an allocation.
func (s *Store) UpdateAllocationSyncStatus(ctx context.Context, scope, teamID, templateID, clusterID, status string, syncError *string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_allocations
		SET sync_status = $5::text, sync_error = $6, last_synced_at = CASE WHEN $5::text = 'synced' THEN NOW() ELSE last_synced_at END
		WHERE scope = $1 AND team_id = $2 AND template_id = $3 AND cluster_id = $4
	`, scope, teamID, templateID, clusterID, status, syncError)
	if err != nil {
		return fmt.Errorf("update allocation sync status: %w", err)
	}
	return nil
}

// DeleteAllocationsByTemplate deletes all allocations for a template.
func (s *Store) DeleteAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM scheduler_template_allocations WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID)
	if err != nil {
		return fmt.Errorf("delete allocations by template: %w", err)
	}
	return nil
}
