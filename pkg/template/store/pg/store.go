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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Store implements template and allocation storage in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a new Store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const templateSelectColumns = `
	template_id, scope, team_id, user_id, spec, created_at, updated_at,
	creation_build_id::text, creation_idempotency_key, creation_request_hash,
	creation_state, creation_stage, creation_started_at, creation_captured_at,
	creation_completed_at, creation_output_image, creation_reason, creation_message
`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTemplate(row rowScanner) (*template.Template, error) {
	var tpl template.Template
	var specJSON []byte
	var buildID, idempotencyKey, requestHash *string
	var creationState string
	var creationStage, outputImage, reason, message *string
	var startedAt, capturedAt, completedAt *time.Time
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
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(specJSON, &tpl.Spec); err != nil {
		return nil, fmt.Errorf("unmarshal spec: %w", err)
	}

	tpl.CreationBuildID = stringValue(buildID)
	tpl.CreationIdempotencyKey = stringValue(idempotencyKey)
	tpl.CreationRequestHash = stringValue(requestHash)
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

func metaTime(value *time.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	out := metav1.NewTime(value.UTC())
	return &out
}

// CreateTemplate creates a new template.
func (s *Store) CreateTemplate(ctx context.Context, tpl *template.Template) error {
	specJSON, err := json.Marshal(tpl.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO scheduler_templates (template_id, scope, team_id, user_id, spec)
		VALUES ($1, $2, $3, $4, $5)
	`, tpl.TemplateID, tpl.Scope, tpl.TeamID, tpl.UserID, specJSON)
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}

// CreateTemplateBuild atomically creates a template and its durable build.
func (s *Store) CreateTemplateBuild(ctx context.Context, tpl *template.Template, build *template.TemplateBuild) (*template.Template, bool, error) {
	if tpl == nil || build == nil {
		return nil, false, fmt.Errorf("template and build are required")
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

	now := time.Now().UTC()
	if !tpl.CreatedAt.IsZero() {
		now = tpl.CreatedAt.UTC()
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO scheduler_templates (
			template_id, scope, team_id, user_id, spec,
			creation_build_id, creation_idempotency_key, creation_request_hash,
			creation_state, creation_stage, creation_started_at
		)
		VALUES ($1, $2, $3, $4, $5, $6::uuid, NULLIF($7, ''), $8,
			'creating', 'capturing', $9)
	`, tpl.TemplateID, tpl.Scope, tpl.TeamID, tpl.UserID, specJSON,
		build.BuildID, build.IdempotencyKey, build.RequestHash, now)
	if err != nil {
		if isUniqueViolation(err) {
			_ = tx.Rollback(ctx)
			return s.resolveTemplateBuildConflict(ctx, tpl, build)
		}
		return nil, false, fmt.Errorf("create template for build: %w", err)
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
	_, err := s.pool.Exec(ctx, `
		DELETE FROM scheduler_templates WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	return nil
}

const templateBuildSelectColumns = `
	build_id::text, template_id, scope, team_id, user_id,
	source_sandbox_id, target_cluster_id, request_hash, idempotency_key,
	status, stage, snapshot_id, capture_metadata, output_image,
	attempt_count, next_attempt_at, lease_owner, lease_expires_at,
	cancel_requested_at, last_error, created_at, updated_at
`

func scanTemplateBuild(row rowScanner) (*template.TemplateBuild, error) {
	var build template.TemplateBuild
	var idempotencyKey, snapshotID, outputImage, leaseOwner, lastError *string
	var captureMetadata []byte
	var leaseExpiresAt, cancelRequestedAt *time.Time
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
// publishing, reconciliation cleanup, and cancellation cleanup may be taken
// over by any manager that shares the region's PostgreSQL and object storage.
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
				b.cancel_requested_at IS NOT NULL
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
			attempt_count = attempt_count + 1
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

// PublishTemplateBuild atomically installs the digest-pinned image spec and
// advances the public creation stage to reconciliation.
func (s *Store) PublishTemplateBuild(ctx context.Context, buildID, workerID string, finalSpec v1alpha1.SandboxTemplateSpec, outputImage string) error {
	specJSON, err := json.Marshal(finalSpec)
	if err != nil {
		return fmt.Errorf("marshal published template spec: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin publish template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := tx.Exec(ctx, `
		UPDATE scheduler_template_builds
		SET stage = 'reconciling', output_image = $3, last_error = NULL
		WHERE build_id = $1::uuid AND lease_owner = $2
		  AND status = 'running' AND cancel_requested_at IS NULL
		  AND (
			stage = 'publishing'
			OR (stage = 'reconciling' AND output_image = $3)
		  )
	`, buildID, workerID, outputImage)
	if err != nil {
		return fmt.Errorf("mark template build published: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
	}
	result, err = tx.Exec(ctx, `
		UPDATE scheduler_templates
		SET spec = $2,
			creation_stage = 'reconciling',
			creation_output_image = $3,
			creation_message = NULL
		WHERE creation_build_id = $1::uuid
		  AND creation_state = 'creating'
		  AND creation_stage IN ('publishing', 'reconciling')
	`, buildID, specJSON, outputImage)
	if err != nil {
		return fmt.Errorf("install published template spec: %w", err)
	}
	if err := requireBuildRow(result.RowsAffected()); err != nil {
		return err
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

// FinishTemplateBuild removes internal queue state after snapshot cleanup.
func (s *Store) FinishTemplateBuild(ctx context.Context, buildID, workerID string) error {
	result, err := s.pool.Exec(ctx, `
		DELETE FROM scheduler_template_builds
		WHERE build_id = $1::uuid AND lease_owner = $2
	`, buildID, workerID)
	if err != nil {
		return fmt.Errorf("finish template build: %w", err)
	}
	return requireBuildRow(result.RowsAffected())
}

// CancelTemplateBuildAndDeleteTemplate atomically removes the visible template
// and leaves a cancellation tombstone until a worker cleans captured artifacts.
func (s *Store) CancelTemplateBuildAndDeleteTemplate(ctx context.Context, scope, teamID, templateID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin cancel template build: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var buildID *string
	err = tx.QueryRow(ctx, `
		SELECT creation_build_id::text
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		FOR UPDATE
	`, scope, teamID, templateID).Scan(&buildID)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock template for cancellation: %w", err)
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
	_, err := s.pool.Exec(ctx, `
		INSERT INTO scheduler_template_allocations (template_id, scope, team_id, cluster_id, min_idle, max_idle, sync_status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (scope, team_id, template_id, cluster_id)
		DO UPDATE SET min_idle = $5, max_idle = $6, sync_status = $7
	`, alloc.TemplateID, alloc.Scope, alloc.TeamID, alloc.ClusterID, alloc.MinIdle, alloc.MaxIdle, alloc.SyncStatus)
	if err != nil {
		return fmt.Errorf("upsert allocation: %w", err)
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
