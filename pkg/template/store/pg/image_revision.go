package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

const templateImageRevisionSelectColumns = `
	revision_id, template_id, scope, team_id, source_image, spec_hash,
	resolved_digest, platform_os, platform_architecture, platform_variant,
	image_fs_head_id, oci_config, state, attempt_count, next_attempt_at,
	lease_owner, lease_expires_at, reason, message, started_at, completed_at,
	created_at, updated_at
`

// EnsureTemplateImageRevision creates or reuses the immutable revision for a
// template spec without selecting it for claims. Selection is a separate
// rollout decision so imports can run in shadow mode.
func (s *Store) EnsureTemplateImageRevision(ctx context.Context, tpl *template.Template) (*template.TemplateImageRevision, bool, error) {
	if s == nil || s.pool == nil {
		return nil, false, fmt.Errorf("template store is not configured")
	}
	revision, err := template.NewTemplateImageRevision(tpl)
	if err != nil {
		return nil, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin template image revision transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO scheduler_template_image_revisions (
			revision_id, template_id, scope, team_id, source_image, spec_hash,
			state, next_attempt_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (scope, team_id, template_id, spec_hash) DO NOTHING
	`, revision.RevisionID, revision.TemplateID, revision.Scope, revision.TeamID,
		revision.SourceImage, revision.SpecHash, revision.State)
	if err != nil {
		return nil, false, fmt.Errorf("create template image revision: %w", err)
	}
	created := tag.RowsAffected() == 1

	loadStored := func() (*template.TemplateImageRevision, error) {
		return scanTemplateImageRevision(tx.QueryRow(ctx, `
			SELECT `+templateImageRevisionSelectColumns+`
			FROM scheduler_template_image_revisions
			WHERE scope = $1 AND team_id = $2 AND template_id = $3 AND spec_hash = $4
		`, revision.Scope, revision.TeamID, revision.TemplateID, revision.SpecHash))
	}
	stored, err := loadStored()
	if err != nil {
		return nil, false, fmt.Errorf("load template image revision: %w", err)
	}
	if !created && stored.State == template.TemplateImageRevisionStateFailed &&
		stored.Reason == template.TemplateImageRevisionReasonSuperseded {
		if _, err := tx.Exec(ctx, `
			UPDATE scheduler_template_image_revisions
			SET resolved_digest = '', platform_os = '', platform_architecture = '',
				platform_variant = '', image_fs_head_id = NULL, oci_config = NULL,
				state = 'resolving', attempt_count = 0, next_attempt_at = NOW(),
				lease_owner = NULL, lease_expires_at = NULL,
				reason = '', message = '', started_at = NULL, completed_at = NULL
			WHERE scope = $1 AND team_id = $2 AND template_id = $3 AND spec_hash = $4
				AND state = 'failed' AND reason = $5
		`, revision.Scope, revision.TeamID, revision.TemplateID, revision.SpecHash,
			template.TemplateImageRevisionReasonSuperseded); err != nil {
			return nil, false, fmt.Errorf("revive superseded template image revision: %w", err)
		}
		stored, err = loadStored()
		if err != nil {
			return nil, false, fmt.Errorf("reload revived template image revision: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit template image revision: %w", err)
	}
	return stored, created, nil
}

// SelectCurrentTemplateImageRevision makes one immutable revision visible to
// readiness checks and routing. The revision must belong to the same template.
func (s *Store) SelectCurrentTemplateImageRevision(ctx context.Context, revision *template.TemplateImageRevision) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("template store is not configured")
	}
	if revision == nil || strings.TrimSpace(revision.RevisionID) == "" {
		return fmt.Errorf("template image revision is required")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE scheduler_templates t
		SET current_image_revision_id = r.revision_id
		FROM scheduler_template_image_revisions r
		WHERE t.scope = $1 AND t.team_id = $2 AND t.template_id = $3
			AND r.revision_id = $4
			AND r.scope = t.scope AND r.team_id = t.team_id AND r.template_id = t.template_id
	`, revision.Scope, revision.TeamID, revision.TemplateID, revision.RevisionID)
	if err != nil {
		return fmt.Errorf("select current template image revision: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("select current template image revision %q: template or revision not found", revision.RevisionID)
	}
	return nil
}

// ClearCurrentTemplateImageRevision returns a template to the legacy claim
// path without deleting shadow-imported immutable revisions.
func (s *Store) ClearCurrentTemplateImageRevision(ctx context.Context, scope, teamID, templateID string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("template store is not configured")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE scheduler_templates
		SET current_image_revision_id = NULL
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
			AND current_image_revision_id IS NOT NULL
	`, scope, teamID, templateID)
	if err != nil {
		return fmt.Errorf("clear current template image revision: %w", err)
	}
	return nil
}

// GetCurrentTemplateImageRevision loads the revision selected by the template.
func (s *Store) GetCurrentTemplateImageRevision(ctx context.Context, scope, teamID, templateID string) (*template.TemplateImageRevision, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	revision, err := scanTemplateImageRevision(s.pool.QueryRow(ctx, `
		SELECT `+templateImageRevisionSelectColumns+`
		FROM scheduler_template_image_revisions r
		JOIN scheduler_templates t ON t.current_image_revision_id = r.revision_id
		WHERE t.scope = $1 AND t.team_id = $2 AND t.template_id = $3
	`, scope, teamID, templateID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get current template image revision: %w", err)
	}
	return revision, nil
}

// ClaimTemplateImageRevision leases one due resolve/import job region-wide.
// Explicit selectors prevent revisions left by an earlier, wider cohort from
// being imported after a rollout narrows.
func (s *Store) ClaimTemplateImageRevision(ctx context.Context, workerID string, leaseDuration time.Duration, teamIDs, templateIDs []string) (*template.TemplateImageRevision, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, fmt.Errorf("template image revision worker id is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("template image revision lease duration must be positive")
	}
	importAll := len(teamIDs) == 0 && len(templateIDs) == 0
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim template image revision: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	revision, err := scanTemplateImageRevision(tx.QueryRow(ctx, `
		SELECT `+templateImageRevisionSelectColumns+`
		FROM scheduler_template_image_revisions
		WHERE state IN ('resolving', 'importing')
			AND next_attempt_at <= NOW()
			AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
			AND ($1 OR template_id = ANY($2::text[])
				OR (scope = 'team' AND team_id = ANY($3::text[])))
		ORDER BY next_attempt_at, created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, importAll, templateIDs, teamIDs))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select template image revision: %w", err)
	}
	leaseExpiresAt := time.Now().UTC().Add(leaseDuration)
	if _, err := tx.Exec(ctx, `
		UPDATE scheduler_template_image_revisions
		SET lease_owner = $2, lease_expires_at = $3,
			attempt_count = attempt_count + 1,
			started_at = COALESCE(started_at, NOW())
		WHERE revision_id = $1
	`, revision.RevisionID, workerID, leaseExpiresAt); err != nil {
		return nil, fmt.Errorf("lease template image revision: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit template image revision lease: %w", err)
	}
	revision.LeaseOwner = workerID
	revision.LeaseExpiresAt = leaseExpiresAt
	revision.AttemptCount++
	if revision.StartedAt.IsZero() {
		revision.StartedAt = time.Now().UTC()
	}
	return revision, nil
}

// RenewTemplateImageRevisionLease extends a worker's active revision lease.
func (s *Store) RenewTemplateImageRevisionLease(ctx context.Context, revisionID, workerID string, leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		return fmt.Errorf("template image revision lease duration must be positive")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_image_revisions
		SET lease_expires_at = NOW() + $3::interval
		WHERE revision_id = $1 AND lease_owner = $2
			AND lease_expires_at > NOW() AND state IN ('resolving', 'importing')
	`, revisionID, workerID, leaseDuration.String())
	if err != nil {
		return fmt.Errorf("renew template image revision lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return template.ErrTemplateImageRevisionLeaseLost
	}
	return nil
}

// MarkTemplateImageRevisionResolved stores the immutable manifest/platform and advances import.
func (s *Store) MarkTemplateImageRevisionResolved(ctx context.Context, revisionID, workerID, digest, platformOS, architecture, variant string, ociConfig json.RawMessage) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_image_revisions
		SET resolved_digest = $3, platform_os = $4,
			platform_architecture = $5, platform_variant = $6,
			oci_config = $7, state = 'importing', reason = '', message = ''
		WHERE revision_id = $1 AND lease_owner = $2
			AND lease_expires_at > NOW() AND state IN ('resolving', 'importing')
	`, revisionID, workerID, digest, platformOS, architecture, variant, nullableJSON(ociConfig))
	if err != nil {
		return fmt.Errorf("mark template image revision resolved: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return template.ErrTemplateImageRevisionLeaseLost
	}
	return nil
}

// MarkTemplateImageRevisionReady publishes the immutable ImageFS head.
func (s *Store) MarkTemplateImageRevisionReady(ctx context.Context, revisionID, workerID, imageFSHeadID string, completedAt time.Time) error {
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_image_revisions
		SET image_fs_head_id = $3, state = 'ready', completed_at = $4,
			lease_owner = NULL, lease_expires_at = NULL, reason = '', message = ''
		WHERE revision_id = $1 AND lease_owner = $2
			AND lease_expires_at > NOW() AND state = 'importing'
	`, revisionID, workerID, imageFSHeadID, completedAt)
	if err != nil {
		return fmt.Errorf("mark template image revision ready: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return template.ErrTemplateImageRevisionLeaseLost
	}
	return nil
}

// FailTemplateImageRevision records a terminal resolve/import failure.
func (s *Store) FailTemplateImageRevision(ctx context.Context, revisionID, workerID, reason, message string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_image_revisions
		SET state = 'failed', reason = $3, message = $4,
			lease_owner = NULL, lease_expires_at = NULL, completed_at = NOW()
		WHERE revision_id = $1 AND lease_owner = $2 AND lease_expires_at > NOW()
	`, revisionID, workerID, strings.TrimSpace(reason), strings.TrimSpace(message))
	if err != nil {
		return fmt.Errorf("fail template image revision: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return template.ErrTemplateImageRevisionLeaseLost
	}
	return nil
}

// ReleaseTemplateImageRevision returns a transient failure to the durable queue.
func (s *Store) ReleaseTemplateImageRevision(ctx context.Context, revisionID, workerID string, retryAt time.Time, message string) error {
	if retryAt.IsZero() {
		retryAt = time.Now().UTC()
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_image_revisions
		SET lease_owner = NULL, lease_expires_at = NULL,
			next_attempt_at = $3, message = $4
		WHERE revision_id = $1 AND lease_owner = $2
	`, revisionID, workerID, retryAt, strings.TrimSpace(message))
	if err != nil {
		return fmt.Errorf("release template image revision: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return template.ErrTemplateImageRevisionLeaseLost
	}
	return nil
}

func scanTemplateImageRevision(row rowScanner) (*template.TemplateImageRevision, error) {
	var revision template.TemplateImageRevision
	var ociConfig []byte
	var leaseOwner *string
	var imageFSHeadID *string
	var leaseExpiresAt, startedAt, completedAt *time.Time
	if err := row.Scan(
		&revision.RevisionID, &revision.TemplateID, &revision.Scope, &revision.TeamID,
		&revision.SourceImage, &revision.SpecHash, &revision.ResolvedDigest,
		&revision.PlatformOS, &revision.PlatformArchitecture, &revision.PlatformVariant,
		&imageFSHeadID, &ociConfig, &revision.State, &revision.AttemptCount,
		&revision.NextAttemptAt, &leaseOwner, &leaseExpiresAt, &revision.Reason,
		&revision.Message, &startedAt, &completedAt, &revision.CreatedAt, &revision.UpdatedAt,
	); err != nil {
		return nil, err
	}
	revision.OCIConfig = append(json.RawMessage(nil), ociConfig...)
	revision.ImageFSHeadID = stringValue(imageFSHeadID)
	revision.LeaseOwner = stringValue(leaseOwner)
	revision.LeaseExpiresAt = timeValue(leaseExpiresAt)
	revision.StartedAt = timeValue(startedAt)
	revision.CompletedAt = timeValue(completedAt)
	return &revision, nil
}

func timeValue(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}
