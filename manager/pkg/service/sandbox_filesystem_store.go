package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const SandboxFilesystemStatusReady = "ready"

var (
	ErrSandboxFilesystemBusy      = errors.New("sandbox filesystem is busy")
	ErrSandboxFilesystemForbidden = errors.New("sandbox filesystem belongs to another team")
)

// SandboxFilesystemRecord is the internal durable rootfs state. It is not a
// public API resource.
type SandboxFilesystemRecord struct {
	FilesystemID            string
	TeamID                  string
	UserID                  string
	BaseImageRef            string
	BaseImageDigest         string
	UpperdirHead            map[string]any
	Status                  string
	OwnerSandboxID          string
	OwnerRuntimeGeneration  int64
	OwnerAcquiredAt         time.Time
	LifecycleOwnerSandboxID string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type SandboxFilesystemAcquireRequest struct {
	FilesystemID            string
	TeamID                  string
	UserID                  string
	BaseImageRef            string
	BaseImageDigest         string
	LifecycleOwnerSandboxID string
	OwnerSandboxID          string
	OwnerRuntimeGeneration  int64
}

type SandboxFilesystemReleaseRequest struct {
	FilesystemID           string
	OwnerSandboxID         string
	OwnerRuntimeGeneration int64
}

type SandboxFilesystemDeleteRequest struct {
	FilesystemID            string
	LifecycleOwnerSandboxID string
}

// SandboxFilesystemStore persists internal sandbox rootfs state and RWO owner
// locks. It intentionally has no public CRUD shape.
type SandboxFilesystemStore interface {
	AcquireOwner(ctx context.Context, req SandboxFilesystemAcquireRequest) (*SandboxFilesystemRecord, error)
	ReleaseOwner(ctx context.Context, req SandboxFilesystemReleaseRequest) error
	DeleteForSandbox(ctx context.Context, req SandboxFilesystemDeleteRequest) error
}

func (s *PGSandboxStore) AcquireOwner(ctx context.Context, req SandboxFilesystemAcquireRequest) (*SandboxFilesystemRecord, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	req = normalizeSandboxFilesystemAcquireRequest(req)
	if req.FilesystemID == "" {
		return nil, fmt.Errorf("filesystem_id is required")
	}
	if req.TeamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if req.OwnerSandboxID == "" {
		return nil, fmt.Errorf("owner sandbox_id is required")
	}
	if req.OwnerRuntimeGeneration <= 0 {
		return nil, fmt.Errorf("owner runtime_generation is required")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin sandbox filesystem acquire tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO manager.sandbox_filesystems (
			filesystem_id, team_id, user_id, base_image_ref, base_image_digest, status, lifecycle_owner_sandbox_id, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		ON CONFLICT (filesystem_id) DO NOTHING
	`, req.FilesystemID, req.TeamID, req.UserID, req.BaseImageRef, req.BaseImageDigest, SandboxFilesystemStatusReady, req.LifecycleOwnerSandboxID)
	if err != nil {
		return nil, fmt.Errorf("insert sandbox filesystem: %w", err)
	}

	record, err := scanSandboxFilesystemRecord(tx.QueryRow(ctx, sandboxFilesystemSelectSQL()+` WHERE filesystem_id = $1 FOR UPDATE`, req.FilesystemID))
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("sandbox filesystem %s not found after insert", req.FilesystemID)
	}
	if record.TeamID != req.TeamID {
		return nil, fmt.Errorf("%w: %s", ErrSandboxFilesystemForbidden, req.FilesystemID)
	}
	if record.OwnerSandboxID != "" &&
		(record.OwnerSandboxID != req.OwnerSandboxID || record.OwnerRuntimeGeneration != req.OwnerRuntimeGeneration) {
		return nil, fmt.Errorf("%w: %s is owned by sandbox %s generation %d", ErrSandboxFilesystemBusy, req.FilesystemID, record.OwnerSandboxID, record.OwnerRuntimeGeneration)
	}
	if record.LifecycleOwnerSandboxID != "" && record.LifecycleOwnerSandboxID != req.OwnerSandboxID {
		return nil, fmt.Errorf("%w: %s lifecycle is owned by sandbox %s", ErrSandboxFilesystemBusy, req.FilesystemID, record.LifecycleOwnerSandboxID)
	}

	baseImageRef := record.BaseImageRef
	if baseImageRef == "" {
		baseImageRef = req.BaseImageRef
	}
	baseImageDigest := record.BaseImageDigest
	if baseImageDigest == "" {
		baseImageDigest = req.BaseImageDigest
	}
	lifecycleOwnerSandboxID := record.LifecycleOwnerSandboxID
	if lifecycleOwnerSandboxID == "" {
		lifecycleOwnerSandboxID = req.LifecycleOwnerSandboxID
	}
	record, err = scanSandboxFilesystemRecord(tx.QueryRow(ctx, `
		UPDATE manager.sandbox_filesystems
		SET base_image_ref = $2,
			base_image_digest = $3,
			owner_sandbox_id = $4,
			owner_runtime_generation = $5,
			owner_acquired_at = COALESCE(owner_acquired_at, NOW()),
			lifecycle_owner_sandbox_id = $6,
			updated_at = NOW()
		WHERE filesystem_id = $1
		RETURNING filesystem_id, team_id, user_id, base_image_ref, base_image_digest, upperdir_head, status,
			owner_sandbox_id, owner_runtime_generation, owner_acquired_at, lifecycle_owner_sandbox_id, created_at, updated_at
	`, req.FilesystemID, baseImageRef, baseImageDigest, req.OwnerSandboxID, req.OwnerRuntimeGeneration, lifecycleOwnerSandboxID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sandbox filesystem acquire tx: %w", err)
	}
	return record, nil
}

func (s *PGSandboxStore) ReleaseOwner(ctx context.Context, req SandboxFilesystemReleaseRequest) error {
	if s == nil || s.pool == nil {
		return nil
	}
	req.FilesystemID = strings.TrimSpace(req.FilesystemID)
	req.OwnerSandboxID = strings.TrimSpace(req.OwnerSandboxID)
	if req.FilesystemID == "" || req.OwnerSandboxID == "" {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.sandbox_filesystems
		SET owner_sandbox_id = '',
			owner_runtime_generation = 0,
			owner_acquired_at = NULL,
			updated_at = NOW()
		WHERE filesystem_id = $1
			AND owner_sandbox_id = $2
			AND ($3::BIGINT <= 0 OR owner_runtime_generation = $3)
	`, req.FilesystemID, req.OwnerSandboxID, req.OwnerRuntimeGeneration)
	if err != nil {
		return fmt.Errorf("release sandbox filesystem owner: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) DeleteForSandbox(ctx context.Context, req SandboxFilesystemDeleteRequest) error {
	if s == nil || s.pool == nil {
		return nil
	}
	req.FilesystemID = strings.TrimSpace(req.FilesystemID)
	req.LifecycleOwnerSandboxID = strings.TrimSpace(req.LifecycleOwnerSandboxID)
	if req.FilesystemID == "" || req.LifecycleOwnerSandboxID == "" {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		DELETE FROM manager.sandbox_filesystems
		WHERE filesystem_id = $1
			AND lifecycle_owner_sandbox_id = $2
	`, req.FilesystemID, req.LifecycleOwnerSandboxID)
	if err != nil {
		return fmt.Errorf("delete sandbox-owned filesystem: %w", err)
	}
	return nil
}

func normalizeSandboxFilesystemAcquireRequest(req SandboxFilesystemAcquireRequest) SandboxFilesystemAcquireRequest {
	req.FilesystemID = strings.TrimSpace(req.FilesystemID)
	req.TeamID = strings.TrimSpace(req.TeamID)
	req.UserID = strings.TrimSpace(req.UserID)
	req.BaseImageRef = strings.TrimSpace(req.BaseImageRef)
	req.BaseImageDigest = strings.TrimSpace(req.BaseImageDigest)
	req.LifecycleOwnerSandboxID = strings.TrimSpace(req.LifecycleOwnerSandboxID)
	req.OwnerSandboxID = strings.TrimSpace(req.OwnerSandboxID)
	return req
}

func sandboxFilesystemSelectSQL() string {
	return `
		SELECT filesystem_id, team_id, user_id, base_image_ref, base_image_digest, upperdir_head, status,
			owner_sandbox_id, owner_runtime_generation, owner_acquired_at, lifecycle_owner_sandbox_id, created_at, updated_at
		FROM manager.sandbox_filesystems`
}

func scanSandboxFilesystemRecord(row sandboxRecordScanner) (*SandboxFilesystemRecord, error) {
	record, err := scanSandboxFilesystemRecordInto(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return record, nil
}

func scanSandboxFilesystemRecordInto(scanner sandboxRecordScanner) (*SandboxFilesystemRecord, error) {
	var record SandboxFilesystemRecord
	var upperdirHeadJSON []byte
	var ownerAcquiredAt *time.Time
	if err := scanner.Scan(
		&record.FilesystemID, &record.TeamID, &record.UserID, &record.BaseImageRef, &record.BaseImageDigest, &upperdirHeadJSON, &record.Status,
		&record.OwnerSandboxID, &record.OwnerRuntimeGeneration, &ownerAcquiredAt, &record.LifecycleOwnerSandboxID, &record.CreatedAt, &record.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if len(upperdirHeadJSON) > 0 {
		if err := json.Unmarshal(upperdirHeadJSON, &record.UpperdirHead); err != nil {
			return nil, fmt.Errorf("unmarshal sandbox filesystem upperdir head: %w", err)
		}
	}
	if record.UpperdirHead == nil {
		record.UpperdirHead = map[string]any{}
	}
	record.OwnerAcquiredAt = derefTime(ownerAcquiredAt)
	return &record, nil
}
