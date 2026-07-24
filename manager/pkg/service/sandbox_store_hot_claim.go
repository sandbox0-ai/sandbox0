package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"k8s.io/apimachinery/pkg/types"
)

var _ HotClaimReservationStore = (*PGSandboxStore)(nil)

func (s *PGSandboxStore) TryReserveHotClaim(ctx context.Context, reservation *HotClaimReservation) (bool, error) {
	if s == nil || s.pool == nil || reservation == nil {
		return false, nil
	}
	if strings.TrimSpace(reservation.SandboxID) == "" ||
		strings.TrimSpace(reservation.ClusterID) == "" ||
		strings.TrimSpace(reservation.Namespace) == "" ||
		strings.TrimSpace(reservation.PodName) == "" ||
		reservation.PodUID == "" {
		return false, fmt.Errorf("hot claim reservation identity is required")
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO manager.hot_claim_reservations (
			sandbox_id, team_id, cluster_id, pod_namespace, pod_name, pod_uid
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
	`, reservation.SandboxID, reservation.TeamID, reservation.ClusterID, reservation.Namespace, reservation.PodName, string(reservation.PodUID))
	if err != nil {
		return false, fmt.Errorf("reserve hot claim: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGSandboxStore) CommitHotClaim(ctx context.Context, record *SandboxRecord, podUID types.UID, metadata HotClaimPodMetadata) error {
	if s == nil || s.pool == nil || record == nil {
		return nil
	}
	labelsJSON, annotationsJSON, finalizersJSON, err := marshalHotClaimPodMetadata(metadata)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin hot claim commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := upsertSandboxRecord(ctx, tx, record); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.hot_claim_reservations
		SET desired_labels = $2,
			desired_annotations = $3,
			desired_finalizers = $4,
			committed_at = NOW(),
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND pod_namespace = $5
			AND pod_name = $6
			AND cluster_id = $7
			AND team_id = $8
			AND pod_uid = $9
	`, record.ID, labelsJSON, annotationsJSON, finalizersJSON, record.CurrentPodNamespace, record.CurrentPodName, record.ClusterID, record.TeamID, string(podUID))
	if err != nil {
		return fmt.Errorf("commit hot claim reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("hot claim reservation %s was lost before commit", record.ID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit hot claim transaction: %w", err)
	}
	return nil
}

func marshalHotClaimPodMetadata(metadata HotClaimPodMetadata) ([]byte, []byte, []byte, error) {
	labelsJSON, err := json.Marshal(metadata.Labels)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal hot claim labels: %w", err)
	}
	annotationsJSON, err := json.Marshal(metadata.Annotations)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal hot claim annotations: %w", err)
	}
	finalizersJSON, err := json.Marshal(metadata.Finalizers)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal hot claim finalizers: %w", err)
	}
	return labelsJSON, annotationsJSON, finalizersJSON, nil
}

func (s *PGSandboxStore) GetHotClaimReservation(ctx context.Context, sandboxID string) (*HotClaimReservation, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	return scanHotClaimReservation(s.pool.QueryRow(ctx, hotClaimReservationSelectSQL()+` WHERE sandbox_id = $1`, sandboxID))
}

func (s *PGSandboxStore) ListHotClaimReservations(ctx context.Context, clusterID string, createdBefore time.Time, limit int) ([]*HotClaimReservation, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = hotClaimReservationReconcileLimit
	}
	rows, err := s.pool.Query(ctx, hotClaimReservationSelectSQL()+`
		WHERE cluster_id = $1
			AND created_at <= $2
		ORDER BY created_at ASC
		LIMIT $3
	`, clusterID, createdBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("list hot claim reservations: %w", err)
	}
	defer rows.Close()
	reservations := make([]*HotClaimReservation, 0)
	for rows.Next() {
		reservation, err := scanHotClaimReservation(rows)
		if err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hot claim reservations: %w", err)
	}
	return reservations, nil
}

func (s *PGSandboxStore) ReleaseHotClaimReservation(ctx context.Context, sandboxID string, podUID types.UID) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		DELETE FROM manager.hot_claim_reservations
		WHERE sandbox_id = $1 AND pod_uid = $2
	`, sandboxID, string(podUID))
	if err != nil {
		return fmt.Errorf("release hot claim reservation: %w", err)
	}
	return nil
}

func hotClaimReservationSelectSQL() string {
	return `
		SELECT sandbox_id, team_id, cluster_id, pod_namespace, pod_name, pod_uid,
			desired_labels, desired_annotations, desired_finalizers,
			committed_at, created_at
		FROM manager.hot_claim_reservations
	`
}

type hotClaimReservationScanner interface {
	Scan(dest ...any) error
}

func scanHotClaimReservation(row hotClaimReservationScanner) (*HotClaimReservation, error) {
	var reservation HotClaimReservation
	var podUID string
	var labelsJSON, annotationsJSON, finalizersJSON []byte
	var committedAt *time.Time
	if err := row.Scan(
		&reservation.SandboxID,
		&reservation.TeamID,
		&reservation.ClusterID,
		&reservation.Namespace,
		&reservation.PodName,
		&podUID,
		&labelsJSON,
		&annotationsJSON,
		&finalizersJSON,
		&committedAt,
		&reservation.CreatedAt,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("scan hot claim reservation: %w", err)
	}
	reservation.PodUID = types.UID(podUID)
	if committedAt != nil {
		reservation.CommittedAt = *committedAt
	}
	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &reservation.Metadata.Labels); err != nil {
			return nil, fmt.Errorf("unmarshal hot claim labels: %w", err)
		}
	}
	if len(annotationsJSON) > 0 {
		if err := json.Unmarshal(annotationsJSON, &reservation.Metadata.Annotations); err != nil {
			return nil, fmt.Errorf("unmarshal hot claim annotations: %w", err)
		}
	}
	if len(finalizersJSON) > 0 {
		if err := json.Unmarshal(finalizersJSON, &reservation.Metadata.Finalizers); err != nil {
			return nil, fmt.Errorf("unmarshal hot claim finalizers: %w", err)
		}
	}
	return &reservation, nil
}
