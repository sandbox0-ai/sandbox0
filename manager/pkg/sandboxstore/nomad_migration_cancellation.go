package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
)

var _ nomadmigration.PreparationCancellationStore = (*PGSandboxStore)(nil)

const nomadMigrationCancellationDue = `(m.preparation_cancel_request IS NOT NULL
    OR l.created_at + INTERVAL '2 minutes' <= clock_timestamp()
    OR m.cpu_preflight_requested_at + INTERVAL '2 minutes' <= clock_timestamp()
    OR EXISTS (SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=l.sandbox_id
        AND (s.desired_state<>'active' OR s.deleted_at IS NOT NULL OR s.hard_expires_at<=clock_timestamp()))
    OR EXISTS (SELECT 1 FROM manager.runtime_slots t WHERE t.slot_id=m.target_slot_id
        AND (t.carrier_retired OR t.state<>'fastpath_ready' OR t.heartbeat_expires_at<=clock_timestamp()
            OR EXISTS (SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=t.cluster_id
                AND f.node_id=t.node_id AND f.node_uid=t.node_uid AND f.state IN ('warming','draining','revoked')))))`

const nomadMigrationCancellationPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
    AND l.phase='barriered' AND m.preparation_request IS NOT NULL AND m.capture_request IS NULL
    AND m.preparation_cancel_receipt IS NULL AND ` + nomadMigrationCancellationDue

func (s *PGSandboxStore) ListNomadMigrationPreparationCancellations(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationCancellationPredicate)
}

type nomadMigrationCancellation struct {
	life                  *SandboxLifecycleTxn
	prepare               procdapi.RuntimeMigrationRequest
	source, target, lease string
	command               *nomadmigration.PreparationCancellation
	receipt               *procdapi.RuntimeMigrationResponse
}

// Historical cancellation must remain recoverable after TTL or desired-state
// changes. It never authorizes a fresh capture, runtime, writer or generation.
func lockNomadMigrationCancellation(ctx context.Context, tx pgx.Tx, id string) (*nomadMigrationCancellation, error) {
	if err := lockRuntimeSlotClaimOperation(ctx, tx, id); err != nil {
		return nil, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, id).Scan(&sandbox); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox)
	if err != nil {
		return nil, err
	}
	c := &nomadMigrationCancellation{}
	c.life, err = scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, id))
	if err != nil {
		return nil, err
	}
	if c.life == nil || c.life.Kind != SandboxLifecycleKindMigrate || c.life.Source != SandboxLifecycleSourceAuto || c.life.Cancelable ||
		(c.life.Phase != SandboxLifecyclePhaseBarriered && c.life.Phase != SandboxLifecyclePhaseAborted) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var prepare, command, receipt []byte
	var prepareDigest, commandDigest *string
	var assignmentDigest, instance string
	var captured bool
	err = tx.QueryRow(ctx, `SELECT preparation_request,preparation_digest,assignment_digest,source_procd_instance_id,
        source_slot_id,target_slot_id,target_resource_lease_id,capture_request IS NOT NULL,
        preparation_cancel_request,preparation_cancel_digest,preparation_cancel_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, id).Scan(&prepare, &prepareDigest,
		&assignmentDigest, &instance, &c.source, &c.target, &c.lease, &captured, &command, &commandDigest, &receipt)
	if err != nil {
		return nil, err
	}
	if captured || json.Unmarshal(prepare, &c.prepare) != nil || prepareDigest == nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	pd, pe := c.prepare.Digest()
	ad, ae := c.prepare.Assignment.Digest()
	if pe != nil || ae != nil || pd != *prepareDigest || ad != assignmentDigest || c.prepare.Action != procdapi.MigrationPrepare ||
		c.prepare.Assignment.OperationID != id || c.prepare.InstanceID != instance || c.prepare.LifecycleEpoch != c.life.Epoch ||
		c.prepare.Assignment.SourceGeneration != c.life.FromGeneration || c.prepare.Assignment.Target.RuntimeGeneration != c.life.ToGeneration ||
		c.prepare.Assignment.Target.SandboxID != sandbox || c.prepare.Assignment.Target.TeamID != record.TeamID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(command) != 0 {
		c.command = &nomadmigration.PreparationCancellation{}
		if json.Unmarshal(command, c.command) != nil || commandDigest == nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		d, err := c.command.Digest()
		expected := c.prepare
		expected.Action = procdapi.MigrationCancel
		expectedDigest, _ := expected.Digest()
		actualDigest, _ := c.command.Request.Digest()
		if err != nil || d != *commandDigest || expectedDigest != actualDigest {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	if len(receipt) != 0 {
		c.receipt = &procdapi.RuntimeMigrationResponse{}
		if c.command == nil || json.Unmarshal(receipt, c.receipt) != nil || c.receipt.ValidateFor(c.command.Request) != nil || c.life.Phase != SandboxLifecyclePhaseAborted {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else if c.life.Phase == SandboxLifecyclePhaseAborted {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return c, nil
}

func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationPreparationCancellation(ctx context.Context, id string) (*nomadmigration.PreparationCancellation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	c, err := lockNomadMigrationCancellation(ctx, tx, id)
	if err != nil || c.receipt != nil {
		return nil, err
	}
	if c.command != nil {
		return c.command, tx.Commit(ctx)
	}
	var due bool
	if err := tx.QueryRow(ctx, `SELECT `+nomadMigrationCancellationDue+` FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id WHERE m.operation_id=$1`, id).Scan(&due); err != nil {
		return nil, err
	}
	if !due {
		return nil, nil
	}
	request := c.prepare
	request.Action = procdapi.MigrationCancel
	command := &nomadmigration.PreparationCancellation{Request: request}
	if err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT preparation_address FROM manager.sandbox_runtime_migrations WHERE operation_id=$5),procd_address) FROM manager.runtime_slots WHERE slot_id=$1
        AND allocation_id=$2 AND allocation_namespace=$3 AND procd_instance_id=$4`, c.source,
		c.life.FromRuntimeID, c.life.FromRuntimeNamespace, request.InstanceID, id).Scan(&command.Address); err != nil {
		return nil, err
	}
	digest, err := command.Digest()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_cancel_request=$2,preparation_cancel_digest=$3 WHERE operation_id=$1`, id, payload, digest); err != nil {
		return nil, err
	}
	return command, tx.Commit(ctx)
}

func (s *PGSandboxStore) CommitNomadSandboxMigrationPreparationCancellation(ctx context.Context, command nomadmigration.PreparationCancellation, receipt procdapi.RuntimeMigrationResponse) error {
	want, err := command.Digest()
	if err != nil {
		return err
	}
	if err := receipt.ValidateFor(command.Request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	c, err := lockNomadMigrationCancellation(ctx, tx, command.Request.Assignment.OperationID)
	if err != nil {
		return err
	}
	if c.command == nil {
		return ErrNomadSandboxMigrationConflict
	}
	got, _ := c.command.Digest()
	if want != got {
		return ErrNomadSandboxMigrationConflict
	}
	if c.receipt != nil {
		if *c.receipt != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_cancel_receipt=$2 WHERE operation_id=$1`, c.life.ID, payload); err != nil {
		return err
	}
	if _, err := releaseUnusedMigrationDestination(ctx, tx, c.life, c.target, c.lease, "migration_preparation_canceled"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
