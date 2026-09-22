package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var _ nomadmigration.SourceRecoveryStore = (*PGSandboxStore)(nil)

// Already-authorized capture custody does not expire with CPU preflight.
// Recovery can inspect it and seal stopped execution, but cannot start capture.
const nomadMigrationSourceWorkPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
    AND l.phase='publishing' AND m.capture_request IS NOT NULL AND m.publication_request IS NULL AND m.capture_failure_request IS NULL`

func (s *PGSandboxStore) ListNomadMigrationSourceRecoveries(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationSourceWorkPredicate)
}

func (s *PGSandboxStore) GetNomadMigrationSourceRecovery(ctx context.Context, id string) (*nomadmigration.SourceRecovery, error) {
	var preparation, capture, cpu, receipt, binding []byte
	var assignmentDigest, prepareDigest, captureDigest, sourceSlot, procdID, sandboxID string
	var epoch, from, to int64
	err := s.pool.QueryRow(ctx, `SELECT m.preparation_request,m.preparation_digest,m.capture_request,m.capture_digest,
        m.cpu_preflight_request,m.cpu_preflight_source,m.assignment_digest,m.source_slot_id,m.source_procd_instance_id,m.source_binding_digest,
        l.sandbox_id,l.epoch,l.from_generation,l.to_generation
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE m.operation_id=$1 AND `+nomadMigrationSourceWorkPredicate, id).Scan(&preparation, &prepareDigest, &capture, &captureDigest, &cpu, &receipt,
		&assignmentDigest, &sourceSlot, &procdID, &binding, &sandboxID, &epoch, &from, &to)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var prepare procdapi.RuntimeMigrationRequest
	var preflight protocol.MigrationCPUPreflightRequest
	var observed protocol.MigrationCPUPreflight
	work := &nomadmigration.SourceRecovery{}
	if json.Unmarshal(preparation, &prepare) != nil || json.Unmarshal(capture, &work.Capture) != nil ||
		json.Unmarshal(cpu, &preflight) != nil || json.Unmarshal(receipt, &observed) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	work.Assignment, work.Launch = prepare.Assignment, observed.Launch
	pd, pErr := prepare.Digest()
	cd, cErr := work.Capture.Digest()
	ad, aErr := work.Assignment.Digest()
	if pErr != nil || cErr != nil || aErr != nil || pd != prepareDigest || cd != captureDigest || ad != assignmentDigest ||
		prepare.Action != procdapi.MigrationPrepare || prepare.LifecycleEpoch != epoch || prepare.InstanceID != procdID ||
		work.Capture.LifecycleEpoch != epoch || work.Capture.ProcdInstanceID != procdID || work.Capture.Target.SlotID != sourceSlot ||
		work.Capture.BindingDigest != hex.EncodeToString(binding) || work.Assignment.OperationID != id ||
		work.Assignment.Target.SandboxID != sandboxID || work.Assignment.SourceGeneration != from || work.Assignment.Target.RuntimeGeneration != to ||
		!preflight.IsSource() || preflight.Source != work.Capture || observed.ValidateFor(preflight) != nil || work.Validate() != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return work, nil
}
