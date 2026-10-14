package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
)

const RuntimeProviderAbsenceGrace = 30 * time.Second
const RootFSWriterProviderAbandonProofVersion = 2

// RuntimeProviderTarget names the original node incarnation, never the new
// host's boot or mount namespace.
type RuntimeProviderTarget struct {
	ClusterID  string `json:"cluster_id"`
	NodeID     string `json:"node_id"`
	NodeUID    string `json:"node_uid"`
	NodeBootID string `json:"node_boot_id"`
}

type RuntimeProviderAbsenceProof struct {
	Version         int                          `json:"version"`
	Target          RuntimeProviderTarget        `json:"target"`
	Scope           providerinstance.Scope       `json:"scope"`
	First           providerinstance.Observation `json:"first"`
	Last            providerinstance.Observation `json:"last"`
	FirstObservedAt time.Time                    `json:"first_observed_at"`
	LastObservedAt  time.Time                    `json:"last_observed_at"`
}

func (p RuntimeProviderAbsenceProof) Digest() ([]byte, error) {
	if err := validateRuntimeProviderTarget(p.Target, p.Scope); err != nil {
		return nil, err
	}
	if p.Version != 1 || p.First.Present || p.Last.Present || p.First.ValidateFor(p.Scope) != nil || p.Last.ValidateFor(p.Scope) != nil ||
		p.First.RequestID == p.Last.RequestID || p.First.IdentityRequestID == p.Last.IdentityRequestID || p.FirstObservedAt.IsZero() ||
		p.LastObservedAt.Sub(p.FirstObservedAt) < RuntimeProviderAbsenceGrace {
		return nil, errors.New("distinct authenticated provider observations must span regional grace")
	}
	p.FirstObservedAt, p.LastObservedAt = p.FirstObservedAt.UTC(), p.LastObservedAt.UTC()
	body, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	return digest[:], nil
}

func validateRuntimeProviderTarget(t RuntimeProviderTarget, s providerinstance.Scope) error {
	for _, v := range []string{t.ClusterID, t.NodeID, t.NodeUID, t.NodeBootID} {
		if v == "" || strings.TrimSpace(v) != v || len(v) > 512 {
			return errors.New("exact canonical provider node incarnation is required")
		}
	}
	if s.Provider != "aliyun" || t.NodeUID != "ecs/"+s.RegionID+"/"+s.InstanceID {
		return errors.New("provider scope does not match original node UID")
	}
	return nil
}

const providerAbsenceColumns = `scope,first_observation,last_observation,first_observed_at,last_observed_at,proof_digest,revoked`

func scanProviderAbsence(row pgx.Row, target RuntimeProviderTarget) (*RuntimeProviderAbsenceProof, []byte, bool, error) {
	p := &RuntimeProviderAbsenceProof{Version: 1, Target: target}
	var scope, first, last, digest []byte
	var revoked bool
	if err := row.Scan(&scope, &first, &last, &p.FirstObservedAt, &p.LastObservedAt, &digest, &revoked); err != nil {
		return nil, nil, false, err
	}
	for _, v := range []struct {
		body  []byte
		value any
	}{{scope, &p.Scope}, {first, &p.First}, {last, &p.Last}} {
		if err := json.Unmarshal(v.body, v.value); err != nil {
			return nil, nil, false, err
		}
	}
	return p, digest, revoked, nil
}

// RecordRuntimeProviderInstanceObservation accepts a trusted, pinned observer's
// receipt, not public/node input. API errors are not absence observations.
// Regional database time owns the grace; response replay never advances it.
func (s *PGSandboxStore) RecordRuntimeProviderInstanceObservation(ctx context.Context, target RuntimeProviderTarget, o providerinstance.Observation) (*RuntimeProviderAbsenceProof, error) {
	if err := validateRuntimeProviderTarget(target, o.Scope); err != nil {
		return nil, err
	}
	if err := o.ValidateFor(o.Scope); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	key, _ := json.Marshal(target)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, string(key)); err != nil {
		return nil, err
	}
	var fenced bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_slots WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 AND state IN ('quiescing','orphaned'))`, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID).Scan(&fenced); err != nil {
		return nil, err
	}
	if !fenced {
		return nil, fmt.Errorf("%w: provider observation requires an exact fenced runtime", ErrRuntimeSlotConflict)
	}
	p, digest, revoked, err := scanProviderAbsence(tx.QueryRow(ctx, `SELECT `+providerAbsenceColumns+` FROM manager.runtime_provider_absence WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 FOR UPDATE`, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID), target)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if p != nil && p.Scope != o.Scope {
		return nil, errors.New("provider observation changed pinned account or region")
	}
	if o.Present {
		if p != nil {
			query := `DELETE FROM manager.runtime_provider_absence WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4`
			if len(digest) > 0 {
				query = `UPDATE manager.runtime_provider_absence SET revoked=TRUE WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4`
			}
			if _, err = tx.Exec(ctx, query, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID); err != nil {
				return nil, err
			}
		}
		return nil, tx.Commit(ctx)
	}
	if revoked {
		return nil, errors.New("provider absence proof was revoked by positive presence")
	}
	if len(digest) > 0 {
		actual, e := p.Digest()
		if e != nil || !bytes.Equal(actual, digest) {
			return nil, errors.New("stored provider proof checksum does not match")
		}
		return p, tx.Commit(ctx)
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if p == nil {
		body, _ := json.Marshal(o)
		scope, _ := json.Marshal(o.Scope)
		_, err = tx.Exec(ctx, `INSERT INTO manager.runtime_provider_absence(cluster_id,node_id,node_uid,node_boot_id,scope,first_observation,last_observation,first_observed_at,last_observed_at) VALUES($1,$2,$3,$4,$5,$6,$6,$7,$7)`, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID, scope, body, now)
		if err != nil {
			return nil, err
		}
		return nil, tx.Commit(ctx)
	}
	if p.First.RequestID == o.RequestID || p.Last.RequestID == o.RequestID || p.First.IdentityRequestID == o.IdentityRequestID || p.Last.IdentityRequestID == o.IdentityRequestID {
		return nil, tx.Commit(ctx)
	}
	p.Last, p.LastObservedAt = o, now
	if now.Sub(p.FirstObservedAt) >= RuntimeProviderAbsenceGrace {
		digest, err = p.Digest()
		if err != nil {
			return nil, err
		}
	}
	body, _ := json.Marshal(o)
	_, err = tx.Exec(ctx, `UPDATE manager.runtime_provider_absence SET last_observation=$5,last_observed_at=$6,proof_digest=$7 WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4`, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID, body, now, digest)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(digest) == 0 {
		return nil, nil
	}
	return p, nil
}

type CompleteRuntimeSlotProviderAbandonRequest struct {
	SlotID                      string
	Proof                       RuntimeProviderAbsenceProof
	Writer                      CompleteRootFSWriterCrashAbandonRequest
	AllocationObservationDigest []byte
}

// CompleteRuntimeSlotProviderAbandon atomically promotes the acknowledged live
// head, aborts the crash lifecycle, and retires the exact writer/slot/resources.
// The caller independently acknowledges Nomad server retirement. Existing node
// proof APIs continue to reject proof version two.
func (s *PGSandboxStore) CompleteRuntimeSlotProviderAbandon(ctx context.Context, r *CompleteRuntimeSlotProviderAbandonRequest) (*RuntimeSlot, error) {
	if r == nil || r.SlotID == "" || len(r.AllocationObservationDigest) != sha256.Size {
		return nil, errors.New("provider completion needs exact slot and acknowledged Nomad observation")
	}
	digest, err := r.Proof.Digest()
	if err != nil {
		return nil, err
	}
	writer, err := validateCompleteRootFSWriterCrashAbandonRequestAtVersion(&r.Writer, RootFSWriterProviderAbandonProofVersion)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(writer.ProofDigest, digest) {
		return nil, errors.New("writer completion changed persisted provider proof")
	}
	preliminary, err := s.GetRuntimeSlot(ctx, r.SlotID)
	if err != nil {
		return nil, err
	}
	if preliminary.SandboxID == "" || preliminary.WriterGrantID != writer.GrantID {
		return nil, ErrRuntimeSlotConflict
	}
	err = s.WithSandboxLock(ctx, preliminary.SandboxID, func(ctx context.Context, base SandboxStoreTx, _ *SandboxRecord) error {
		t := base.(sandboxStoreTx)
		grant, e := getRootFSWriterGrantForUpdate(ctx, t.tx, writer.GrantID)
		if e != nil {
			return e
		}
		p, stored, revoked, e := scanProviderAbsence(t.tx.QueryRow(ctx, `SELECT `+providerAbsenceColumns+` FROM manager.runtime_provider_absence WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 FOR SHARE`, r.Proof.Target.ClusterID, r.Proof.Target.NodeID, r.Proof.Target.NodeUID, r.Proof.Target.NodeBootID), r.Proof.Target)
		if e != nil {
			return e
		}
		actual, e := p.Digest()
		if e != nil || revoked || !bytes.Equal(stored, digest) || !bytes.Equal(actual, digest) {
			return errors.New("persisted provider proof is missing, revoked or changed")
		}
		slot, e := scanRuntimeSlot(t.tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, r.SlotID))
		if e != nil {
			return e
		}
		target := RuntimeProviderTarget{slot.ClusterID, slot.NodeID, slot.NodeUID, slot.NodeBootID}
		if target != r.Proof.Target || slot.WriterGrantID != grant.ID || slot.SandboxID != grant.SandboxID || slot.ClaimID != grant.ClaimID || slot.FilesystemID != grant.FilesystemID || slot.SourceGenerationID != grant.InitialGenerationID || slot.NodeUID != writer.NodeUID || slot.NodeBootID != writer.NodeBootID || slot.AllocationID != grant.RuntimeIncarnationID || slot.AllocationNamespace != grant.RuntimeNamespace {
			return ErrRuntimeSlotConflict
		}
		if slot.State != RuntimeSlotStateQuiescing && slot.State != RuntimeSlotStateOrphaned && slot.State != RuntimeSlotStateTerminal {
			return ErrRuntimeSlotNotDue
		}
		var protected bool
		if e = t.tx.QueryRow(ctx, `SELECT migration_source_operation_id IS NOT NULL OR EXISTS(SELECT 1 FROM manager.sandbox_runtime_checkpoints c JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id WHERE c.source_slot_id=$1 AND l.phase IN ('preparing','barriered','publishing','committing')) FROM manager.runtime_slots WHERE slot_id=$1`, slot.ID).Scan(&protected); e != nil {
			return e
		}
		if protected {
			return fmt.Errorf("%w: provider loss cannot bypass checkpoint or migration custody", ErrRuntimeSlotConflict)
		}
		if _, e = completeValidatedRootFSWriterCrashAbandon(ctx, t, writer); e != nil {
			return e
		}
		body, _ := json.Marshal(struct {
			SlotID, ClaimID, GrantID, OperationID string
			ProviderProof, AllocationProof        []byte
		}{slot.ID, slot.ClaimID, grant.ID, writer.OperationID, digest, r.AllocationObservationDigest})
		terminal := sha256.Sum256(body)
		if _, e = t.tx.Exec(ctx, `INSERT INTO manager.runtime_provider_absence_retirements(grant_id,operation_id,proof_digest,allocation_observation_digest,terminal_digest) VALUES($1,$2,$3,$4,$5) ON CONFLICT(grant_id) DO NOTHING`, grant.ID, writer.OperationID, digest, r.AllocationObservationDigest, terminal[:]); e != nil {
			return e
		}
		var exact bool
		if e = t.tx.QueryRow(ctx, `SELECT operation_id=$2 AND proof_digest=$3 AND allocation_observation_digest=$4 AND terminal_digest=$5 FROM manager.runtime_provider_absence_retirements WHERE grant_id=$1`, grant.ID, writer.OperationID, digest, r.AllocationObservationDigest, terminal[:]).Scan(&exact); e != nil {
			return e
		}
		if !exact {
			return errors.New("provider retirement is not an exact retry")
		}
		// Preserve every unrelated existing rollout fence.
		if _, e = t.tx.Exec(ctx, `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason) VALUES($1,$2,$3,'draining','confirmed provider instance absence') ON CONFLICT DO NOTHING`, slot.ClusterID, slot.NodeID, slot.NodeUID); e != nil {
			return e
		}
		_, e = finalizeRuntimeSlotTx(ctx, t.tx, slot, &FinalizeRuntimeSlotRequest{SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, Reason: "provider_instance_absent", ProofDigest: terminal[:], ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest, ResourceCgroupAbsent: !slot.ResourceLease.IsZero()})
		return e
	})
	if err != nil {
		return nil, err
	}
	return s.GetRuntimeSlot(ctx, r.SlotID)
}
