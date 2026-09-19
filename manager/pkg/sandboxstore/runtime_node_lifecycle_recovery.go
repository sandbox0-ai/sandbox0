package sandboxstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const maxRuntimeNodeLifecycleRecoveryActions = 299

const runtimeNodeLifecycleActionColumns = `
	lifecycle_action_token, pool_id, lifecycle_hook_id,
	transition, state, first_observed_at, completed_at, updated_at,
	recovery_owner_id, recovery_epoch, recovery_lease_expires_at,
	recovery_deadline_at, provider_action_last_observed_at,
	provider_action_absent_since, provider_instance_absent_since,
	convergence_proof
`

func (s *PGSandboxStore) ListRuntimeNodeLifecycleActions(
	ctx context.Context,
	poolID string,
) ([]*RuntimeNodeLifecycleAction, error) {
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return nil, fmt.Errorf("runtime node pool ID is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+runtimeNodeLifecycleActionColumns+`
		FROM manager.runtime_node_lifecycle_actions
		WHERE pool_id = $1 AND state IN ('pending', 'draining')
		ORDER BY first_observed_at, lifecycle_action_token
		LIMIT $2
	`, poolID, maxRuntimeNodeLifecycleRecoveryActions)
	if err != nil {
		return nil, fmt.Errorf("list runtime node lifecycle actions: %w", err)
	}
	var actions []*RuntimeNodeLifecycleAction
	for rows.Next() {
		action, err := scanRuntimeNodeLifecycleAction(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan runtime node lifecycle action: %w", err)
		}
		actions = append(actions, action)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("scan runtime node lifecycle actions: %w", err)
	}
	rows.Close()
	if err := attachRuntimeNodeLifecycleInstances(ctx, s.pool, actions); err != nil {
		return nil, err
	}
	return actions, nil
}

// AcquireRuntimeNodeLifecycleAction gives one manager a bounded recovery
// lease. Incrementing the epoch on every claim fences writes from a prior
// owner whose provider or Nomad call was still in flight when its lease ended.
func (s *PGSandboxStore) AcquireRuntimeNodeLifecycleAction(
	ctx context.Context,
	poolID, token, ownerID string,
	leaseTTL time.Duration,
) (*RuntimeNodeLifecycleAction, error) {
	poolID, token, ownerID = strings.TrimSpace(poolID), strings.TrimSpace(token), strings.TrimSpace(ownerID)
	if poolID == "" || token == "" || ownerID == "" || len(ownerID) > 256 ||
		leaseTTL < time.Second || leaseTTL > 5*time.Minute {
		return nil, fmt.Errorf("runtime node lifecycle recovery claim is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	row := tx.QueryRow(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions
		SET recovery_owner_id = $3,
			recovery_epoch = recovery_epoch + 1,
			recovery_lease_expires_at = NOW() + ($4 * INTERVAL '1 millisecond'),
			updated_at = NOW()
		WHERE pool_id = $1
			AND lifecycle_action_token = $2
			AND state IN ('pending', 'draining')
			AND (
				recovery_owner_id = ''
				OR recovery_owner_id = $3
				OR recovery_lease_expires_at IS NULL
				OR recovery_lease_expires_at <= NOW()
			)
		RETURNING `+runtimeNodeLifecycleActionColumns,
		poolID, token, ownerID, leaseTTL.Milliseconds())
	action, err := scanRuntimeNodeLifecycleAction(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("acquire runtime node lifecycle action: %w", err)
	}
	if err := attachRuntimeNodeLifecycleInstances(ctx, tx, []*RuntimeNodeLifecycleAction{action}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return action, nil
}

func (s *PGSandboxStore) ObserveRuntimeNodeLifecycleProviderAction(
	ctx context.Context,
	poolID, token, ownerID string,
	recoveryEpoch int64,
	present bool,
) (*RuntimeNodeLifecycleAction, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	action, err := updateFencedRuntimeNodeLifecycleAction(ctx, tx, poolID, token, ownerID, recoveryEpoch, `
		provider_action_last_observed_at = CASE WHEN $5 THEN NOW() ELSE NULL END,
		provider_action_absent_since = CASE
			WHEN $5 THEN NULL
			ELSE COALESCE(provider_action_absent_since, NOW())
		END,
		updated_at = NOW()`, present)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return action, nil
}

func (s *PGSandboxStore) ObserveRuntimeNodeLifecycleProviderInstances(
	ctx context.Context,
	poolID, token, ownerID string,
	recoveryEpoch int64,
	absent bool,
) (*RuntimeNodeLifecycleAction, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	action, err := updateFencedRuntimeNodeLifecycleAction(ctx, tx, poolID, token, ownerID, recoveryEpoch, `
		provider_instance_absent_since = CASE
			WHEN $5 THEN COALESCE(provider_instance_absent_since, NOW())
			ELSE NULL
		END,
		updated_at = NOW()`, absent)
	if err != nil {
		return nil, err
	}
	if action.ProviderActionAbsentSince == nil {
		return nil, fmt.Errorf("provider instance absence requires a durable provider action absence observation")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return action, nil
}

func (s *PGSandboxStore) BeginRuntimeNodeLifecycleActionCleanupForOwner(
	ctx context.Context,
	poolID, token, ownerID string,
	recoveryEpoch int64,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := updateFencedRuntimeNodeLifecycleAction(ctx, tx, poolID, token, ownerID, recoveryEpoch, `
		state = CASE WHEN state = 'pending' THEN 'draining' ELSE state END,
		updated_at = NOW()`, nil); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGSandboxStore) CompleteRuntimeNodeLifecycleActionRecovery(
	ctx context.Context,
	poolID, token, ownerID string,
	recoveryEpoch int64,
	state string,
	proof any,
) error {
	poolID, token, ownerID = strings.TrimSpace(poolID), strings.TrimSpace(token), strings.TrimSpace(ownerID)
	state = strings.TrimSpace(state)
	proofPayload, err := marshalRuntimeNodeLifecycleProof(proof)
	if err != nil {
		return err
	}
	if poolID == "" || token == "" || ownerID == "" || recoveryEpoch <= 0 ||
		(state != "completed" && state != "abandoned") || proofPayload == nil {
		return fmt.Errorf("fenced runtime node lifecycle completion is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions
		SET state = $5,
			completed_at = COALESCE(completed_at, NOW()),
			convergence_proof = COALESCE(convergence_proof, $6::jsonb),
			updated_at = NOW()
		WHERE pool_id = $1
			AND lifecycle_action_token = $2
			AND state IN ('pending', 'draining')
			AND recovery_owner_id = $3
			AND recovery_epoch = $4
			AND recovery_lease_expires_at > NOW()
	`, poolID, token, ownerID, recoveryEpoch, state, proofPayload)
	if err != nil {
		return fmt.Errorf("complete fenced runtime node lifecycle action: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node lifecycle recovery lease is not current")
	}
	return nil
}

// TerminalizeRuntimeSlotsForProviderAbsentInstance retires only unclaimed,
// expired warm carriers. Claimed slots retain their normal RootFS writer and
// physical-cleanup recovery path even when the provider instance disappeared.
func (s *PGSandboxStore) TerminalizeRuntimeSlotsForProviderAbsentInstance(
	ctx context.Context,
	poolID, providerInstanceID, token, ownerID string,
	recoveryEpoch int64,
	proofDigest []byte,
) (int, error) {
	if len(proofDigest) != sha256.Size {
		return 0, fmt.Errorf("provider absence proof digest is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	action, err := updateFencedRuntimeNodeLifecycleAction(ctx, tx, poolID, token, ownerID, recoveryEpoch, `updated_at = updated_at`, nil)
	if err != nil {
		return 0, err
	}
	if action.ProviderActionAbsentSince == nil || action.ProviderInstanceAbsentSince == nil {
		return 0, fmt.Errorf("provider absence observations are required before retiring warm carriers")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.runtime_slots AS slot
		SET state = 'terminal',
			revision = slot.revision + 1,
			heartbeat_expires_at = LEAST(slot.heartbeat_expires_at, NOW()),
			carrier_retired = TRUE,
			terminal_reason = 'provider_instance_absent',
			terminal_proof_digest = $3,
			terminal_at = NOW(),
			updated_at = NOW()
		FROM manager.runtime_node_instances AS instance
		WHERE instance.pool_id = $1
			AND instance.provider_instance_id = $2
			AND instance.state <> 'revoked'
			AND slot.cluster_id = instance.cluster_id
			AND slot.node_id = instance.nomad_node_id
			AND slot.node_uid = instance.node_uid
			AND slot.state IN ('registered', 'fastpath_ready')
			AND slot.heartbeat_expires_at <= NOW()
			AND slot.claim_operation_id = ''
			AND slot.claim_id = ''
			AND slot.sandbox_id IS NULL
			AND slot.filesystem_id IS NULL
			AND slot.source_generation_id IS NULL
			AND slot.writer_grant_id IS NULL
			AND slot.resource_lease_id IS NULL
	`, poolID, providerInstanceID, proofDigest)
	if err != nil {
		return 0, fmt.Errorf("retire warm carriers for absent provider instance: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func updateFencedRuntimeNodeLifecycleAction(
	ctx context.Context,
	tx pgx.Tx,
	poolID, token, ownerID string,
	recoveryEpoch int64,
	assignments string,
	argument any,
) (*RuntimeNodeLifecycleAction, error) {
	poolID, token, ownerID = strings.TrimSpace(poolID), strings.TrimSpace(token), strings.TrimSpace(ownerID)
	if poolID == "" || token == "" || ownerID == "" || recoveryEpoch <= 0 {
		return nil, fmt.Errorf("runtime node lifecycle recovery fencing is invalid")
	}
	row := tx.QueryRow(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions
		SET `+assignments+`
		WHERE pool_id = $1
			AND lifecycle_action_token = $2
			AND state IN ('pending', 'draining')
			AND recovery_owner_id = $3
			AND recovery_epoch = $4
			AND recovery_lease_expires_at > NOW()
			RETURNING `+runtimeNodeLifecycleActionColumns,
		runtimeNodeLifecycleFencingArgs(poolID, token, ownerID, recoveryEpoch, argument)...)
	action, err := scanRuntimeNodeLifecycleAction(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("runtime node lifecycle recovery lease is not current")
		}
		return nil, fmt.Errorf("update fenced runtime node lifecycle action: %w", err)
	}
	if err := attachRuntimeNodeLifecycleInstances(ctx, tx, []*RuntimeNodeLifecycleAction{action}); err != nil {
		return nil, err
	}
	return action, nil
}

type runtimeNodeLifecycleInstanceQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func attachRuntimeNodeLifecycleInstances(
	ctx context.Context,
	queryer runtimeNodeLifecycleInstanceQueryer,
	actions []*RuntimeNodeLifecycleAction,
) error {
	for _, action := range actions {
		rows, err := queryer.Query(ctx, `
			SELECT provider_instance_id
			FROM manager.runtime_node_lifecycle_action_instances
			WHERE lifecycle_action_token = $1
			ORDER BY provider_instance_id
		`, action.Token)
		if err != nil {
			return fmt.Errorf("query runtime node lifecycle instances: %w", err)
		}
		action.ProviderInstanceIDs = nil
		for rows.Next() {
			var instanceID string
			if err := rows.Scan(&instanceID); err != nil {
				rows.Close()
				return fmt.Errorf("scan runtime node lifecycle instance: %w", err)
			}
			action.ProviderInstanceIDs = append(action.ProviderInstanceIDs, instanceID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("scan runtime node lifecycle instances: %w", err)
		}
		rows.Close()
		if len(action.ProviderInstanceIDs) == 0 {
			return fmt.Errorf("runtime node lifecycle action has no durable instance set")
		}
		if !slices.IsSorted(action.ProviderInstanceIDs) {
			return fmt.Errorf("runtime node lifecycle instance set is not canonical")
		}
	}
	return nil
}

func runtimeNodeLifecycleFencingArgs(
	poolID, token, ownerID string,
	recoveryEpoch int64,
	argument any,
) []any {
	args := []any{poolID, token, ownerID, recoveryEpoch}
	if argument != nil {
		args = append(args, argument)
	}
	return args
}
