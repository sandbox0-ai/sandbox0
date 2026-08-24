package sandboxstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
)

func stageNomadSandboxNetworkMutationBindings(
	ctx context.Context,
	tx pgx.Tx,
	operationID, teamID, sandboxID string,
	bindings []egressauthstore.CredentialBinding,
) error {
	resolved, err := egressauthstore.ResolveCurrentBindingsTx(ctx, tx, teamID, bindings)
	if err != nil {
		return fmt.Errorf("resolve pending Nomad credential bindings: %w", err)
	}
	for _, binding := range resolved {
		projection, err := json.Marshal(binding.Projection)
		if err != nil {
			return fmt.Errorf("marshal pending credential projection %q: %w", binding.Ref, err)
		}
		cachePolicy, err := json.Marshal(binding.CachePolicy)
		if err != nil {
			return fmt.Errorf("marshal pending credential cache policy %q: %w", binding.Ref, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.sandbox_network_mutation_bindings (
				operation_id, sandbox_id, team_id, ref, source_ref, source_id,
				projection, cache_policy
			) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
		`, operationID, sandboxID, teamID, binding.Ref, binding.SourceRef,
			binding.SourceID, string(projection), string(cachePolicy)); err != nil {
			return fmt.Errorf("stage pending credential binding %q: %w", binding.Ref, err)
		}
	}
	return nil
}

func loadNomadSandboxNetworkMutationBindings(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
) ([]egressauthstore.CredentialBinding, error) {
	rows, err := tx.Query(ctx, `
		SELECT ref, source_ref, source_id, projection, cache_policy
		FROM manager.sandbox_network_mutation_bindings
		WHERE operation_id = $1
		ORDER BY ref
	`, operationID)
	if err != nil {
		return nil, fmt.Errorf("load pending Nomad credential bindings: %w", err)
	}
	defer rows.Close()
	bindings := make([]egressauthstore.CredentialBinding, 0)
	for rows.Next() {
		var binding egressauthstore.CredentialBinding
		var projection, cachePolicy []byte
		if err := rows.Scan(&binding.Ref, &binding.SourceRef, &binding.SourceID,
			&projection, &cachePolicy); err != nil {
			return nil, fmt.Errorf("scan pending Nomad credential binding: %w", err)
		}
		if err := json.Unmarshal(projection, &binding.Projection); err != nil {
			return nil, fmt.Errorf("decode pending credential projection %q: %w", binding.Ref, err)
		}
		if len(cachePolicy) > 0 && string(cachePolicy) != "null" {
			if err := json.Unmarshal(cachePolicy, &binding.CachePolicy); err != nil {
				return nil, fmt.Errorf("decode pending credential cache policy %q: %w", binding.Ref, err)
			}
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending Nomad credential bindings: %w", err)
	}
	return bindings, nil
}

func publishNomadSandboxNetworkMutationBindings(
	ctx context.Context,
	tx pgx.Tx,
	mutation *NomadSandboxNetworkMutation,
) error {
	if mutation == nil {
		return fmt.Errorf("nomad sandbox network mutation is required")
	}
	pending, err := loadNomadSandboxNetworkMutationBindings(ctx, tx, mutation.OperationID)
	if err != nil {
		return err
	}
	if digest := credentialbinding.DigestStore(pending); digest != mutation.CredentialBindingDigest {
		return fmt.Errorf("%w: pending credential binding digest changed",
			ErrNomadSandboxNetworkMutationConflict)
	}
	materialized, err := egressauthstore.ReplaceCurrentBindingsTx(
		ctx, tx, mutation.TeamID, mutation.SandboxID, pending, time.Time{},
	)
	if err != nil {
		return fmt.Errorf("publish Nomad credential bindings: %w", err)
	}
	if len(materialized) != len(pending) {
		return fmt.Errorf("%w: pending credential binding count changed",
			ErrNomadSandboxNetworkMutationConflict)
	}
	for index := range materialized {
		if materialized[index].SourceID != pending[index].SourceID {
			return fmt.Errorf("%w: pending credential source identity changed",
				ErrNomadSandboxNetworkMutationConflict)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET credential_binding_digest = $2
		WHERE sandbox_id = $1 AND phase = $3
	`, mutation.SandboxID, mutation.CredentialBindingDigest, SandboxRuntimeClaimPhaseReady)
	if err != nil {
		return fmt.Errorf("publish Nomad credential binding digest: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: credential binding claim authority changed",
			ErrNomadSandboxNetworkMutationConflict)
	}
	return nil
}

func deleteNomadSandboxNetworkMutationBindings(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.sandbox_network_mutation_bindings
		WHERE operation_id = $1
	`, operationID); err != nil {
		return fmt.Errorf("delete pending Nomad credential bindings: %w", err)
	}
	return nil
}

func deleteNomadSandboxNetworkMutationBindingsForSandbox(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID string,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.sandbox_network_mutation_bindings
		WHERE sandbox_id = $1
	`, sandboxID); err != nil {
		return fmt.Errorf("delete superseded Nomad credential bindings: %w", err)
	}
	return nil
}
