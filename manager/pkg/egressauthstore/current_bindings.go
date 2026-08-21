package egressauthstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// MaxCredentialBindingsPerSandbox bounds source locks and transactional row
// replacement for one sandbox.
const MaxCredentialBindingsPerSandbox = 256

type bindingQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// GetCurrentBindings reads the single current credential-binding projection
// for a sandbox. Source versions may advance independently during rotation.
func GetCurrentBindings(ctx context.Context, exec bindingQuerier, teamID, sandboxID string) (*BindingRecord, error) {
	if exec == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if err := validateBindingOwner(teamID, sandboxID); err != nil {
		return nil, err
	}
	var (
		record       BindingRecord
		bindingsJSON []byte
	)
	err := exec.QueryRow(ctx, `
		SELECT
			sandbox_id,
			team_id,
			COALESCE(
				jsonb_agg(
					jsonb_build_object(
						'ref', ref,
						'sourceRef', source_ref,
						'sourceId', source_id,
						'sourceVersion', source_version,
						'projection', projection,
						'cachePolicy', cache_policy
					) ORDER BY ref
				),
				'[]'::jsonb
			) AS bindings,
			MAX(updated_at) AS updated_at
		FROM sandbox_egress_credential_bindings
		WHERE team_id = $1 AND sandbox_id = $2
		GROUP BY team_id, sandbox_id
	`, teamID, sandboxID).Scan(
		&record.SandboxID, &record.TeamID, &bindingsJSON, &record.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get bindings: %w", err)
	}
	if len(bindingsJSON) > 0 {
		if err := json.Unmarshal(bindingsJSON, &record.Bindings); err != nil {
			return nil, fmt.Errorf("unmarshal bindings: %w", err)
		}
	}
	return &record, nil
}

// ReplaceCurrentBindingsTx resolves and locks every referenced source before
// atomically replacing a sandbox's current bindings. Locking sources in name
// order serializes this path with source rotation and prevents publishing a
// binding at an already-obsolete source version.
func ReplaceCurrentBindingsTx(
	ctx context.Context,
	tx pgx.Tx,
	teamID, sandboxID string,
	bindings []CredentialBinding,
	updatedAt time.Time,
) ([]CredentialBinding, error) {
	if tx == nil {
		return nil, fmt.Errorf("binding transaction is not configured")
	}
	if err := validateBindingOwner(teamID, sandboxID); err != nil {
		return nil, err
	}
	materialized, err := ResolveCurrentBindingsTx(ctx, tx, teamID, bindings)
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE team_id = $1 AND sandbox_id = $2
	`, teamID, sandboxID); err != nil {
		return nil, fmt.Errorf("delete existing bindings: %w", err)
	}
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	for _, binding := range materialized {
		projectionJSON, err := json.Marshal(binding.Projection)
		if err != nil {
			return nil, fmt.Errorf("marshal projection for %q: %w", binding.Ref, err)
		}
		cachePolicyJSON, err := json.Marshal(binding.CachePolicy)
		if err != nil {
			return nil, fmt.Errorf("marshal cache policy for %q: %w", binding.Ref, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sandbox_egress_credential_bindings (
				team_id, sandbox_id, ref, source_ref, source_id, source_version,
				projection, cache_policy, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
		`, teamID, sandboxID, binding.Ref, binding.SourceRef, binding.SourceID,
			binding.SourceVersion, string(projectionJSON), string(cachePolicyJSON), updatedAt); err != nil {
			return nil, fmt.Errorf("insert binding %q: %w", binding.Ref, err)
		}
	}
	return materialized, nil
}

// ResolveCurrentBindingsTx locks every referenced source in deterministic
// order and attaches its current ID/version without publishing binding rows.
func ResolveCurrentBindingsTx(
	ctx context.Context,
	tx pgx.Tx,
	teamID string,
	bindings []CredentialBinding,
) ([]CredentialBinding, error) {
	if tx == nil {
		return nil, fmt.Errorf("binding transaction is not configured")
	}
	if teamID == "" || teamID != strings.TrimSpace(teamID) || len(teamID) > 512 {
		return nil, fmt.Errorf("team_id is required, canonical, and at most 512 bytes")
	}
	normalized, sourceRefs, err := normalizeSemanticBindings(bindings)
	if err != nil {
		return nil, err
	}
	sources := make(map[string]CredentialSource, len(sourceRefs))
	if len(sourceRefs) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT id, team_id, name, resolver_kind, current_version, status, created_at, updated_at
			FROM credential_sources
			WHERE team_id = $1 AND name = ANY($2::text[])
			ORDER BY name
			FOR SHARE
		`, teamID, sourceRefs)
		if err != nil {
			return nil, fmt.Errorf("lock credential sources: %w", err)
		}
		for rows.Next() {
			var source CredentialSource
			if err := rows.Scan(&source.ID, &source.TeamID, &source.Name, &source.ResolverKind,
				&source.CurrentVersion, &source.Status, &source.CreatedAt, &source.UpdatedAt); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan locked credential source: %w", err)
			}
			sources[source.Name] = source
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate locked credential sources: %w", err)
		}
		rows.Close()
		if len(sources) != len(sourceRefs) {
			for _, ref := range sourceRefs {
				if _, ok := sources[ref]; !ok {
					return nil, fmt.Errorf("credential source %q not found", ref)
				}
			}
		}
	}
	for index := range normalized {
		source := sources[normalized[index].SourceRef]
		normalized[index].SourceID = source.ID
		normalized[index].SourceVersion = source.CurrentVersion
	}
	return normalized, nil
}

// DeleteCurrentBindingsTx removes current materialization while retaining the
// logical sandbox cleanup transaction that fences retries.
func DeleteCurrentBindingsTx(ctx context.Context, tx pgx.Tx, teamID, sandboxID string) error {
	if tx == nil {
		return fmt.Errorf("binding transaction is not configured")
	}
	if err := validateBindingOwner(teamID, sandboxID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE team_id = $1 AND sandbox_id = $2
	`, teamID, sandboxID); err != nil {
		return fmt.Errorf("delete bindings: %w", err)
	}
	return nil
}

func normalizeSemanticBindings(bindings []CredentialBinding) ([]CredentialBinding, []string, error) {
	if len(bindings) > MaxCredentialBindingsPerSandbox {
		return nil, nil, fmt.Errorf("credential binding count exceeds %d", MaxCredentialBindingsPerSandbox)
	}
	normalized := make([]CredentialBinding, len(bindings))
	copy(normalized, bindings)
	seenRefs := make(map[string]struct{}, len(normalized))
	seenSources := make(map[string]struct{}, len(normalized))
	sourceRefs := make([]string, 0, len(normalized))
	for index := range normalized {
		binding := &normalized[index]
		if binding.Ref == "" || binding.Ref != strings.TrimSpace(binding.Ref) || len(binding.Ref) > 512 {
			return nil, nil, fmt.Errorf("credential binding ref is required, canonical, and at most 512 bytes")
		}
		if binding.SourceRef == "" || binding.SourceRef != strings.TrimSpace(binding.SourceRef) || len(binding.SourceRef) > 512 {
			return nil, nil, fmt.Errorf("credential source ref for %q is required, canonical, and at most 512 bytes", binding.Ref)
		}
		if _, ok := seenRefs[binding.Ref]; ok {
			return nil, nil, fmt.Errorf("duplicate credential binding ref %q", binding.Ref)
		}
		seenRefs[binding.Ref] = struct{}{}
		binding.SourceID = 0
		binding.SourceVersion = 0
		if _, ok := seenSources[binding.SourceRef]; !ok {
			seenSources[binding.SourceRef] = struct{}{}
			sourceRefs = append(sourceRefs, binding.SourceRef)
		}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Ref < normalized[j].Ref })
	sort.Strings(sourceRefs)
	return normalized, sourceRefs, nil
}

func validateBindingOwner(teamID, sandboxID string) error {
	if teamID == "" || teamID != strings.TrimSpace(teamID) || len(teamID) > 512 {
		return fmt.Errorf("team_id is required, canonical, and at most 512 bytes")
	}
	if sandboxID == "" || sandboxID != strings.TrimSpace(sandboxID) || len(sandboxID) > 512 {
		return fmt.Errorf("sandbox_id is required, canonical, and at most 512 bytes")
	}
	return nil
}
