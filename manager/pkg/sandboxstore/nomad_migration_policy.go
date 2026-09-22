package sandboxstore

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func validateMigrationSourcePolicy(assignment runtimecontrol.MigrationAssignment, policy string) error {
	if nomadmigration.ValidateSourcePolicy(assignment, policy) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}

// migrationSourcePolicyDigest binds new target attachment to the source policy
// captured before its API gate. It never rebuilds policy from a later config.
func migrationSourcePolicyDigest(ctx context.Context, tx pgx.Tx, assignment runtimecontrol.MigrationAssignment) (string, error) {
	var policy, digest *string
	if err := tx.QueryRow(ctx, `SELECT source_network_policy,source_network_policy_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&policy, &digest); err != nil {
		return "", err
	}
	if policy == nil || digest == nil || validateMigrationSourcePolicy(assignment, *policy) != nil || protocol.NetworkPolicyDigest(*policy) != *digest {
		return "", ErrNomadSandboxMigrationConflict
	}
	return *digest, nil
}
