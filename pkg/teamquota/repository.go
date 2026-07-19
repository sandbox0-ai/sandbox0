package teamquota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultPolicyLockID int64 = 0x7465616d71756f74

// Repository stores effective policies and capacity allocations in the
// region's shared PostgreSQL database.
type Repository struct {
	pool *pgxpool.Pool
}

var _ PolicyReader = (*Repository)(nil)

// NewRepository returns a PostgreSQL team quota repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	if pool == nil {
		return nil
	}
	return &Repository{pool: pool}
}

// Pool returns the shared region database pool.
func (r *Repository) Pool() *pgxpool.Pool {
	if r == nil {
		return nil
	}
	return r.pool
}

func (r *Repository) inTx(ctx context.Context, operation string, fn func(pgx.Tx) error) error {
	if r == nil || r.pool == nil {
		return &UnavailableError{Operation: operation, Err: fmt.Errorf("database pool is not configured")}
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return &UnavailableError{Operation: operation, Err: fmt.Errorf("begin transaction: %w", err)}
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return &UnavailableError{Operation: operation, Err: fmt.Errorf("commit transaction: %w", err)}
	}
	return nil
}

func lockTeam(ctx context.Context, tx pgx.Tx, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO quota.team_states (team_id)
		VALUES ($1)
		ON CONFLICT (team_id) DO NOTHING
	`, teamID); err != nil {
		return &UnavailableError{Operation: "lock team quota", Err: err}
	}
	var (
		revision          int64
		admissionDisabled bool
		deleted           bool
	)
	if err := tx.QueryRow(ctx, `
		SELECT revision, admission_disabled, deleted_at IS NOT NULL
		FROM quota.team_states
		WHERE team_id = $1
		FOR UPDATE
	`, teamID).Scan(&revision, &admissionDisabled, &deleted); err != nil {
		return &UnavailableError{Operation: "lock team quota", Err: err}
	}
	if admissionDisabled || deleted {
		return &UnavailableError{
			Operation: "lock team quota",
			Err:       &TeamAdmissionDisabledError{TeamID: teamID},
		}
	}
	return nil
}

// UnsafeReplaceDefaultPoliciesForTest seeds region defaults without publishing
// a distributed policy guard. It exists only for isolated repository fixtures;
// production policy writes must use PolicyManager.
func (r *Repository) UnsafeReplaceDefaultPoliciesForTest(ctx context.Context, policies []Policy) error {
	normalized, err := normalizeDefaultPolicies(policies)
	if err != nil {
		return err
	}
	return r.inTx(ctx, "replace default policies", func(tx pgx.Tx) error {
		return r.replaceDefaultPoliciesTx(ctx, tx, normalized)
	})
}

func (r *Repository) replaceDefaultPoliciesTx(ctx context.Context, tx pgx.Tx, policies []Policy) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	normalized, err := normalizeDefaultPolicies(policies)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, defaultPolicyLockID); err != nil {
		return &UnavailableError{Operation: "lock default policies", Err: err}
	}
	existing, err := listDefaultPoliciesTx(ctx, tx)
	if err != nil {
		return err
	}
	wanted := make(map[Key]Policy, len(normalized))
	for _, policy := range normalized {
		wanted[policy.Key] = policy
		current, ok := existing[policy.Key]
		if ok && policyContentEqual(current, policy) {
			continue
		}
		if err := upsertPolicyTx(ctx, tx, "quota.region_default_policies", "", policy); err != nil {
			return err
		}
	}
	for key := range existing {
		if _, ok := wanted[key]; ok {
			continue
		}
		if _, err := tx.Exec(ctx, `DELETE FROM quota.region_default_policies WHERE quota_key = $1`, string(key)); err != nil {
			return &UnavailableError{Operation: "delete default policy", Err: err}
		}
	}
	return nil
}

func normalizeDefaultPolicies(policies []Policy) ([]Policy, error) {
	normalized := make([]Policy, 0, len(policies))
	seen := make(map[Key]struct{}, len(policies))
	for _, policy := range policies {
		policy.TeamID = ""
		policy.Revision = 0
		if err := policy.Validate(); err != nil {
			return nil, err
		}
		if _, ok := seen[policy.Key]; ok {
			return nil, fmt.Errorf("duplicate default team quota policy %q", policy.Key)
		}
		seen[policy.Key] = struct{}{}
		normalized = append(normalized, policy)
	}
	if len(seen) != len(keyKinds) {
		missing := make([]string, 0, len(keyKinds)-len(seen))
		for _, key := range Keys() {
			if _, ok := seen[key]; !ok {
				missing = append(missing, string(key))
			}
		}
		return nil, fmt.Errorf("default team quota policies must include every known key; missing: %s", strings.Join(missing, ", "))
	}
	return normalized, nil
}

// UnsafePutTeamPolicyForTest seeds a team override without publishing a
// distributed policy guard. Production policy writes must use PolicyManager.
func (r *Repository) UnsafePutTeamPolicyForTest(ctx context.Context, teamID string, policy Policy) error {
	return r.inTx(ctx, "put team policy", func(tx pgx.Tx) error {
		return r.putTeamPolicyTx(ctx, tx, teamID, policy)
	})
}

func (r *Repository) putTeamPolicyTx(ctx context.Context, tx pgx.Tx, teamID string, policy Policy) error {
	teamID = strings.TrimSpace(teamID)
	policy.TeamID = teamID
	policy.Revision = 0
	if err := policy.Validate(); err != nil {
		return err
	}
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if err := lockTeam(ctx, tx, teamID); err != nil {
		return err
	}
	current, err := teamPolicyTx(ctx, tx, teamID, policy.Key)
	if err != nil {
		return err
	}
	if current != nil && policyContentEqual(*current, policy) {
		return nil
	}
	return upsertPolicyTx(ctx, tx, "quota.team_policies", teamID, policy)
}

// UnsafeDeleteTeamPolicyForTest removes a seeded team override without
// publishing a distributed policy guard. Production policy writes must use
// PolicyManager.
func (r *Repository) UnsafeDeleteTeamPolicyForTest(ctx context.Context, teamID string, key Key) error {
	return r.inTx(ctx, "delete team policy", func(tx pgx.Tx) error {
		return r.deleteTeamPolicyTx(ctx, tx, teamID, key)
	})
}

func (r *Repository) deleteTeamPolicyTx(ctx context.Context, tx pgx.Tx, teamID string, key Key) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if !KnownKey(key) {
		return fmt.Errorf("unknown team quota key %q", key)
	}
	if err := lockTeam(ctx, tx, teamID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM quota.team_policies
		WHERE team_id = $1 AND quota_key = $2
	`, teamID, string(key)); err != nil {
		return &UnavailableError{Operation: "delete team policy", Err: err}
	}
	return nil
}

// EffectivePolicy returns a team override or its region default.
func (r *Repository) EffectivePolicy(ctx context.Context, teamID string, key Key) (*Policy, error) {
	if r == nil || r.pool == nil {
		return nil, &UnavailableError{Operation: "get effective policy", Err: fmt.Errorf("database pool is not configured")}
	}
	return effectivePolicy(ctx, r.pool, teamID, key)
}

// EffectivePolicyTx returns an effective policy within a caller-owned transaction.
func (r *Repository) EffectivePolicyTx(ctx context.Context, tx pgx.Tx, teamID string, key Key) (*Policy, error) {
	if tx == nil {
		return nil, fmt.Errorf("team quota transaction is required")
	}
	return effectivePolicy(ctx, tx, teamID, key)
}

// ListStatus returns a consistent PostgreSQL snapshot of all effective policies
// and capacity allocations for a team.
func (r *Repository) ListStatus(ctx context.Context, teamID string) ([]Status, error) {
	var statuses []Status
	err := r.inTx(ctx, "list team quota status", func(tx pgx.Tx) error {
		var err error
		statuses, err = r.ListStatusTx(ctx, tx, teamID)
		return err
	})
	return statuses, err
}

// ListStatusTx returns team status inside a caller-owned transaction.
func (r *Repository) ListStatusTx(ctx context.Context, tx pgx.Tx, teamID string) ([]Status, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	disabled, err := teamAdmissionDisabled(ctx, tx, teamID)
	if err != nil {
		return nil, err
	}
	if disabled {
		return nil, &UnavailableError{
			Operation: "list team quota status",
			Err:       &TeamAdmissionDisabledError{TeamID: teamID},
		}
	}
	effectivePolicies, err := listEffectivePoliciesTx(ctx, tx, teamID)
	if err != nil {
		return nil, err
	}
	usage, err := listUsageTx(ctx, tx, teamID)
	if err != nil {
		return nil, err
	}
	statuses := make([]Status, 0, len(effectivePolicies))
	for _, effective := range effectivePolicies {
		policy := effective.Policy
		current := usage[policy.Key]
		status := Status{
			TeamID:    teamID,
			Key:       policy.Key,
			Kind:      policy.Kind,
			Unit:      UnitForKey(policy.Key),
			Source:    effective.Source,
			Policy:    policy,
			Committed: current.committed,
			Reserved:  current.reserved,
			Used:      current.committed + current.reserved,
		}
		if policy.Kind == KindCapacity {
			remaining := policy.Limit - status.Used
			if remaining < 0 {
				remaining = 0
			}
			status.Remaining = &remaining
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func effectivePolicy(ctx context.Context, query rowQuerier, teamID string, key Key) (*Policy, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if !KnownKey(key) {
		return nil, fmt.Errorf("unknown team quota key %q", key)
	}
	disabled, err := teamAdmissionDisabled(ctx, query, teamID)
	if err != nil {
		return nil, err
	}
	if disabled {
		return nil, &UnavailableError{
			Operation: "get effective policy",
			Err:       &TeamAdmissionDisabledError{TeamID: teamID},
		}
	}
	row := query.QueryRow(ctx, `
		SELECT team_id, quota_key, kind, revision,
			limit_value, rate_tokens, rate_interval_ms, rate_burst
		FROM (
			SELECT team_id, quota_key, kind, revision,
				limit_value, rate_tokens, rate_interval_ms, rate_burst, 0 AS priority
			FROM quota.team_policies
			WHERE team_id = $1 AND quota_key = $2
			UNION ALL
			SELECT '' AS team_id, quota_key, kind, revision,
				limit_value, rate_tokens, rate_interval_ms, rate_burst, 1 AS priority
			FROM quota.region_default_policies
			WHERE quota_key = $2
		) effective
		ORDER BY priority
		LIMIT 1
	`, teamID, string(key))
	policy, err := scanPolicy(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{Operation: "get effective policy", Err: err}
	}
	if policy.TeamID == "" {
		policy.TeamID = teamID
	}
	return policy, nil
}

func teamPolicyTx(ctx context.Context, tx pgx.Tx, teamID string, key Key) (*Policy, error) {
	policy, err := scanPolicy(tx.QueryRow(ctx, `
		SELECT team_id, quota_key, kind, revision,
			limit_value, rate_tokens, rate_interval_ms, rate_burst
		FROM quota.team_policies
		WHERE team_id = $1 AND quota_key = $2
	`, teamID, string(key)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, &UnavailableError{Operation: "get team policy", Err: err}
	}
	return policy, nil
}

func listDefaultPoliciesTx(ctx context.Context, tx pgx.Tx) (map[Key]Policy, error) {
	rows, err := tx.Query(ctx, `
		SELECT '' AS team_id, quota_key, kind, revision,
			limit_value, rate_tokens, rate_interval_ms, rate_burst
		FROM quota.region_default_policies
		ORDER BY quota_key
	`)
	if err != nil {
		return nil, &UnavailableError{Operation: "list default policies", Err: err}
	}
	defer rows.Close()
	policies := make(map[Key]Policy)
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			return nil, &UnavailableError{Operation: "scan default policy", Err: err}
		}
		policies[policy.Key] = *policy
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "list default policies", Err: err}
	}
	return policies, nil
}

type sourcedPolicy struct {
	Policy Policy
	Source PolicySource
}

func listEffectivePoliciesTx(ctx context.Context, tx pgx.Tx, teamID string) ([]sourcedPolicy, error) {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT ON (quota_key)
			team_id, quota_key, kind, revision,
			limit_value, rate_tokens, rate_interval_ms, rate_burst
		FROM (
			SELECT team_id, quota_key, kind, revision,
				limit_value, rate_tokens, rate_interval_ms, rate_burst, 0 AS priority
			FROM quota.team_policies
			WHERE team_id = $1
			UNION ALL
			SELECT '' AS team_id, quota_key, kind, revision,
				limit_value, rate_tokens, rate_interval_ms, rate_burst, 1 AS priority
			FROM quota.region_default_policies
		) effective
		ORDER BY quota_key, priority
	`, teamID)
	if err != nil {
		return nil, &UnavailableError{Operation: "list effective policies", Err: err}
	}
	defer rows.Close()
	var policies []sourcedPolicy
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			return nil, &UnavailableError{Operation: "scan effective policy", Err: err}
		}
		source := PolicySourceOverride
		if policy.TeamID == "" {
			source = PolicySourceDefault
		}
		policy.TeamID = teamID
		policies = append(policies, sourcedPolicy{
			Policy: *policy,
			Source: source,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "list effective policies", Err: err}
	}
	return policies, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanPolicy(row rowScanner) (*Policy, error) {
	var policy Policy
	var limit, tokens, interval, burst sql.NullInt64
	if err := row.Scan(
		&policy.TeamID, &policy.Key, &policy.Kind, &policy.Revision,
		&limit, &tokens, &interval, &burst,
	); err != nil {
		return nil, err
	}
	if limit.Valid {
		policy.Limit = limit.Int64
	}
	if tokens.Valid {
		policy.Tokens = tokens.Int64
	}
	if interval.Valid {
		policy.IntervalMillis = interval.Int64
	}
	if burst.Valid {
		policy.Burst = burst.Int64
	}
	return &policy, nil
}

func upsertPolicyTx(ctx context.Context, tx pgx.Tx, table, teamID string, policy Policy) error {
	limit, tokens, interval, burst := policyDatabaseValues(policy)
	var query string
	var args []any
	if table == "quota.team_policies" {
		query = `
			INSERT INTO quota.team_policies (
				team_id, quota_key, kind, revision,
				limit_value, rate_tokens, rate_interval_ms, rate_burst
			) VALUES ($1, $2, $3, nextval('quota.policy_revision_seq'), $4, $5, $6, $7)
			ON CONFLICT (team_id, quota_key) DO UPDATE
			SET kind = EXCLUDED.kind,
				revision = nextval('quota.policy_revision_seq'),
				limit_value = EXCLUDED.limit_value,
				rate_tokens = EXCLUDED.rate_tokens,
				rate_interval_ms = EXCLUDED.rate_interval_ms,
				rate_burst = EXCLUDED.rate_burst,
				updated_at = NOW()
		`
		args = []any{teamID, string(policy.Key), string(policy.Kind), limit, tokens, interval, burst}
	} else {
		query = `
			INSERT INTO quota.region_default_policies (
				quota_key, kind, revision,
				limit_value, rate_tokens, rate_interval_ms, rate_burst
			) VALUES ($1, $2, nextval('quota.policy_revision_seq'), $3, $4, $5, $6)
			ON CONFLICT (quota_key) DO UPDATE
			SET kind = EXCLUDED.kind,
				revision = nextval('quota.policy_revision_seq'),
				limit_value = EXCLUDED.limit_value,
				rate_tokens = EXCLUDED.rate_tokens,
				rate_interval_ms = EXCLUDED.rate_interval_ms,
				rate_burst = EXCLUDED.rate_burst,
				updated_at = NOW()
		`
		args = []any{string(policy.Key), string(policy.Kind), limit, tokens, interval, burst}
	}
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return &UnavailableError{Operation: "upsert team quota policy", Err: err}
	}
	return nil
}

func policyDatabaseValues(policy Policy) (limit, tokens, interval, burst any) {
	if policy.Kind == KindCapacity || policy.Kind == KindConcurrency {
		return policy.Limit, nil, nil, nil
	}
	return nil, policy.Tokens, policy.IntervalMillis, policy.Burst
}

func policyContentEqual(a, b Policy) bool {
	return a.Key == b.Key &&
		a.Kind == b.Kind &&
		a.Limit == b.Limit &&
		a.Tokens == b.Tokens &&
		a.IntervalMillis == b.IntervalMillis &&
		a.Burst == b.Burst
}

type usageValues struct {
	committed int64
	reserved  int64
}

func listUsageTx(ctx context.Context, tx pgx.Tx, teamID string) (map[Key]usageValues, error) {
	rows, err := tx.Query(ctx, `
		SELECT quota_key, committed_value, reserved_value
		FROM quota.team_usage
		WHERE team_id = $1
	`, teamID)
	if err != nil {
		return nil, &UnavailableError{Operation: "list team quota usage", Err: err}
	}
	defer rows.Close()
	usage := make(map[Key]usageValues)
	for rows.Next() {
		var key Key
		var value usageValues
		if err := rows.Scan(&key, &value.committed, &value.reserved); err != nil {
			return nil, &UnavailableError{Operation: "scan team quota usage", Err: err}
		}
		usage[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "list team quota usage", Err: err}
	}
	return usage, nil
}
