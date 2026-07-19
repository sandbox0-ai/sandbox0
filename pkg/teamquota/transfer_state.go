package teamquota

import (
	"context"
	"fmt"
	"strings"
)

// TransferStates returns durable states for the requested operation IDs.
func (r *Repository) TransferStates(
	ctx context.Context,
	teamID string,
	operationIDs []string,
) (map[string]string, error) {
	if r == nil || r.pool == nil {
		return nil, &UnavailableError{
			Operation: "load team quota transfer states",
			Err:       fmt.Errorf("database pool is not configured"),
		}
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("team id is required")
	}
	normalized := make([]string, 0, len(operationIDs))
	seen := make(map[string]struct{}, len(operationIDs))
	for _, operationID := range operationIDs {
		operationID = strings.TrimSpace(operationID)
		if operationID == "" {
			return nil, fmt.Errorf("transfer operation id is required")
		}
		if _, ok := seen[operationID]; ok {
			continue
		}
		seen[operationID] = struct{}{}
		normalized = append(normalized, operationID)
	}
	states := make(map[string]string, len(normalized))
	if len(normalized) == 0 {
		return states, nil
	}
	rows, err := r.pool.Query(ctx, `
		SELECT operation_id, state
		FROM quota.transfer_operations
		WHERE team_id = $1 AND operation_id = ANY($2)
	`, teamID, normalized)
	if err != nil {
		return nil, &UnavailableError{Operation: "load team quota transfer states", Err: err}
	}
	defer rows.Close()
	for rows.Next() {
		var operationID, state string
		if err := rows.Scan(&operationID, &state); err != nil {
			return nil, &UnavailableError{Operation: "scan team quota transfer state", Err: err}
		}
		states[operationID] = state
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{Operation: "iterate team quota transfer states", Err: err}
	}
	return states, nil
}
