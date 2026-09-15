package clickhouse

import (
	"context"
	"fmt"
	"strings"
)

type windowCandidateSet struct {
	predicate string
	args      []any
	observed  map[windowLookupKey]struct{}
}

// windowCandidates bounds FINAL to primary keys with at least one raw version
// matching the page. FINAL still reads ALL versions of those keys, preserving
// team changes, stale replays and legacy version formats. A new canonical
// version absent from this snapshot fails the page rather than advancing its
// cursor. Incomplete or oversized candidate sets use the original FINAL scan.
func (r *Repository) windowCandidates(ctx context.Context, where string, args []any, cursor *pageCursor) (*windowCandidateSet, error) {
	if cursor == nil {
		return nil, nil
	}
	const maxCandidates = 1024
	const maxPredicateBytes = 128 << 10
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT DISTINCT recorded_at, region_id, producer, window_id, version
FROM %s
%s AND recorded_at >= %s
LIMIT %d
`, qualified(r.cfg.Database, r.cfg.WindowsTable), where, dateTime64NanoPlaceholder, maxCandidates+1),
		append(append([]any(nil), args...), dateTime64NanoArg(cursor.RecordedAt))...)
	if err != nil {
		return nil, fmt.Errorf("query usage window candidates: %w", err)
	}
	defer rows.Close()
	result := &windowCandidateSet{observed: make(map[windowLookupKey]struct{})}
	identities := make(map[windowLookupIdentity]struct{})
	var predicates []string
	bytes := 0
	for rows.Next() {
		var key windowLookupKey
		if err := rows.Scan(&key.RecordedAt, &key.RegionID, &key.Producer, &key.WindowID, &key.Version); err != nil {
			return nil, fmt.Errorf("scan usage window candidates: %w", err)
		}
		if len(result.observed) == maxCandidates {
			return nil, nil
		}
		key.RecordedAt = key.RecordedAt.UTC()
		result.observed[key] = struct{}{}
		if _, exists := identities[key.identity()]; exists {
			continue
		}
		// Budget conservatively for quoted/escaped values and tuple syntax.
		bytes += 4*(len(key.RegionID)+len(key.Producer)+len(key.WindowID)) + 32
		if bytes > maxPredicateBytes {
			return nil, nil
		}
		identities[key.identity()] = struct{}{}
		predicates = append(predicates, "(?, ?, ?)")
		result.args = append(result.args, key.RegionID, key.Producer, key.WindowID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage window candidates: %w", err)
	}
	result.predicate = "0"
	if len(predicates) != 0 {
		result.predicate = "(region_id, producer, window_id) IN (" + strings.Join(predicates, ", ") + ")"
	}
	return result, nil
}
