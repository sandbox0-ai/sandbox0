package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type ledgerStatus struct {
	sequence int64
	cursor   string
}

type ledgerStatusRow struct {
	sequence   int64
	recordedAt time.Time
	region     string
	producer   string
	id         string
	version    uint64
}

// readLedgerStatus discovers extremal raw rows using only the sequence and
// timestamp columns, then resolves ALL versions of their primary keys. Raw
// maxima are upper bounds, never authoritative status: stale replays and
// historical version encodings can make their rows lose under FINAL.
func (r *Repository) readLedgerStatus(ctx context.Context, table, idColumn string) (*ledgerStatus, error) {
	var count uint64
	var sequence int64
	var recordedAt time.Time
	if err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT count(), max(sequence), max(recorded_at)
FROM %s
SETTINGS max_threads = 1
`, table)).Scan(&count, &sequence, &recordedAt); err != nil {
		return nil, fmt.Errorf("query ledger status bounds: %w", err)
	}
	if count == 0 {
		return &ledgerStatus{}, nil
	}
	status, err := r.ledgerStatusCandidates(ctx, table, idColumn, sequence, recordedAt.UTC())
	if err != nil || status != nil {
		return status, err
	}
	return r.scanLedgerStatus(ctx, table, idColumn)
}

// ledgerStatusCandidates returns nil when completeness cannot be proved within
// the bounded candidate set. The fallback is one canonical aggregate scan,
// rather than separate whole-ledger sequence and cursor scans.
func (r *Repository) ledgerStatusCandidates(ctx context.Context, table, idColumn string, sequence int64, recordedAt time.Time) (*ledgerStatus, error) {
	const maxCandidates = 1024
	const maxPredicateBytes = 128 << 10
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT DISTINCT sequence, recorded_at, region_id, producer, %s, version
FROM %s
PREWHERE sequence >= ? OR recorded_at >= %s
LIMIT %d
SETTINGS max_threads = 1
`, idColumn, table, dateTime64NanoPlaceholder, maxCandidates+1), sequence, dateTime64NanoArg(recordedAt))
	if err != nil {
		return nil, fmt.Errorf("query ledger status candidates: %w", err)
	}
	observed := make(map[ledgerStatusRow]struct{})
	identities := make(map[windowLookupIdentity]struct{})
	var predicates []string
	var args []any
	bytes := 0
	for rows.Next() {
		var row ledgerStatusRow
		if err := rows.Scan(&row.sequence, &row.recordedAt, &row.region, &row.producer, &row.id, &row.version); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan ledger status candidate: %w", err)
		}
		if len(observed) == maxCandidates {
			_ = rows.Close()
			return nil, nil
		}
		row.recordedAt = row.recordedAt.UTC()
		observed[row] = struct{}{}
		identity := windowLookupIdentity{RegionID: row.region, Producer: row.producer, WindowID: row.id}
		if _, exists := identities[identity]; exists {
			continue
		}
		bytes += 4*(len(row.region)+len(row.producer)+len(row.id)) + 32
		if bytes > maxPredicateBytes {
			_ = rows.Close()
			return nil, nil
		}
		identities[identity] = struct{}{}
		predicates = append(predicates, "(?, ?, ?)")
		args = append(args, row.region, row.producer, row.id)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, fmt.Errorf("iterate ledger status candidates: %w", err)
	}
	if len(predicates) == 0 {
		return nil, nil
	}
	rows, err = r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT sequence, recorded_at, region_id, producer, %s, version
FROM %s FINAL
WHERE (region_id, producer, %s) IN (%s)
SETTINGS max_threads = 1
`, idColumn, table, idColumn, strings.Join(predicates, ", ")), args...)
	if err != nil {
		return nil, fmt.Errorf("query canonical ledger status candidates: %w", err)
	}
	defer rows.Close()
	var latest ledgerStatusRow
	var maxSequence int64
	count := 0
	for rows.Next() {
		var row ledgerStatusRow
		if err := rows.Scan(&row.sequence, &row.recordedAt, &row.region, &row.producer, &row.id, &row.version); err != nil {
			return nil, fmt.Errorf("scan canonical ledger status candidate: %w", err)
		}
		row.recordedAt = row.recordedAt.UTC()
		// A replacement outside the observed version set must not advance the
		// status cursor past another, unseen winner.
		if _, exists := observed[row]; !exists {
			return nil, nil
		}
		if count == 0 || row.sequence > maxSequence {
			maxSequence = row.sequence
		}
		if count == 0 || ledgerCursorAfter(row, latest) {
			latest = row
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate canonical ledger status candidates: %w", err)
	}
	if count == 0 || maxSequence < sequence || latest.recordedAt.Before(recordedAt) {
		return nil, nil
	}
	cursor, err := encodeCursor(latest.recordedAt, latest.producer, latest.id)
	if err != nil {
		return nil, err
	}
	return &ledgerStatus{sequence: maxSequence, cursor: cursor}, nil
}

func ledgerCursorAfter(a, b ledgerStatusRow) bool {
	if !a.recordedAt.Equal(b.recordedAt) {
		return a.recordedAt.After(b.recordedAt)
	}
	if a.producer != b.producer {
		return a.producer > b.producer
	}
	return a.id > b.id
}

func (r *Repository) scanLedgerStatus(ctx context.Context, table, idColumn string) (*ledgerStatus, error) {
	var count uint64
	var sequence int64
	var recordedAt time.Time
	var producer, id string
	err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT count(), max(sequence),
    tupleElement(max(tuple(recorded_at, producer, %s)), 1),
    tupleElement(max(tuple(recorded_at, producer, %s)), 2),
    tupleElement(max(tuple(recorded_at, producer, %s)), 3)
FROM %s FINAL
SETTINGS max_threads = 1
`, idColumn, idColumn, idColumn, table)).Scan(&count, &sequence, &recordedAt, &producer, &id)
	if err != nil {
		return nil, fmt.Errorf("query canonical ledger status: %w", err)
	}
	if count == 0 {
		return &ledgerStatus{}, nil
	}
	cursor, err := encodeCursor(recordedAt.UTC(), producer, id)
	if err != nil {
		return nil, err
	}
	return &ledgerStatus{sequence: sequence, cursor: cursor}, nil
}
