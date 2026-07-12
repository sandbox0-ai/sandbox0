package clickhouse

import (
	"strconv"
	"strings"
	"testing"
)

func TestValidateCanonicalAuditEventTableMetadataAcceptsCanonicalSchema(t *testing.T) {
	table, columns := canonicalAuditTestMetadata(90)
	if err := validateCanonicalAuditEventTableMetadata(table, columns, 90); err != nil {
		t.Fatalf("validateCanonicalAuditEventTableMetadata() error = %v", err)
	}
}

func TestValidateCanonicalAuditEventTableMetadataRejectsWrongTableSemantics(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*auditTableMetadata)
		wantError string
	}{
		{
			name: "engine version",
			mutate: func(table *auditTableMetadata) {
				table.EngineFull = "ReplacingMergeTree(other_version) PARTITION BY toYYYYMM(occurred_at)"
			},
			wantError: "engine_full",
		},
		{
			name: "sorting key",
			mutate: func(table *auditTableMetadata) {
				table.SortingKey = "team_id, sandbox_id, occurred_at, event_id"
			},
			wantError: "sorting key",
		},
		{
			name: "partition key",
			mutate: func(table *auditTableMetadata) {
				table.PartitionKey = "toYYYYMM(ingested_at)"
			},
			wantError: "partition key",
		},
		{
			name: "retention TTL",
			mutate: func(table *auditTableMetadata) {
				table.CreateTableQuery = canonicalAuditCreateTableQuery(7)
			},
			wantError: "table TTL",
		},
		{
			name: "missing TTL",
			mutate: func(table *auditTableMetadata) {
				table.CreateTableQuery = "CREATE TABLE audit (event_id String) ENGINE = ReplacingMergeTree(version) ORDER BY event_id"
			},
			wantError: "TTL is missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, columns := canonicalAuditTestMetadata(90)
			tt.mutate(&table)
			err := validateCanonicalAuditEventTableMetadata(table, columns, 90)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateCanonicalAuditEventTableMetadata() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateCanonicalAuditEventTableMetadataRejectsWrongColumnSemantics(t *testing.T) {
	tests := []struct {
		name      string
		column    string
		mutate    func(*auditColumnMetadata)
		wantError string
	}{
		{
			name:   "timestamp precision",
			column: "occurred_at",
			mutate: func(column *auditColumnMetadata) {
				column.Type = "DateTime('UTC')"
			},
			wantError: "type",
		},
		{
			name:   "version kind",
			column: "version",
			mutate: func(column *auditColumnMetadata) {
				column.DefaultKind = "DEFAULT"
			},
			wantError: "default kind",
		},
		{
			name:   "version expression",
			column: "version",
			mutate: func(column *auditColumnMetadata) {
				column.DefaultExpression = "toUnixTimestamp(ingested_at)"
			},
			wantError: "default expression",
		},
		{
			name:   "unexpected ordinary default",
			column: "event_id",
			mutate: func(column *auditColumnMetadata) {
				column.DefaultKind = "DEFAULT"
				column.DefaultExpression = "generateUUIDv4()"
			},
			wantError: "default kind",
		},
		{
			name:   "unknown extra column",
			column: "",
			mutate: func(*auditColumnMetadata) {
			},
			wantError: "unexpected column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, columns := canonicalAuditTestMetadata(90)
			if tt.column == "" {
				columns = append(columns, auditColumnMetadata{Name: "extra", Type: "String"})
			} else {
				for i := range columns {
					if columns[i].Name == tt.column {
						tt.mutate(&columns[i])
						break
					}
				}
			}
			err := validateCanonicalAuditEventTableMetadata(table, columns, 90)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateCanonicalAuditEventTableMetadata() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateCanonicalAuditTTLAcceptsClickHouseCanonicalInterval(t *testing.T) {
	query := "CREATE TABLE ttl_database.audit_ttl_events (event_id String) ENGINE = ReplacingMergeTree(version) ORDER BY event_id TTL toDateTime(ingested_at) + toIntervalDay(90) SETTINGS index_granularity = 8192"
	if err := validateCanonicalAuditTTL(query, 90); err != nil {
		t.Fatalf("validateCanonicalAuditTTL() error = %v", err)
	}
}

func canonicalAuditTestMetadata(retentionDays int) (auditTableMetadata, []auditColumnMetadata) {
	columns := append([]auditColumnMetadata(nil), canonicalAuditColumns...)
	return auditTableMetadata{
		Engine:           "ReplacingMergeTree",
		EngineFull:       "ReplacingMergeTree(version) PARTITION BY toYYYYMM(occurred_at) ORDER BY (team_id, sandbox_id, occurred_at, event_id, payload_hash)",
		SortingKey:       "team_id, sandbox_id, occurred_at, event_id, payload_hash",
		PartitionKey:     "toYYYYMM(occurred_at)",
		CreateTableQuery: canonicalAuditCreateTableQuery(retentionDays),
	}, columns
}

func canonicalAuditCreateTableQuery(retentionDays int) string {
	return "CREATE TABLE audit (event_id String) ENGINE = ReplacingMergeTree(version) " +
		"PARTITION BY toYYYYMM(occurred_at) ORDER BY (team_id, sandbox_id, occurred_at, event_id, payload_hash) " +
		"TTL toDateTime(ingested_at) + INTERVAL " + strconv.Itoa(retentionDays) + " DAY DELETE " +
		"SETTINGS index_granularity = 8192"
}
