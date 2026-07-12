package migrations

import "embed"

// FS contains the PostgreSQL metering outbox migrations.
//
//go:embed *.sql
var FS embed.FS
