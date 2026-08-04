package migrations

import "embed"

// FS contains manager sandbox-store database migrations.
//
//go:embed *.sql
var FS embed.FS
