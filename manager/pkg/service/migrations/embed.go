package migrations

import "embed"

// FS contains manager service database migrations.
//
//go:embed *.sql
var FS embed.FS
