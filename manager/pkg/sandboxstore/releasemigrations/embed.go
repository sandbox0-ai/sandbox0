package releasemigrations

import "embed"

// FS contains the independently versioned runtime release admission migrations.
//
//go:embed *.sql
var FS embed.FS
