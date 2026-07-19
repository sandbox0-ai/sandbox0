// +kubebuilder:object:generate=true
package config

const (
	DefaultAuditSpoolMaxBytes       int64 = 1 << 30
	DefaultAuditSpoolMaxEntries     int64 = 100_000
	DefaultAuditSpoolMaxTeamBytes   int64 = 256 << 20
	DefaultAuditSpoolMaxTeamEntries int64 = 25_000
	DefaultAuditSpoolMinFreeBytes   int64 = 1 << 30
	DefaultAuditSpoolMaxRecordBytes int64 = 1 << 20
)

// AuditSpoolLimitsConfig protects one local delivery volume from an unbounded
// per-team audit backlog.
type AuditSpoolLimitsConfig struct {
	MaxBytes       int64 `yaml:"max_bytes" json:"maxBytes"`
	MaxEntries     int64 `yaml:"max_entries" json:"maxEntries"`
	MaxTeamBytes   int64 `yaml:"max_team_bytes" json:"maxTeamBytes"`
	MaxTeamEntries int64 `yaml:"max_team_entries" json:"maxTeamEntries"`
	MinFreeBytes   int64 `yaml:"min_free_bytes" json:"minFreeBytes"`
	MaxRecordBytes int64 `yaml:"max_record_bytes" json:"maxRecordBytes"`
}

func applyAuditSpoolLimitsDefaults(limits *AuditSpoolLimitsConfig) {
	if limits == nil {
		return
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = DefaultAuditSpoolMaxBytes
	}
	if limits.MaxEntries == 0 {
		limits.MaxEntries = DefaultAuditSpoolMaxEntries
	}
	if limits.MaxTeamBytes == 0 {
		limits.MaxTeamBytes = DefaultAuditSpoolMaxTeamBytes
	}
	if limits.MaxTeamEntries == 0 {
		limits.MaxTeamEntries = DefaultAuditSpoolMaxTeamEntries
	}
	if limits.MinFreeBytes == 0 {
		limits.MinFreeBytes = DefaultAuditSpoolMinFreeBytes
	}
	if limits.MaxRecordBytes == 0 {
		limits.MaxRecordBytes = DefaultAuditSpoolMaxRecordBytes
	}
}
