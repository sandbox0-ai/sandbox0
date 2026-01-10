package db

import (
	"time"
)

// SandboxVolume represents a sandbox volume metadata stored in the database
type SandboxVolume struct {
	ID        string `json:"id"`
	TeamID    string `json:"team_id"`
	UserID    string `json:"user_id"`
	ClusterID string `json:"cluster_id"`

	// Volume Configuration
	CacheSize  string `json:"cache_size"`
	Prefetch   int    `json:"prefetch"`
	BufferSize string `json:"buffer_size"`
	Writeback  bool   `json:"writeback"`
	ReadOnly   bool   `json:"read_only"`

	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
