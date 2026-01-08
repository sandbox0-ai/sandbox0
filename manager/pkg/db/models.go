package db

import (
	"time"
)

// Sandbox represents a sandbox instance
type Sandbox struct {
	ID           string    `json:"id"`
	TemplateID   string    `json:"template_id"`
	TeamID       string    `json:"team_id"`
	UserID       string    `json:"user_id"`
	ProcdAddress string    `json:"procd_address"`
	Status       string    `json:"status"`
	PodName      string    `json:"pod_name"`
	Namespace    string    `json:"namespace"`
	ExpiresAt    time.Time `json:"expires_at"`
	ClaimedAt    time.Time `json:"claimed_at"`
	CreatedAt    time.Time `json:"created_at"`
}

// SandboxStatus represents possible sandbox statuses
const (
	SandboxStatusPending   = "pending"
	SandboxStatusStarting  = "starting"
	SandboxStatusRunning   = "running"
	SandboxStatusFailed    = "failed"
	SandboxStatusCompleted = "completed"
)
