package egressauth

import "time"

// ResolveRequest describes an auth material lookup for a matched egress auth rule.
type ResolveRequest struct {
	SandboxID       string `json:"sandboxId"`
	TeamID          string `json:"teamId,omitempty"`
	AuthRef         string `json:"authRef"`
	RuleName        string `json:"ruleName,omitempty"`
	Destination     string `json:"destination,omitempty"`
	DestinationPort int    `json:"destinationPort,omitempty"`
	Transport       string `json:"transport,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
}

// ResolveResponse describes the resolved outbound auth material.
type ResolveResponse struct {
	AuthRef   string            `json:"authRef"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt *time.Time        `json:"expiresAt,omitempty"`
}
