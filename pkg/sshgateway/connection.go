package sshgateway

import "strings"

// ConnectionInfo describes how a user can reach a sandbox over SSH.
type ConnectionInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
}

// BuildConnectionInfo returns a user-facing SSH connection payload when the
// region-scoped SSH endpoint is configured.
func BuildConnectionInfo(host string, port int, sandboxID string) *ConnectionInfo {
	host = strings.TrimSpace(host)
	sandboxID = strings.TrimSpace(sandboxID)
	if host == "" || port <= 0 || sandboxID == "" {
		return nil
	}
	return &ConnectionInfo{
		Host:     host,
		Port:     port,
		Username: sandboxID,
	}
}
