package sshgateway

// ResolvedTarget describes the authorized sandbox runtime target returned by
// the regional SSH routing surface.
type ResolvedTarget struct {
	SandboxID string `json:"sandbox_id"`
	TeamID    string `json:"team_id"`
	UserID    string `json:"user_id"`
	ProcdURL  string `json:"procd_url"`
}
