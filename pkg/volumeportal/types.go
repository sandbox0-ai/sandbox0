package volumeportal

// PrepareBindRequest carries the metadata needed to prepare a sandbox volume
// before ctld binds its portal.
type PrepareBindRequest struct {
	TeamID      string `json:"team_id"`
	UserID      string `json:"user_id"`
	VolumeID    string `json:"volume_id"`
	Namespace   string `json:"namespace"`
	PodName     string `json:"pod_name"`
	PodUID      string `json:"pod_uid"`
	PortalName  string `json:"portal_name"`
	MountPath   string `json:"mount_path"`
	SandboxID   string `json:"sandbox_id"`
	OwnerTeamID string `json:"owner_team_id"`
}
