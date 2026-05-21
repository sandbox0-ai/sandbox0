package volumeportal

// PrepareBindRequest is the manager-to-storage-proxy request used before
// binding a sandbox volume portal to a target ctld.
type PrepareBindRequest struct {
	TeamID          string `json:"team_id"`
	UserID          string `json:"user_id"`
	VolumeID        string `json:"volume_id"`
	TargetClusterID string `json:"target_cluster_id"`
	TargetCtldAddr  string `json:"target_ctld_addr"`
	Namespace       string `json:"namespace"`
	PodName         string `json:"pod_name"`
	PodUID          string `json:"pod_uid"`
	PortalName      string `json:"portal_name"`
	MountPath       string `json:"mount_path"`
	SandboxID       string `json:"sandbox_id"`
	OwnerTeamID     string `json:"owner_team_id"`
}
