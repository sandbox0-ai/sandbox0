package volumeportal

import "strings"

const (
	DriverName = "volume.sandbox0.ai"

	AttributePortalName = "sandbox0.ai/portal-name"
	AttributeMountPath  = "sandbox0.ai/mount-path"

	WebhookStatePortalName = "sandbox0-webhook-state"
	WebhookStateMountPath  = "/var/lib/sandbox0/procd"

	PodInfoName      = "csi.storage.k8s.io/pod.name"
	PodInfoNamespace = "csi.storage.k8s.io/pod.namespace"
	PodInfoUID       = "csi.storage.k8s.io/pod.uid"
)

// NormalizePortalName returns the stable portal identity shared by Kubernetes
// volume attributes and the manager-to-ctld bind request.
func NormalizePortalName(name, mountPath string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}
	mountPath = strings.Trim(strings.TrimSpace(mountPath), "/")
	if mountPath == "" {
		return ""
	}
	return strings.NewReplacer("/", "-", "_", "-", ".", "-").Replace(mountPath)
}
