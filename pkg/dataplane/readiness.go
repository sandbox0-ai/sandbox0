package dataplane

const (
	NodeDataPlaneReadyLabel                 = "sandbox0.ai/data-plane-ready"
	NodeCtldReadyLabel                      = "sandbox0.ai/ctld-ready"
	NodeRootFSSnapshotterInstanceAnnotation = "sandbox0.ai/rootfs-snapshotter-instance"
	CtldHASlotLabel                         = "sandbox0.ai/ctld-ha-slot"
	CtldHASlotA                             = "a"
	CtldHASlotB                             = "b"
	ReadyLabelValue                         = "true"
	NotReadyLabelValue                      = "false"
)
