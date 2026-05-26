package sandboxdevices

const (
	Fuse   = "fuse"
	NetTun = "net-tun"

	ResourceFuse   = "sandbox0.ai/fuse"
	ResourceNetTun = "sandbox0.ai/net-tun"
)

type DeviceNode struct {
	HostPath      string
	ContainerPath string
	Permissions   string
}

func Supported() []string {
	return []string{Fuse, NetTun}
}

func ResourceName(device string) (string, bool) {
	switch device {
	case Fuse:
		return ResourceFuse, true
	case NetTun:
		return ResourceNetTun, true
	default:
		return "", false
	}
}

func DeviceNodesForResource(resourceName string) ([]DeviceNode, bool) {
	switch resourceName {
	case ResourceFuse:
		return []DeviceNode{fuseNode()}, true
	case ResourceNetTun:
		return []DeviceNode{netTunNode()}, true
	default:
		return nil, false
	}
}

func fuseNode() DeviceNode {
	return DeviceNode{
		HostPath:      "/dev/fuse",
		ContainerPath: "/dev/fuse",
		Permissions:   "rwm",
	}
}

func netTunNode() DeviceNode {
	return DeviceNode{
		HostPath:      "/dev/net/tun",
		ContainerPath: "/dev/net/tun",
		Permissions:   "rwm",
	}
}
