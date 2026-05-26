package deviceplugin

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxdevices"
	"go.uber.org/zap"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

func TestPluginReportsHealthyVirtualDevicesWhenHostNodesExist(t *testing.T) {
	dir := t.TempDir()
	tun := writeDeviceNode(t, dir, "tun")

	plugin := NewPlugin(PluginConfig{
		ResourceName: sandboxdevices.ResourceNetTun,
		DeviceNodes: []sandboxdevices.DeviceNode{
			{HostPath: tun, ContainerPath: "/dev/net/tun", Permissions: "rwm"},
		},
		PluginDir: t.TempDir(),
		Capacity:  2,
		Logger:    zap.NewNop(),
	})

	devices := plugin.devices()
	if len(devices) != 2 {
		t.Fatalf("devices length = %d, want 2", len(devices))
	}
	for _, device := range devices {
		if device.Health != pluginapi.Healthy {
			t.Fatalf("device health = %q, want %q", device.Health, pluginapi.Healthy)
		}
	}
}

func TestPluginMarksDevicesUnhealthyWhenHostNodeMissing(t *testing.T) {
	plugin := NewPlugin(PluginConfig{
		ResourceName: sandboxdevices.ResourceFuse,
		DeviceNodes: []sandboxdevices.DeviceNode{{
			HostPath:      filepath.Join(t.TempDir(), "missing"),
			ContainerPath: "/dev/fuse",
			Permissions:   "rwm",
		}},
		PluginDir: t.TempDir(),
		Capacity:  1,
		Logger:    zap.NewNop(),
	})

	devices := plugin.devices()
	if len(devices) != 1 {
		t.Fatalf("devices length = %d, want 1", len(devices))
	}
	if devices[0].Health != pluginapi.Unhealthy {
		t.Fatalf("device health = %q, want %q", devices[0].Health, pluginapi.Unhealthy)
	}
}

func TestPluginAllocateReturnsConfiguredDeviceSpecs(t *testing.T) {
	dir := t.TempDir()
	tun := writeDeviceNode(t, dir, "tun")

	plugin := NewPlugin(PluginConfig{
		ResourceName: sandboxdevices.ResourceNetTun,
		DeviceNodes: []sandboxdevices.DeviceNode{
			{HostPath: tun, ContainerPath: "/dev/net/tun", Permissions: "rwm"},
		},
		PluginDir: t.TempDir(),
		Capacity:  1,
		Logger:    zap.NewNop(),
	})

	resp, err := plugin.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{
			DevicesIds: []string{"sandbox0-ai-net-tun-0"},
		}},
	})
	if err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if len(resp.ContainerResponses) != 1 {
		t.Fatalf("container responses = %d, want 1", len(resp.ContainerResponses))
	}
	specs := resp.ContainerResponses[0].Devices
	if len(specs) != 1 {
		t.Fatalf("device specs = %d, want 1", len(specs))
	}
	if specs[0].HostPath != tun || specs[0].ContainerPath != "/dev/net/tun" || specs[0].Permissions != "rwm" {
		t.Fatalf("tun device spec = %#v", specs[0])
	}
}

func writeDeviceNode(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
		t.Fatalf("write device node fixture: %v", err)
	}
	return path
}
