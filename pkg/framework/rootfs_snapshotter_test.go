package framework

import (
	"strings"
	"testing"
)

func TestKindRootFSSnapshotterConfigureScriptCoversContainerdCRIPaths(t *testing.T) {
	script := kindRootFSSnapshotterConfigureScript()
	for _, want := range []string{
		"[proxy_plugins.sandbox0-rootfs]",
		`address = "${socket}"`,
		`[proxy_plugins.sandbox0-rootfs.exports]`,
		`root = "/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs"`,
		`append_runtime_handler 'plugins."io.containerd.cri.v1.runtime".containerd'`,
		`append_runtime_handler 'plugins."io.containerd.grpc.v1.cri".containerd'`,
		`runtime_type = "io.containerd.runc.v2"`,
		`snapshotter = "${plugin}"`,
		`SystemdCgroup = true`,
		"systemctl restart containerd",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q\n%s", want, script)
		}
	}
	if strings.Contains(script, `snapshotter = \"" plugin "\"`) {
		t.Fatalf("script rewrites the default CRI snapshotter\n%s", script)
	}
}

func TestKindRootFSRestoreContainerdScriptRestoresBackup(t *testing.T) {
	script := kindRootFSRestoreContainerdScript()
	for _, want := range []string{
		`backup="${cfg}.sandbox0-rootfs.bak"`,
		`cp "${backup}" "${cfg}"`,
		"systemctl restart containerd",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("restore script missing %q\n%s", want, script)
		}
	}
}
