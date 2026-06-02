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
		`[plugins."io.containerd.cri.v1.runtime".containerd]`,
		`[plugins."io.containerd.grpc.v1.cri".containerd]`,
		`snapshotter = \"" plugin "\"`,
		"systemctl restart containerd",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q\n%s", want, script)
		}
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
