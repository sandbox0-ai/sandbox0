package framework

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
)

func TestKindRootFSSnapshotterConfigureScriptCoversContainerdCRIPaths(t *testing.T) {
	script := kindRootFSSnapshotterConfigureScript()
	for _, want := range []string{
		"[proxy_plugins.sandbox0-rootfs]",
		`address = "${socket}"`,
		`[proxy_plugins.sandbox0-rootfs.exports]`,
		`root = "` + rootfs.SnapshotterHostRootPath + `"`,
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

func TestRootFSSnapshotterTargetKindNodesPrefersWorkers(t *testing.T) {
	nodes := []string{
		"sandbox0-pr-e2e-control-plane",
		"sandbox0-pr-e2e-worker",
		"sandbox0-pr-e2e-worker2",
	}

	got := rootFSSnapshotterTargetKindNodes(nodes)
	want := []string{"sandbox0-pr-e2e-worker", "sandbox0-pr-e2e-worker2"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("target nodes = %#v, want %#v", got, want)
	}
}

func TestRootFSSnapshotterTargetKindNodesFallsBackToAllNodes(t *testing.T) {
	nodes := []string{"custom-node-a", "custom-node-b"}

	got := rootFSSnapshotterTargetKindNodes(nodes)
	if strings.Join(got, ",") != strings.Join(nodes, ",") {
		t.Fatalf("target nodes = %#v, want %#v", got, nodes)
	}
}

func TestKindRootFSSnapshotterVerifyScriptChecksRuntimeHandler(t *testing.T) {
	script := kindRootFSSnapshotterVerifyScript()
	for _, want := range []string{
		"ctr plugins ls | grep -q sandbox0-rootfs",
		"containerd config dump | grep -q 'runtimes.sandbox0-rootfs'",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("verify script missing %q\n%s", want, script)
		}
	}
}
