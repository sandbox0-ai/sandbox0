package framework

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
)

const (
	rootFSSnapshotterName       = "sandbox0-rootfs"
	rootFSSnapshotterSocketPath = "/run/containerd/sandbox0-rootfs-snapshotter.sock"
)

// ConfigureKindRootFSSnapshotter configures each kind node with the Sandbox0
// rootfs proxy snapshotter runtime handler.
func ConfigureKindRootFSSnapshotter(ctx context.Context, clusterName string) error {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return fmt.Errorf("cluster name is required")
	}

	nodes, err := kindNodeNames(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no kind nodes found for cluster %q", clusterName)
	}

	rootfsNodes, err := kindNodesWithRootFSSnapshotterSocket(ctx, nodes, 2*time.Minute)
	if err != nil {
		return err
	}
	for _, node := range rootfsNodes {
		if err := configureKindNodeRootFSSnapshotter(ctx, node); err != nil {
			return err
		}
	}
	return nil
}

// RestoreKindContainerdConfig restores the containerd config backed up by
// ConfigureKindRootFSSnapshotter.
func RestoreKindContainerdConfig(ctx context.Context, clusterName string) error {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return fmt.Errorf("cluster name is required")
	}

	nodes, err := kindNodeNames(ctx, clusterName)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := restoreKindNodeContainerdConfig(ctx, node); err != nil {
			return err
		}
	}
	return nil
}

func kindNodeNames(ctx context.Context, clusterName string) ([]string, error) {
	output, err := RunCommandOutput(
		ctx,
		"docker",
		"ps",
		"--filter",
		"label=io.x-k8s.kind.cluster="+clusterName,
		"--format",
		"{{.Names}}",
	)
	if err != nil {
		return nil, fmt.Errorf("list kind nodes: %w", err)
	}
	var nodes []string
	for _, line := range strings.Split(output, "\n") {
		node := strings.TrimSpace(line)
		if node != "" {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func kindNodesWithRootFSSnapshotterSocket(ctx context.Context, nodes []string, timeout time.Duration) ([]string, error) {
	targetNodes := rootFSSnapshotterTargetKindNodes(nodes)
	if len(targetNodes) == 0 {
		return nil, fmt.Errorf("no kind nodes available for rootfs snapshotter configuration")
	}

	deadline := time.Now().Add(timeout)
	for {
		var rootfsNodes []string
		var missingNodes []string
		for _, node := range targetNodes {
			if err := RunCommand(ctx, "docker", "exec", node, "test", "-S", rootFSSnapshotterSocketPath); err == nil {
				rootfsNodes = append(rootfsNodes, node)
				continue
			}
			missingNodes = append(missingNodes, node)
		}
		if len(missingNodes) == 0 {
			return rootfsNodes, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("rootfs snapshotter socket %s did not appear on kind nodes: %s", rootFSSnapshotterSocketPath, strings.Join(missingNodes, ", "))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func rootFSSnapshotterTargetKindNodes(nodes []string) []string {
	var normalized []string
	var workers []string
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		normalized = append(normalized, node)
		if strings.HasSuffix(node, "-control-plane") {
			continue
		}
		workers = append(workers, node)
	}
	if len(workers) > 0 {
		return workers
	}
	return normalized
}

func restoreKindNodeContainerdConfig(ctx context.Context, node string) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("kind node name is required")
	}
	if err := RunCommand(ctx, "docker", "exec", node, "sh", "-ec", kindRootFSRestoreContainerdScript()); err != nil {
		return fmt.Errorf("restore containerd config on kind node %s: %w", node, err)
	}
	return nil
}

func kindRootFSRestoreContainerdScript() string {
	return `
set -eu

cfg=/etc/containerd/config.toml
backup="${cfg}.sandbox0-rootfs.bak"

if [ -f "${backup}" ]; then
  cp "${backup}" "${cfg}"
  systemctl restart containerd
fi
`
}

func configureKindNodeRootFSSnapshotter(ctx context.Context, node string) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("kind node name is required")
	}
	if err := RunCommand(ctx, "docker", "exec", node, "sh", "-ec", kindRootFSSnapshotterConfigureScript()); err != nil {
		return fmt.Errorf("configure rootfs snapshotter on kind node %s: %w", node, err)
	}
	if err := RunCommand(ctx, "docker", "exec", node, "sh", "-ec", kindRootFSSnapshotterVerifyScript()); err != nil {
		return fmt.Errorf("verify rootfs snapshotter runtime handler on kind node %s: %w", node, err)
	}
	return nil
}

func kindRootFSSnapshotterVerifyScript() string {
	return `
set -eu

ctr plugins ls | grep -q sandbox0-rootfs
containerd config dump | grep -q 'runtimes.sandbox0-rootfs'
`
}

func kindRootFSSnapshotterConfigureScript() string {
	return `
set -eu

cfg=/etc/containerd/config.toml
plugin="` + rootFSSnapshotterName + `"
socket="` + rootFSSnapshotterSocketPath + `"
backup="${cfg}.sandbox0-rootfs.bak"

if [ ! -f "${backup}" ]; then
  cp "${cfg}" "${backup}"
fi

tmp="$(mktemp)"
awk '
  /^# BEGIN sandbox0 rootfs snapshotter$/ { skip = 1; next }
  /^# END sandbox0 rootfs snapshotter$/ { skip = 0; next }
  skip != 1 { print }
' "${cfg}" > "${tmp}"

cat >> "${tmp}" <<EOF
# BEGIN sandbox0 rootfs snapshotter
[proxy_plugins.sandbox0-rootfs]
  type = "snapshot"
  address = "${socket}"
  [proxy_plugins.sandbox0-rootfs.exports]
    root = "` + rootfs.SnapshotterHostRootPath + `"
EOF

append_runtime_handler() {
  containerd_table="$1"
  runtime_table="[${containerd_table}.runtimes.${plugin}]"
  options_table="[${containerd_table}.runtimes.${plugin}.options]"
  cat >> "${tmp}" <<EOF

${runtime_table}
  runtime_type = "io.containerd.runc.v2"
  snapshotter = "${plugin}"
EOF
  if [ -f /etc/containerd/cri-base.json ]; then
    printf '  base_runtime_spec = "/etc/containerd/cri-base.json"\n' >> "${tmp}"
  fi
  cat >> "${tmp}" <<EOF
${options_table}
  SystemdCgroup = true
EOF
}

configured=0
if grep -q '^\[plugins\."io\.containerd\.cri\.v1\.runtime"\.containerd\]' "${tmp}"; then
  append_runtime_handler 'plugins."io.containerd.cri.v1.runtime".containerd'
  configured=1
fi
if grep -q '^\[plugins\."io\.containerd\.grpc\.v1\.cri"\.containerd\]' "${tmp}"; then
  append_runtime_handler 'plugins."io.containerd.grpc.v1.cri".containerd'
  configured=1
fi
if [ "${configured}" = "0" ]; then
  append_runtime_handler 'plugins."io.containerd.cri.v1.runtime".containerd'
fi

cat >> "${tmp}" <<EOF
# END sandbox0 rootfs snapshotter
EOF

mv "${tmp}" "${cfg}"
systemctl restart containerd
`
}
