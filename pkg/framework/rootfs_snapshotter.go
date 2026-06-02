package framework

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	rootFSSnapshotterName       = "sandbox0-rootfs"
	rootFSSnapshotterSocketPath = "/run/containerd/sandbox0-rootfs-snapshotter.sock"
)

// ConfigureKindRootFSSnapshotter configures each kind node to use the Sandbox0
// rootfs proxy snapshotter for new CRI containers.
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
	deadline := time.Now().Add(timeout)
	for {
		var rootfsNodes []string
		for _, node := range nodes {
			if err := RunCommand(ctx, "docker", "exec", node, "test", "-S", rootFSSnapshotterSocketPath); err == nil {
				rootfsNodes = append(rootfsNodes, node)
			}
		}
		if len(rootfsNodes) > 0 {
			return rootfsNodes, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("rootfs snapshotter socket %s did not appear on any kind node", rootFSSnapshotterSocketPath)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
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
	if err := RunCommand(ctx, "docker", "exec", node, "sh", "-ec", "ctr plugins ls | grep -q sandbox0-rootfs"); err != nil {
		return fmt.Errorf("verify rootfs snapshotter plugin on kind node %s: %w", node, err)
	}
	return nil
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
# END sandbox0 rootfs snapshotter
EOF

set_snapshotter() {
  table="$1"
  next="$(mktemp)"
  awk -v table="${table}" -v plugin="${plugin}" '
    BEGIN { in_table = 0; wrote = 0 }
    /^\[.*\]$/ {
      if (in_table == 1 && wrote == 0) {
        print "  snapshotter = \"" plugin "\""
        wrote = 1
      }
      in_table = ($0 == table)
    }
    in_table == 1 && /^[[:space:]]*snapshotter[[:space:]]*=/ {
      print "  snapshotter = \"" plugin "\""
      wrote = 1
      next
    }
    { print }
    END {
      if (in_table == 1 && wrote == 0) {
        print "  snapshotter = \"" plugin "\""
      }
    }
  ' "${tmp}" > "${next}"
  mv "${next}" "${tmp}"
}

if grep -q '^\[plugins\."io\.containerd\.cri\.v1\.runtime"\.containerd\]' "${tmp}"; then
  set_snapshotter '[plugins."io.containerd.cri.v1.runtime".containerd]'
elif grep -q '^\[plugins\."io\.containerd\.grpc\.v1\.cri"\.containerd\]' "${tmp}"; then
  set_snapshotter '[plugins."io.containerd.grpc.v1.cri".containerd]'
else
  cat >> "${tmp}" <<EOF

[plugins."io.containerd.cri.v1.runtime".containerd]
  snapshotter = "${plugin}"
EOF
fi

mv "${tmp}" "${cfg}"
systemctl restart containerd
`
}
