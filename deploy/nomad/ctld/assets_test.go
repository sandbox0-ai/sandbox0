// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctlddeploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/pkg/config"
	"gopkg.in/yaml.v3"
)

func TestSystemdUnitPreservesHostNamespacesAndABIsolation(t *testing.T) {
	payload, err := os.ReadFile("sandbox0-ctld@.service")
	if err != nil {
		t.Fatal(err)
	}
	unit := string(payload)
	for _, required := range []string{
		"PrivateMounts=false",
		"PrivateNetwork=false",
		"-ha-slot=%i",
		"-node-name=${SANDBOX0_NODE_NAME}",
		"ctld-%i-ha.sock",
		"/run/sandbox0/ctld-runtime-slot-network.sock",
		"-runtime-slot-netns-root=/run/netns",
		"ExecStartPre=/usr/local/libexec/sandbox0/ctld-resource-cgroup-setup",
		"SANDBOX0_RESOURCE_CGROUP_ROOT=/sys/fs/cgroup/sandbox0",
		"TimeoutStopSec=45s",
		"KillMode=mixed",
	} {
		if !strings.Contains(unit, required) {
			t.Fatalf("systemd unit lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"ProtectSystem=", "PrivateDevices=", "RootDirectory=", "BindPaths=", "BindReadOnlyPaths=",
		"/host-run/", "nomad-rootfs-sessiond",
	} {
		if strings.Contains(unit, forbidden) {
			t.Fatalf("systemd unit contains mount-namespace or old-runtime directive %q", forbidden)
		}
	}
}

func TestExampleConfigsDecodeAfterEnvironmentExpansion(t *testing.T) {
	environment := map[string]string{
		"SANDBOX0_DATABASE_URL":              "postgres://sandbox0@postgres.internal/sandbox0?sslmode=verify-full",
		"SANDBOX0_REGION_ID":                 "region-1",
		"SANDBOX0_CLUSTER_ID":                "cluster-1",
		"SANDBOX0_ROOTFS_BUCKET":             "rootfs",
		"SANDBOX0_ROOTFS_REGION":             "us-east-1",
		"SANDBOX0_ROOTFS_ENDPOINT":           "https://s3.internal",
		"SANDBOX0_ROOTFS_ACCESS_KEY":         "access",
		"SANDBOX0_ROOTFS_SECRET_KEY":         "secret",
		"SANDBOX0_ROOTFS_NBD_DEVICES":        "/dev/nbd0,/dev/nbd1",
		"SANDBOX0_MANAGER_AUTHORITY_URL":     "https://manager.internal:9444",
		"SANDBOX0_MANAGER_AUTHORITY_URI_SAN": "spiffe://sandbox0.internal/region/runtime-slot-channel",
		"SANDBOX0_NOMAD_ADDRESS":             "https://10.0.0.10:4646",
		"SANDBOX0_NOMAD_NODE_ID":             "node-1",
		"SANDBOX0_NODE_UID":                  "node-uid-1",
		"SANDBOX0_NODE_NAME":                 "node-1",
		"SANDBOX0_RESOURCE_CPU_MILLICORES":   "8000",
		"SANDBOX0_RESOURCE_MEMORY_BYTES":     "17179869184",
		"SANDBOX0_RESOURCE_CPUSET_CPUS":      "0-7",
		"SANDBOX0_RESOURCE_CPUSET_MEMS":      "0",
		"SANDBOX0_CLUSTER_DNS_CIDR":          "10.0.0.53/32",
		"SANDBOX0_PLATFORM_ALLOWED_CIDRS":    "10.0.0.0/8",
	}
	for key, value := range environment {
		t.Setenv(key, value)
	}
	ctldPayload, err := os.ReadFile("ctld.yaml.example")
	if err != nil {
		t.Fatal(err)
	}
	var ctldConfig apiconfig.CtldConfig
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(ctldPayload))), &ctldConfig); err != nil {
		t.Fatalf("decode ctld example: %v", err)
	}
	if !ctldConfig.NomadRuntime.Enabled || len(ctldConfig.NomadRuntime.NBDDevices) != 2 ||
		ctldConfig.NomadRuntime.ConsumerMountRoot != "/opt/nomad" ||
		ctldConfig.NomadRuntime.ConsumerNetNSRoot != "/run/netns" ||
		ctldConfig.NomadRuntime.ResourceCgroupRoot != "/sys/fs/cgroup/sandbox0" ||
		ctldConfig.NomadRuntime.ResourceCPUMillicores != 8000 ||
		ctldConfig.NomadRuntime.ResourceMemoryBytes != 17179869184 ||
		ctldConfig.NomadRuntime.ResourceCPUSetCPUs != "0-7" ||
		ctldConfig.NomadRuntime.ResourceCPUSetMems != "0" {
		t.Fatalf("decoded ctld Nomad runtime = %+v", ctldConfig.NomadRuntime)
	}
	networkPath := filepath.Join(t.TempDir(), "network.yaml")
	networkPayload, err := os.ReadFile("ctld-networking.yaml.example")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(networkPath, networkPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	networkConfig, err := apiconfig.LoadNetworkRuntimeConfigFromPath(networkPath)
	if err != nil {
		t.Fatalf("decode network example: %v", err)
	}
	if networkConfig.NodeName != "node-1" || !networkConfig.FailClosed {
		t.Fatalf("decoded network runtime = %+v", networkConfig)
	}
	if err := networkConfig.ValidateListenerPorts(map[int]string{8095: "ctld HTTP"}); err != nil {
		t.Fatalf("network listener validation: %v", err)
	}
}

func TestNomadPluginExampleUsesCtldRuntimeOnly(t *testing.T) {
	payload, err := os.ReadFile("nomad-plugin.hcl.example")
	if err != nil {
		t.Fatal(err)
	}
	config := string(payload)
	for _, required := range []string{
		`plugin "sandbox0-gvisor"`,
		`runsc_operation_timeout_seconds = 30`,
		`rootfs_node_socket            = "/run/sandbox0/ctld-nomad-runtime.sock"`,
		`resource_cgroup_root          = "/sys/fs/cgroup/sandbox0"`,
		`procd_internal_jwt_public_key_file = "/etc/sandbox0/internal-auth/data-public.pem"`,
		`runtime_slot_cluster_id           = "replace-with-cluster-id"`,
	} {
		if !strings.Contains(config, required) {
			t.Fatalf("Nomad plugin example lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"rootfs_sessiond", "rootfs_object_bucket", "rootfs_nbd_devices", "rootfs_mount_root",
		"rootfs_max_dirty_tail_bytes", "rootfs_max_node_dirty_tail_bytes",
		"rootfs_dirty_tail_retirement_reserve_bytes",
		"runtime_slot_enabled", "rootfs_enabled", "network_policy_enabled",
		"dev_smoke_enabled", "allowed_rootfs_dir",
	} {
		if strings.Contains(config, forbidden) {
			t.Fatalf("Nomad plugin example contains ctld-owned setting %q", forbidden)
		}
	}
}

func TestInstallerProducesBoundedHostLayout(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("staged installer ownership test requires root")
	}
	directory := t.TempDir()
	inputs := filepath.Join(directory, "inputs")
	root := filepath.Join(directory, "root")
	if err := os.MkdirAll(inputs, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string, mode os.FileMode) string {
		path := filepath.Join(inputs, name)
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	ctld := write("ctld", "#!/bin/sh\n", 0o755)
	driver := write("driver", "#!/bin/sh\n", 0o755)
	runsc := write("runsc", "#!/bin/sh\n", 0o755)
	config := write("ctld.yaml", "nomad_runtime:\n  enabled: true\n", 0o600)
	network := write("network.yaml", "node_name: node-1\n", 0o600)
	nomadConfig := write("nomad.hcl", "plugin \"sandbox0-gvisor\" {}\n", 0o600)
	environment := write("ctld.env", "SANDBOX0_NODE_NAME=node-1\n", 0o600)
	staleDrivers := []string{
		filepath.Join(root, "opt/nomad/plugins/nomad-driver-sandbox0"),
		filepath.Join(root, "opt/nomad/plugins/nomad-driver-sandbox0-gvisor"),
	}
	if err := os.MkdirAll(filepath.Dir(staleDrivers[0]), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, staleDriver := range staleDrivers {
		if err := os.WriteFile(staleDriver, []byte("stale"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("sh", "./install-node.sh",
		"--ctld", ctld, "--driver", driver, "--runsc", runsc,
		"--config", config, "--network-config", network, "--nomad-config", nomadConfig, "--env", environment,
		"--root", root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("staged install: %v\n%s", err, output)
	}
	for _, path := range []string{
		"usr/local/bin/ctld",
		"usr/local/bin/runsc",
		"opt/nomad/plugins/sandbox0-gvisor",
		"etc/nomad.d/30-sandbox0-gvisor.hcl",
		"usr/local/libexec/sandbox0/ctld-host-check",
		"usr/local/libexec/sandbox0/ctld-resource-cgroup-setup",
		"usr/local/libexec/sandbox0/ctld-rollout-node",
		"etc/systemd/system/sandbox0-ctld@.service",
		"etc/systemd/system/nomad.service.d/20-sandbox0-ctld.conf",
		"etc/modules-load.d/sandbox0-ctld.conf",
		"etc/modprobe.d/sandbox0-nbd.conf",
		"etc/sysctl.d/90-sandbox0-ctld.conf",
		"etc/tmpfiles.d/sandbox0-ctld.conf",
		"run/netns",
	} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("installed asset %s: %v", path, err)
		}
	}
	for _, staleDriver := range staleDrivers {
		if _, err := os.Stat(staleDriver); !os.IsNotExist(err) {
			t.Fatalf("stale misnamed driver remains after install: %v", err)
		}
	}
	info, err := os.Stat(filepath.Join(root, "etc/sandbox0/ctld.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ctld config mode = %#o, want 0600", info.Mode().Perm())
	}
}

func TestShellAssetsParse(t *testing.T) {
	for _, path := range []string{"ctld-host-check", "ctld-resource-cgroup-setup", "install-node.sh", "rollout-node.sh"} {
		if output, err := exec.Command("sh", "-n", path).CombinedOutput(); err != nil {
			t.Fatalf("sh -n %s: %v\n%s", path, err, output)
		}
	}
}

func TestRolloutWaitsForBothRolesAfterEveryRestart(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "calls")
	statePath := filepath.Join(root, "state")
	retryPath := filepath.Join(root, "promoted-primary-retry")
	writeExecutable := func(name, body string) string {
		t.Helper()
		path := filepath.Join(bin, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	writeExecutable("id", `echo 0
`)
	writeExecutable("sleep", `exit 0
`)
	writeExecutable("systemctl", `
echo "restart:$2" >>"$ROLLOUT_LOG"
case "$2" in
  sandbox0-ctld@b.service) echo b >"$ROLLOUT_STATE" ;;
  sandbox0-ctld@a.service) echo a >"$ROLLOUT_STATE" ;;
  *) exit 1 ;;
esac
`)
	ctld := writeExecutable("ctld", `
slot=""
for argument do
  case "$argument" in
    -ha-probe-socket=*) slot=${argument#*/ctld-}; slot=${slot%-ha.sock} ;;
  esac
done
state=$(cat "$ROLLOUT_STATE")
echo "probe:$state:$slot" >>"$ROLLOUT_LOG"
if [ "$state:$slot" = a:b ] && [ ! -f "$ROLLOUT_RETRY" ]; then
  : >"$ROLLOUT_RETRY"
  exit 1
fi
`)

	command := exec.Command("sh", "./rollout-node.sh")
	command.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"CTLD_BIN="+ctld,
		"ROLLOUT_LOG="+logPath,
		"ROLLOUT_STATE="+statePath,
		"ROLLOUT_RETRY="+retryPath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("rollout: %v\n%s", err, output)
	}
	payload, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "restart:sandbox0-ctld@b.service\n" +
		"probe:b:a\n" +
		"probe:b:b\n" +
		"restart:sandbox0-ctld@a.service\n" +
		"probe:a:a\n" +
		"probe:a:b\n" +
		"probe:a:b\n"
	if got := string(payload); got != want {
		t.Fatalf("rollout calls =\n%s\nwant:\n%s", got, want)
	}
}
