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

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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
		"SANDBOX0_NOMAD_ADDRESS":             "https://127.0.0.1:4646",
		"SANDBOX0_NOMAD_NODE_ID":             "node-1",
		"SANDBOX0_NODE_UID":                  "node-uid-1",
		"SANDBOX0_NODE_NAME":                 "node-1",
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
		ctldConfig.NomadRuntime.ConsumerNetNSRoot != "/run/netns" {
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
		`rootfs_node_socket            = "/run/sandbox0/ctld-nomad-runtime.sock"`,
		`runtime_slot_enabled              = true`,
	} {
		if !strings.Contains(config, required) {
			t.Fatalf("Nomad plugin example lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"rootfs_sessiond", "rootfs_object_bucket", "rootfs_nbd_devices", "rootfs_mount_root",
		"rootfs_max_dirty_tail_bytes", "rootfs_max_node_dirty_tail_bytes",
		"rootfs_dirty_tail_retirement_reserve_bytes",
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
		"opt/nomad/plugins/nomad-driver-sandbox0",
		"etc/nomad.d/30-sandbox0-gvisor.hcl",
		"usr/local/libexec/sandbox0/ctld-host-check",
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
	info, err := os.Stat(filepath.Join(root, "etc/sandbox0/ctld.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("ctld config mode = %#o, want 0600", info.Mode().Perm())
	}
}

func TestShellAssetsParse(t *testing.T) {
	for _, path := range []string{"ctld-host-check", "install-node.sh", "rollout-node.sh"} {
		if output, err := exec.Command("sh", "-n", path).CombinedOutput(); err != nil {
			t.Fatalf("sh -n %s: %v\n%s", path, err, output)
		}
	}
}
