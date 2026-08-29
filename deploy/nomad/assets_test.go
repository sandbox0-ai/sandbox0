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

package nomaddeploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnomad"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"gopkg.in/yaml.v3"
)

const minimumAcceptanceWidth = 8

func TestAcceptanceExamplesUseDedicatedResourceNeutralWarmCarriers(t *testing.T) {
	warmJob := readAsset(t, "../../nomad-driver-sandbox0/example/warm-slot.nomad")
	if !regexp.MustCompile(`(?m)^variable\s+"datacenter"\s*\{$`).MatchString(warmJob) ||
		!regexp.MustCompile(`(?m)^\s*datacenters\s*=\s*\[var\.datacenter\]\s*$`).MatchString(warmJob) {
		t.Fatal("warm carriers must accept the deployment region's Nomad datacenter")
	}
	groups := regexp.MustCompile(`(?m)^\s*group\s+"warm-[0-7]"\s*\{$`).FindAllString(warmJob, -1)
	if len(groups) != minimumAcceptanceWidth {
		t.Fatalf("warm-slot system groups = %d, want exactly %d", len(groups), minimumAcceptanceWidth)
	}
	if !regexp.MustCompile(`(?m)^\s*type\s*=\s*"system"\s*$`).MatchString(warmJob) {
		t.Fatal("warm carriers must run once per group on every admitted node")
	}
	if standard, privileged := strings.Count(warmJob, `security_class = "standard"`),
		strings.Count(warmJob, `security_class = "privileged"`); standard != 6 || privileged != 2 {
		t.Fatalf("warm carrier security classes = standard %d privileged %d, want 6/2", standard, privileged)
	}
	if regexp.MustCompile(`(?m)^\s*cores\s*=`).MatchString(warmJob) {
		t.Fatal("warm carriers must not encode sandbox CPU as Nomad dedicated cores")
	}
	if cpu, memory := strings.Count(warmJob, "cpu    = 50"), strings.Count(warmJob, "memory = 64"); cpu != minimumAcceptanceWidth || memory != minimumAcceptanceWidth {
		t.Fatalf("warm carrier overhead records = cpu %d memory %d, want %d each", cpu, memory, minimumAcceptanceWidth)
	}
	if !strings.Contains(warmJob, `attribute = "${meta.sandbox0_dedicated}"`) ||
		!strings.Contains(warmJob, `attribute = "${meta.sandbox0_admitted}"`) ||
		!strings.Contains(warmJob, `value     = "true"`) {
		t.Fatal("warm carriers must require dedicated and admitted Sandbox0 nodes")
	}
	if !regexp.MustCompile(`(?m)^\s*node_pool\s*=\s*"sandbox0"\s*$`).MatchString(warmJob) {
		t.Fatal("warm carriers must target the sandbox0 Nomad node pool")
	}
	restartBlocks := regexp.MustCompile(`(?s)restart\s*\{.*?\}`).FindAllString(warmJob, -1)
	if len(restartBlocks) != minimumAcceptanceWidth {
		t.Fatalf("restart blocks = %d, want %d", len(restartBlocks), minimumAcceptanceWidth)
	}
	for index, restartBlock := range restartBlocks {
		if !regexp.MustCompile(`(?m)^\s*attempts\s*=\s*0\s*$`).MatchString(restartBlock) ||
			!regexp.MustCompile(`(?m)^\s*mode\s*=\s*"fail"\s*$`).MatchString(restartBlock) {
			t.Fatalf("warm carrier %d does not disable same-allocation restarts", index)
		}
	}

	environment := readAsset(t, "ctld/ctld.env.example")
	var devices []string
	for _, line := range strings.Split(environment, "\n") {
		if value, found := strings.CutPrefix(line, "SANDBOX0_ROOTFS_NBD_DEVICES="); found {
			devices = strings.Split(value, ",")
			break
		}
	}
	if len(devices) < minimumAcceptanceWidth {
		t.Fatalf("ctld example configures %d NBD devices, want at least %d", len(devices), minimumAcceptanceWidth)
	}
	seen := make(map[string]struct{}, len(devices))
	devicePattern := regexp.MustCompile(`^/dev/nbd[0-9]+$`)
	for _, device := range devices {
		if !devicePattern.MatchString(device) {
			t.Fatalf("noncanonical NBD device %q", device)
		}
		if _, duplicate := seen[device]; duplicate {
			t.Fatalf("duplicate NBD device %q", device)
		}
		seen[device] = struct{}{}
	}
}

func TestNomadEndpointExampleUsesDedicatedControlDirectory(t *testing.T) {
	const (
		clusterID = "cluster-uuid"
		nodeID    = "00000000-0000-0000-0000-000000000000"
	)
	catalogPath, err := filepath.Abs("../../nomad-driver-sandbox0/example/nomad-endpoints.example.json")
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := runtimeslotnomad.LoadStaticEndpointResolver(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	server, err := resolver.ServerEndpoint(t.Context(), clusterID)
	if err != nil {
		t.Fatal(err)
	}
	client, err := resolver.ClientEndpoint(t.Context(), clusterID, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	const mount = "/etc/sandbox0/node-authority/control"
	for index, endpoint := range []runtimeslotnomad.Endpoint{server, client} {
		for name, path := range map[string]string{
			"CA": endpoint.CAFile, "certificate": endpoint.ClientCertFile,
			"key": endpoint.ClientKeyFile, "token": endpoint.TokenFile,
		} {
			if filepath.Dir(path) != mount || filepath.Clean(path) != path {
				t.Fatalf("endpoint %d %s path %q is outside %s", index, name, path, mount)
			}
		}
	}
}

func TestControlServiceUnitUsesDirectPerServiceConfiguration(t *testing.T) {
	unit := readAsset(t, "control/sandbox0-control@.service")
	for _, required := range []string{
		"EnvironmentFile=-/etc/sandbox0/%i.env",
		"Environment=CONFIG_PATH=/etc/sandbox0/%i.yaml",
		"ExecStart=/usr/local/bin/%i",
		"User=sandbox0",
		"ProtectSystem=strict",
		"WantedBy=sandbox0-control.target multi-user.target",
	} {
		if !strings.Contains(unit, required) {
			t.Fatalf("control unit is missing %q", required)
		}
	}
	manager := readAsset(t, "control/manager.yaml.example")
	for _, required := range []string{
		"class_catalog_file: /etc/sandbox0/node-authority/claim/runtime-classes.json",
		"writer_token_key_file: /etc/sandbox0/node-authority/claim/writer-token.key",
		"nomad_endpoints_file: /etc/sandbox0/node-authority/control/nomad-endpoints.json",
		"agent_uid: replace-with-node-agent-identity",
	} {
		if !strings.Contains(manager, required) {
			t.Fatalf("manager example is missing %q", required)
		}
	}
}

func TestManagerExampleRespectsWriterRenewalGraceBound(t *testing.T) {
	manager := readAsset(t, "control/manager.yaml.example")
	var cfg config.ManagerConfig
	if err := yaml.Unmarshal([]byte(manager), &cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.NodeAuthority.WriterRenewalGrace.Duration; got < 0 || got > sandboxstore.RootFSWriterMaxRenewGrace {
		t.Fatalf("writer renewal grace = %s, want between zero and %s", got, sandboxstore.RootFSWriterMaxRenewGrace)
	}
}

func TestDisposableNodeHostAssetsFailClosed(t *testing.T) {
	agent := readAsset(t, "host/sandbox0-nomad-agent")
	for _, required := range []string{
		"/etc/nomad.d/client-intro.token",
		"/etc/sandbox0/nomad-exact-node.json",
		"-client-intro-token=",
		"/opt/sandbox0/current/bin/nomad",
	} {
		if !strings.Contains(agent, required) {
			t.Fatalf("Nomad agent wrapper is missing %q", required)
		}
	}
	unit := readAsset(t, "host/nomad.service")
	if strings.Contains(unit, "cloud-final.service") {
		t.Fatal("Nomad must be startable by the post-cloud-final enrollment unit")
	}
	if !strings.Contains(unit, "ExecReload=/bin/kill -HUP $MAINPID") {
		t.Fatal("Nomad unit must support certificate reload without stopping allocations")
	}
	renewal := readAsset(t, "host/sandbox0-node-bootstrap.service")
	timer := readAsset(t, "host/sandbox0-node-bootstrap.timer")
	if !strings.Contains(renewal, "--renew") ||
		!strings.Contains(renewal, "ConditionPathExists=/etc/sandbox0/node-bootstrap-complete") ||
		!strings.Contains(timer, "OnUnitActiveSec=6h") ||
		!strings.Contains(timer, "RandomizedDelaySec=30min") {
		t.Fatal("disposable node exact identities must renew through the bounded timer")
	}
}

func TestNodeRuntimeTemplateBuilderProducesExactIdentityArchive(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	files := map[string]string{
		"etc/sandbox0/ctld.yaml":            "runtime: true\n",
		"etc/sandbox0/ctld-networking.yaml": "network: true\n",
		"etc/sandbox0/ctld.env.tmpl": strings.Join([]string{
			"SANDBOX0_NODE_NAME={{.NodeName}}",
			"SANDBOX0_NOMAD_NODE_ID={{.NodeID}}",
			"SANDBOX0_NODE_UID={{.NodeUID}}",
			"SANDBOX0_REGION_ID={{.RegionID}}",
			"SANDBOX0_CLUSTER_ID={{.ClusterID}}",
		}, "\n") + "\n",
		"etc/sandbox0/ctld-a.env":                    "SANDBOX0_CTLD_HA_METRICS_ADDR=:9192\n",
		"etc/sandbox0/ctld-b.env":                    "SANDBOX0_CTLD_HA_METRICS_ADDR=:9193\n",
		"etc/sandbox0/internal-auth/data-public.pem": "public-key\n",
		"etc/sandbox0/pki/manager-ca.pem":            "manager-ca\n",
		"etc/sandbox0/tokens/nomad.token":            "scoped-token\n",
		"etc/nomad.d/30-sandbox0-gvisor.hcl":         "plugin config {}\n",
		"opt/cni/config/10-sandbox0.conflist.tmpl":   `{"subnet":"{{.AllocationCIDR}}"}`,
	}
	for name, payload := range files {
		path := filepath.Join(source, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(t.TempDir(), "runtime-template.tar.gz")
	command := exec.Command("bash", "control/build-node-runtime-template.sh", "--source", source, "--output", output)
	if payload, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build runtime template: %v: %s", err, payload)
	}
	template, err := nodeenrollment.NewRuntimeConfigTemplateFromFile(output,
		"https://manager.internal:8421",
		"spiffe://sandbox0.internal/ali-ue1/runtime-slot-channel")
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := template.Render(nodeenrollment.RuntimeConfigIdentity{
		NodeName: "s0-i-123", NodeID: "11111111-1111-1111-1111-111111111111",
		NodeUID: "ecs/us-east-1/i-123", AgentUID: "ctld/ali-ue1/i-123",
		PrivateIP: "10.0.1.9", AllocationCIDR: "172.27.0.0/26",
		RegionID: "ali-ue1", ClusterID: "nomad",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rendered) == 0 {
		t.Fatal("rendered runtime archive is empty")
	}
}

func readAsset(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}
