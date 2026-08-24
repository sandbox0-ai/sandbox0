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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnomad"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"gopkg.in/yaml.v3"
)

const minimumAcceptanceWidth = 8

func TestAcceptanceExamplesUseDedicatedResourceNeutralWarmCarriers(t *testing.T) {
	warmJob := readAsset(t, "../../nomad-driver-sandbox0/example/warm-slot.nomad")
	countMatch := regexp.MustCompile(`(?m)^\s*count\s*=\s*([0-9]+)\s*$`).FindStringSubmatch(warmJob)
	if len(countMatch) != 2 {
		t.Fatal("warm-slot example does not contain one literal group count")
	}
	warmCount, err := strconv.Atoi(countMatch[1])
	if err != nil || warmCount < minimumAcceptanceWidth {
		t.Fatalf("warm-slot count = %q, want at least %d", countMatch[1], minimumAcceptanceWidth)
	}
	if regexp.MustCompile(`(?m)^\s*cores\s*=`).MatchString(warmJob) {
		t.Fatal("warm carriers must not encode sandbox CPU as Nomad dedicated cores")
	}
	if !regexp.MustCompile(`(?m)^\s*cpu\s*=\s*50\s*$`).MatchString(warmJob) ||
		!regexp.MustCompile(`(?m)^\s*memory\s*=\s*64\s*$`).MatchString(warmJob) {
		t.Fatal("warm carriers must reserve only the documented 50 MHz/64 MiB overhead")
	}
	if !strings.Contains(warmJob, `attribute = "${meta.sandbox0_dedicated}"`) ||
		!strings.Contains(warmJob, `value     = "true"`) {
		t.Fatal("warm carriers must require the dedicated Sandbox0 Nomad node pool")
	}
	if !regexp.MustCompile(`(?m)^\s*node_pool\s*=\s*"sandbox0"\s*$`).MatchString(warmJob) {
		t.Fatal("warm carriers must target the sandbox0 Nomad node pool")
	}
	restartBlock := regexp.MustCompile(`(?s)restart\s*\{.*?\}`).FindString(warmJob)
	if restartBlock == "" || !regexp.MustCompile(`(?m)^\s*attempts\s*=\s*0\s*$`).MatchString(restartBlock) ||
		!regexp.MustCompile(`(?m)^\s*mode\s*=\s*"fail"\s*$`).MatchString(restartBlock) {
		t.Fatal("one-shot warm-slot allocations must disable same-allocation task restarts")
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

func readAsset(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}
