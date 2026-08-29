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

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderUsesExactSharedRuntimeTemplate(t *testing.T) {
	template := filepath.Join(t.TempDir(), "template.tar.gz")
	writeTemplate(t, template)
	output := filepath.Join(t.TempDir(), "rendered.tar.gz")
	err := render([]string{
		"--template", template, "--output", output,
		"--manager-authority-url", "https://authority.internal:8421",
		"--manager-authority-peer-uri", "spiffe://sandbox0.internal/ali-ue1/runtime-slot-channel",
		"--node-name", "sandbox-1", "--node-id", "node-1",
		"--node-uid", "ecs/us-east-1/i-fixed", "--agent-uid", "ctld/ali-ue1/i-fixed",
		"--private-ip", "10.0.1.2", "--allocation-cidr", "172.26.0.0/20",
		"--region-id", "ali-ue1", "--cluster-id", "nomad",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := readArchive(output)
	if err != nil {
		t.Fatal(err)
	}
	files := readRendered(t, payload)
	for _, expected := range []string{
		"SANDBOX0_NODE_NAME=sandbox-1",
		"SANDBOX0_NOMAD_NODE_ID=node-1",
		"SANDBOX0_NODE_UID=ecs/us-east-1/i-fixed",
		"SANDBOX0_REGION_ID=ali-ue1",
		"SANDBOX0_CLUSTER_ID=nomad",
	} {
		if !strings.Contains(files["node-runtime/etc/sandbox0/ctld.env"], expected) {
			t.Fatalf("rendered ctld environment is missing %q", expected)
		}
	}
	if !strings.Contains(files["node-runtime/opt/cni/config/10-sandbox0.conflist"], "172.26.0.0/20") {
		t.Fatal("rendered CNI config does not bind the fixed allocation CIDR")
	}
}

func TestWriteArchiveRejectsNoncanonicalDestination(t *testing.T) {
	if err := writeArchive("relative.tar.gz", []byte("payload")); err == nil {
		t.Fatal("relative output path was accepted")
	}
	target := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := os.Symlink("missing", target); err != nil {
		t.Fatal(err)
	}
	if err := writeArchive(target, []byte("payload")); err == nil {
		t.Fatal("symlink output path was accepted")
	}
}

func writeTemplate(t *testing.T, destination string) {
	t.Helper()
	files := map[string]string{
		"etc/sandbox0/ctld.yaml":            "runtime: true\n",
		"etc/sandbox0/ctld-networking.yaml": "network: true\n",
		"etc/sandbox0/ctld.env.tmpl": strings.Join([]string{
			"SANDBOX0_NODE_NAME={{.NodeName}}",
			"SANDBOX0_NOMAD_NODE_ID={{.NodeID}}",
			"SANDBOX0_NODE_UID={{.NodeUID}}",
			"SANDBOX0_AGENT_UID={{.AgentUID}}",
			"SANDBOX0_PRIVATE_IP={{.PrivateIP}}",
			"SANDBOX0_REGION_ID={{.RegionID}}",
			"SANDBOX0_CLUSTER_ID={{.ClusterID}}",
			"SANDBOX0_AUTHORITY_URL={{.ManagerAuthorityURL}}",
			"SANDBOX0_AUTHORITY_PEER={{.ManagerAuthorityPeerURI}}",
		}, "\n") + "\n",
		"etc/sandbox0/ctld-a.env":                    "SANDBOX0_CTLD_HA_METRICS_ADDR=:9192\n",
		"etc/sandbox0/ctld-b.env":                    "SANDBOX0_CTLD_HA_METRICS_ADDR=:9193\n",
		"etc/sandbox0/internal-auth/data-public.pem": "public-key\n",
		"etc/sandbox0/pki/manager-ca.pem":            "manager-ca\n",
		"etc/sandbox0/tokens/nomad.token":            "nomad-token\n",
		"etc/nomad.d/30-sandbox0-gvisor.hcl":         "plugin config {}\n",
		"opt/cni/config/10-sandbox0.conflist.tmpl":   `{"subnet":"{{.AllocationCIDR}}"}`,
	}
	file, err := os.Create(destination)
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	for name, contents := range files {
		header := &tar.Header{
			Name: "node-runtime-template/" + name, Mode: 0o600,
			Size: int64(len(contents)), Typeflag: tar.TypeReg,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write([]byte(contents)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func readRendered(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	archive := tar.NewReader(reader)
	files := make(map[string]string)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		contents, err := io.ReadAll(archive)
		if err != nil {
			t.Fatal(err)
		}
		files[header.Name] = string(contents)
	}
	return files
}
