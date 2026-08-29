package nodeenrollment

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeConfigTemplateRendersExactNodeWithoutLivePeerState(t *testing.T) {
	required := map[string]string{
		"etc/sandbox0/ctld.yaml.tmpl":                "node_uid: {{.NodeUID}}\nnode_id: {{.NodeID}}\n",
		"etc/sandbox0/ctld-networking.yaml.tmpl":     "node_name: {{.NodeName}}\n",
		"etc/sandbox0/ctld.env.tmpl":                 "SANDBOX0_NODE_UID={{.NodeUID}}\nSANDBOX0_PRIVATE_IP={{.PrivateIP}}\nSANDBOX0_NOMAD_ADDRESS=https://{{.PrivateIP}}:4646\n",
		"etc/sandbox0/ctld-a.env":                    "SANDBOX0_CTLD_HA_METRICS_ADDR=:9192\n",
		"etc/sandbox0/ctld-b.env":                    "SANDBOX0_CTLD_HA_METRICS_ADDR=:9193\n",
		"etc/sandbox0/internal-auth/data-public.pem": "public-key\n",
		"etc/sandbox0/pki/manager-ca.pem":            "manager-ca\n",
		"etc/sandbox0/tokens/nomad.token":            "read-only-token\n",
		"etc/nomad.d/30-sandbox0-gvisor.hcl.tmpl":    "# {{.ClusterID}} {{.ManagerAuthorityURL}}\n",
		"opt/cni/config/10-sandbox0.conflist.tmpl":   `{"subnet":"{{.AllocationCIDR}}"}` + "\n",
	}
	templateArchive := writeRuntimeConfigArchive(t, required)
	renderer, err := newRuntimeConfigTemplate(templateArchive,
		"https://manager.internal:8421",
		"spiffe://sandbox0.internal/ali-ue1/runtime-slot-channel")
	require.NoError(t, err)

	payload, err := renderer.Render(RuntimeConfigIdentity{
		NodeName: "s0-i-123", NodeID: "node-id", NodeUID: "ecs/us-east-1/i-123",
		AgentUID: "ctld/ali-ue1/i-123", PrivateIP: "10.0.0.10",
		AllocationCIDR: "172.27.0.0/26", RegionID: "ali-ue1", ClusterID: "nomad",
	})
	require.NoError(t, err)
	files := readRuntimeConfigArchive(t, payload)
	require.Contains(t, files["node-runtime/etc/sandbox0/ctld.yaml"], "ecs/us-east-1/i-123")
	require.Contains(t, files["node-runtime/etc/sandbox0/ctld.env"],
		"SANDBOX0_NOMAD_ADDRESS=https://10.0.0.10:4646")
	require.Contains(t, files["node-runtime/opt/cni/config/10-sandbox0.conflist"], "172.27.0.0/26")
	for name, contents := range files {
		require.NotContains(t, contents, "{{", name)
	}
}

func TestRuntimeConfigTemplateRejectsLinksAndMissingInventory(t *testing.T) {
	var raw bytes.Buffer
	gzipWriter := gzip.NewWriter(&raw)
	tarWriter := tar.NewWriter(gzipWriter)
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{
		Name: "node-runtime-template/escape", Typeflag: tar.TypeSymlink,
		Linkname: "/etc/shadow", Mode: 0o600,
	}))
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	_, err := newRuntimeConfigTemplate(raw.Bytes(), "https://manager:8421",
		"spiffe://sandbox0.internal/region/runtime-slot-channel")
	require.ErrorContains(t, err, "unsafe")
}

func writeRuntimeConfigArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var raw bytes.Buffer
	gzipWriter := gzip.NewWriter(&raw)
	tarWriter := tar.NewWriter(gzipWriter)
	for name, contents := range files {
		require.NoError(t, tarWriter.WriteHeader(&tar.Header{
			Name: "node-runtime-template/" + name, Typeflag: tar.TypeReg,
			Mode: 0o600, Size: int64(len(contents)), Uid: 0, Gid: 0,
		}))
		_, err := tarWriter.Write([]byte(contents))
		require.NoError(t, err)
	}
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	return raw.Bytes()
}

func readRuntimeConfigArchive(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	gzipReader, err := gzip.NewReader(bytes.NewReader(payload))
	require.NoError(t, err)
	defer gzipReader.Close()
	reader := tar.NewReader(gzipReader)
	result := make(map[string]string)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		contents, err := io.ReadAll(reader)
		require.NoError(t, err)
		result[header.Name] = strings.TrimSpace(string(contents))
	}
	return result
}
