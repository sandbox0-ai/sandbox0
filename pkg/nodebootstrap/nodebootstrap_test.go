package nodebootstrap

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validTestConfig() Config {
	return Config{
		SchemaVersion: 1, EnrollmentURL: "https://enroll.internal:8422",
		EnrollmentCAFile: "/etc/sandbox0/enrollment-ca.pem",
		NomadServers:     []string{"10.0.0.10:4647"}, RegionID: "ali-ue1", ClusterID: "nomad",
		ReservedCPUMHz: 2000, ReservedMemoryMB: 8192, ReservedDiskMB: 20480,
	}
}

func TestConfigNormalizesClosedWorkerContract(t *testing.T) {
	config := validTestConfig()
	require.NoError(t, config.normalize())
	require.Equal(t, "/opt/sandbox0", config.RuntimeRoot)
	require.Equal(t, "/var/lib/sandbox0", config.DataMount)
	require.Equal(t, 12*60*60, config.RenewBeforeSeconds)

	config.NomadServers = []string{"8.8.8.8:4647"}
	require.ErrorContains(t, config.normalize(), "private IPv4")
}

func TestRenderNomadClientKeepsNodeFencedUntilAdmission(t *testing.T) {
	config := validTestConfig()
	require.NoError(t, config.normalize())
	payload, err := renderNomadClientConfig(config, "s0-i-123", "10.0.1.9", false)
	require.NoError(t, err)
	text := string(payload)
	require.Contains(t, text, `node_pool        = "sandbox0"`)
	require.Contains(t, text, `servers          = ["10.0.0.10:4647"]`)
	require.Contains(t, text, `sandbox0_admitted  = "false"`)
	require.NotContains(t, text, "172.27.")

	payload, err = renderNomadClientConfig(config, "s0-i-123", "10.0.1.9", true)
	require.NoError(t, err)
	require.Contains(t, string(payload), `sandbox0_admitted  = "true"`)
}

func TestRuntimeConfigArchiveBindsExactNodeIdentity(t *testing.T) {
	files := map[string]string{}
	for _, name := range requiredRuntimeConfigPaths() {
		files[name] = "value\n"
	}
	files["etc/sandbox0/ctld.env"] = strings.Join([]string{
		"SANDBOX0_NODE_NAME=s0-i-123",
		"SANDBOX0_NOMAD_NODE_ID=11111111-1111-1111-1111-111111111111",
		"SANDBOX0_NODE_UID=ecs/us-east-1/i-123",
		"SANDBOX0_REGION_ID=ali-ue1",
		"SANDBOX0_CLUSTER_ID=nomad",
	}, "\n") + "\n"
	files["opt/cni/config/10-sandbox0.conflist"] = `{"subnet":"172.27.0.0/26"}`
	staged, err := stageRuntimeConfigAt(runtimeConfigArchive(t, files), t.TempDir())
	require.NoError(t, err)
	defer staged.close()
	require.NoError(t, validateRuntimeConfigIdentity(staged,
		"s0-i-123", "11111111-1111-1111-1111-111111111111", "ecs/us-east-1/i-123",
		"ali-ue1", "nomad", "172.27.0.0/26"))
}

func TestRuntimeConfigArchiveRejectsHostMutationOutsideContract(t *testing.T) {
	files := map[string]string{"etc/systemd/system/attacker.service": "bad"}
	_, err := stageRuntimeConfigAt(runtimeConfigArchive(t, files), t.TempDir())
	require.ErrorContains(t, err, "outside the installation contract")
}

func TestMetadataClientUsesIMDSv2ForSignedIdentity(t *testing.T) {
	var tokenRequests int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/latest/api/token":
			tokenRequests++
			require.Equal(t, http.MethodPut, request.Method)
			require.Equal(t, "600", request.Header.Get("X-aliyun-ecs-metadata-token-ttl-seconds"))
			_, _ = io.WriteString(writer, "imds-token")
		case "/latest/dynamic/instance-identity/document":
			require.Equal(t, "imds-token", request.Header.Get("X-aliyun-ecs-metadata-token"))
			_ = json.NewEncoder(writer).Encode(map[string]string{
				"instance-id": "i-123", "private-ipv4": "10.0.1.9",
			})
		case "/latest/dynamic/instance-identity/pkcs7":
			require.Equal(t, "audience with space", request.URL.Query().Get("audience"))
			_, _ = io.WriteString(writer, " c2lnbmF0dXJl\n")
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := &MetadataClient{http: server.Client(), origin: server.URL}
	identity, err := client.SignedIdentity(context.Background(), "audience with space")
	require.NoError(t, err)
	require.Equal(t, "i-123", identity.instanceID)
	require.Equal(t, "10.0.1.9", identity.privateIP)
	require.Equal(t, "c2lnbmF0dXJl", identity.signature)
	require.Equal(t, 1, tokenRequests)
}

func runtimeConfigArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	for name, payload := range files {
		require.NoError(t, tarWriter.WriteHeader(&tar.Header{
			Name: "node-runtime/" + name, Mode: 0o600, Uid: 0, Gid: 0,
			Typeflag: tar.TypeReg, Size: int64(len(payload)),
		}))
		_, err := tarWriter.Write([]byte(payload))
		require.NoError(t, err)
	}
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	return output.Bytes()
}

func TestLoadConfigRejectsTrailingDocument(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "config.json")
	require.NoError(t, os.WriteFile(file, []byte(`{"schema_version":1}{}`), 0o600))
	_, err := LoadConfig(file)
	require.Error(t, err)
}
