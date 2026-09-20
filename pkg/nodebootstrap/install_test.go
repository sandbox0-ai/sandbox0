package nodebootstrap

import (
	"archive/tar"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
	"github.com/stretchr/testify/require"
)

func TestRuntimeReleaseUsesPublishedDeploymentLayout(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	release := filepath.Join(root, "releases", "fixture")
	require.NoError(t, os.MkdirAll(filepath.Join(release, "bin"), 0o755))
	commit := strings.Repeat("a", 40)
	metadata := `{"source_commit":"` + commit + `","target":{"os":"linux","architecture":"amd64"},"runsc":{"distribution":"official-stock"}}`
	require.NoError(t, os.WriteFile(filepath.Join(release, "metadata.json"), []byte(metadata), 0o644))
	for _, binary := range []string{"nomad", "node-bootstrap", "ctld", "runsc", "sandbox0-gvisor", "procd"} {
		require.NoError(t, os.WriteFile(filepath.Join(release, "bin", binary), []byte("fixture"), 0o755))
	}
	// Mirror bundle publication using the real checked-in deployment assets.
	assets := filepath.Join(release, "share/sandbox0/deploy")
	require.NoError(t, os.CopyFS(assets, os.DirFS("../../deploy/nomad")))
	require.NoError(t, os.Symlink(release, filepath.Join(root, "current")))
	bootstrapper := &Bootstrapper{config: Config{RuntimeRoot: root}}
	artifact := nodeenrollment.RuntimeArtifact{SourceCommit: commit}
	resolved, err := bootstrapper.validateRuntimeRelease(artifact)
	require.NoError(t, err)
	require.Equal(t, release, resolved)
	for _, asset := range []string{"ctld/install-node.sh", "host/nomad.service", "host/sandbox0-node-bootstrap.timer"} {
		_, err := os.Stat(runtimeDeploymentPath(resolved, asset))
		require.NoError(t, err)
	}
	// Reject an incomplete archive before mounting or changing host services.
	require.NoError(t, os.Remove(filepath.Join(assets, "ctld/install-node.sh")))
	_, err = bootstrapper.validateRuntimeRelease(artifact)
	require.ErrorContains(t, err, "deployment asset ctld/install-node.sh")
}

func TestCNIArchiveAcceptsOfficialRootDirectoryAndFlatInventory(t *testing.T) {
	for _, prefix := range []string{"", "./", "././"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			contents, err := readCNIPlugins(bytes.NewReader(cniArchiveFixture(t, prefix, nil)))
			require.NoError(t, err)
			require.Len(t, contents, 20)
			require.Equal(t, []byte("plugin:bridge"), contents["bridge"])
			require.Nil(t, contents["LICENSE"])
		})
	}
}

func TestCNIArchiveRejectsUnsafeAndDuplicateMembers(t *testing.T) {
	for _, header := range []tar.Header{
		{Name: "../bridge", Typeflag: tar.TypeReg, Size: 1},
		{Name: "/bridge", Typeflag: tar.TypeReg, Size: 1},
		{Name: "./subdir/bridge", Typeflag: tar.TypeReg, Size: 1},
		{Name: "./bridge", Typeflag: tar.TypeSymlink, Linkname: "/bin/sh"},
		{Name: "./bridge", Typeflag: tar.TypeReg, Size: 1},
		{Name: ".", Typeflag: tar.TypeReg, Size: 1},
		{Name: "unexpected", Typeflag: tar.TypeReg, Size: 1},
	} {
		t.Run(header.Name+fmt.Sprint(header.Typeflag), func(t *testing.T) {
			_, err := readCNIPlugins(bytes.NewReader(cniArchiveFixture(t, "./", &header)))
			require.Error(t, err)
		})
	}
	var incomplete bytes.Buffer
	writer := tar.NewWriter(&incomplete)
	require.NoError(t, writer.Close())
	_, err := readCNIPlugins(&incomplete)
	require.ErrorContains(t, err, "inventory")
}

func cniArchiveFixture(t *testing.T, prefix string, extra *tar.Header) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	// The official release begins with a root directory header named "./".
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "./", Typeflag: tar.TypeDir, Mode: 0o755}))
	for _, name := range []string{
		"bandwidth", "bridge", "dhcp", "dummy", "firewall", "host-device",
		"host-local", "ipvlan", "loopback", "macvlan", "portmap", "ptp", "sbr",
		"static", "tap", "tuning", "vlan", "vrf", "LICENSE", "README.md",
	} {
		payload := []byte("plugin:" + name)
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: prefix + name, Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(payload))}))
		_, err := writer.Write(payload)
		require.NoError(t, err)
	}
	if extra != nil {
		require.NoError(t, writer.WriteHeader(extra))
		if extra.Typeflag == tar.TypeReg {
			_, err := writer.Write(bytes.Repeat([]byte("x"), int(extra.Size)))
			require.NoError(t, err)
		}
	}
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}
