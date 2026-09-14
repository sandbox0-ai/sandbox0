package nodebootstrap

import (
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
	for _, binary := range []string{"nomad", "node-bootstrap", "ctld", "runsc", "sandbox0-gvisor"} {
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
