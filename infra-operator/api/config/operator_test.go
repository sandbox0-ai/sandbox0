package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadOperatorConfigDefaultsRootFSSnapshotterImageTagToServiceTag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("imageTag: service-next\n"), 0o600))

	config, err := LoadOperatorConfig(path)

	require.NoError(t, err)
	assert.Equal(t, "service-next", config.ImageTag)
	assert.Equal(t, "service-next", config.RootFSSnapshotterImageTag)
}

func TestLoadOperatorConfigKeepsExplicitRootFSSnapshotterImageTag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("imageTag: service-next\nrootfsSnapshotterImageTag: snapshotter-stable\n"), 0o600))

	config, err := LoadOperatorConfig(path)

	require.NoError(t, err)
	assert.Equal(t, "service-next", config.ImageTag)
	assert.Equal(t, "snapshotter-stable", config.RootFSSnapshotterImageTag)
}
