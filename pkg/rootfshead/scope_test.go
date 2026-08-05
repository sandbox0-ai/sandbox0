package rootfshead

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTeamObjectPrefixIsStableAndDoesNotExposeTeamID(t *testing.T) {
	first, err := TeamObjectPrefix("team-secret")
	require.NoError(t, err)
	second, err := TeamObjectPrefix(" team-secret ")
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.NotContains(t, first, "team-secret")
}

func TestTeamPrefixFromObjectKey(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	key, err := ObjectKey(prefix, ChunkMediaType, digest.FromString("payload").String())
	require.NoError(t, err)
	got, err := TeamPrefixFromObjectKey(key)
	require.NoError(t, err)
	assert.Equal(t, prefix, got)

	_, err = TeamPrefixFromObjectKey("sandbox-rootfs/cow-v3/teams/sha256/bad/chunks/sha256/value")
	assert.Error(t, err)
}

func TestTeamScopeAllowsForkedFilesystemsToShareObjects(t *testing.T) {
	prefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	object := testObject(t, prefix, ChunkMediaType, []byte("shared by source and fork"))

	assert.NoError(t, ValidateObjectScope(prefix, object))
	forkPrefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	assert.NoError(t, ValidateObjectScope(forkPrefix, object))
}

func TestTeamScopeRejectsCrossTeamObject(t *testing.T) {
	sourcePrefix, err := TeamObjectPrefix("team-1")
	require.NoError(t, err)
	targetPrefix, err := TeamObjectPrefix("team-2")
	require.NoError(t, err)
	object := testObject(t, sourcePrefix, ChunkMediaType, []byte("private"))
	assert.Error(t, ValidateObjectScope(targetPrefix, object))
}

func TestObjectKeyRequiresKnownMediaType(t *testing.T) {
	_, err := ObjectKey("prefix", "unknown", digest.FromString("payload").String())
	assert.Error(t, err)
}
