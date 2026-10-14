package rootfsimporter

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestSparseImportIdentityBindsInitialAndDeviceSizes(t *testing.T) {
	spec := OperationSpec{
		SourceOCIRef:     "registry.example/sandbox@" + digest.FromString("sparse-source").String(),
		Platform:         ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 2, ProcdProtocol: "procd-http-v1", ProcdDigest: digest.FromString("procd").String(),
		LogicalSizeBytes: 64 << 40, InitialFilesystemSizeBytes: 8 << 30,
	}
	id, normalized, err := DeterministicOperation(spec)
	require.NoError(t, err)
	require.Equal(t, spec.InitialFilesystemSizeBytes, normalized.InitialFilesystemSizeBytes)
	retry, _, err := DeterministicOperation(normalized)
	require.NoError(t, err)
	require.Equal(t, id, retry)
	for _, change := range []func(*OperationSpec){
		func(s *OperationSpec) { s.InitialFilesystemSizeBytes = 16 << 30 },
		func(s *OperationSpec) { s.LogicalSizeBytes = 32 << 40 },
	} {
		other := spec
		change(&other)
		otherID, _, err := DeterministicOperation(other)
		require.NoError(t, err)
		require.NotEqual(t, id, otherID)
	}
	for _, invalid := range []struct{ device, initial int64 }{
		{64 << 40, 0}, {65 << 40, 8 << 30}, {1 << 30, 8 << 30}, {64 << 40, -1},
		{64 << 40, 4096}, {64 << 40, (1 << 40) + 4096}, {(64 << 40) - 1, 8 << 30},
	} {
		require.Error(t, ValidateGeometry(invalid.device, invalid.initial))
	}
	logical, initial, err := ResolveImportGeometry(8<<30, 64<<40)
	require.NoError(t, err)
	require.Equal(t, int64(64<<40), logical)
	require.Equal(t, int64(8<<30), initial)
	logical, initial, err = ResolveImportGeometry(8<<30, 0)
	require.NoError(t, err)
	require.Equal(t, int64(8<<30), logical)
	require.Zero(t, initial)
}
