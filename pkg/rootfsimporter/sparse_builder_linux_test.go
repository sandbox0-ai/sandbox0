//go:build linux

package rootfsimporter

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestBlockBuilderSparseDeviceBindsInitialFilesystemWithoutLargeStaging(t *testing.T) {
	f := newOCIBlockBuildFixture(t)
	f.request.InitialFilesystemSizeBytes = rootfsartifact.MinimumLogicalSizeBytes
	f.request.LogicalSizeBytes = 64 << 40
	built, err := f.builder().Build(t.Context(), f.request)
	require.NoError(t, err)
	require.Equal(t, f.request.InitialFilesystemSizeBytes, f.filesystem.lastSize)
	require.Equal(t, f.request.LogicalSizeBytes, built.Descriptor.LogicalSizeBytes)
	require.Less(t, built.Bytes, int64(1<<20))
	proof, payload, id, err := built.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.Equal(t, ReadyArtifactSparseAttestationVersion, proof.Version)
	require.Equal(t, f.request.InitialFilesystemSizeBytes, proof.InitialFilesystemSizeBytes)
	decoded, err := DecodeReadyArtifactAttestation(payload)
	require.NoError(t, err)
	require.Equal(t, proof, decoded)
	changed := built
	changed.InitialFilesystemSizeBytes *= 2
	_, _, otherID, err := changed.Attest(2, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, id, otherID)
	reader, err := rootfsblock.NewReader(f.publisher, built.Descriptor, 0)
	require.NoError(t, err)
	tail := make([]byte, rootfsblock.LogicalBlockSize)
	_, err = reader.ReadAt(tail, built.LogicalSizeBytes-rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, make([]byte, rootfsblock.LogicalBlockSize), tail)
}
