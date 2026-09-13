package session

import (
	"bytes"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// The fake host covers the session/branch publication contract, not privileged
// XFS mounts, network transport, or a sandbox's wall-clock startup latency.
func TestSessionRetirementMaterializesCompressedGeneration(t *testing.T) {
	manager, runtime, request := newTestManager(t, "compressed-session")
	expected := bytes.Repeat([]byte{0x41}, 1<<20)
	built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), bytes.NewReader(expected), int64(len(expected)), runtime.source,
		rootfsblock.BuildOptions{FormatVersion: rootfsblock.CompressedFormatVersion})
	require.NoError(t, err)
	request.Generation.Descriptor = built.Payload
	request.Generation.BaseBlockRoot, request.Generation.CurrentBlockHead = built.Descriptor.MappingRoot.RootDigest, built.Descriptor.MappingRoot.RootDigest
	request.Generation.FormatGeneration = rootfsblock.CompressedFormatVersion
	_, err = manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	manager.mu.Lock()
	branch := manager.live[request.Parent].branch
	manager.mu.Unlock()
	actual := make([]byte, 1)
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected[:1], actual)
	// Exceed the descriptor's bounded composite tail so retirement must use
	// the incremental materializer and preserve a partial compressed base unit.
	for block := range 12 {
		payload := bytes.Repeat([]byte{byte(0xb0 + block)}, rootfsblock.LogicalBlockSize)
		_, err := branch.WriteAt(payload, int64(block*rootfsblock.LogicalBlockSize))
		require.NoError(t, err)
		copy(expected[block*rootfsblock.LogicalBlockSize:], payload)
	}
	require.NoError(t, branch.Flush())
	require.NoError(t, manager.BeginRetire(request.Parent, request.Identity, "retire-compressed"))
	require.NoError(t, manager.Release(t.Context(), request.Identity))
	result, err := manager.RetireResult(request.Parent, request.Identity, "retire-compressed")
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityS3, result.DurabilityState)
	descriptor, err := rootfsblock.DecodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.CompressedFormatVersion, descriptor.Version)
	reader, err := rootfsblock.NewReader(runtime.source, descriptor, 0)
	require.NoError(t, err)
	actual = make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
