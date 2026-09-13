package sandboxstore

import (
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestRootFSImportSelectAndReturningShareLayoutColumnOrder(t *testing.T) {
	columns := rootFSImportOperationReturningColumns("")
	require.Equal(t, "SELECT "+columns+" FROM manager.rootfs_import_operations ", rootFSImportOperationSelectSQL())
	names := strings.Split(columns, ", ")
	require.Len(t, names, 27)
	require.Equal(t, "data_layout_policy", names[14])
	require.Equal(t, "mapping_group_policy", names[15])
	aliased := strings.Split(rootFSImportOperationReturningColumns("operation"), ", ")
	require.Len(t, aliased, len(names))
	for index, name := range names {
		require.Equal(t, "operation."+name, aliased[index])
	}
}

func layoutImportFixture(t *testing.T, suffix, policy string) (*BeginRootFSImportRequest, rootfsimporter.BuildResult, rootfsblock.ObjectReference) {
	t.Helper()
	begin, result, ref := rootFSImportTestFixture(t, suffix)
	begin.Spec.FormatGeneration = 2
	begin.Spec.BlockOptions = rootfsblock.BuildOptions{ObjectPrefix: begin.Spec.BlockOptions.ObjectPrefix}
	begin.Spec.DataLayoutPolicy = policy
	id, spec, err := rootfsimporter.DeterministicOperation(begin.Spec)
	require.NoError(t, err)
	begin.OperationID, begin.Spec = id, spec
	page, err := rootfsblock.EncodeMappingPage(rootfsblock.MappingPage{Version: 2, StartBlock: 0, BlockCount: uint64(spec.LogicalSizeBytes / rootfsblock.LogicalBlockSize)})
	require.NoError(t, err)
	ref.Key = spec.BlockOptions.ObjectPrefix + "/maps/sha256/" + digest.FromBytes(page).Encoded()
	ref.Checksum, ref.Size = digest.FromBytes(page).String(), int64(len(page))
	result.Descriptor.Version, result.Descriptor.MappingRoot.Version = 2, 2
	result.Descriptor.MappingRoot.RootDigest = ref.Checksum
	result.Descriptor.MappingRoot.Object = rootfsblock.ObjectRange{Key: ref.Key, Length: ref.Size, Checksum: ref.Checksum}
	result.DescriptorBytes, err = rootfsblock.EncodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	result.DescriptorDigest, result.BaseBlockRoot = digest.FromBytes(result.DescriptorBytes), digest.Digest(ref.Checksum)
	result.References, result.Bytes = []rootfsblock.ObjectReference{ref}, ref.Size
	result.DataLayoutPolicy = policy
	if policy != "" {
		result.DataLayoutRangeBytes = 64 << 10
	}
	require.NoError(t, result.Validate())
	return begin, result, ref
}

func TestRootFSImportLayoutResultMustMatchDurablePolicy(t *testing.T) {
	begin, result, _ := layoutImportFixture(t, "layout-validation", rootfsimporter.XFSFileRangesV1)
	op := &RootFSImportOperation{Spec: begin.Spec, SourceOCIDigest: result.SourceOCIDigest.String()}
	_, _, _, err := validateRootFSImportResult(op, result)
	require.NoError(t, err)
	changed := result
	changed.DataLayoutPolicy = ""
	changed.DataLayoutRangeBytes = 0
	_, _, _, err = validateRootFSImportResult(op, changed)
	require.ErrorIs(t, err, ErrRootFSImportConflict)
	changed = result
	changed.DataLayoutRangeBytes = 16 << 10
	_, _, _, err = validateRootFSImportResult(op, changed)
	require.ErrorIs(t, err, ErrRootFSImportConflict)
	row := compressedImportScanner{values: []any{
		begin.OperationID, begin.Spec.SourceOCIRef, result.SourceOCIDigest.String(), begin.Spec.Platform.OS, begin.Spec.Platform.Architecture,
		begin.Spec.Platform.Variant, begin.Spec.FormatGeneration, begin.Spec.ProcdProtocol, begin.Spec.ProcdDigest, begin.Spec.LogicalSizeBytes,
		begin.Spec.BlockOptions.DataRangeBytes, begin.Spec.BlockOptions.PackBytes, begin.Spec.BlockOptions.PageEntries, begin.Spec.BlockOptions.ObjectPrefix, begin.Spec.DataLayoutPolicy,
	}}
	reloaded, err := scanRootFSImportOperation(row)
	require.NoError(t, err)
	require.Equal(t, begin.Spec, reloaded.Spec)
}
