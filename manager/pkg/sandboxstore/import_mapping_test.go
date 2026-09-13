package sandboxstore

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func mappingImportFixture(t *testing.T, suffix, policy string) (*BeginRootFSImportRequest, rootfsimporter.BuildResult, rootfsblock.ObjectReference) {
	t.Helper()
	begin, result, ref := layoutImportFixture(t, suffix, "")
	begin.Spec.BlockOptions.MappingGroupPolicy = policy
	var err error
	begin.OperationID, begin.Spec, err = rootfsimporter.DeterministicOperation(begin.Spec)
	require.NoError(t, err)
	result.MappingGroupPolicy = policy
	require.NoError(t, result.Validate())
	return begin, result, ref
}

func TestRootFSImportMappingResultMatchesDurablePolicyAndScan(t *testing.T) {
	begin, result, _ := mappingImportFixture(t, "mapping-validation", rootfsblock.ContiguousMappingV1)
	op := &RootFSImportOperation{Spec: begin.Spec, SourceOCIDigest: result.SourceOCIDigest.String()}
	proof, _, _, err := validateRootFSImportResult(op, result)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.ContiguousMappingV1, proof.MappingGroupPolicy)
	changed := result
	changed.MappingGroupPolicy = ""
	_, _, _, err = validateRootFSImportResult(op, changed)
	require.ErrorIs(t, err, ErrRootFSImportConflict)
	row := compressedImportScanner{values: []any{
		begin.OperationID, begin.Spec.SourceOCIRef, result.SourceOCIDigest.String(), begin.Spec.Platform.OS, begin.Spec.Platform.Architecture,
		begin.Spec.Platform.Variant, begin.Spec.FormatGeneration, begin.Spec.ProcdProtocol, begin.Spec.ProcdDigest, begin.Spec.LogicalSizeBytes,
		begin.Spec.BlockOptions.DataRangeBytes, begin.Spec.BlockOptions.PackBytes, begin.Spec.BlockOptions.PageEntries, begin.Spec.BlockOptions.ObjectPrefix,
		begin.Spec.DataLayoutPolicy, begin.Spec.BlockOptions.MappingGroupPolicy,
	}}
	reloaded, err := scanRootFSImportOperation(row)
	require.NoError(t, err)
	require.Equal(t, begin.Spec, reloaded.Spec)
}
