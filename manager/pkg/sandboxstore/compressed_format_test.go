package sandboxstore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

type compressedImportScanner struct{ values []any }

func (s compressedImportScanner) Scan(dest ...any) error {
	for index, value := range s.values {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

// Exercise the actual persisted-column scan, not an in-memory OperationSpec
// roundtrip. The fake row performs no SQL and does not claim DB integration.
func TestCompressedImportScanRestoresFormatFromDurableGeneration(t *testing.T) {
	begin, result, _ := rootFSImportTestFixture(t, "compressed-scan")
	begin.Spec.FormatGeneration = rootfsblock.CompressedFormatVersion
	begin.Spec.BlockOptions.FormatVersion = rootfsblock.CompressedFormatVersion
	begin.Spec.BlockOptions.DataRangeBytes = rootfsblock.CompressedDataRangeBytes
	spec, err := rootfsimporter.NormalizeOperationSpec(begin.Spec)
	require.NoError(t, err)
	row := compressedImportScanner{values: []any{
		begin.OperationID, spec.SourceOCIRef, result.SourceOCIDigest.String(), spec.Platform.OS, spec.Platform.Architecture,
		spec.Platform.Variant, spec.FormatGeneration, spec.ProcdProtocol, spec.ProcdDigest, spec.LogicalSizeBytes,
		spec.BlockOptions.DataRangeBytes, spec.BlockOptions.PackBytes, spec.BlockOptions.PageEntries, spec.BlockOptions.ObjectPrefix,
	}}
	restored, err := scanRootFSImportOperation(row)
	require.NoError(t, err)
	require.Equal(t, spec, restored.Spec)
	require.True(t, rootFSImportOperationMatchesSpec(restored, spec, result.SourceOCIDigest.String()))
}

func TestDurableGenerationRequiresMatchingCompressedFormat(t *testing.T) {
	for _, test := range []struct {
		descriptorVersion int
		generation        int
		valid             bool
	}{
		{1, 1, false}, {1, 3, false}, {1, 2, false},
		{2, 2, true}, {2, 1, false}, {2, 3, false},
	} {
		t.Run(fmt.Sprintf("descriptor-%d-generation-%d", test.descriptorVersion, test.generation), func(t *testing.T) {
			base := readyRootFSBaseArtifactTestRequest()
			descriptor, err := rootfsblock.DecodeDescriptor(base.Descriptor)
			require.NoError(t, err)
			descriptor.Version = test.descriptorVersion
			descriptor.MappingRoot.Version = test.descriptorVersion
			// Use raw JSON to exercise rejection of retired stored descriptors;
			// the current encoder itself no longer permits Format1 output.
			payload, err := json.Marshal(descriptor)
			require.NoError(t, err)
			generation := &RootFSGeneration{
				ID: "compressed-generation", FilesystemID: "compressed-filesystem", ParentGenerationID: "parent",
				SourceOCIDigest: base.SourceOCIDigest, BaseArtifactDigest: base.ArtifactDigest,
				BaseBlockRoot: base.BaseBlockRoot, CurrentBlockHead: descriptor.MappingRoot.RootDigest,
				WriterEpoch: 1, FormatGeneration: test.generation, LocatorVersion: 1,
				DurabilityState: RootFSGenerationStateS3Materialized, Descriptor: payload,
			}
			_, err = normalizeDurableRootFSGeneration(generation, "parent")
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
