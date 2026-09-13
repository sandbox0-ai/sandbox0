package rootfsblock

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMappingPolicyCannotLeakIntoIncrementalPublication(t *testing.T) {
	store := &buildTestStore{objects: map[string][]byte{}}
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(make([]byte, LogicalBlockSize)), LogicalBlockSize, store, BuildOptions{FormatVersion: 2})
	require.NoError(t, err)
	before := len(store.objects)
	options := BuildOptions{MappingGroupPolicy: ContiguousMappingV1}
	_, err = BuildIncrementalGeneration(t.Context(), store, base.Descriptor, nil, store, options)
	require.ErrorContains(t, err, "materialized image imports only")
	_, err = BuildIncrementalGenerationsBatch(t.Context(), store, []BatchIncrementalInput{{ID: "snapshot", Descriptor: base.Descriptor}}, store, options)
	require.ErrorContains(t, err, "materialized image imports only")
	require.Len(t, store.objects, before)
}
