package rootfsimportworker

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestDurableBuilderUsesPersistedLayoutPolicy(t *testing.T) {
	builder, err := NewDurableBuilder(DurableBuilderConfig{Store: &fakeStore{}, Unpacker: unusedUnpacker{}, Filesystem: unusedFilesystem{}, Publisher: &recordingPublisher{}, WorkRoot: "/owned/import", ProcdPath: "/owned/procd"})
	require.NoError(t, err)
	op := testOperation(1)
	op.Spec.FormatGeneration = 2
	op.Spec.BlockOptions = rootfsblock.BuildOptions{}
	op.Spec.DataLayoutPolicy = rootfsimporter.XFSFileRangesV1
	op.Spec, err = rootfsimporter.NormalizeOperationSpec(op.Spec)
	require.NoError(t, err)
	lease, err := op.Lease()
	require.NoError(t, err)
	called := false
	builder.build = func(_ context.Context, _ rootfsimporter.BlockBuilder, request rootfsimporter.BuildRequest) (rootfsimporter.BuildResult, error) {
		called = true
		require.Equal(t, op.Spec.DataLayoutPolicy, request.DataLayoutPolicy)
		require.Equal(t, op.Spec.BlockOptions, request.BlockOptions)
		return rootfsimporter.BuildResult{}, nil
	}
	_, err = builder.Build(t.Context(), op, lease)
	require.NoError(t, err)
	require.True(t, called)
}
