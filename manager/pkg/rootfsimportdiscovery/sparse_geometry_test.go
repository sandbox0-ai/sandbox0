package rootfsimportdiscovery

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/stretchr/testify/require"
)

func TestSparseDiscoveryBindsInitialSizesToDistinctOperations(t *testing.T) {
	image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
	imports := &fakeImports{}
	worker, err := New(Config{
		Sources: &fakeSources{items: []templatestore.ImageSource{
			{Cursor: templatestore.ImageSourceCursor{Scope: "team", TeamID: "team-1", TemplateID: "a"}, Image: image},
			{Cursor: templatestore.ImageSourceCursor{Scope: "team", TeamID: "team-1", TemplateID: "b"}, Image: image, EphemeralStorage: "16Gi"},
		}},
		Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}},
		FormatGeneration: 2, ProcdProtocol: "procd-http-v1", ProcdDigest: "sha256:" + strings.Repeat("b", 64),
		SparseDeviceSizeBytes: 64 << 40,
	})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, result.Ensured)
	require.Len(t, imports.begun, 2)
	require.NotEqual(t, imports.begun[0].OperationID, imports.begun[1].OperationID)
	for index, initial := range []int64{8 << 30, 16 << 30} {
		require.Equal(t, int64(64<<40), imports.begun[index].Spec.LogicalSizeBytes)
		require.Equal(t, initial, imports.begun[index].Spec.InitialFilesystemSizeBytes)
		require.Equal(t, initial, imports.lookups[index].InitialFilesystemSizeBytes)
	}
}
