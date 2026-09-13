package rootfsimportdiscovery

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/stretchr/testify/require"
)

func TestDiscoverySeparatesMappingSelectionAndDurableIdentity(t *testing.T) {
	image := "registry.example/sandbox@" + digest.FromString("discovery-layout").String()
	platform := sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"}
	var ids []string
	for _, policy := range []string{"", rootfsblock.ContiguousMappingV1} {
		imports := &fakeImports{}
		w, err := New(Config{Sources: &fakeSources{items: []templatestore.ImageSource{{Cursor: templatestore.ImageSourceCursor{Scope: "public", TemplateID: "layout"}, Image: image}}},
			Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{platform}, FormatGeneration: 2, BlockOptions: rootfsblock.BuildOptions{MappingGroupPolicy: policy}, ProcdProtocol: "procd-http-v1", ProcdDigest: digest.FromString("procd").String()})
		require.NoError(t, err)
		result, err := w.RunOnce(t.Context())
		require.NoError(t, err)
		require.Equal(t, 1, result.Ensured)
		require.Len(t, imports.lookups, 1)
		require.Len(t, imports.begun, 1)
		require.Equal(t, policy, imports.lookups[0].ImportMappingGroupPolicy)
		require.Equal(t, policy, imports.begun[0].Spec.BlockOptions.MappingGroupPolicy)
		require.Equal(t, 64<<10, imports.begun[0].Spec.BlockOptions.DataRangeBytes)
		ids = append(ids, imports.begun[0].OperationID)
	}
	require.NotEqual(t, ids[0], ids[1])
}
