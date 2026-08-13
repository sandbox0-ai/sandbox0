package rootfs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportRootFSImageCapturesOpaqueCompleteOCIView(t *testing.T) {
	merged := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(merged, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(merged, "etc", "user-image"), []byte("present"), 0o644))
	base, baseConfig := testRootFSBase(t)
	info := rootFSInfo("gvisor")
	info.BaseImageDigest = "sha256:user-source"
	runtime := &fakeV3Runtime{
		fakeRuntime: &fakeRuntime{info: info}, upperdir: t.TempDir(), mergedRoot: merged,
		base: base, baseConfig: baseConfig,
	}
	store := objectstore.NewMemoryStore(t.Name())
	controller := NewController(Config{Runtime: runtime, Store: store})
	response, status := controller.ImportRootFSImage(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ImportRootFSImageRequest{
		Target: rootFSTarget(), RevisionID: "tir-test", TeamID: rootfshead.PublicImageFSTeamID,
		HeadID: "imagefs-tir-test", BaseImageRef: "alpine:3.20",
	})
	require.Equal(t, http.StatusOK, status, response.Error)
	require.NoError(t, response.Reference.Validate())
	assert.Equal(t, base, response.Head.Base)
	assert.True(t, response.Head.Root.Opaque)
	assert.Equal(t, info.BaseImageDigest, response.SourceDigest)

	loaded, err := rootfsstore.LoadHead(context.Background(), store, response.Reference)
	require.NoError(t, err)
	etc, found, err := lookupV3TestEntry(context.Background(), store, response.Reference, loaded.Root, "etc")
	require.NoError(t, err)
	require.True(t, found)
	_, found, err = lookupV3TestEntry(context.Background(), store, response.Reference, etc, "user-image")
	require.NoError(t, err)
	assert.True(t, found)
}
