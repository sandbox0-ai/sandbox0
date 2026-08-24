package rootfsblock_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// TestRustFSBatchMaterializationTenThousandTinyGenerations proves physical
// object growth follows pack bytes rather than generation cardinality. It is
// opt-in because it requires an isolated real RustFS endpoint and bucket.
func TestRustFSBatchMaterializationTenThousandTinyGenerations(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"))
	if endpoint == "" {
		t.Skip("set SANDBOX0_RUSTFS_ENDPOINT to an isolated RustFS endpoint")
	}
	bucket := strings.TrimSpace(os.Getenv("SANDBOX0_RUSTFS_BUCKET"))
	if bucket == "" {
		bucket = "sandbox0-materializer-test"
	}
	store, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"),
		SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY"),
	})
	require.NoError(t, err)
	conditional, ok := store.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	require.True(t, objectstore.SupportsContextConditionalCreate(store))
	if err := store.Create(); err != nil {
		require.Contains(t, strings.ToLower(err.Error()), "already")
	}
	prefix := fmt.Sprintf("rootfs/integration/materializer-10k/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, key := range listRustFSIntegrationKeys(t, store, prefix) {
			require.NoError(t, store.Delete(key))
		}
	})
	baselineFiles, baselineBytes := rustFSIntegrationDirectoryUsage(t)
	publisher := rootfsblock.ObjectStorePublisher{Store: conditional}
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, publisher,
		rootfsblock.BuildOptions{ObjectPrefix: prefix + "/base"},
	)
	require.NoError(t, err)
	inputs := make([]rootfsblock.BatchIncrementalInput, 10_000)
	for index := range inputs {
		_, payload, buildErr := rootfsblock.BuildCompositeGeneration(
			base.Descriptor,
			[]rootfsblock.BlockUpdate{{
				Sequence: 1, Block: 0,
				Data: bytes.Repeat([]byte{byte(index%251 + 1)}, rootfsblock.LogicalBlockSize),
			}},
		)
		require.NoError(t, buildErr)
		descriptor, decodeErr := rootfsblock.DecodeDescriptor(payload)
		require.NoError(t, decodeErr)
		inputs[index] = rootfsblock.BatchIncrementalInput{
			ID: fmt.Sprintf("generation-%05d", index), Descriptor: descriptor,
		}
	}
	built, err := rootfsblock.BuildIncrementalGenerationsBatch(
		t.Context(), conditional, inputs, publisher,
		rootfsblock.BuildOptions{
			DataRangeBytes: rootfsblock.LogicalBlockSize,
			PackBytes:      rootfsblock.DefaultPackBytes,
			ObjectPrefix:   prefix + "/shared",
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, built.Objects)
	require.Len(t, built.Results, 10_000)
	keys := listRustFSIntegrationKeys(t, store, prefix)
	require.Len(t, keys, 3, "one Base map, one shared data pack, and one shared mapping pack")

	for _, index := range []int{0, 4_999, 9_999} {
		result := built.Results[fmt.Sprintf("generation-%05d", index)]
		reader, readErr := rootfsblock.NewReader(conditional, result.Descriptor, rootfsblock.DefaultReadCacheBytes)
		require.NoError(t, readErr)
		actual := make([]byte, rootfsblock.LogicalBlockSize)
		_, readErr = reader.ReadAt(actual, 0)
		require.NoError(t, readErr)
		require.Equal(t, bytes.Repeat([]byte{byte(index%251 + 1)}, rootfsblock.LogicalBlockSize), actual)
	}
	peakFiles, peakBytes := rustFSIntegrationDirectoryUsage(t)
	if baselineFiles >= 0 {
		fileGrowth := peakFiles - baselineFiles
		byteGrowth := peakBytes - baselineBytes
		t.Logf("RustFS 10k packed growth: objects=%d files=%d bytes=%d", len(keys), fileGrowth, byteGrowth)
		require.LessOrEqual(t, fileGrowth, int64(64), "physical file growth must be pack-bounded")
		require.LessOrEqual(t, byteGrowth, int64(128<<20), "physical byte growth must remain near packed payload bytes")
	}
}

func listRustFSIntegrationKeys(t *testing.T, store objectstore.Store, prefix string) []string {
	t.Helper()
	var result []string
	var token string
	for {
		items, truncated, next, err := store.List(prefix, "", token, "", 1000)
		require.NoError(t, err)
		for _, item := range items {
			if !item.IsPrefix {
				result = append(result, item.Key)
			}
		}
		if !truncated {
			break
		}
		require.NotEmpty(t, next)
		token = next
	}
	return result
}

func rustFSIntegrationDirectoryUsage(t *testing.T) (int64, int64) {
	t.Helper()
	root := strings.TrimSpace(os.Getenv("SANDBOX0_RUSTFS_DATA_DIR"))
	if root == "" {
		return -1, -1
	}
	var files, size int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files++
			size += info.Size()
		}
		return nil
	})
	require.NoError(t, err)
	return files, size
}
