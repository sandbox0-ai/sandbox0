package procdartifact

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestSourceAndObjectKey(t *testing.T) {
	source := Source{Endpoint: "https://oss-us-east-1-internal.aliyuncs.com", Bucket: "private-runtime"}
	require.NoError(t, source.Validate())
	for _, invalid := range []Source{
		{Endpoint: "http://oss.example", Bucket: source.Bucket},
		{Endpoint: "https://user@oss.example", Bucket: source.Bucket},
		{Endpoint: "https://oss.example/path", Bucket: source.Bucket},
		{Endpoint: source.Endpoint, Bucket: "../other"},
	} {
		require.Error(t, invalid.Validate())
	}
	a := Artifact{Digest: digest.FromString("historical procd").String(), Protocol: "sandbox0.procd.v3"}
	key, err := ObjectKey(a)
	require.NoError(t, err)
	require.Equal(t, "sandbox0-nomad-runtime/procd/sha256/"+strings.TrimPrefix(a.Digest, "sha256:")+"/procd", key)
	_, err = ObjectKey(Artifact{Digest: "sha256:../escape", Protocol: a.Protocol})
	require.Error(t, err)
}

func TestEnsureFetchesMissingVersionOnceAndFailsClosedOnCorruption(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned source and cache")
	}
	root, err := os.MkdirTemp("/root", "procd-remote-test-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cache := filepath.Join(root, "cache")
	sourceFile := filepath.Join(root, "source.json")
	require.NoError(t, os.WriteFile(sourceFile, []byte(`{"oss_endpoint":"https://oss-us-east-1-internal.aliyuncs.com","oss_bucket":"private-runtime"}`), 0600))
	body := []byte("older procd binary")
	a := Artifact{Digest: digest.FromBytes(body).String(), Protocol: "sandbox0.procd.v3"}
	var downloads atomic.Int32
	download := func(_ context.Context, _ Source, _ string, _ string, target string) error {
		downloads.Add(1)
		return os.WriteFile(target, body, 0600)
	}
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, fetchErr := ensure(context.Background(), cache, sourceFile, a, download)
			require.NoError(t, fetchErr)
		}()
	}
	group.Wait()
	require.EqualValues(t, 1, downloads.Load())
	path, err := Resolve(cache, a)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0755))
	_, err = ensure(context.Background(), cache, sourceFile, a, download)
	require.ErrorContains(t, err, "immutable")
	require.EqualValues(t, 1, downloads.Load())
}
