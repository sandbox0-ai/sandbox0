//go:build linux

package gvisorcli

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRootFSPrunePathsRetainsBundleAndFindsBindAliases(t *testing.T) {
	data := []byte(`1 0 8:1 / / rw - ext4 /dev/sda rw
2 1 43:0 / /runtime/a/xfs rw - xfs /dev/nbd0 rw
3 2 43:0 /lower /runtime/a/xfs/lower ro - xfs /dev/nbd0 ro
4 1 0:40 / /runtime/a/merged rw - overlay overlay rw,lowerdir=/runtime/a/xfs/lower
5 1 0:40 / /alloc/old/rootfs rw - overlay overlay rw
6 1 43:0 / /external/xfs-alias rw - xfs /dev/nbd0 rw
7 1 43:1 / /runtime/b/xfs rw - xfs /dev/nbd1 rw
8 1 0:41 / /runtime/b/merged rw - overlay overlay rw
9 1 0:41 / /alloc/current/rootfs rw - overlay overlay rw
10 9 0:9 / /alloc/current/rootfs/proc rw - proc proc rw
11 1 0:1 / /run rw - tmpfs tmpfs rw
12 1 0:1 / /runtime/temporary rw - tmpfs tmpfs rw
13 1 43:0 / /extra-bind rw - xfs /dev/nbd0 rw
`)
	paths, err := rootFSPrunePaths(data, "/runtime", []string{"/alloc/current/rootfs", "/extra-bind"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"/runtime/a/xfs", "/runtime/a/xfs/lower", "/runtime/a/merged", "/alloc/old/rootfs", "/external/xfs-alias", "/runtime/b/xfs", "/runtime/b/merged", "/runtime/temporary"}, paths)
	require.Equal(t, "/runtime/a/xfs/lower", paths[0], "children must be unmounted first")
	require.NotContains(t, paths, "/run", "tmpfs aliases are not RootFS bind aliases")
}

func TestRootFSPrunePathsEscapesAndRejectsMalformedInput(t *testing.T) {
	paths, err := rootFSPrunePaths([]byte(`1 0 8:1 / / rw - ext4 /dev/sda rw
2 1 43:0 / /runtime/a\040b rw - xfs /dev/nbd0 rw
`), "/runtime", []string{"/bundle/rootfs"})
	require.NoError(t, err)
	require.Equal(t, []string{"/runtime/a b"}, paths)
	_, err = rootFSPrunePaths([]byte("malformed"), "/runtime", nil)
	require.Error(t, err)
}
