//go:build linux

package rootfsimporter

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// The OCI unpacker is a trusted test fixture; XFS construction, scanning,
// publication, attestation and reconstruction all use the production code.
func TestPrivilegedBlockBuilderDataLayoutAndBudgetFallback(t *testing.T) {
	if os.Getenv("SANDBOX0_PRIVILEGED_XFS_BUILDER") != "1" {
		t.Skip("set SANDBOX0_PRIVILEGED_XFS_BUILDER=1 on an isolated Linux host")
	}
	require.Zero(t, os.Geteuid())
	for _, budget := range []bool{false, true} {
		name := "preferred"
		if budget {
			name = "budget-fallback"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			work, err := os.MkdirTemp("", "s0-layout-import-")
			require.NoError(t, err)
			t.Cleanup(func() {
				out, err := exec.Command("findmnt", "-rn", "-o", "TARGET").Output()
				if err != nil {
					t.Errorf("retain %s: mount absence not proven", work)
					return
				}
				for _, p := range strings.Fields(string(out)) {
					if p == work || strings.HasPrefix(p, work+"/") {
						t.Errorf("retain mounted root %s", work)
						return
					}
				}
				require.NoError(t, os.RemoveAll(work))
			})
			f := newOCIBlockBuildFixture(t)
			f.request.Image.WorkRoot = work
			f.request.DataLayoutPolicy = XFSFileRangesV1
			f.request.BlockOptions.FormatVersion = 2
			payload := bytes.Repeat([]byte("complete immutable filesystem data\n"), 8192)
			last := ""
			f.unpacker.populate = func(root string) error {
				if err := os.WriteFile(filepath.Join(root, "program"), payload, 0o700); err != nil {
					return err
				}
				if err := os.Link(filepath.Join(root, "program"), filepath.Join(root, "alias")); err != nil {
					return err
				}
				if budget {
					for range 257 {
						last = filepath.Join(last, "d")
						if err := os.Mkdir(filepath.Join(root, last), 0o700); err != nil {
							return err
						}
					}
					return os.WriteFile(filepath.Join(root, last, "marker"), []byte("past optimization budget"), 0o600)
				}
				return nil
			}
			built, err := (BlockBuilder{Unpacker: f.unpacker, Filesystem: rootfsartifact.XFSBuilder{}, Publisher: f.publisher}).Build(ctx, f.request)
			require.NoError(t, err)
			require.Equal(t, XFSFileRangesV1, built.DataLayoutPolicy)
			fallback := ""
			if budget {
				fallback = rootfsartifact.XFSDataRangeBudgetFallback
			}
			require.Equal(t, fallback, built.DataLayoutFallback)
			proof, _, _, err := built.Attest(2, "procd-http-v1")
			require.NoError(t, err)
			require.Equal(t, fallback, proof.DataLayoutFallback)
			require.NoDirExists(t, f.unpacker.lastRoot)
			require.NoFileExists(t, f.unpacker.lastRoot+".xfs")
			reader, err := rootfsblock.NewReader(f.publisher, built.Descriptor, 4<<20)
			require.NoError(t, err)
			imagePath := filepath.Join(work, "reconstructed.xfs")
			image, err := os.OpenFile(imagePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			require.NoError(t, err)
			require.NoError(t, image.Truncate(built.LogicalSizeBytes))
			buffer, zero := make([]byte, 1<<20), make([]byte, 1<<20)
			for offset := int64(0); offset < built.LogicalSizeBytes; offset += int64(len(buffer)) {
				require.NoError(t, ctx.Err())
				n, err := reader.ReadAt(buffer, offset)
				require.NoError(t, err)
				require.Equal(t, len(buffer), n)
				if !bytes.Equal(buffer, zero) {
					_, err = image.WriteAt(buffer, offset)
					require.NoError(t, err)
				}
			}
			require.NoError(t, image.Sync())
			require.NoError(t, image.Close())
			require.NoError(t, runRootFSImporterCommand(ctx, "xfs_repair", "-n", imagePath))
			mount := filepath.Join(work, "verify")
			require.NoError(t, os.Mkdir(mount, 0o700))
			require.NoError(t, runRootFSImporterCommand(ctx, "mount", "-t", "xfs", "-o", "loop,ro,nouuid,noatime", imagePath, mount))
			mounted := true
			defer func() {
				if mounted {
					c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					require.NoError(t, runRootFSImporterCommand(c, "umount", mount))
				}
			}()
			for _, name := range []string{"program", "alias"} {
				actual, err := os.ReadFile(filepath.Join(mount, "lower", name))
				require.NoError(t, err)
				require.Equal(t, payload, actual)
			}
			if budget {
				actual, err := os.ReadFile(filepath.Join(mount, "lower", last, "marker"))
				require.NoError(t, err)
				require.Equal(t, "past optimization budget", string(actual))
			}
			require.NoError(t, runRootFSImporterCommand(ctx, "umount", mount))
			mounted = false
			t.Logf("policy=%s fallback=%q full_image_bytes=%d objects=%d bytes=%d", proof.DataLayoutPolicy, proof.DataLayoutFallback, built.LogicalSizeBytes, built.Objects, built.Bytes)
		})
	}
}
