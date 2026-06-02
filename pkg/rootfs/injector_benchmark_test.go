package rootfs

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

func BenchmarkPrepareAndRewriteOverlayMounts(b *testing.B) {
	client := &recordingPrepareClient{
		response: &ctldapi.PrepareRootFSResponse{
			Prepared: true,
			UpperDir: "/var/lib/sandbox0/ctld/rootfs/team-a/sandbox-a/rootfs-a/s0fs/upper",
			WorkDir:  "/var/lib/sandbox0/ctld/rootfs/team-a/sandbox-a/rootfs-a/s0fs/work",
		},
	}
	meta := Metadata{
		SandboxID: "sandbox-a",
		TeamID:    "team-a",
		Mode:      ModeS0FSUpperdir,
		VolumeID:  "rootfs-a",
		CtldPort:  8095,
	}
	mounts := []Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: []string{"index=off", "lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"},
	}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := PrepareAndRewriteOverlayMounts(context.Background(), client, "", meta, mounts)
		if err != nil {
			b.Fatal(err)
		}
		if !result.Rewritten {
			b.Fatal("rootfs mount was not rewritten")
		}
	}
}
