package fsserver

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestLinkRejectsUnsupportedBackendWithoutLegacyFallback(t *testing.T) {
	t.Parallel()

	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-legacy": {
				VolumeID: "vol-legacy",
				TeamID:   "team-a",
				Backend:  "legacy",
			},
		},
	}, nil, nil)

	_, err := server.Link(authContext("team-a", ""), &pb.LinkRequest{
		VolumeId:  "vol-legacy",
		Inode:     2,
		NewParent: 1,
		NewName:   "linked",
	})
	if fserror.CodeOf(err) != fserror.FailedPrecondition {
		t.Fatalf("Link() error code = %v, want FailedPrecondition (err=%v)", fserror.CodeOf(err), err)
	}
}
