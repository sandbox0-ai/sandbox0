package portal

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	fsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

type fakeMountedVolumeRepo struct {
	volumes map[string]*db.SandboxVolume
}

func (r *fakeMountedVolumeRepo) GetSandboxVolume(_ context.Context, id string) (*db.SandboxVolume, error) {
	if r != nil && r.volumes != nil {
		if vol, ok := r.volumes[id]; ok {
			return vol, nil
		}
	}
	return nil, db.ErrNotFound
}

func (r *fakeMountedVolumeRepo) GetSandboxVolumeOwner(context.Context, string) (*db.SandboxVolumeOwner, error) {
	return nil, db.ErrNotFound
}

type fakeMountedVolumeAttacher struct {
	volumes *localVolumeManager
	calls   int
	lastReq ctldapi.AttachVolumeOwnerRequest
}

func (a *fakeMountedVolumeAttacher) AttachOwner(_ context.Context, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, error) {
	a.calls++
	a.lastReq = req
	a.volumes.add(&volume.VolumeContext{
		VolumeID: req.SandboxVolumeID,
		TeamID:   req.TeamID,
		Access:   volume.AccessModeRWO,
	})
	return ctldapi.AttachVolumeOwnerResponse{Attached: true}, nil
}

func TestMountedVolumeFileRPCMountVolumeAttachesMissingSourceVolume(t *testing.T) {
	mgr := newLocalVolumeManager()
	repo := &fakeMountedVolumeRepo{volumes: map[string]*db.SandboxVolume{
		"vol-source": {
			ID:         "vol-source",
			TeamID:     "team-a",
			AccessMode: string(volume.AccessModeRWO),
		},
	}}
	attacher := &fakeMountedVolumeAttacher{volumes: mgr}
	logger := logrus.New()
	fs := fsserver.NewFileSystemServer(mgr, repo, nil, nil, logger, nil, nil)
	rpc := &mountedVolumeFileRPC{FileSystemServer: fs, volumes: mgr, attacher: attacher}
	ctx := internalauth.WithClaims(context.Background(), &internalauth.Claims{
		TeamID:   "team-a",
		IsSystem: true,
	})

	resp, err := rpc.MountVolume(ctx, &pb.MountVolumeRequest{VolumeId: "vol-source"})
	if err != nil {
		t.Fatalf("MountVolume() error = %v", err)
	}
	if resp == nil || resp.MountSessionId != "local-vol-source" {
		t.Fatalf("MountVolume() response = %+v, want local session", resp)
	}
	if attacher.calls != 1 {
		t.Fatalf("AttachOwner() calls = %d, want 1", attacher.calls)
	}
	if attacher.lastReq.TeamID != "team-a" || attacher.lastReq.SandboxVolumeID != "vol-source" {
		t.Fatalf("AttachOwner() request = %+v", attacher.lastReq)
	}
	if _, err := mgr.GetVolume("vol-source"); err != nil {
		t.Fatalf("source volume was not mounted locally: %v", err)
	}
}
