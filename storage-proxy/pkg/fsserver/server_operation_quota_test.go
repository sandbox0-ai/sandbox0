package fsserver

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/router"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type recordingStorageOperationQuota struct {
	teamIDs []string
	err     error
}

func (q *recordingStorageOperationQuota) Admit(_ context.Context, teamID string) error {
	q.teamIDs = append(q.teamIDs, teamID)
	return q.err
}

func (*recordingStorageOperationQuota) Close() error { return nil }

type operationQuotaVolumeRepository struct {
	volumes map[string]*db.SandboxVolume
}

func (r *operationQuotaVolumeRepository) GetSandboxVolume(_ context.Context, id string) (*db.SandboxVolume, error) {
	vol, ok := r.volumes[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return vol, nil
}

func (*operationQuotaVolumeRepository) GetSandboxVolumeOwner(context.Context, string) (*db.SandboxVolumeOwner, error) {
	return nil, db.ErrNotFound
}

func TestFileSystemServerAdmitsStorageOperationForVolumeOwner(t *testing.T) {
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": newMountedS0FSVolumeContext(t, "vol-1", "team-a"),
		},
	}, nil, nil)
	quota := &recordingStorageOperationQuota{}
	server.SetStorageOperationQuota(quota)

	if _, err := server.GetAttr(authContext("team-a", "sandbox-a"), &pb.GetAttrRequest{
		VolumeId: "vol-1",
		Inode:    1,
	}); err != nil {
		t.Fatalf("GetAttr() error = %v", err)
	}
	if len(quota.teamIDs) != 1 || quota.teamIDs[0] != "team-a" {
		t.Fatalf("admitted teams = %v, want [team-a]", quota.teamIDs)
	}
}

func TestOpenTruncateConsumesOneStorageOperation(t *testing.T) {
	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	node, err := volCtx.S0FS.CreateFile(s0fs.RootInode, "truncate.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)
	quota := &recordingStorageOperationQuota{}
	server.SetStorageOperationQuota(quota)

	if _, err := server.Open(authContext("team-a", "sandbox-a"), &pb.OpenRequest{
		VolumeId: "vol-1",
		Inode:    node.Inode,
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}); err != nil {
		t.Fatalf("Open(O_TRUNC) error = %v", err)
	}
	if len(quota.teamIDs) != 1 || quota.teamIDs[0] != "team-a" {
		t.Fatalf("admitted teams = %v, want exactly [team-a]", quota.teamIDs)
	}
}

func TestMountVolumeRemotePrimaryDoesNotConsumeStorageOperation(t *testing.T) {
	volMgr := &fakeVolumeManager{}
	server := newTestFileSystemServer(volMgr, &operationQuotaVolumeRepository{
		volumes: map[string]*db.SandboxVolume{
			"vol-1": {
				ID:         "vol-1",
				TeamID:     "team-a",
				AccessMode: string(volume.AccessModeRWO),
			},
		},
	}, nil)
	quota := &recordingStorageOperationQuota{}
	server.SetStorageOperationQuota(quota)
	volumeRouter := router.NewVolumeRouter()
	volumeRouter.SetRoute(router.Route{
		VolumeID:      "vol-1",
		PrimaryNodeID: "node-b",
		PrimaryAddr:   "10.0.0.2:8080",
		Epoch:         2,
		LocalPrimary:  false,
	})
	server.SetVolumeRouter(volumeRouter)

	_, err := server.MountVolume(authContext("team-a", "sandbox-a"), &pb.MountVolumeRequest{
		VolumeId: "vol-1",
	})
	if redirect := fserror.RedirectOf(err); redirect == nil {
		t.Fatalf("MountVolume() redirect = nil, want remote primary redirect (err=%v)", err)
	}
	if len(quota.teamIDs) != 0 {
		t.Fatalf("admitted teams = %v, want no admission for redirect", quota.teamIDs)
	}
	if volMgr.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want 0", volMgr.mountCalls)
	}
}

func TestMountVolumeLocalPrimaryConsumesOneStorageOperation(t *testing.T) {
	volMgr := &fakeVolumeManager{}
	server := newTestFileSystemServer(volMgr, &operationQuotaVolumeRepository{
		volumes: map[string]*db.SandboxVolume{
			"vol-1": {
				ID:         "vol-1",
				TeamID:     "team-a",
				AccessMode: string(volume.AccessModeRWO),
			},
		},
	}, nil)
	quota := &recordingStorageOperationQuota{}
	server.SetStorageOperationQuota(quota)

	if _, err := server.MountVolume(authContext("team-a", "sandbox-a"), &pb.MountVolumeRequest{
		VolumeId: "vol-1",
	}); err != nil {
		t.Fatalf("MountVolume() error = %v", err)
	}
	if len(quota.teamIDs) != 1 || quota.teamIDs[0] != "team-a" {
		t.Fatalf("admitted teams = %v, want exactly [team-a]", quota.teamIDs)
	}
	if volMgr.mountCalls != 1 {
		t.Fatalf("mount calls = %d, want 1", volMgr.mountCalls)
	}
}

func TestFileSystemServerMapsStorageOperationQuotaFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code fserror.Code
	}{
		{
			name: "exhausted",
			err: &teamquota.RateExceededError{
				TeamID:     "team-a",
				Key:        teamquota.KeyStorageOperations,
				RetryAfter: time.Second,
			},
			code: fserror.ResourceExhausted,
		},
		{
			name: "unavailable",
			err: &teamquota.UnavailableError{
				Operation: "admit storage operation",
				Err:       context.DeadlineExceeded,
			},
			code: fserror.Unavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestFileSystemServer(&fakeVolumeManager{
				volumes: map[string]*volume.VolumeContext{
					"vol-1": newMountedS0FSVolumeContext(t, "vol-1", "team-a"),
				},
			}, nil, nil)
			server.SetStorageOperationQuota(&recordingStorageOperationQuota{err: test.err})

			_, err := server.GetAttr(authContext("team-a", "sandbox-a"), &pb.GetAttrRequest{
				VolumeId: "vol-1",
				Inode:    1,
			})
			if got := fserror.CodeOf(err); got != test.code {
				t.Fatalf("GetAttr() code = %v, want %v (err=%v)", got, test.code, err)
			}
		})
	}
}
