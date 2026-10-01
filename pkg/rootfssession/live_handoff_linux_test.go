//go:build linux

package session

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type liveTestDevice struct {
	Device
	file                *os.File
	prepared, committed bool
	mu                  sync.Mutex
}

func (d *liveTestDevice) PrepareHandoff(context.Context) (*os.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prepared = true
	return os.Open(d.file.Name())
}
func (d *liveTestDevice) AbortHandoff() { d.mu.Lock(); defer d.mu.Unlock(); d.prepared = false }
func (d *liveTestDevice) CommitHandoff() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.committed = true
	return nil
}

type liveTestHost struct {
	*fakeHostRuntime
	adopted      int
	preflightErr error
}

func (h *liveTestHost) ValidateLiveDevice(string, string, string, string, int64) error {
	return h.preflightErr
}

func (h *liveTestHost) AdoptLiveDevice(_ context.Context, path, allocation, xfs, merged string, file *os.File, branch rootfsblock.WritableBlockDevice) (Device, error) {
	h.adopted++
	return &fakeDevice{path: path, runtime: h.fakeHostRuntime}, nil
}

func TestLiveManagerTransfersCompleteJournalWithoutUnmountOrRetirement(t *testing.T) {
	source, runtime, stage := newTestManager(t, "live-transfer")
	_, err := source.Ensure(t.Context(), stage)
	require.NoError(t, err)
	consumer := ConsumerRegistration{LeaseID: "driver", ActiveKey: "task", ContainerID: "guest", StableMount: "/task/root", HostMountNamespace: "mnt:[123]", LeaseExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano), RenewalProtocol: 1, OwnerProcess: "driver-process"}
	require.NoError(t, source.RegisterConsumer(stage.Parent, stage.Identity, consumer))
	file, err := os.CreateTemp(t.TempDir(), "fake-device-socket")
	require.NoError(t, err)
	defer file.Close()
	device := &liveTestDevice{Device: source.live[stage.Parent].device, file: file}
	source.live[stage.Parent].device = device
	branch := source.live[stage.Parent].branch
	_, err = branch.WriteAt([]byte("old-owner"), 0)
	require.NoError(t, err)
	first, err := source.PrepareLiveHandoff(t.Context())
	require.NoError(t, err)
	first.Abort()
	first.Abort()
	require.False(t, device.prepared)
	_, err = branch.WriteAt([]byte("still-serving"), rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	prepared, err := source.PrepareLiveHandoff(t.Context())
	require.NoError(t, err)
	defer func() {
		for _, file := range prepared.Files {
			_ = file.Close()
		}
	}()
	require.Len(t, prepared.Files, 2)
	durable, err := source.load(stage.Parent)
	require.NoError(t, err)
	statePath := source.db.Path()
	preflightHost := &liveTestHost{fakeHostRuntime: runtime}
	preflightConfig := Config{BranchRoot: source.branchRoot, MountRoot: source.mountRoot, Source: source.source, Runtime: preflightHost}
	require.NoError(t, PreflightLiveHandoff(t.Context(), preflightConfig, prepared.Manifest, prepared.Files))
	require.False(t, device.committed, "candidate preflight must preserve source ownership")
	require.Zero(t, preflightHost.adopted)
	preflightHost.preflightErr = fmt.Errorf("exact overlay is absent")
	require.ErrorContains(t, PreflightLiveHandoff(t.Context(), preflightConfig, prepared.Manifest, prepared.Files), "overlay is absent")
	preflightHost.preflightErr = nil
	invalid := prepared.Manifest
	invalid.Sessions = append([]LiveHandoffSession(nil), invalid.Sessions...)
	invalid.Sessions[0].Record = []byte(`{}`)
	require.ErrorContains(t, PreflightLiveHandoff(t.Context(), preflightConfig, invalid, prepared.Files), "record changed")
	invalidFiles := append([]*os.File(nil), prepared.Files...)
	invalidFiles[1] = file
	require.Error(t, PreflightLiveHandoff(t.Context(), preflightConfig, prepared.Manifest, invalidFiles))
	require.False(t, device.committed)
	require.NoError(t, prepared.Commit())
	require.True(t, device.committed)
	require.NoError(t, source.Close())
	for _, call := range runtime.callsSnapshot() {
		require.NotContains(t, call, "unmount")
	}
	host := &liveTestHost{fakeHostRuntime: runtime}
	candidate, err := New(Config{StatePath: statePath, BranchRoot: source.branchRoot, MountRoot: source.mountRoot, Source: source.source, Publisher: source.publisher, Runtime: host})
	require.NoError(t, err)
	defer candidate.Close()
	wrong := prepared.Manifest
	wrong.Sessions = nil
	require.Error(t, candidate.AdoptLiveHandoff(t.Context(), wrong, nil))
	require.Zero(t, host.adopted)
	wrong = prepared.Manifest
	wrong.Sessions = append([]LiveHandoffSession(nil), wrong.Sessions...)
	wrong.Sessions[0].RecordDigest = "changed"
	require.Error(t, candidate.AdoptLiveHandoff(t.Context(), wrong, prepared.Files))
	require.Zero(t, host.adopted)
	require.NoError(t, candidate.AdoptLiveHandoff(t.Context(), prepared.Manifest, prepared.Files))
	require.Equal(t, 1, host.adopted)
	after, err := candidate.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, durable, after, "handoff is not retirement, crash abandonment, or a new writer")
	actual := make([]byte, len("still-serving"))
	_, err = candidate.live[stage.Parent].branch.ReadAt(actual, rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, "still-serving", string(actual))
	_, err = candidate.live[stage.Parent].branch.WriteAt([]byte("new-owner"), 0)
	require.NoError(t, err)
	require.NoError(t, candidate.live[stage.Parent].branch.Flush())
}

func TestLiveManagerRejectsLegacyConsumersAndMissingOwnersBeforePause(t *testing.T) {
	manager, _, stage := newTestManager(t, "legacy-live")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	consumer := ConsumerRegistration{LeaseID: "driver", ActiveKey: "task", ContainerID: "guest", StableMount: "/task/root", HostMountNamespace: "mnt:[123]", LeaseExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}
	require.NoError(t, manager.RegisterConsumer(stage.Parent, stage.Identity, consumer))
	_, err = manager.PrepareLiveHandoff(t.Context())
	require.ErrorContains(t, err, "cannot retry a live update")
	require.False(t, manager.handoff)
	manager.mu.Lock()
	live := manager.live[stage.Parent]
	delete(manager.live, stage.Parent)
	manager.mu.Unlock()
	_, err = manager.PrepareLiveHandoff(t.Context())
	require.ErrorContains(t, err, "owner set is incomplete")
	manager.mu.Lock()
	manager.live[stage.Parent] = live
	manager.mu.Unlock()
	require.False(t, manager.handoff)
}
