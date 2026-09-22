//go:build linux

package gvisorcli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsobjectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This opt-in experiment owns a unique capture prefix, runsc root and cgroup.
// It measures actual stock checkpoint/encrypted publication overlap against the
// legacy planned publisher. Its static fixture filesystem is not a regional
// RootFS transaction, so these results cannot establish full migration latency.
// Object credentials are read only by the host test, never added to guest env.
func startCheckpointUploadProbe(t *testing.T, ctx context.Context, directory string) func(time.Time) string {
	mode := os.Getenv("SANDBOX0_CAPTURE_UPLOAD_PROBE_MODE")
	if mode == "" {
		return func(time.Time) string { return directory }
	}
	require.Contains(t, []string{"legacy", "growing", "growing-peer", "growing-peer-full"}, mode)
	path := os.Getenv("SANDBOX0_CAPTURE_UPLOAD_PROBE_CONFIG")
	require.NotEmpty(t, path)
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var cfg struct {
		Objects config.RootFSObjectStorageConfig `yaml:"rootfs_object_storage"`
	}
	require.NoError(t, yaml.Unmarshal(payload, &cfg))
	require.True(t, cfg.Objects.ObjectEncryptionEnabled, "probe must retain regional object encryption")
	objects, err := rootfsobjectstore.Create(cfg.Objects, nil)
	require.NoError(t, err)
	store, err := runtimecheckpoint.New(objects, 512<<20)
	require.NoError(t, err)
	collector, err := runtimecheckpoint.NewCollector(objects)
	require.NoError(t, err)
	id := "capture-upload-probe-" + filepath.Base(filepath.Dir(directory))
	source, assignment := checkpointUploadProbeIdentity(id)
	d := digest.FromString("isolated-static-fixture").String()
	scope, err := runtimecheckpoint.BindCapture(id, source, assignment, d, d)
	require.NoError(t, err)
	sourceDigest, err := source.BindingDigest()
	require.NoError(t, err)
	revision, err := assignment.Revision()
	require.NoError(t, err)
	binding := runtimecheckpoint.Binding{OperationID: id, SandboxID: assignment.SandboxID, TeamID: assignment.TeamID,
		SourceBindingDigest:        digest.NewDigestFromBytes(digest.SHA256, sourceDigest[:]).String(),
		RuntimeCompatibilityDigest: d, AssignmentRevision: "sha256:" + revision, CPUFeaturesDigest: d,
		RootFSGenerationID: "isolated-static-rootfs", RootFSDescriptorDigest: d}
	require.NoError(t, binding.Validate())
	var stage *runtimecheckpoint.CaptureStager
	var peer *checkpointPeerUploadProbe
	var stopOnce sync.Once
	stop := func() {}
	var uploadErr error
	t.Cleanup(func() {
		stop()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for page := 0; page < 16; page++ {
			var done bool
			var err error
			if mode != "legacy" {
				done, err = collector.CollectCapture(cleanupCtx, scope)
			} else {
				done, err = collector.Collect(cleanupCtx, binding)
			}
			require.NoError(t, err)
			if done {
				t.Logf("capture_upload_cleanup mode=%s complete=true", mode)
				return
			}
		}
		t.Error("isolated capture prefix was not fully collected")
	})
	if mode != "legacy" {
		stage, err = store.OpenCaptureStaging(ctx, scope, 512<<20)
		require.NoError(t, err)
		growingCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		if mode == "growing-peer" || mode == "growing-peer-full" {
			peer = newCheckpointPeerUploadProbe(t, ctx, scope, binding, directory+"-peer")
		}
		if mode == "growing-peer" {
			go func() { done <- stage.UploadGrowingWithPeer(growingCtx, directory, peer.peer.WriteChunk) }()
		} else {
			go func() { done <- stage.UploadGrowing(growingCtx, directory) }()
		}
		stop = func() {
			stopOnce.Do(func() {
				cancel()
				if peer != nil {
					uploadErr = peer.stop()
				}
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) && (peer == nil || !errors.Is(err, io.ErrClosedPipe)) {
					uploadErr = errors.Join(uploadErr, err)
				}
			})
		}
	}
	return func(started time.Time) string {
		remainingStarted := time.Now()
		stop()
		if uploadErr != nil && !errors.Is(uploadErr, context.Canceled) {
			require.NoError(t, uploadErr)
		}
		var plan runtimecheckpoint.LocalImagePlan
		if mode != "legacy" {
			plan, err = stage.PlanLocal(ctx, binding, directory)
		} else {
			plan, err = store.PlanLocal(ctx, binding, directory)
		}
		require.NoError(t, err)
		if peer != nil {
			return finishCheckpointPeerUploadProbe(t, ctx, peer, store, stage, binding, plan, directory, started, remainingStarted)
		}
		var ref runtimecheckpoint.Reference
		if mode != "legacy" {
			ref, err = stage.PublishPlanned(ctx, binding, plan, directory)
		} else {
			ref, err = store.PublishPlanned(ctx, binding, plan, directory)
		}
		require.NoError(t, err)
		duration, remaining := time.Since(started), time.Since(remainingStarted)
		require.Equal(t, plan.Reference, ref)
		// Restore from the downloaded encrypted image, not the original local
		// image. Download latency is reported separately from the capture/upload
		// comparison; this probe does not claim an end-to-end migration result.
		downloadStarted := time.Now()
		downloaded := directory + "-downloaded"
		_, err = store.Download(ctx, binding, ref, downloaded)
		require.NoError(t, err)
		downloadDuration := time.Since(downloadStarted)
		var size int64
		for _, file := range plan.Manifest.Files {
			size += file.Size
		}
		t.Logf("capture_upload mode=%s image_bytes=%d capture_sync_publish_us=%d remaining_publish_us=%d download_us=%d", mode, size, duration.Microseconds(), remaining.Microseconds(), downloadDuration.Microseconds())
		return downloaded
	}
}

func checkpointUploadProbeIdentity(id string) (rootfshandoff.StageRequest, runtimecontrol.Assignment) {
	d := digest.FromString("isolated-static-fixture").String()
	source := rootfshandoff.StageRequest{BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent: d, InitialGeneration: "initial", Identity: rootfshandoff.Identity{
			NodeUID: id, BootID: "probe-boot", RuntimeGeneration: "1", AllocationID: id,
			NetworkIncarnationID: "network", TaskName: "probe", SourceOCIDigest: d,
			RootFSDriver: "nomad-driver", RuntimeClass: "sandbox0-gvisor", SlotNonce: "nonce",
			ClaimID: id, LaunchAttempt: id, RootFSID: id, WriterEpoch: 1,
			WriterGrantID: id, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest("isolated-probe"),
		}, ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: id, NetworkIncarnationID: "network", ClaimID: id,
			NetworkEpoch: 1, PolicyDigest: d, SourceIP: "10.0.0.2", CtldGeneration: "probe", NetNSIdentity: "netns",
		}}
	return source, runtimecontrol.Assignment{SandboxID: id, TeamID: "isolated-probe", RuntimeGeneration: 1, SecurityClass: "standard"}
}
