package nomadruntime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestCapturePeerAcceptsOnlyExactAdoptedRuntimeForSubsequentMove(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed_generation_%t", changed), func(t *testing.T) {
			d, adoption, _, _ := migrationAdoptionNodeFixture(t)
			_, err := d.AdoptMigrationDestination(t.Context(), adoption)
			require.NoError(t, err)
			record, err := d.journal.Get(adoption.Target.SlotID)
			require.NoError(t, err)
			restore := record.MigrationDestination.Restore.Request
			binding, err := restore.Stage.BindingDigest()
			require.NoError(t, err)
			revision, err := restore.Image.Publication.Assignment.Target.Revision()
			require.NoError(t, err)
			resources, err := restore.Image.Resources.Digest()
			require.NoError(t, err)
			capture := protocol.MigrationCaptureRequest{Target: adoption.Target, OperationID: "next-migration", LifecycleEpoch: 4,
				SandboxID: adoption.SandboxID, SourceGeneration: adoption.RuntimeGeneration, ProcdInstanceID: adoption.ProcdInstanceID,
				AssignmentRevision: revision, BindingDigest: fmt.Sprintf("%x", binding), ResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:")}
			if changed {
				capture.SourceGeneration++
			}
			staging := migrationStagingSourceRequest(t, record.Registration)
			staging.Source, staging.Target = capture, capture.Target
			// The next destination is a new carrier, distinct from this source.
			staging.Destination = restore.Image.Publication.Capture.Request.Target
			staging.CaptureUpload, err = protocol.NewMigrationCaptureUpload(capture, "team", digest.FromString("compat").String(), digest.FromString("cpu").String(), staging.Bytes)
			require.NoError(t, err)
			identity, err := runtimecheckpoint.NewPeerIdentity()
			require.NoError(t, err)
			endpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.1:18992", identity)
			require.NoError(t, err)
			d.migrationPeer = &migrationPeer{identity: identity, endpoint: endpoint}
			local, err := d.ReserveMigrationStaging(t.Context(), staging)
			require.NoError(t, err)
			remote := staging
			remote.Target = remote.Destination
			want, err := remote.Digest()
			require.NoError(t, err)
			other, err := runtimecheckpoint.NewPeerIdentity()
			require.NoError(t, err)
			otherEndpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.2:18992", other)
			require.NoError(t, err)
			request := protocol.MigrationCapturePeerRequest{Staging: staging, Source: *local, Destination: protocol.MigrationStagingReserved{
				RequestDigest: want, PeerCertificateSHA256: runtimecheckpoint.PeerCertificateDigest(other), Peer: otherEndpoint}}
			receipt, err := d.PrepareMigrationCapturePeer(t.Context(), request)
			if changed {
				require.ErrorContains(t, err, "changed adopted runtime")
				require.Nil(t, receipt)
				return
			}
			require.NoError(t, err)
			require.NoError(t, receipt.ValidateFor(request))
			record, err = d.journal.Get(adoption.Target.SlotID)
			require.NoError(t, err)
			require.True(t, record.MigrationDestination.Adopted())
			require.NotNil(t, record.MigrationStaging.CapturePeer)
			require.NoError(t, d.journal.RecordMigrationCapture(stagingCapture(t, staging)))
		})
	}
}

func capturePeerJournalFixture(t *testing.T, source bool, images ...*protocol.MigrationImagePrepareRequest) (*nodeRuntime, protocol.MigrationCapturePeerRequest) {
	t.Helper()
	d, staging := migrationStagingReservationFixture(t)
	if !source {
		var image protocol.MigrationImagePrepareRequest
		d, image, _ = migrationImageDestinationFixture(t)
		if len(images) != 0 {
			*images[0] = image
		}
		resources, err := image.Resources.Digest()
		require.NoError(t, err)
		staging = protocol.MigrationStagingRequest{Target: image.Target, Destination: image.Target, Source: image.Publication.Capture.Request,
			DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 16 << 20, Inodes: 2048}
	}
	var err error
	staging.CaptureUpload, err = protocol.NewMigrationCaptureUpload(staging.Source, "team", digest.FromString("compat").String(), digest.FromString("cpu").String(), staging.Bytes)
	require.NoError(t, err)
	cert, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	endpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.1:18992", cert)
	require.NoError(t, err)
	d.migrationPeer = &migrationPeer{identity: cert, endpoint: endpoint}
	local, err := d.ReserveMigrationStaging(t.Context(), staging)
	require.NoError(t, err)
	remoteStaging := staging
	remoteStaging.Target = staging.Source.Target
	if source {
		remoteStaging.Target = staging.Destination
	}
	want, err := remoteStaging.Digest()
	require.NoError(t, err)
	remoteCert, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	remoteEndpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.2:18992", remoteCert)
	require.NoError(t, err)
	remote := protocol.MigrationStagingReserved{RequestDigest: want, PeerCertificateSHA256: runtimecheckpoint.PeerCertificateDigest(remoteCert), Peer: remoteEndpoint}
	r := protocol.MigrationCapturePeerRequest{Staging: staging, Source: remote, Destination: *local}
	if source {
		r.Source, r.Destination = *local, remote
	}
	require.NoError(t, r.Validate())
	return d, r
}

func TestCapturePeerJournalRetainsExactGrantAndRejectsReplacement(t *testing.T) {
	for _, source := range []bool{true, false} {
		t.Run(map[bool]string{true: "source", false: "destination"}[source], func(t *testing.T) {
			d, request := capturePeerJournalFixture(t, source)
			slot := request.Staging.Target.SlotID
			receipt, err := d.PrepareMigrationCapturePeer(t.Context(), request)
			require.NoError(t, err)
			require.NoError(t, receipt.ValidateFor(request))
			again, err := d.PrepareMigrationCapturePeer(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, receipt, again)
			record, err := d.journal.Get(slot)
			require.NoError(t, err)
			require.Equal(t, runtimeSlotCapturePeerJournalVersion, record.Version)
			require.Equal(t, !source, record.hasMigrationImageCustody())
			require.Equal(t, !source, record.retainsMigrationCountAdmission())
			require.Equal(t, source, record.MigrationStaging.CapturePeer.ImageDirectory == "")
			changed := request
			if source {
				changed.Destination.Peer.Address = "https://127.0.0.3:18992"
			} else {
				changed.Source.Peer.Address = "https://127.0.0.3:18992"
			}
			require.NoError(t, changed.Validate())
			require.ErrorIs(t, d.journal.recordMigrationCapturePeer(changed), errdefs.ErrAlreadyExists)
			changed = request
			if source {
				changed.Source.Peer.Address = "https://127.0.0.3:18992"
			} else {
				changed.Destination.Peer.Address = "https://127.0.0.3:18992"
			}
			require.ErrorIs(t, d.journal.recordMigrationCapturePeer(changed), errdefs.ErrFailedPrecondition)
			path := d.journal.db.Path()
			require.NoError(t, d.journal.Close())
			d.journal, err = newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
			require.NoError(t, d.journal.recordMigrationCapturePeer(request))
			oldPeer := d.migrationPeer
			d.migrationPeer = nil
			_, err = d.PrepareMigrationCapturePeer(t.Context(), request)
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			d.migrationPeer = oldPeer
			for _, mutate := range []func(*runtimeSlotJournalRecord){
				func(r *runtimeSlotJournalRecord) { r.Version = runtimeSlotCaptureUploadJournalVersion },
				func(r *runtimeSlotJournalRecord) {
					r.MigrationStaging.CapturePeer.RequestDigest = strings.Repeat("ab", 32)
				},
				func(r *runtimeSlotJournalRecord) { r.MigrationStaging.Peer.Address = "https://127.0.0.3:18992" },
				func(r *runtimeSlotJournalRecord) { r.MigrationStaging.Ready = false },
				func(r *runtimeSlotJournalRecord) { r.MigrationStaging.CapturePeer.ImageDirectory = "/unowned/path" },
			} {
				payload, err := json.Marshal(record)
				require.NoError(t, err)
				var changed runtimeSlotJournalRecord
				require.NoError(t, json.Unmarshal(payload, &changed))
				mutate(&changed)
				payload, err = json.Marshal(changed)
				require.NoError(t, err)
				_, err = decodeRuntimeSlotJournalRecord(payload)
				require.Error(t, err)
			}
		})
	}
}

func TestCapturePeerGrantCannotArriveAfterCancellationOrCapture(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		d, request := capturePeerJournalFixture(t, false)
		require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request.Staging))
		require.ErrorIs(t, d.journal.recordMigrationCapturePeer(request), errdefs.ErrFailedPrecondition)
	})
	t.Run("capture_started", func(t *testing.T) {
		d, request := capturePeerJournalFixture(t, true)
		require.NoError(t, d.journal.RecordMigrationCapture(stagingCapture(t, request.Staging)))
		require.ErrorIs(t, d.journal.recordMigrationCapturePeer(request), errdefs.ErrFailedPrecondition)
	})
}

func TestCapturePeerPartialCacheFallsBackToPublishedImage(t *testing.T) {
	var image protocol.MigrationImagePrepareRequest
	d, request := capturePeerJournalFixture(t, false, &image)
	_, err := d.PrepareMigrationCapturePeer(t.Context(), request)
	require.NoError(t, err)
	record, err := d.journal.Get(request.Staging.Target.SlotID)
	require.NoError(t, err)
	c := record.MigrationStaging.CapturePeer
	require.NoError(t, os.MkdirAll(c.ImageDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(c.ImageDirectory, "partial.img"), []byte("uncommitted"), 0o600))
	// Primary restart discards its TLS identity. The retained tentative path
	// must not strand normal regionally authorized image preparation.
	d.migrationPeer = nil
	prepared, err := d.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	record, err = d.journal.Get(request.Staging.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.CapturePeer.Removed)
	_, err = os.Stat(c.ImageDirectory)
	require.ErrorIs(t, err, os.ErrNotExist)
	data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
	require.Equal(t, 1, d.runtime.(*migrationImageDownloadTestRuntime).downloadCalls)
	require.ErrorIs(t, d.journal.recordMigrationCapturePeer(request), errdefs.ErrFailedPrecondition)
}

func TestCapturePeerCacheMustBeRemovedBeforeReleaseOrCarrierCleanup(t *testing.T) {
	d, request := capturePeerJournalFixture(t, false)
	require.NoError(t, d.journal.recordMigrationCapturePeer(request))
	record, err := d.journal.Get(request.Staging.Target.SlotID)
	require.NoError(t, err)
	c := record.MigrationStaging.CapturePeer
	require.NoError(t, os.MkdirAll(c.ImageDirectory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(c.ImageDirectory, "pages.img"), []byte("tentative"), 0o600))
	require.ErrorIs(t, d.journal.releaseMigrationStaging(request.Staging), errdefs.ErrFailedPrecondition)
	_, err = d.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	pruned, err := d.journal.Prune(time.Now().Add(100 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, pruned)
	d.migrationStaging = nil
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), request.Staging), errdefs.ErrUnavailable)
	_, err = os.Stat(c.ImageDirectory)
	require.NoError(t, err)
	d.migrationStaging = testMigrationStagingGuard{}
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request.Staging))
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request.Staging))
	_, err = os.Stat(c.ImageDirectory)
	require.ErrorIs(t, err, os.ErrNotExist)
	record, err = d.journal.Get(request.Staging.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Released)
	require.True(t, record.MigrationStaging.CapturePeer.Removed)
	require.False(t, record.hasMigrationImageCustody())
	require.ErrorIs(t, d.journal.recordMigrationCapturePeer(request), errdefs.ErrFailedPrecondition)
}
