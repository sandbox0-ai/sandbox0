package nomadruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationCapturePeerImageRuntime interface {
	WriteMigrationCapturePeerFinal(context.Context, runtimecheckpoint.Binding, runtimecheckpoint.Reference, *runtimecheckpoint.LocalImagePlan, string, runtimecheckpoint.CapturePeerInventory, io.Writer) error
}

func (r *rootfsRuntime) WriteMigrationCapturePeerFinal(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference,
	plan *runtimecheckpoint.LocalImagePlan, directory string, inventory runtimecheckpoint.CapturePeerInventory, output io.Writer) error {
	if r == nil || r.checkpoints == nil {
		return errdefs.ErrUnavailable
	}
	if plan != nil {
		return r.checkpoints.WriteCapturePeerFinal(ctx, binding, *plan, directory, inventory, output)
	}
	return r.checkpoints.WritePublishedCapturePeerFinal(ctx, binding, ref, directory, inventory, output)
}

// The existing publication read owns stopped-source custody and has already
// authenticated the destination. Reuse additionally requires its exact early
// grant; inventory supplied by a peer is never independent read authority.
func (d *nodeRuntime) writeMigrationCapturePeerFinal(ctx context.Context, read migrationPeerRead, binding runtimecheckpoint.Binding,
	ref runtimecheckpoint.Reference, directory, destinationCertificate string, plan *runtimecheckpoint.LocalImagePlan, output http.ResponseWriter) error {
	record, err := d.journal.Get(read.SlotID)
	if err != nil {
		return err
	}
	if record.MigrationStaging == nil || record.MigrationStaging.CapturePeer == nil || record.MigrationStaging.Released ||
		record.Migration == nil || d.migrationPeer == nil || read.CaptureInventory == nil {
		return errdefs.ErrFailedPrecondition
	}
	grant := record.MigrationStaging.CapturePeer.Request
	if !grant.Staging.IsSource() || grant.Staging.Source != record.Migration.Capture.Request ||
		grant.Source.Peer != d.migrationPeer.endpoint || grant.Source.PeerCertificateSHA256 != runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity) ||
		grant.Destination.PeerCertificateSHA256 != destinationCertificate {
		return errdefs.ErrPermissionDenied
	}
	scope, err := grant.Staging.CaptureUpload.Scope(grant.Staging.Source)
	if err != nil {
		return err
	}
	if _, err := read.CaptureInventory.Encode(scope, min(grant.Staging.Bytes, runtimecheckpoint.MaxImageBytes)); err != nil {
		return err
	}
	runtime, ok := d.runtime.(migrationCapturePeerImageRuntime)
	if !ok {
		return errdefs.ErrUnavailable
	}
	output.Header().Set("Content-Type", "application/vnd.sandbox0.checkpoint-stream")
	output.Header().Set("Cache-Control", "no-store")
	return runtime.WriteMigrationCapturePeerFinal(ctx, binding, ref, plan, directory, *read.CaptureInventory, migrationCapturePeerResponseWriter{output})
}

// Flush exposes reuse frames before source validation so destination hashing
// can overlap it. ResponseController preserves wrapped writers and write errors.
type migrationCapturePeerResponseWriter struct{ http.ResponseWriter }

func (w migrationCapturePeerResponseWriter) Flush() error {
	return http.NewResponseController(w.ResponseWriter).Flush()
}

// A same-process cache retains the descriptors that observed write errors.
// Restarted or invalid caches are discarded before a full transfer, so repair
// never needs a second image's disk quota. The caller owns slot reconciliation.
func (d *nodeRuntime) repairMigrationCapturePeer(ctx context.Context, record runtimeSlotJournalRecord,
	request protocol.MigrationImagePrefetchRequest, directory string) (runtimecheckpoint.Manifest, bool, error) {
	if !record.hasMigrationCapturePeerCache() {
		return runtimecheckpoint.Manifest{}, false, nil
	}
	c := record.MigrationStaging.CapturePeer
	slot := request.Staging.Target.SlotID
	cache := d.capturePeerCache(slot, c.RequestDigest)
	var manifest runtimecheckpoint.Manifest
	var repaired bool
	if cache != nil && c.Request.Staging == request.Staging && c.Request.Source.Peer == request.Plan.Peer &&
		c.Request.Destination.Peer == d.migrationPeer.endpoint && c.Request.Destination.PeerCertificateSHA256 == request.Publication.DestinationPeerCertificateSHA256 {
		started := time.Now()
		inventory, err := cache.Inventory(ctx)
		if err == nil {
			manifest, err = d.receiveMigrationCapturePeerFinal(ctx, request, inventory, cache)
		}
		if err == nil {
			if err := d.closeMigrationCapturePeerCache(slot, c.RequestDigest); err != nil {
				return runtimecheckpoint.Manifest{}, false, err
			}
			if err := os.Rename(c.ImageDirectory, directory); err != nil {
				return runtimecheckpoint.Manifest{}, false, err
			}
			repaired = true
			var retained int64
			for _, f := range inventory.Files {
				retained += f.Size
			}
			d.logMigrationTiming(request.Publication.Assignment.OperationID, "capture-peer-repair", started, "tentative_bytes", retained)
		}
	}
	// Also synchronizes the parent after a successful rename. A crash before
	// the tombstone/receipt safely falls back rather than adopting partial data.
	if err := d.discardMigrationCapturePeer(ctx, slot, c); err != nil {
		return runtimecheckpoint.Manifest{}, false, err
	}
	return manifest, repaired, nil
}

func (d *nodeRuntime) receiveMigrationCapturePeerFinal(ctx context.Context, request protocol.MigrationImagePrefetchRequest,
	inventory runtimecheckpoint.CapturePeerInventory, cache *runtimecheckpoint.CapturePeerCache) (runtimecheckpoint.Manifest, error) {
	client, err := runtimecheckpoint.NewPeerClient(request.Plan.Peer, d.migrationPeer.identity)
	if err != nil {
		return runtimecheckpoint.Manifest{}, err
	}
	defer client.CloseIdleConnections()
	read := migrationPeerRead{SlotID: request.Publication.Capture.Request.Target.SlotID,
		RequestDigest: request.Plan.RequestDigest, Planned: true, CaptureInventory: &inventory}
	payload, err := json.Marshal(read)
	if err != nil {
		return runtimecheckpoint.Manifest{}, err
	}
	var response *http.Response
	for attempt := 0; ; attempt++ {
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, request.Plan.Peer.Address+runtimecheckpoint.PeerImagePath, bytes.NewReader(payload))
		if err != nil {
			return runtimecheckpoint.Manifest{}, err
		}
		httpRequest.Header.Set("Content-Type", "application/json")
		response, err = client.Do(httpRequest)
		if err != nil {
			return runtimecheckpoint.Manifest{}, err
		}
		if response.StatusCode != http.StatusServiceUnavailable || attempt == 3 {
			break
		}
		// A finished publication retires its shared reader before releasing
		// exclusive custody. Retry this short handoff only before ReceiveFinal
		// mutates the retained cache. Authentication and stream errors never
		// retry, and persistent unavailability keeps the ordinary fallback.
		_ = response.Body.Close()
		timer := time.NewTimer(time.Duration(attempt+1) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return runtimecheckpoint.Manifest{}, ctx.Err()
		case <-timer.C:
		}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/vnd.sandbox0.checkpoint-stream" {
		return runtimecheckpoint.Manifest{}, fmt.Errorf("capture repair rejected: HTTP %d", response.StatusCode)
	}
	return cache.ReceiveFinal(ctx, request.Plan.Binding, request.Plan.Reference, response.Body)
}
