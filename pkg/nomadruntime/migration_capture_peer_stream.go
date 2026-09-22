package nomadruntime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

const migrationCapturePeerPath = "/internal/v1/migration-capture"
const migrationCapturePeerHeader = "X-Sandbox0-Capture-Peer"

type migrationCapturePeerCache struct {
	digest string
	cache  *runtimecheckpoint.CapturePeerCache
}

type migrationCapturePeerRead struct {
	SlotID        string `json:"slot_id"`
	RequestDigest string `json:"request_digest"`
}

// Source transport shares the capture uploader's bounded buffers. A failed or
// slow peer disables only this disposable consumer; regional staging continues.
// close joins the HTTP writer before source reconciliation may be released.
type migrationCapturePeerUpload struct {
	client  *http.Client
	reader  *io.PipeReader
	output  *io.PipeWriter
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	writer  *runtimecheckpoint.CapturePeerWriter
	initErr error
	scope   runtimecheckpoint.CaptureScope
}

func (d *nodeRuntime) openMigrationCapturePeerUpload(ctx context.Context, record runtimeSlotJournalRecord, scope runtimecheckpoint.CaptureScope) *migrationCapturePeerUpload {
	staging := record.MigrationStaging
	if staging == nil || staging.CapturePeer == nil || record.Migration == nil || d.migrationPeer == nil {
		return nil
	}
	grant := staging.CapturePeer.Request
	if !grant.Staging.IsSource() || grant.Staging.Source != record.Migration.Capture.Request || grant.Source.Peer != d.migrationPeer.endpoint ||
		grant.Source.PeerCertificateSHA256 != runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity) {
		return nil
	}
	client, err := runtimecheckpoint.NewPeerClient(grant.Destination.Peer, d.migrationPeer.identity)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	reader, output := io.Pipe()
	p := &migrationCapturePeerUpload{client: client, reader: reader, output: output, cancel: cancel, done: make(chan struct{}), scope: scope}
	destination := grant
	destination.Staging.Target = destination.Staging.Destination
	want, err := destination.Digest()
	if err != nil {
		cancel()
		_ = reader.Close()
		_ = output.Close()
		client.CloseIdleConnections()
		return nil
	}
	payload, _ := json.Marshal(migrationCapturePeerRead{SlotID: destination.Staging.Target.SlotID, RequestDigest: want})
	go func() {
		defer close(p.done)
		defer cancel()
		defer func() { _ = reader.Close(); _ = output.Close() }()
		interrupted := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			_ = reader.CloseWithError(ctx.Err())
			_ = output.CloseWithError(ctx.Err())
			close(interrupted)
		})
		defer func() {
			if !stop() {
				<-interrupted
			}
		}()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, grant.Destination.Peer.Address+migrationCapturePeerPath, reader)
		if err != nil {
			return
		}
		request.Header.Set(migrationCapturePeerHeader, string(payload))
		response, err := client.Do(request)
		if err != nil {
			return
		}
		// Neither HTTP completion nor its status is a capture/publication
		// acknowledgement. Final repair independently validates retained bytes.
		_ = response.Body.Close()
	}()
	return p
}

func (p *migrationCapturePeerUpload) writeChunk(ctx context.Context, name string, offset int64, data []byte) error {
	// A slow peer must not pin the regional consumer's borrowed buffer for the
	// whole checkpoint duration. Cancellation interrupts all queued writes.
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	interrupted := make(chan struct{})
	stop := context.AfterFunc(bounded, func() { p.cancel(); close(interrupted) })
	defer func() {
		if !stop() {
			<-interrupted
		}
		cancel()
	}()
	p.once.Do(func() { p.writer, p.initErr = runtimecheckpoint.NewCapturePeerWriter(bounded, p.scope, p.output) })
	if p.initErr == nil {
		if err := p.writer.WriteChunk(bounded, name, offset, data); err != nil {
			p.cancel()
		}
	}
	// Preserve cancellation from the enclosing regional upload, but a peer's
	// network or cache failure is only a miss for final destination preparation.
	return ctx.Err()
}

func (p *migrationCapturePeerUpload) close() {
	p.cancel()
	<-p.done
	p.client.CloseIdleConnections()
}

// A cache keeps the original write descriptors through final fsync. All users
// also hold slot reconciliation, so closing it cannot race receive or repair.
func (d *nodeRuntime) capturePeerCache(slot, want string) *runtimecheckpoint.CapturePeerCache {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c := d.migrationCapturePeerCaches[slot]; c != nil && c.digest == want {
		return c.cache
	}
	return nil
}

func (d *nodeRuntime) closeMigrationCapturePeerCache(slot, want string) error {
	d.mu.Lock()
	c := d.migrationCapturePeerCaches[slot]
	d.mu.Unlock()
	if c == nil {
		return nil
	}
	if c.digest != want {
		return errdefs.ErrFailedPrecondition
	}
	if err := c.cache.Close(); err != nil {
		return err
	}
	d.mu.Lock()
	delete(d.migrationCapturePeerCaches, slot)
	d.mu.Unlock()
	return nil
}

// Primary shutdown first cancels and joins all node workers and peer handlers.
// Dirty cache paths remain journal-owned for the successor's authorized cleanup.
func (d *nodeRuntime) closeMigrationCapturePeerCaches() error {
	d.mu.Lock()
	caches := d.migrationCapturePeerCaches
	d.migrationCapturePeerCaches = nil
	d.mu.Unlock()
	var result error
	for _, c := range caches {
		result = errors.Join(result, c.cache.Close())
	}
	return result
}

func (d *nodeRuntime) openMigrationCapturePeerCache(ctx context.Context, record runtimeSlotJournalRecord) (*runtimecheckpoint.CapturePeerCache, error) {
	c := record.MigrationStaging.CapturePeer
	slot := record.Registration.SlotID
	if cache := d.capturePeerCache(slot, c.RequestDigest); cache != nil {
		return cache, nil
	}
	d.mu.Lock()
	exhausted := len(d.migrationCapturePeerCaches) >= maxMigrationImageCustodies || d.migrationCapturePeerCaches[slot] != nil
	d.mu.Unlock()
	if exhausted {
		return nil, errdefs.ErrResourceExhausted
	}
	if err := ensureMigrationStagingDirectory(d.journal.migrationRoot); err != nil {
		return nil, err
	}
	staging := c.Request.Staging
	scope, err := staging.CaptureUpload.Scope(staging.Source)
	if err != nil {
		return nil, err
	}
	admit := func(bytes int64, inodes uint64) error {
		if bytes > staging.Bytes || inodes > staging.Inodes {
			return errdefs.ErrResourceExhausted
		}
		return d.checkMigrationStagingBudget(bytes, inodes)
	}
	// Never reopen or remove an earlier process's path here. A restart loses
	// tentative reuse knowledge; only normal preparation/release may discard it.
	cache, err := runtimecheckpoint.NewCapturePeerCache(ctx, scope, c.ImageDirectory, min(staging.Bytes, runtimecheckpoint.MaxImageBytes), admit)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.migrationCapturePeerCaches == nil {
		d.migrationCapturePeerCaches = make(map[string]*migrationCapturePeerCache)
	}
	d.migrationCapturePeerCaches[slot] = &migrationCapturePeerCache{digest: c.RequestDigest, cache: cache}
	d.mu.Unlock()
	return cache, nil
}

func (d *nodeRuntime) serveMigrationCapturePeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.TLS == nil || len(r.TLS.PeerCertificates) != 1 {
		http.Error(w, "invalid capture peer request", http.StatusBadRequest)
		return
	}
	if d.journal == nil || d.runtime == nil || d.runner == nil {
		http.Error(w, "capture peer unavailable", http.StatusServiceUnavailable)
		return
	}
	defer r.Body.Close()
	payload := r.Header.Get(migrationCapturePeerHeader)
	var read migrationCapturePeerRead
	if len(payload) > 2048 || json.Unmarshal([]byte(payload), &read) != nil || read.SlotID == "" || len(read.SlotID) > 512 || len(read.RequestDigest) != 64 {
		http.Error(w, "invalid capture peer identity", http.StatusBadRequest)
		return
	}
	canonical, _ := json.Marshal(read)
	if string(canonical) != payload {
		http.Error(w, "noncanonical capture peer identity", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	if controller.SetReadDeadline(deadline) != nil || controller.SetWriteDeadline(deadline) != nil {
		http.Error(w, "capture peer deadlines unavailable", http.StatusServiceUnavailable)
		return
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = controller.SetReadDeadline(time.Now())
		_ = controller.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()
	if !d.beginReconciliation(read.SlotID, cancel) {
		http.Error(w, "capture custody busy", http.StatusServiceUnavailable)
		return
	}
	defer d.endReconciliation(read.SlotID)
	record, err := d.journal.Get(read.SlotID)
	if err != nil || !record.hasMigrationCapturePeerCache() || record.MigrationDestination != nil || record.MigrationStaging.Prefetch != nil {
		http.Error(w, "capture custody unavailable", http.StatusForbidden)
		return
	}
	c := record.MigrationStaging.CapturePeer
	cert := r.TLS.PeerCertificates[0]
	if c.RequestDigest != read.RequestDigest || d.migrationPeer == nil || d.migrationPeer.endpoint != c.Request.Destination.Peer ||
		c.Request.Destination.PeerCertificateSHA256 != runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity) ||
		c.Request.Source.PeerCertificateSHA256 != digest.FromBytes(cert.Raw).String() || time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
		http.Error(w, "capture peer is not the reserved source", http.StatusForbidden)
		return
	}
	if err := d.checkMigrationStaging(false); err != nil {
		http.Error(w, "capture staging unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := d.requireUnusedMigrationDestination(ctx, record.Registration, c.Request.Staging.Target); err != nil {
		http.Error(w, "capture destination is occupied", http.StatusConflict)
		return
	}
	cache, err := d.openMigrationCapturePeerCache(ctx, record)
	if err == nil {
		err = cache.ReceiveGrowing(ctx, http.MaxBytesReader(w, r.Body, c.Request.Staging.Bytes+runtimecheckpoint.MaxManifestBytes))
	}
	if err != nil {
		http.Error(w, "capture cache unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
