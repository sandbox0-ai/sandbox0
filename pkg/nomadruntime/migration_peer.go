package nomadruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/net/netutil"
)

type migrationPeer struct {
	identity tls.Certificate
	endpoint runtimecheckpoint.PeerEndpoint
	server   *http.Server
	cancel   context.CancelFunc
	served   chan struct{}
	mu       sync.Mutex
	closing  bool
	handlers sync.WaitGroup
}

type migrationPeerRead struct {
	SlotID           string                                  `json:"slot_id"`
	RequestDigest    string                                  `json:"request_digest"`
	Planned          bool                                    `json:"planned,omitempty"`
	CaptureInventory *runtimecheckpoint.CapturePeerInventory `json:"capture_inventory,omitempty"`
}

type migrationPeerRuntime interface {
	WriteMigrationPeerImage(context.Context, runtimecheckpoint.Binding, runtimecheckpoint.Reference, string, io.Writer) error
}

func (r *rootfsRuntime) WriteMigrationPeerImage(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference, directory string, output io.Writer) error {
	if r == nil || r.checkpoints == nil {
		return fmt.Errorf("checkpoint store unavailable")
	}
	return r.checkpoints.WritePeerImage(ctx, binding, ref, directory, output)
}

// The primary owns this listener just like its journal and NBD pool. Peer keys
// are public-pinned through the existing regional node channel, independently
// of the node-enrollment certificates, which are client-only certificates.
func (d *nodeRuntime) startMigrationPeer(ctx context.Context, address string) (*migrationPeer, error) {
	if err := runtimecheckpoint.ValidatePeerAddress(address); err != nil {
		return nil, err
	}
	identity, err := runtimecheckpoint.NewPeerIdentity()
	if err != nil {
		return nil, err
	}
	endpoint, err := runtimecheckpoint.NewPeerEndpoint(address, identity)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	peer := &migrationPeer{identity: identity, endpoint: endpoint, cancel: cancel, served: make(chan struct{})}
	peer.server = &http.Server{Handler: peer.handler(d),
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4096, BaseContext: func(net.Listener) context.Context { return ctx }}
	// TLS proves possession; the handler checks the exact operation's client
	// certificate before looking up or returning any workload image bytes.
	tlsListener := tls.NewListener(netutil.LimitListener(listener, 8), &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity}, ClientAuth: tls.RequireAnyClientCert})
	d.wg.Add(1)
	go func() { defer d.wg.Done(); defer close(peer.served); _ = peer.server.Serve(tlsListener) }()
	return peer, nil
}

func (p *migrationPeer) handler(d *nodeRuntime) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		if p.closing {
			p.mu.Unlock()
			http.Error(w, "peer is stopping", http.StatusServiceUnavailable)
			return
		}
		p.handlers.Add(1)
		p.mu.Unlock()
		defer p.handlers.Done()
		d.serveMigrationPeer(w, r)
	})
}

// http.Server.Close closes sockets but does not join their handlers. The
// primary must retain its journal and runtime until all readers release them.
func (p *migrationPeer) close() {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.cancel()
	_ = p.server.Close()
	<-p.served
	p.handlers.Wait()
}

func (d *nodeRuntime) serveMigrationPeer(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == migrationCapturePeerPath {
		d.serveMigrationCapturePeer(writer, request)
		return
	}
	if request.Method != http.MethodPost || request.URL.Path != runtimecheckpoint.PeerImagePath || request.URL.RawQuery != "" {
		http.NotFound(writer, request)
		return
	}
	if request.TLS == nil || len(request.TLS.PeerCertificates) != 1 {
		http.Error(writer, "peer identity required", http.StatusUnauthorized)
		return
	}
	defer request.Body.Close()
	var read migrationPeerRead
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, runtimecheckpoint.MaxManifestBytes+2048))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&read) != nil || decoder.Decode(&struct{}{}) != io.EOF || len(read.SlotID) == 0 || len(read.SlotID) > 512 || len(read.RequestDigest) != 64 {
		http.Error(writer, "invalid peer read", http.StatusBadRequest)
		return
	}
	// Bound source custody independently of the caller's deadline. Cancellation
	// alone cannot interrupt a blocked socket write, so expire its write deadline
	// as well. Regional cleanup may preempt this disposable transfer.
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Minute)
	defer cancel()
	controller := http.NewResponseController(writer)
	deadline, _ := ctx.Deadline()
	if err := controller.SetWriteDeadline(deadline); err != nil {
		http.Error(writer, "peer write deadline unavailable", http.StatusServiceUnavailable)
		return
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = controller.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()
	if read.Planned {
		scope, err := d.borrowMigrationPublication(read, request.TLS.PeerCertificates[0])
		if err != nil && !errdefs.IsNotFound(err) {
			http.Error(writer, "planned image unavailable", http.StatusForbidden)
			return
		}
		if scope != nil {
			defer scope.readers.Done()
			stopPublicationCancel := context.AfterFunc(scope.ctx, cancel)
			defer stopPublicationCancel()
			if read.CaptureInventory != nil {
				if err := d.writeMigrationCapturePeerFinal(ctx, read, scope.plan.Manifest.Binding, scope.plan.Reference,
					scope.directory, scope.certificate, &scope.plan, writer); err != nil {
					panic(http.ErrAbortHandler)
				}
				return
			}
			runtime, ok := d.runtime.(migrationPlannedImageRuntime)
			if !ok {
				http.Error(writer, "planned image unavailable", http.StatusServiceUnavailable)
				return
			}
			writer.Header().Set("Content-Type", "application/vnd.sandbox0.checkpoint-stream")
			writer.Header().Set("Cache-Control", "no-store")
			if err := runtime.WritePlannedMigrationPeerImage(ctx, scope.plan.Manifest.Binding, scope.plan, scope.directory, writer); err != nil {
				panic(http.ErrAbortHandler)
			}
			return
		}
		// Publication may finish before the destination connects. Fall through
		// to the existing exact, durable receipt and exclusive read admission.
	}
	if !d.beginReconciliation(read.SlotID, cancel) {
		http.Error(writer, "image custody busy", http.StatusServiceUnavailable)
		return
	}
	defer d.endReconciliation(read.SlotID)
	record, err := d.journal.Get(read.SlotID)
	if err != nil || record.Migration == nil {
		http.Error(writer, "image custody unavailable", http.StatusForbidden)
		return
	}
	custody := record.Migration
	if custody.ExecutionInvalidated || custody.PublicationRequest == nil || custody.Publication == nil ||
		custody.Capture.State != protocol.MigrationCaptureComplete || custody.Publication.RequestDigest != read.RequestDigest {
		http.Error(writer, "image custody unavailable", http.StatusForbidden)
		return
	}
	cert := request.TLS.PeerCertificates[0]
	now := time.Now()
	if custody.PublicationRequest.DestinationPeerCertificateSHA256 != digest.FromBytes(cert.Raw).String() || now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		http.Error(writer, "peer is not the reserved destination", http.StatusForbidden)
		return
	}
	if read.CaptureInventory != nil {
		if err := d.writeMigrationCapturePeerFinal(ctx, read, custody.Publication.Binding, custody.Publication.Reference,
			custody.ImageDirectory, custody.PublicationRequest.DestinationPeerCertificateSHA256, nil, writer); err != nil {
			panic(http.ErrAbortHandler)
		}
		return
	}
	runtime, ok := d.runtime.(migrationPeerRuntime)
	if !ok {
		http.Error(writer, "peer image unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/vnd.sandbox0.checkpoint-stream")
	writer.Header().Set("Cache-Control", "no-store")
	if err := runtime.WriteMigrationPeerImage(ctx, custody.Publication.Binding, custody.Publication.Reference, custody.ImageDirectory, writer); err != nil {
		// A successful HTTP EOF is part of the stream completion proof. Abort
		// the connection on late disk/hash errors, even after all chunks wrote.
		panic(http.ErrAbortHandler)
	}
}

func (d *nodeRuntime) receiveMigrationPeer(ctx context.Context, request protocol.MigrationImagePrepareRequest, directory string) (runtimecheckpoint.Manifest, error) {
	return d.receiveMigrationPeerImage(ctx, request.Receipt.Peer,
		migrationPeerRead{SlotID: request.Publication.Capture.Request.Target.SlotID, RequestDigest: request.Receipt.RequestDigest},
		request.Receipt.Binding, request.Receipt.Reference, directory, runtimecheckpoint.MaxImageBytes, d.checkMigrationStagingBudget)
}

func (d *nodeRuntime) receiveMigrationPeerImage(ctx context.Context, endpoint runtimecheckpoint.PeerEndpoint, read migrationPeerRead,
	binding runtimecheckpoint.Binding, reference runtimecheckpoint.Reference, directory string, maxBytes int64, admit func(int64, uint64) error) (runtimecheckpoint.Manifest, error) {
	client, err := runtimecheckpoint.NewPeerClient(endpoint, d.migrationPeer.identity)
	if err != nil {
		return runtimecheckpoint.Manifest{}, err
	}
	defer client.CloseIdleConnections()
	payload, err := json.Marshal(read)
	if err != nil {
		return runtimecheckpoint.Manifest{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.Address+runtimecheckpoint.PeerImagePath, bytes.NewReader(payload))
	if err != nil {
		return runtimecheckpoint.Manifest{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		return runtimecheckpoint.Manifest{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/vnd.sandbox0.checkpoint-stream" {
		return runtimecheckpoint.Manifest{}, fmt.Errorf("peer image rejected: HTTP %d", response.StatusCode)
	}
	return runtimecheckpoint.ReceivePeerImage(ctx, binding, reference, directory, response.Body, maxBytes, admit)
}
