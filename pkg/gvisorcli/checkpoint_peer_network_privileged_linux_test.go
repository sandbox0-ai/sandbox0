//go:build linux

package gvisorcli

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsobjectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

// These endpoints exist only inside an explicitly enabled test binary. One
// pinned client owns one bounded fixture on an isolated receiver filesystem;
// they are never registered on ctld or treated as regional migration authority.
type checkpointPeerNetServer struct {
	mu       sync.Mutex
	root     string
	store    *runtimecheckpoint.Store
	pin      string
	session  *checkpointPeerNetSession
	stop     chan struct{}
	stopOnce sync.Once
}
type checkpointPeerNetSession struct {
	scope     runtimecheckpoint.CaptureScope
	key       string
	directory string
	cache     *runtimecheckpoint.CapturePeerCache
	manifest  runtimecheckpoint.Manifest
	reference runtimecheckpoint.Reference
	verified  bool
}
type checkpointPeerNetFinal struct {
	Binding   runtimecheckpoint.Binding   `json:"binding"`
	Reference runtimecheckpoint.Reference `json:"reference"`
}
type checkpointPeerNetConfig struct {
	Endpoint        runtimecheckpoint.PeerEndpoint `json:"endpoint"`
	CertificateFile string                         `json:"certificate_file"`
	KeyFile         string                         `json:"key_file"`
}

func TestPrivilegedCapturePeerClientIdentity(t *testing.T) {
	root := os.Getenv("SANDBOX0_CAPTURE_PEER_IDENTITY_DIR")
	if root == "" {
		t.Skip("explicit peer identity fixture only")
	}
	require.Zero(t, os.Geteuid())
	require.True(t, filepath.IsAbs(root))
	require.NoError(t, os.Mkdir(root, 0o700))
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	key, err := x509.MarshalECPrivateKey(identity.PrivateKey.(*ecdsa.PrivateKey))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "client.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: key}), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "client.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.Certificate[0]}), 0o600))
	// Only public certificate metadata leaves the source host.
	t.Logf("capture_peer_client_pin=%s", runtimecheckpoint.PeerCertificateDigest(identity))
}

func TestPrivilegedCapturePeerReceiverServer(t *testing.T) {
	address := os.Getenv("SANDBOX0_CAPTURE_PEER_LISTEN")
	if address == "" {
		t.Skip("explicit isolated peer receiver only")
	}
	require.Zero(t, os.Geteuid())
	require.NoError(t, runtimecheckpoint.ValidatePeerAddress(address))
	root := os.Getenv("SANDBOX0_CAPTURE_PEER_ROOT")
	info, err := os.Lstat(root)
	require.NoError(t, err)
	require.True(t, info.IsDir() && info.Mode().Perm()&0o077 == 0)
	pin := os.Getenv("SANDBOX0_CAPTURE_PEER_CLIENT_PIN")
	require.NoError(t, runtimecheckpoint.ValidatePeerCertificateDigest(pin))
	payload, err := os.ReadFile(os.Getenv("SANDBOX0_CAPTURE_UPLOAD_PROBE_CONFIG"))
	require.NoError(t, err)
	var cfg struct {
		Objects config.RootFSObjectStorageConfig `yaml:"rootfs_object_storage"`
	}
	require.NoError(t, yaml.Unmarshal(payload, &cfg))
	require.True(t, cfg.Objects.ObjectEncryptionEnabled)
	objects, err := rootfsobjectstore.Create(cfg.Objects, nil)
	require.NoError(t, err)
	store, err := runtimecheckpoint.New(objects, 512<<20)
	require.NoError(t, err)
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	endpoint, err := runtimecheckpoint.NewPeerEndpoint(address, identity)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	defer listener.Close()
	state := &checkpointPeerNetServer{root: root, store: store, pin: pin, stop: make(chan struct{})}
	server := &http.Server{Handler: state, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 90 * time.Second, WriteTimeout: 90 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	ready, err := json.Marshal(endpoint)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(os.Getenv("SANDBOX0_CAPTURE_PEER_READY"), ready, 0o600))
	stopped := make(chan error, 1)
	go func() {
		<-state.stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- server.Shutdown(ctx)
	}()
	err = server.Serve(tls.NewListener(listener, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity}, ClientAuth: tls.RequireAnyClientCert}))
	require.ErrorIs(t, err, http.ErrServerClosed)
	require.NoError(t, <-stopped)
}

func checkpointPeerDecode(reader io.Reader, value any, limit int64) error {
	payload, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return err
	}
	if int64(len(payload)) > limit {
		return errors.New("probe metadata too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("trailing probe metadata")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, payload) {
		return errors.New("noncanonical probe metadata")
	}
	return nil
}
func (s *checkpointPeerNetServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.TLS == nil || len(r.TLS.PeerCertificates) != 1 {
		http.Error(w, "invalid peer request", 400)
		return
	}
	cert := r.TLS.PeerCertificates[0]
	if digest.FromBytes(cert.Raw).String() != s.pin || time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
		http.Error(w, "untrusted fixture client", 403)
		return
	}
	defer r.Body.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	fail := func(err error) { http.Error(w, err.Error(), 409) }
	if r.URL.Path == "/shutdown" {
		if s.session != nil {
			fail(errors.New("fixture still owned"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
		s.stopOnce.Do(func() { close(s.stop) })
		return
	}
	if r.URL.Path == "/begin" {
		if s.session != nil {
			fail(errors.New("fixture already owned"))
			return
		}
		var source runtimecheckpoint.Binding
		if err := checkpointPeerDecode(r.Body, &source, 8192); err != nil {
			fail(err)
			return
		}
		scope, err := runtimecheckpoint.NewCaptureScope(source)
		if err != nil {
			fail(err)
			return
		}
		key, _ := scope.Digest()
		directory := filepath.Join(s.root, digest.Digest(key).Encoded())
		cache, err := runtimecheckpoint.NewCapturePeerCache(r.Context(), scope, directory, 512<<20, func(int64, uint64) error { return nil })
		if err != nil {
			fail(err)
			return
		}
		s.session = &checkpointPeerNetSession{scope: scope, key: key, directory: directory, cache: cache}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	session := s.session
	if session == nil || r.Header.Get("X-Capture-Scope") != session.key {
		fail(errors.New("fixture scope changed"))
		return
	}
	switch r.URL.Path {
	case "/growing":
		err := session.cache.ReceiveGrowing(r.Context(), http.MaxBytesReader(w, r.Body, (512<<20)+runtimecheckpoint.MaxManifestBytes))
		if err != nil {
			fail(err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "/inventory":
		inventory, err := session.cache.Inventory(r.Context())
		if err != nil {
			fail(err)
			return
		}
		payload, err := inventory.Encode(session.scope, 512<<20)
		if err != nil {
			fail(err)
			return
		}
		_, _ = w.Write(payload)
	case "/final", "/verify", "/image":
		encoded := r.Header.Get("X-Capture-Final")
		if len(encoded) > 8192 {
			fail(errors.New("oversized final metadata"))
			return
		}
		payload, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil {
			fail(err)
			return
		}
		var final checkpointPeerNetFinal
		if err := checkpointPeerDecode(bytes.NewReader(payload), &final, 8192); err != nil {
			fail(err)
			return
		}
		if err := final.Reference.ValidateFor(final.Binding); err != nil {
			fail(err)
			return
		}
		source := final.Binding
		source.RootFSGenerationID = ""
		source.RootFSDescriptorDigest = ""
		scope, err := runtimecheckpoint.NewCaptureScope(source)
		if err != nil || scope != session.scope {
			fail(errors.New("final binding changed source"))
			return
		}
		if r.URL.Path == "/final" {
			manifest, err := session.cache.ReceiveFinal(r.Context(), final.Binding, final.Reference, http.MaxBytesReader(w, r.Body, (512<<20)+runtimecheckpoint.MaxManifestBytes))
			if err != nil {
				fail(err)
				return
			}
			session.manifest = manifest
			session.reference = final.Reference
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if session.reference != final.Reference || session.manifest.Binding != final.Binding {
			fail(errors.New("final receipt missing"))
			return
		}
		if r.URL.Path == "/verify" {
			if _, err := s.store.VerifyLocal(r.Context(), final.Binding, final.Reference, session.directory); err != nil {
				fail(err)
				return
			}
			session.verified = true
			_ = json.NewEncoder(w).Encode(session.cache.Timings())
			return
		}
		if !session.verified {
			fail(errors.New("regional verification missing"))
			return
		}
		if err := s.store.WritePlannedPeerImage(r.Context(), final.Binding, runtimecheckpoint.LocalImagePlan{Manifest: session.manifest, Reference: session.reference}, session.directory, w); err != nil {
			panic(http.ErrAbortHandler)
		}
	case "/release":
		if err := session.cache.Close(); err != nil {
			fail(err)
			return
		}
		if err := os.RemoveAll(session.directory); err != nil {
			fail(err)
			return
		}
		s.session = nil
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

type checkpointPeerRemote struct {
	client   *http.Client
	endpoint string
	key      string
	scope    runtimecheckpoint.CaptureScope
	cancel   context.CancelFunc
	timings  runtimecheckpoint.CapturePeerTimings
}

func (r *checkpointPeerRemote) request(ctx context.Context, path string, body io.Reader, final *checkpointPeerNetFinal) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Capture-Scope", r.key)
	if final != nil {
		payload, err := json.Marshal(final)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Capture-Final", base64.StdEncoding.EncodeToString(payload))
	}
	response, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 && response.StatusCode != 204 {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		_ = response.Body.Close()
		return nil, fmt.Errorf("fixture peer %s: HTTP %d: %s", path, response.StatusCode, payload)
	}
	return response, nil
}
func newCheckpointPeerRemoteProbe(t *testing.T, ctx context.Context, scope runtimecheckpoint.CaptureScope, binding runtimecheckpoint.Binding, directory string) *checkpointPeerUploadProbe {
	payload, err := os.ReadFile(os.Getenv("SANDBOX0_CAPTURE_PEER_REMOTE_CONFIG"))
	require.NoError(t, err)
	var cfg checkpointPeerNetConfig
	require.NoError(t, json.Unmarshal(payload, &cfg))
	identity, err := tls.LoadX509KeyPair(cfg.CertificateFile, cfg.KeyFile)
	require.NoError(t, err)
	client, err := runtimecheckpoint.NewPeerClient(cfg.Endpoint, identity)
	require.NoError(t, err)
	key, err := scope.Digest()
	require.NoError(t, err)
	remote := &checkpointPeerRemote{client: client, endpoint: cfg.Endpoint.Address, key: key, scope: scope}
	source := binding
	source.RootFSGenerationID = ""
	source.RootFSDescriptorDigest = ""
	payload, err = json.Marshal(source)
	require.NoError(t, err)
	response, err := remote.request(ctx, "/begin", bytes.NewReader(payload), nil)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	reader, writer := io.Pipe()
	growingCtx, cancel := context.WithCancel(ctx)
	remote.cancel = cancel
	p := &checkpointPeerUploadProbe{directory: directory, reader: reader, writer: writer, received: make(chan error, 1), early: &checkpointPeerByteCounter{Writer: writer}, remote: remote}
	t.Cleanup(func() {
		_ = p.stop()
		_ = reader.Close()
		_ = writer.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		response, err := remote.request(cleanupCtx, "/release", nil, nil)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		client.CloseIdleConnections()
	})
	go func() {
		response, err := remote.request(growingCtx, "/growing", reader, nil)
		if response != nil {
			_ = response.Body.Close()
		}
		p.received <- err
	}()
	p.peer, err = runtimecheckpoint.NewCapturePeerWriter(ctx, scope, p.early)
	require.NoError(t, err)
	return p
}
func (p *checkpointPeerUploadProbe) finishRemote(ctx context.Context, store *runtimecheckpoint.Store, stage *runtimecheckpoint.CaptureStager,
	binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, source string) (int64, int64, error) {
	remote := p.remote
	response, err := remote.request(ctx, "/inventory", nil, nil)
	if err != nil {
		return 0, 0, err
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, runtimecheckpoint.MaxManifestBytes+1))
	_ = response.Body.Close()
	if err != nil {
		return 0, 0, err
	}
	inventory, err := runtimecheckpoint.DecodeCapturePeerInventory(payload, remote.scope, 512<<20)
	if err != nil {
		return 0, 0, err
	}
	var earlyBytes int64
	for _, file := range inventory.Files {
		earlyBytes += file.Size
	}
	final := &checkpointPeerNetFinal{Binding: binding, Reference: plan.Reference}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	counter := &checkpointPeerByteCounter{Writer: writer}
	group, groupCtx := errgroup.WithContext(ctx)
	stop := context.AfterFunc(groupCtx, func() { _ = reader.CloseWithError(groupCtx.Err()); _ = writer.CloseWithError(groupCtx.Err()) })
	defer stop()
	group.Go(func() error {
		started := time.Now()
		defer func() { p.publishDuration = time.Since(started) }()
		ref, err := stage.PublishPlanned(groupCtx, binding, plan, source)
		if err == nil && ref != plan.Reference {
			return errors.New("publication changed reference")
		}
		return err
	})
	group.Go(func() error {
		err := store.WriteCapturePeerFinal(groupCtx, binding, plan, source, inventory, counter)
		_ = writer.CloseWithError(err)
		return err
	})
	group.Go(func() error {
		started := time.Now()
		defer func() { p.repairDuration = time.Since(started) }()
		response, err := remote.request(groupCtx, "/final", reader, final)
		if response != nil {
			_ = response.Body.Close()
		}
		return err
	})
	if err := group.Wait(); err != nil {
		return earlyBytes, counter.bytes, err
	}
	started := time.Now()
	response, err = remote.request(ctx, "/verify", nil, final)
	if err != nil {
		return earlyBytes, counter.bytes, err
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&remote.timings)
	_ = response.Body.Close()
	p.verifyDuration = time.Since(started)
	return earlyBytes, counter.bytes, err
}
func (p *checkpointPeerUploadProbe) downloadRemote(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference) error {
	response, err := p.remote.request(ctx, "/image", nil, &checkpointPeerNetFinal{Binding: binding, Reference: ref})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, err = runtimecheckpoint.ReceivePeerImage(ctx, binding, ref, p.directory, response.Body, 512<<20, func(int64, uint64) error { return nil })
	return err
}

func TestPrivilegedCapturePeerReceiverShutdown(t *testing.T) {
	path := os.Getenv("SANDBOX0_CAPTURE_PEER_REMOTE_CONFIG")
	if path == "" {
		t.Skip("explicit fixture shutdown only")
	}
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var cfg checkpointPeerNetConfig
	require.NoError(t, json.Unmarshal(payload, &cfg))
	identity, err := tls.LoadX509KeyPair(cfg.CertificateFile, cfg.KeyFile)
	require.NoError(t, err)
	client, err := runtimecheckpoint.NewPeerClient(cfg.Endpoint, identity)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	remote := &checkpointPeerRemote{client: client, endpoint: cfg.Endpoint.Address}
	response, err := remote.request(ctx, "/shutdown", nil, nil)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
}
