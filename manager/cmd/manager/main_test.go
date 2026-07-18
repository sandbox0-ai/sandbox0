package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	s0k8s "github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"go.uber.org/zap"
	"k8s.io/client-go/rest"
)

type recordingTemplateReconcilerQuiescer struct {
	called  chan struct{}
	release chan struct{}
}

func (q *recordingTemplateReconcilerQuiescer) Quiesce(context.Context) error {
	close(q.called)
	<-q.release
	return nil
}

func TestServeTemplateReconcilerQuiesceSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	quiescer := &recordingTemplateReconcilerQuiescer{
		called:  make(chan struct{}),
		release: make(chan struct{}),
	}
	tempDir := t.TempDir()
	supportedMarkerPath := filepath.Join(tempDir, "supported")
	quiescedMarkerPath := filepath.Join(tempDir, "quiesced")

	go serveTemplateReconcilerQuiesceSignals(
		ctx,
		signals,
		quiescer,
		supportedMarkerPath,
		quiescedMarkerPath,
		zap.NewNop(),
	)
	waitForTestFile(t, supportedMarkerPath)
	signals <- syscall.SIGUSR1

	select {
	case <-quiescer.called:
	case <-time.After(time.Second):
		t.Fatal("quiesce signal was not handled")
	}
	if _, err := os.Stat(quiescedMarkerPath); !os.IsNotExist(err) {
		t.Fatalf("quiesced marker exists before reconciliation drained: %v", err)
	}
	close(quiescer.release)
	waitForTestFile(t, quiescedMarkerPath)
}

func waitForTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("marker %q was not created", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWrapRootFSObjectStoreEncryptionReadsLogicalRanges(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test encryption key: %v", err)
	}
	keyPath := filepath.Join(t.TempDir(), "rootfs-object-key.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write test encryption key: %v", err)
	}

	encryptionConfig := &managerconfig.StorageProxyConfig{
		ObjectEncryptionEnabled: true,
		ObjectEncryptionKeyPath: keyPath,
		ObjectEncryptionAlgo:    "aes256gcm-rsa",
	}
	rawStore := objectstore.NewMemoryStore("rootfs")
	ctldStore, err := wrapRootFSObjectStoreEncryption(rawStore, encryptionConfig)
	if err != nil {
		t.Fatalf("wrap ctld rootfs object store: %v", err)
	}
	managerStore, err := wrapRootFSObjectStoreEncryption(rawStore, encryptionConfig)
	if err != nil {
		t.Fatalf("wrap manager rootfs object store: %v", err)
	}
	if !strings.HasPrefix(managerStore.String(), "encrypted(") {
		t.Fatalf("expected encrypted object store, got %q", managerStore.String())
	}

	const objectKey = "sandbox-rootfs/team/sandbox/layer.tar"
	want := []byte("rootfs layer")
	if err := ctldStore.Put(objectKey, bytes.NewReader(want)); err != nil {
		t.Fatalf("put encrypted rootfs object: %v", err)
	}
	reader, err := managerStore.Get(objectKey, 0, -1)
	if err != nil {
		t.Fatalf("get encrypted rootfs object: %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read encrypted rootfs object: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("rootfs object = %q, want %q", got, want)
	}

	rangeReader, err := managerStore.Get(objectKey, 2, 5)
	if err != nil {
		t.Fatalf("get encrypted rootfs object range: %v", err)
	}
	defer rangeReader.Close()
	rangeGot, err := io.ReadAll(rangeReader)
	if err != nil {
		t.Fatalf("read encrypted rootfs object range: %v", err)
	}
	if rangeWant := want[2:7]; !bytes.Equal(rangeGot, rangeWant) {
		t.Fatalf("rootfs object range = %q, want %q", rangeGot, rangeWant)
	}

	const legacyObjectKey = "sandbox-rootfs/team/sandbox/legacy-layer.tar"
	if err := rawStore.Put(legacyObjectKey, bytes.NewReader(want)); err != nil {
		t.Fatalf("put legacy rootfs object: %v", err)
	}
	legacyReader, err := managerStore.Get(legacyObjectKey, 0, -1)
	if err != nil {
		t.Fatalf("get legacy rootfs object: %v", err)
	}
	defer legacyReader.Close()
	legacyGot, err := io.ReadAll(legacyReader)
	if err != nil {
		t.Fatalf("read legacy rootfs object: %v", err)
	}
	if !bytes.Equal(legacyGot, want) {
		t.Fatalf("legacy rootfs object = %q, want %q", legacyGot, want)
	}
}

func TestConfigureK8sClientRateLimiterUsesConfiguredValues(t *testing.T) {
	cfg := &rest.Config{}

	configureK8sClientRateLimiter(cfg, 25, 50)

	if cfg.QPS != 25 {
		t.Fatalf("qps = %v, want 25", cfg.QPS)
	}
	if cfg.Burst != 50 {
		t.Fatalf("burst = %d, want 50", cfg.Burst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}

func TestConfigureK8sClientRateLimiterDefaultsWhenUnset(t *testing.T) {
	cfg := &rest.Config{}

	configureK8sClientRateLimiter(cfg, 0, 0)

	if cfg.QPS != s0k8s.DefaultClientQPS {
		t.Fatalf("qps = %v, want %v", cfg.QPS, s0k8s.DefaultClientQPS)
	}
	if cfg.Burst != s0k8s.DefaultClientBurst {
		t.Fatalf("burst = %d, want %d", cfg.Burst, s0k8s.DefaultClientBurst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}
