package legacyackmigration

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

func TestLayerApplierVerifiesChainAndAppliesWhiteouts(t *testing.T) {
	root := t.TempDir()
	store := objectstore.NewMemoryStore("")
	first := legacyTar(t,
		tarEntry{name: "old", body: "remove me"},
		tarEntry{name: "keep", body: "first"},
	)
	second := legacyTar(t,
		tarEntry{name: ".wh.old"},
		tarEntry{name: "keep", body: "second"},
	)
	if err := store.Put("layers/first", bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("layers/second", bytes.NewReader(second)); err != nil {
		t.Fatal(err)
	}
	chain := []Layer{
		legacyLayer("first", "layers/first", first),
		legacyLayer("second", "layers/second", second),
	}
	if err := (LayerApplier{Store: store}).Apply(context.Background(), root, chain); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "old")); !os.IsNotExist(err) {
		t.Fatalf("whiteout target stat error = %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(root, "keep"))
	if err != nil || string(payload) != "second" {
		t.Fatalf("keep = %q, %v", payload, err)
	}
}

func TestLayerApplierRejectsSizeDigestAndUnsafeEntries(t *testing.T) {
	payload := legacyTar(t, tarEntry{name: "file", body: "payload"})
	tests := []struct {
		name    string
		layer   func(Layer) Layer
		payload []byte
		wantErr string
	}{
		{
			name: "short size",
			layer: func(layer Layer) Layer {
				layer.DiffSize--
				return layer
			},
			payload: payload, wantErr: "decrypted layer size",
		},
		{
			name: "digest",
			layer: func(layer Layer) Layer {
				layer.DiffDigest = digest.FromString("other").String()
				layer.DiffID = layer.DiffDigest
				return layer
			},
			payload: payload, wantErr: "decrypted layer digest",
		},
		{
			name:    "device",
			layer:   func(layer Layer) Layer { return layer },
			payload: legacyTar(t, tarEntry{name: "device", kind: tar.TypeChar}),
			wantErr: "device and FIFO",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store := objectstore.NewMemoryStore("")
			if err := store.Put("layer", bytes.NewReader(test.payload)); err != nil {
				t.Fatal(err)
			}
			layer := legacyLayer("layer", "layer", test.payload)
			layer = test.layer(layer)
			err := (LayerApplier{Store: store}).Apply(context.Background(), root, []Layer{layer})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Apply() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

type tarEntry struct {
	name string
	body string
	kind byte
}

func legacyTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := tar.NewWriter(&payload)
	for _, entry := range entries {
		kind := entry.kind
		if kind == 0 {
			kind = tar.TypeReg
		}
		header := &tar.Header{Name: entry.name, Mode: 0o600, Typeflag: kind}
		if kind == tar.TypeReg {
			header.Size = int64(len(entry.body))
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size != 0 {
			if _, err := writer.Write([]byte(entry.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return payload.Bytes()
}

func legacyLayer(id, key string, payload []byte) Layer {
	diff := digest.FromBytes(payload).String()
	return Layer{
		ID: id, DiffObjectKey: key, DiffSize: int64(len(payload)),
		DiffDigest: diff, DiffID: diff, DiffMediaType: ocispec.MediaTypeImageLayer,
	}
}
