package legacyackmigration

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

const (
	defaultMaxLegacyLayerBytes int64 = 256 << 30
	defaultMaxLegacyChainBytes int64 = 1 << 40
	defaultMaxLegacyEntries          = 10_000_000
	defaultMaxLegacyPathBytes        = 4096
	defaultMaxLegacyFileBytes  int64 = 256 << 30
)

// LayerApplyLimits bound untrusted legacy tar metadata and content before it
// can become a target block-COW generation.
type LayerApplyLimits struct {
	MaxLayerBytes int64
	MaxChainBytes int64
	MaxEntries    int
	MaxPathBytes  int
	MaxFileBytes  int64
}

// LayerApplier verifies and applies one normalized parent-to-child legacy OCI
// layer chain. The destination is operation-owned staging and must be removed
// by the caller after any error because verification completes after apply.
type LayerApplier struct {
	Store  objectstore.Store
	Limits LayerApplyLimits
}

// Apply verifies exact decrypted sizes and SHA-256 digests while applying OCI
// whiteouts and filesystem changes to root.
func (a LayerApplier) Apply(ctx context.Context, root string, chain []Layer) error {
	if a.Store == nil {
		return fmt.Errorf("legacy layer object store is required")
	}
	root, err := validateLegacyApplyRoot(root)
	if err != nil {
		return err
	}
	limits, err := normalizeLayerApplyLimits(a.Limits)
	if err != nil {
		return err
	}
	if len(chain) == 0 {
		return fmt.Errorf("legacy layer chain is empty")
	}
	state := legacyLayerApplyState{limits: limits}
	for index, layer := range chain {
		if err := state.apply(ctx, a.Store, root, layer); err != nil {
			return fmt.Errorf("apply legacy layer %d (%s): %w", index, layer.ID, err)
		}
	}
	return nil
}

type legacyLayerApplyState struct {
	limits     LayerApplyLimits
	chainBytes int64
	entries    int
	fileBytes  int64
}

func (s *legacyLayerApplyState) apply(
	ctx context.Context,
	store objectstore.Store,
	root string,
	layer Layer,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(layer.ID) == "" || strings.TrimSpace(layer.DiffObjectKey) == "" {
		return fmt.Errorf("layer identity and object key are required")
	}
	if layer.DiffSize <= 0 || layer.DiffSize > s.limits.MaxLayerBytes ||
		s.chainBytes > s.limits.MaxChainBytes-layer.DiffSize {
		return fmt.Errorf("decrypted layer size exceeds configured bounds")
	}
	expected, err := digest.Parse(strings.TrimSpace(layer.DiffDigest))
	if err != nil || expected.Algorithm() != digest.SHA256 || expected.String() != layer.DiffDigest {
		return fmt.Errorf("layer digest must be canonical SHA-256")
	}
	if layer.DiffID != "" && layer.DiffID != layer.DiffDigest {
		return fmt.Errorf("uncompressed layer digest and DiffID differ")
	}
	reader, err := getLegacyLayer(ctx, store, layer.DiffObjectKey)
	if err != nil {
		return fmt.Errorf("read encrypted legacy object: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = reader.Close()
		}
	}()

	bounded := &io.LimitedReader{R: reader, N: layer.DiffSize + 1}
	digester := digest.SHA256.Digester()
	counted := &countingReader{reader: io.TeeReader(bounded, digester.Hash())}
	_, applyErr := archive.Apply(ctx, root, counted, archive.WithFilter(s.filter))
	if applyErr == nil {
		_, applyErr = io.Copy(io.Discard, counted)
	}
	closeErr := reader.Close()
	closed = true
	if err := errors.Join(applyErr, closeErr); err != nil {
		return err
	}
	if counted.read != layer.DiffSize {
		return fmt.Errorf("decrypted layer size is %d, expected %d", counted.read, layer.DiffSize)
	}
	if actual := digester.Digest(); actual != expected {
		return fmt.Errorf("decrypted layer digest is %s, expected %s", actual, expected)
	}
	s.chainBytes += counted.read
	return nil
}

func (s *legacyLayerApplyState) filter(header *tar.Header) (bool, error) {
	if header == nil {
		return false, fmt.Errorf("layer contains a nil tar header")
	}
	s.entries++
	if s.entries > s.limits.MaxEntries {
		return false, fmt.Errorf("layer chain contains more than %d entries", s.limits.MaxEntries)
	}
	name := filepath.ToSlash(header.Name)
	// The migration and its target runtime are Linux-only. A backslash is a
	// literal Linux filename byte, not a path separator. Legacy layers can
	// therefore preserve it without weakening the slash-based traversal checks.
	if name == "" || len(name) > s.limits.MaxPathBytes || strings.ContainsRune(name, '\x00') ||
		strings.HasPrefix(name, "/") || name == ".." || strings.HasPrefix(name, "../") {
		return false, fmt.Errorf("layer path is unsafe or exceeds %d bytes", s.limits.MaxPathBytes)
	}
	if len(header.Linkname) > s.limits.MaxPathBytes || strings.ContainsRune(header.Linkname, '\x00') {
		return false, fmt.Errorf("layer link target exceeds configured bounds")
	}
	if header.Size < 0 || header.Size > s.limits.MaxFileBytes ||
		s.fileBytes > s.limits.MaxChainBytes-header.Size {
		return false, fmt.Errorf("layer entry or cumulative file bytes exceed configured bounds")
	}
	s.fileBytes += header.Size
	switch header.Typeflag {
	case tar.TypeReg, tar.TypeRegA, tar.TypeDir, tar.TypeLink, tar.TypeSymlink, tar.TypeXGlobalHeader:
		return true, nil
	case tar.TypeBlock, tar.TypeChar, tar.TypeFifo:
		return false, fmt.Errorf("layer device and FIFO entries are not accepted automatically")
	default:
		return false, fmt.Errorf("layer entry type %d is unsupported", header.Typeflag)
	}
}

type countingReader struct {
	reader io.Reader
	read   int64
}

func (r *countingReader) Read(payload []byte) (int, error) {
	read, err := r.reader.Read(payload)
	r.read += int64(read)
	return read, err
}

func getLegacyLayer(ctx context.Context, store objectstore.Store, key string) (io.ReadCloser, error) {
	if contextual, ok := store.(objectstore.ContextConditionalStore); ok {
		return contextual.GetContext(ctx, key, 0, -1)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return store.Get(key, 0, -1)
}

func validateLegacyApplyRoot(root string) (string, error) {
	clean := filepath.Clean(strings.TrimSpace(root))
	if root != clean || !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return "", fmt.Errorf("legacy layer apply root must be a canonical non-root absolute path")
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("stat legacy layer apply root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("legacy layer apply root must be a directory without symlinks")
	}
	return clean, nil
}

func normalizeLayerApplyLimits(input LayerApplyLimits) (LayerApplyLimits, error) {
	limits := input
	if limits.MaxLayerBytes == 0 {
		limits.MaxLayerBytes = defaultMaxLegacyLayerBytes
	}
	if limits.MaxChainBytes == 0 {
		limits.MaxChainBytes = defaultMaxLegacyChainBytes
	}
	if limits.MaxEntries == 0 {
		limits.MaxEntries = defaultMaxLegacyEntries
	}
	if limits.MaxPathBytes == 0 {
		limits.MaxPathBytes = defaultMaxLegacyPathBytes
	}
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = defaultMaxLegacyFileBytes
	}
	if limits.MaxLayerBytes <= 0 || limits.MaxChainBytes <= 0 || limits.MaxLayerBytes > limits.MaxChainBytes ||
		limits.MaxEntries <= 0 || limits.MaxPathBytes <= 0 || limits.MaxFileBytes <= 0 ||
		limits.MaxFileBytes > limits.MaxChainBytes {
		return LayerApplyLimits{}, fmt.Errorf("legacy layer apply limits are invalid")
	}
	return limits, nil
}
