//go:build linux

// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ocirootfs

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

const maxProcdBytes = 512 << 20

// Importer resolves, verifies, and applies digest-pinned OCI images. The
// supplied resolver owns registry authentication; no credential enters the
// result or staging filesystem.
type Importer struct {
	resolver     remotes.Resolver
	limits       Limits
	allowNonRoot bool
}

// NewImporter creates a production importer. Import requires a root process,
// a root-owned 0700 work directory, and a root-owned immutable procd source.
func NewImporter(resolver remotes.Resolver, limits Limits) (*Importer, error) {
	if resolver == nil {
		return nil, fmt.Errorf("OCI resolver is required")
	}
	normalized, err := normalizeLimits(limits)
	if err != nil {
		return nil, err
	}
	return &Importer{resolver: resolver, limits: normalized}, nil
}

// ValidateLocalImportEnvironment verifies privileged local inputs without
// contacting a registry or creating staging state.
func ValidateLocalImportEnvironment(workRoot, procdPath string, expectedProcdDigest digest.Digest) error {
	if _, err := validateWorkRoot(workRoot, false); err != nil {
		return err
	}
	procd, _, _, err := openVerifiedProcd(procdPath, expectedProcdDigest, false)
	if err != nil {
		return err
	}
	if err := procd.Close(); err != nil {
		return fmt.Errorf("close verified production procd: %w", err)
	}
	return nil
}

// Import creates a new operation-owned staging directory, applies all layers
// with OCI whiteout semantics, injects exact procd, and syncs the filesystem.
// Any failure removes the partial staging directory.
func (i *Importer) Import(ctx context.Context, request Request) (_ Result, resultErr error) {
	if i == nil || i.resolver == nil {
		return Result{}, fmt.Errorf("OCI RootFS importer is not configured")
	}
	if !i.allowNonRoot && os.Geteuid() != 0 {
		return Result{}, fmt.Errorf("OCI RootFS import requires root")
	}
	workRoot, err := validateWorkRoot(request.WorkRoot, i.allowNonRoot)
	if err != nil {
		return Result{}, err
	}
	procd, procdDigest, owner, err := openVerifiedProcd(
		request.ProcdPath,
		request.ExpectedProcdDigest,
		i.allowNonRoot,
	)
	if err != nil {
		return Result{}, err
	}
	defer procd.Close()
	image, err := resolveImage(ctx, i.resolver, request.Reference, request.Platform, i.limits)
	if err != nil {
		return Result{}, err
	}
	root, err := os.MkdirTemp(workRoot, "oci-rootfs-")
	if err != nil {
		return Result{}, fmt.Errorf("create OCI RootFS staging directory: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return Result{}, fmt.Errorf("protect OCI RootFS staging directory: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, os.RemoveAll(root))
		}
	}()

	state := &applyState{limits: i.limits}
	for index, layer := range image.manifest.Layers {
		if err := applyLayer(ctx, image.fetcher, root, layer, image.config.RootFS.DiffIDs[index], state); err != nil {
			return Result{}, fmt.Errorf("apply OCI layer %d (%s): %w", index, layer.Digest, err)
		}
	}
	if err := injectProcd(root, procd, procdDigest, owner); err != nil {
		return Result{}, err
	}
	if err := syncDirectory(root); err != nil {
		return Result{}, err
	}
	layers := make([]digest.Digest, len(image.manifest.Layers))
	for index, layer := range image.manifest.Layers {
		layers[index] = layer.Digest
	}
	return Result{
		Reference: image.reference, SourceDigest: image.sourceDigest,
		ManifestDigest: image.manifestDigest, ConfigDigest: image.configDigest,
		Platform: image.platform, LayerDigests: layers,
		DiffIDs:     append([]digest.Digest(nil), image.config.RootFS.DiffIDs...),
		ProcdDigest: procdDigest, RootPath: root,
		UnpackedBytes: state.unpackedBytes, Files: state.files,
	}, nil
}

type fileOwner struct {
	uid int
	gid int
}

func validateWorkRoot(raw string, allowNonRoot bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	clean := filepath.Clean(trimmed)
	if raw == "" || raw != trimmed || trimmed != clean || !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return "", fmt.Errorf("OCI work root must be a canonical non-root absolute path")
	}
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil || resolved != clean {
		return "", fmt.Errorf("OCI work root path must not traverse symlinks")
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("stat OCI work root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return "", fmt.Errorf("OCI work root must be a mode 0700 directory without symlinks")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !allowNonRoot && stat.Uid != 0 {
		return "", fmt.Errorf("OCI work root must be owned by root")
	}
	return clean, nil
}

func openVerifiedProcd(
	raw string,
	expected digest.Digest,
	allowNonRoot bool,
) (*os.File, digest.Digest, fileOwner, error) {
	trimmed := strings.TrimSpace(raw)
	clean := filepath.Clean(trimmed)
	if raw == "" || raw != trimmed || trimmed != clean || !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return nil, "", fileOwner{}, fmt.Errorf("procd path must be a canonical non-root absolute path")
	}
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil || resolved != clean {
		return nil, "", fileOwner{}, fmt.Errorf("procd path must not traverse symlinks")
	}
	if err := validateSHA256Digest(expected); err != nil {
		return nil, "", fileOwner{}, fmt.Errorf("expected procd digest: %w", err)
	}
	fd, err := unix.Open(clean, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "", fileOwner{}, fmt.Errorf("open production procd: %w", err)
	}
	file := os.NewFile(uintptr(fd), clean)
	if file == nil {
		_ = unix.Close(fd)
		return nil, "", fileOwner{}, fmt.Errorf("wrap production procd file descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, "", fileOwner{}, fmt.Errorf("stat production procd: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxProcdBytes ||
		info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 || !allowNonRoot && stat.Uid != 0 {
		_ = file.Close()
		return nil, "", fileOwner{}, fmt.Errorf("production procd must be a root-owned, executable, non-writable regular file within 1..%d bytes", maxProcdBytes)
	}
	hasher := digest.SHA256.Digester()
	if _, err := io.Copy(hasher.Hash(), file); err != nil {
		_ = file.Close()
		return nil, "", fileOwner{}, fmt.Errorf("hash production procd: %w", err)
	}
	actual := hasher.Digest()
	if actual != expected {
		_ = file.Close()
		return nil, "", fileOwner{}, fmt.Errorf("production procd digest is %s, expected %s", actual, expected)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, "", fileOwner{}, fmt.Errorf("rewind production procd: %w", err)
	}
	owner := fileOwner{uid: int(stat.Uid), gid: int(stat.Gid)}
	if !allowNonRoot {
		owner = fileOwner{}
	}
	return file, actual, owner, nil
}

type applyState struct {
	limits        Limits
	unpackedBytes int64
	files         int
	fileBytes     int64
}

func applyLayer(
	ctx context.Context,
	fetcher remotes.Fetcher,
	root string,
	descriptor ocispec.Descriptor,
	expectedDiffID digest.Digest,
	state *applyState,
) error {
	raw, err := fetcher.Fetch(ctx, descriptor)
	if err != nil {
		return err
	}
	verified := newVerifiedDescriptorReader(raw, descriptor)
	decompressed, err := compression.DecompressStream(verified)
	if err != nil {
		_ = raw.Close()
		return fmt.Errorf("decompress layer: %w", err)
	}
	expectedCompression, err := expectedLayerCompression(descriptor.MediaType)
	if err != nil {
		_ = decompressed.Close()
		_ = raw.Close()
		return err
	}
	actualCompression := map[compression.Compression]string{
		compression.Uncompressed: "none",
		compression.Gzip:         "gzip",
		compression.Zstd:         "zstd",
	}[decompressed.GetCompression()]
	if actualCompression == "" || actualCompression != expectedCompression {
		_ = decompressed.Close()
		_ = raw.Close()
		return fmt.Errorf("layer compression is %q, media type requires %q", actualCompression, expectedCompression)
	}
	remaining := state.limits.MaxImageUnpacked - state.unpackedBytes
	if remaining <= 0 {
		_ = decompressed.Close()
		_ = raw.Close()
		return fmt.Errorf("image unpacked byte limit is exhausted")
	}
	limit := min(state.limits.MaxLayerUnpacked, remaining)
	bounded := &hardLimitReader{reader: decompressed, limit: limit, name: "OCI layer unpacked bytes"}
	digester := digest.SHA256.Digester()
	stream := io.TeeReader(bounded, digester.Hash())
	filter := state.layerFilter()
	_, applyErr := archive.Apply(ctx, root, stream, archive.WithFilter(filter))
	if applyErr == nil {
		_, applyErr = io.Copy(io.Discard, stream)
	}
	closeDecompressedErr := decompressed.Close()
	closeRawErr := raw.Close()
	verifyErr := verified.Verify()
	if err := errors.Join(applyErr, closeDecompressedErr, closeRawErr, verifyErr); err != nil {
		return err
	}
	state.unpackedBytes += bounded.read
	if actual := digester.Digest(); actual != expectedDiffID {
		return fmt.Errorf("layer diff_id is %s, expected %s", actual, expectedDiffID)
	}
	return nil
}

func (s *applyState) layerFilter() archive.Filter {
	return func(header *tar.Header) (bool, error) {
		if header == nil {
			return false, fmt.Errorf("OCI layer contains a nil tar header")
		}
		s.files++
		if s.files > s.limits.MaxFiles {
			return false, fmt.Errorf("OCI image contains more than %d entries", s.limits.MaxFiles)
		}
		name := filepath.ToSlash(header.Name)
		if name == "" || len(name) > s.limits.MaxPathBytes || strings.ContainsRune(name, '\x00') ||
			strings.Contains(name, "\\") || strings.HasPrefix(name, "/") || name == ".." || strings.HasPrefix(name, "../") {
			return false, fmt.Errorf("OCI layer path is unsafe or exceeds %d bytes", s.limits.MaxPathBytes)
		}
		if len(header.Linkname) > s.limits.MaxPathBytes || strings.ContainsRune(header.Linkname, '\x00') {
			return false, fmt.Errorf("OCI layer link target exceeds configured bounds")
		}
		if header.Size < 0 || header.Size > s.limits.MaxFileBytes || s.fileBytes > s.limits.MaxImageUnpacked-header.Size {
			return false, fmt.Errorf("OCI entry or cumulative file bytes exceed configured bounds")
		}
		s.fileBytes += header.Size
		switch header.Typeflag {
		case tar.TypeReg, tar.TypeRegA, tar.TypeDir, tar.TypeLink, tar.TypeSymlink, tar.TypeXGlobalHeader:
		case tar.TypeBlock, tar.TypeChar, tar.TypeFifo:
			return false, fmt.Errorf("OCI layer device and FIFO entries are forbidden")
		default:
			return false, fmt.Errorf("OCI layer tar type %d is unsupported", header.Typeflag)
		}
		if len(header.PAXRecords) > s.limits.MaxPAXRecords {
			return false, fmt.Errorf("OCI entry has more than %d PAX records", s.limits.MaxPAXRecords)
		}
		paxBytes := 0
		for key, value := range header.PAXRecords {
			paxBytes += len(key) + len(value)
			if paxBytes > s.limits.MaxPAXBytes {
				return false, fmt.Errorf("OCI entry PAX data exceeds %d bytes", s.limits.MaxPAXBytes)
			}
		}
		return true, nil
	}
}

type hardLimitReader struct {
	reader io.Reader
	limit  int64
	read   int64
	name   string
}

func (r *hardLimitReader) Read(payload []byte) (int, error) {
	if r.read >= r.limit {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n > 0 {
			return 0, fmt.Errorf("%s exceeds %d", r.name, r.limit)
		}
		return 0, err
	}
	remaining := r.limit - r.read
	if int64(len(payload)) > remaining {
		payload = payload[:remaining]
	}
	n, err := r.reader.Read(payload)
	r.read += int64(n)
	return n, err
}

type verifiedDescriptorReader struct {
	reader     io.ReadCloser
	expected   ocispec.Descriptor
	hasher     hash.Hash
	read       int64
	reachedEOF bool
	err        error
}

func newVerifiedDescriptorReader(reader io.ReadCloser, descriptor ocispec.Descriptor) *verifiedDescriptorReader {
	return &verifiedDescriptorReader{reader: reader, expected: descriptor, hasher: digest.SHA256.Hash()}
}

func (r *verifiedDescriptorReader) Read(payload []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.reader.Read(payload)
	if n > 0 {
		r.read += int64(n)
		_, _ = r.hasher.Write(payload[:n])
		if r.read > r.expected.Size {
			r.err = fmt.Errorf("layer compressed bytes exceed descriptor size %d", r.expected.Size)
			return n, r.err
		}
	}
	if err == io.EOF {
		r.reachedEOF = true
		r.err = r.validate()
		if r.err != nil {
			return n, r.err
		}
	}
	return n, err
}

func (r *verifiedDescriptorReader) Verify() error {
	if r.err != nil {
		return r.err
	}
	if !r.reachedEOF {
		return fmt.Errorf("layer stream did not reach EOF")
	}
	return r.validate()
}

func (r *verifiedDescriptorReader) validate() error {
	if r.read != r.expected.Size {
		return fmt.Errorf("layer compressed size is %d, expected %d", r.read, r.expected.Size)
	}
	actual := digest.NewDigest(digest.SHA256, r.hasher)
	if actual != r.expected.Digest {
		return fmt.Errorf("layer compressed digest is %s, expected %s", actual, r.expected.Digest)
	}
	return nil
}

func injectProcd(root string, source *os.File, expected digest.Digest, owner fileOwner) error {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind production procd: %w", err)
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open imported root directory: %w", err)
	}
	defer unix.Close(rootFD)
	var stat unix.Stat_t
	if err := unix.Fstatat(rootFD, "procd", &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			return fmt.Errorf("OCI image contains a directory at /procd")
		}
		if err := unix.Unlinkat(rootFD, "procd", 0); err != nil {
			return fmt.Errorf("replace OCI /procd: %w", err)
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("inspect OCI /procd: %w", err)
	}
	fd, err := unix.Openat(rootFD, "procd", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o555)
	if err != nil {
		return fmt.Errorf("create injected /procd: %w", err)
	}
	target := os.NewFile(uintptr(fd), filepath.Join(root, "procd"))
	if target == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("wrap injected /procd file descriptor")
	}
	hasher := digest.SHA256.Digester()
	_, copyErr := io.Copy(io.MultiWriter(target, hasher.Hash()), source)
	metadataErr := errors.Join(target.Chmod(0o555), target.Chown(owner.uid, owner.gid), target.Sync())
	closeErr := target.Close()
	if err := errors.Join(copyErr, metadataErr, closeErr); err != nil {
		return fmt.Errorf("inject production procd: %w", err)
	}
	if actual := hasher.Digest(); actual != expected {
		return fmt.Errorf("injected procd digest is %s, expected %s", actual, expected)
	}
	return verifyInjectedProcd(rootFD, expected, owner)
}

func verifyInjectedProcd(rootFD int, expected digest.Digest, owner fileOwner) error {
	fd, err := unix.Openat(rootFD, "procd", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("reopen injected /procd: %w", err)
	}
	file := os.NewFile(uintptr(fd), "injected-/procd")
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("wrap injected /procd verification descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o555 || int(stat.Uid) != owner.uid || int(stat.Gid) != owner.gid {
		return fmt.Errorf("injected /procd metadata does not match the production contract")
	}
	hasher := digest.SHA256.Digester()
	if _, err := io.Copy(hasher.Hash(), file); err != nil {
		return err
	}
	if actual := hasher.Digest(); actual != expected {
		return fmt.Errorf("reopened /procd digest is %s, expected %s", actual, expected)
	}
	return nil
}

func syncDirectory(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open imported root for sync: %w", err)
	}
	defer unix.Close(fd)
	if err := unix.Syncfs(fd); err != nil {
		return fmt.Errorf("sync imported OCI rootfs: %w", err)
	}
	return nil
}
