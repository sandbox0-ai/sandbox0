//go:build linux

package session

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
	"github.com/moby/sys/mountinfo"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	demandReadImageBytes = int64(512 << 20)
	demandReadFileBytes  = 4 << 20
)

// TestPrivilegedDemandReadEncryptedRootFSReattachesXFSOverlay owns only the
// explicitly selected, unused NBD device and a generated disposable fixture.
// The runner must also exclude that device from every ctld allocation pool.
func TestPrivilegedDemandReadEncryptedRootFSReattachesXFSOverlay(t *testing.T) {
	for _, test := range []struct {
		name          string
		rangeBytes    int
		frameBytes    int64
		cacheBytes    int64
		prefixBytes   int64
		parallelBytes int64
	}{
		{"legacy-1m", 1 << 20, 1 << 20, 0, 0, 0},
		{"adaptive-64k-no-cache", 64 << 10, 64 << 10, 0, 0, 0},
		{"adaptive-64k-cached", 64 << 10, 64 << 10, rootfsblock.DefaultReadCacheBytes, 0, 0},
		{"prefix-64k-cached", 64 << 10, 64 << 10, rootfsblock.DefaultReadCacheBytes, 256 << 10, 0},
		{"prefix-legacy-frame-cached", 1 << 20, 1 << 20, rootfsblock.DefaultReadCacheBytes, 256 << 10, 0},
		{"parallel-64k-cached", 64 << 10, 64 << 10, rootfsblock.DefaultReadCacheBytes, 256 << 10, 256 << 10},
		{"parallel-legacy-frame-cached", 64 << 10, 1 << 20, rootfsblock.DefaultReadCacheBytes, 256 << 10, 256 << 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			testPrivilegedDemandReadGeometry(t, rootfsblock.DescriptorVersion, test.rangeBytes, test.frameBytes, test.cacheBytes, test.prefixBytes, test.parallelBytes)
		})
	}
}

// The current reader combines compressed ranges, 16 KiB encrypted frames and a
// node-lifetime context. Legacy geometry alone cannot validate that path through
// kernel NBD, XFS copy-up and a fresh-cache WAL reattachment.
func TestPrivilegedCompressedDemandReadEncryptedRootFSReattachesXFSOverlay(t *testing.T) {
	for _, test := range []struct {
		name                   string
		frameBytes, cacheBytes int64
	}{
		{"current-16k-cached", 16 << 10, rootfsblock.DefaultReadCacheBytes},
		{"current-16k-no-data-cache", 16 << 10, 0},
		{"legacy-1m-frame-fallback", 1 << 20, rootfsblock.DefaultReadCacheBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			testPrivilegedDemandReadGeometry(t, rootfsblock.CompressedFormatVersion,
				rootfsblock.CompressedDataRangeBytes, test.frameBytes, test.cacheBytes, 256<<10, 256<<10)
		})
	}
}

func testPrivilegedDemandReadGeometry(t *testing.T, formatVersion, rangeBytes int, frameBytes, cacheBytes, prefixBytes, parallelBytes int64) {
	devicePath := os.Getenv(privilegedNBDDeviceEnv)
	if devicePath == "" {
		t.Skipf("set %s to an unused NBD device outside the ctld pool", privilegedNBDDeviceEnv)
	}
	require.Zero(t, os.Geteuid(), "privileged demand-read test requires root")
	require.Regexp(t, `^/dev/nbd[0-9]+$`, devicePath)
	for _, command := range []string{"mkfs.xfs", "mount", "cp", "umount", "xfs_repair"} {
		_, err := exec.LookPath(command)
		require.NoError(t, err, "required fixture command: %s", command)
	}
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{devicePath}})
	require.NoError(t, err)
	require.NoError(t, demandReadUnusedDevice(runtime, devicePath), "selected NBD device must start unused")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	root, err := os.MkdirTemp("", "sandbox0-demand-read-")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Logf("disposable fixture: %s; exclusive test device: %s", root, devicePath)
	var mounts []*demandReadMount
	t.Cleanup(func() {
		defer cancel()
		var cleanupErr error
		for index := len(mounts) - 1; index >= 0; index-- {
			cleanupErr = errors.Join(cleanupErr, mounts[index].close())
		}
		// Do not let automatic TempDir removal descend into a failed build or
		// runtime mount. Retain the exact scratch directory on cleanup failure.
		entries, err := mountinfo.GetMounts(nil)
		cleanupErr = errors.Join(cleanupErr, err)
		for _, entry := range entries {
			relative, err := filepath.Rel(root, entry.Mountpoint)
			if err == nil && filepath.IsLocal(relative) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("fixture mount remains: %s", entry.Mountpoint))
			}
		}
		cleanupErr = errors.Join(cleanupErr, demandReadUnusedDevice(runtime, devicePath))
		if cleanupErr != nil {
			t.Errorf("cleanup failed; retaining %s: %v", root, cleanupErr)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove detached fixture %s: %v", root, err)
		}
	})

	sourceRoot := filepath.Join(root, "source")
	require.NoError(t, os.Mkdir(sourceRoot, 0o700))
	files := []string{"first.bin", "second.bin"}
	expected := [][]byte{demandReadPattern(1), demandReadPattern(2)}
	for index, name := range files {
		require.NoError(t, os.WriteFile(filepath.Join(sourceRoot, name), expected[index], 0o600))
	}
	imagePath := filepath.Join(root, "base.xfs")
	require.NoError(t, (rootfsartifact.XFSBuilder{}).Build(ctx, sourceRoot, imagePath, demandReadImageBytes))
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	encryptor, err := objectstore.NewKeyEncryptor(string(keyPEM), "")
	require.NoError(t, err)
	objects := objectstore.NewMemoryStore(t.Name())
	encryption := objectstore.EncryptionConfig{Enabled: true, KeyEncryptor: encryptor, ChunkSize: frameBytes}
	writer := objectstore.Encrypting(objects, encryption).(objectstore.ContextConditionalStore)
	image, err := os.Open(imagePath)
	require.NoError(t, err)
	built, buildErr := rootfsblock.BuildMaterializedGeneration(ctx, image, demandReadImageBytes,
		rootfsblock.ObjectStorePublisher{Store: writer}, rootfsblock.BuildOptions{
			FormatVersion:  formatVersion,
			DataRangeBytes: rangeBytes, PackBytes: 8 << 20, ObjectPrefix: "rootfs/demand-read-test",
		})
	require.NoError(t, errors.Join(buildErr, image.Close()))
	require.Equal(t, formatVersion, built.Descriptor.Version)
	// The attached device must reconstruct from immutable objects and the WAL;
	// neither the original image nor a preexisting tenant rootfs is available.
	require.NoError(t, os.Remove(imagePath))
	require.NoError(t, os.RemoveAll(sourceRoot))
	identity := rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: "privileged-demand-read",
		GenerationID: "encrypted-base", WriterEpoch: 1, LogicalSizeBytes: demandReadImageBytes,
		BaseRootDigest: built.Descriptor.MappingRoot.RootDigest,
	}
	branchPath := filepath.Join(root, "branch.wal")

	attach := func(phase string) *demandReadMount {
		t.Helper()
		unwraps := &demandReadEncryptor{Encryptor: encryptor, calls: make(map[[32]byte]int)}
		config := encryption
		config.KeyEncryptor = unwraps
		if prefixBytes > 0 {
			// Current reader defaults must also support older stored frames.
			config.ChunkSize = 64 << 10
			if formatVersion == rootfsblock.CompressedFormatVersion {
				config.ChunkSize = 16 << 10
			}
		}
		store := objectstore.EncryptingImmutable(objects, config, objectstore.EncryptedHeaderCacheConfig{
			MaxEntries: 128, MaxBytes: 8 << 20, MaxPrefixBytes: prefixBytes,
			MaxParallelReadBytes: parallelBytes,
		}).(objectstore.ContextConditionalStore)
		source := &demandReadSource{ctx: ctx, store: store, calls: make(map[rootfsblock.ObjectRange]int)}
		mounted := &demandReadMount{
			runtime: runtime, devicePath: devicePath, xfs: filepath.Join(root, phase, "xfs"),
			merged: filepath.Join(root, phase, "merged"), source: source, unwraps: unwraps,
			contextReader: formatVersion == rootfsblock.CompressedFormatVersion,
		}
		mounts = append(mounts, mounted) // Register cleanup before acquiring resources.
		// Each attachment starts with an independently empty cache. Cases
		// cover both exact demand without caching and bounded bulk read-ahead.
		var reader *rootfsblock.Reader
		if mounted.contextReader {
			cache, cacheErr := rootfsblock.NewReadCache(cacheBytes)
			require.NoError(t, cacheErr)
			reader, err = rootfsblock.NewReaderWithCacheContext(ctx, source, built.Descriptor, cache)
		} else {
			reader, err = rootfsblock.NewReader(source, built.Descriptor, cacheBytes)
		}
		require.NoError(t, err)
		mounted.branch, err = rootfsblock.OpenBranch(branchPath, identity, reader)
		require.NoError(t, err)
		mounted.backend = &demandReadBackend{Branch: mounted.branch}
		mounted.device, err = rootfsblock.StartKernelNBD(ctx, ctx, mounted.backend, rootfsblock.KernelNBDOptions{
			DevicePath: devicePath, RequestTimeout: 10 * time.Second, ReadyTimeout: 10 * time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, runtime.MountXFS(devicePath, mounted.xfs))
		require.NoError(t, runtime.MountOverlay(mounted.xfs, mounted.merged))
		return mounted
	}

	first := attach("first")
	// FIEMAP reads extent metadata, not file contents. Gate only the mapped
	// data ranges so unrelated XFS metadata can progress during the test.
	page := demandReadRootPage(t, ctx, first.source.store, built.Descriptor)
	gate := &demandReadGate{targets: make(map[rootfsblock.ObjectRange]bool), entered: make(chan struct{}, 2), release: make(chan struct{})}
	var directFiles []*os.File
	var directOffsets []int64
	for _, name := range files {
		lower := first.open(t, filepath.Join(first.xfs, "lower", name), false)
		directFiles = append(directFiles, first.open(t, filepath.Join(first.merged, name), true))
		selected := false
		// A coarse 1 MiB range read during mount/path lookup can cache an
		// otherwise untouched file offset. Gate only an independently cold
		// source range, proven from actual previous transport spans. FIEMAP
		// itself reads metadata, not candidate file contents.
		for _, offset := range []int64{2 << 20, 3 << 20, 1 << 20, 0} {
			physical := demandReadPhysicalOffset(t, lower, uint64(offset), uint64(rangeBytes))
			object := demandReadMappedRange(t, ctx, first.source.store, page, physical)
			first.source.mu.Lock()
			previousReads := 0
			for prior, count := range first.source.calls {
				if prior.Key == object.Key && prior.Offset < object.Offset+object.Length && object.Offset < prior.Offset+prior.Length {
					previousReads += count
				}
			}
			first.source.mu.Unlock()
			t.Logf("gated candidate %s offset=%d previous_overlapping_reads=%d", name, offset, previousReads)
			if cacheBytes > 0 && previousReads > 0 {
				continue
			}
			require.NotContains(t, gate.targets, object, "fixture reads must target independent immutable ranges")
			gate.targets[object] = false
			directOffsets = append(directOffsets, offset)
			selected = true
			break
		}
		require.True(t, selected, "fixture needs an actually cold source range for %s", name)
	}
	// Drain mount/Overlay setup writes before holding READs. An intervening
	// legitimate FLUSH barrier must not be mistaken for read serialization.
	require.NoError(t, syncPath(first.xfs))
	first.source.setGate(gate)
	results := make(chan error, len(files))
	// Use one block per gated read so splitting a large request cannot fill
	// every NBD read slot with waiters for only one file's immutable range.
	for index, file := range directFiles {
		first.readers.Add(1)
		go func() {
			defer first.readers.Done()
			offset := directOffsets[index]
			results <- demandReadDirect(file, offset, expected[index][offset:offset+rootfsblock.LogicalBlockSize])
		}()
	}
	gateCtx, stopGate := context.WithTimeout(ctx, 5*time.Second)
	for range files {
		select {
		case <-gate.entered:
		case <-gateCtx.Done():
			stopGate()
			t.Fatal("two independent checksummed source reads did not overlap behind kernel NBD")
		}
	}
	stopGate()
	gate.unblock()
	first.readers.Wait()
	for range files {
		require.NoError(t, <-results)
	}
	for index, file := range directFiles {
		require.NoError(t, demandReadDirect(file, 0, expected[index]))
	}
	beforeWrite := first.branch.DirtyTailUsage().DirtyBytes
	patch := bytes.Repeat([]byte{0xa7}, 2*rootfsblock.LogicalBlockSize+333)
	const patchOffset = (1 << 20) + 123
	require.NoError(t, demandReadWriteOverride(filepath.Join(first.merged, files[0]), patch, patchOffset))
	changed := append([]byte(nil), expected[0]...)
	copy(changed[patchOffset:], patch)
	// Reopen after Overlay copy-up; the old open file may still refer to lower.
	changedFile := first.open(t, filepath.Join(first.merged, files[0]), true)
	require.NoError(t, demandReadDirect(changedFile, 0, changed))
	lower := first.open(t, filepath.Join(first.xfs, "lower", files[0]), true)
	require.NoError(t, demandReadDirect(lower, 0, expected[0]), "immutable lower must retain original data")
	require.FileExists(t, filepath.Join(first.xfs, "upper", files[0]))
	require.Greater(t, first.branch.DirtyTailUsage().DirtyBytes, beforeWrite)
	// Bootstrap also creates nested platform directories and atomically binds a
	// sandbox identity. Exercise those metadata writes on the attached Overlay,
	// including file fsync, instead of testing only an existing file's copy-up.
	metadataPath := filepath.Join("platform", "sessions", "sandbox-id")
	metadataDir := filepath.Dir(filepath.Join(first.merged, metadataPath))
	require.NoError(t, os.MkdirAll(metadataDir, 0o700))
	metadata, err := os.CreateTemp(metadataDir, ".sandbox-id-*")
	require.NoError(t, err)
	_, writeErr := metadata.WriteString("metadata-fixture\n")
	require.NoError(t, errors.Join(writeErr, metadata.Sync(), metadata.Close()))
	require.NoError(t, os.Rename(metadata.Name(), filepath.Join(first.merged, metadataPath)))
	metadataInfo, err := os.Stat(filepath.Join(first.merged, metadataPath))
	require.NoError(t, err)
	require.Equal(t, int64(len("metadata-fixture\n")), metadataInfo.Size())
	require.NoError(t, first.close())
	first.assertEncryptedReads(t)
	require.Positive(t, first.dirtyBytes)

	second := attach("reattached")
	require.GreaterOrEqual(t, second.branch.DirtyTailUsage().DirtyBytes, first.dirtyBytes)
	for index, name := range files {
		want := expected[index]
		if index == 0 {
			want = changed
		}
		file := second.open(t, filepath.Join(second.merged, name), true)
		require.NoError(t, demandReadDirect(file, 0, want), "reattached Overlay data: %s", name)
	}
	lower = second.open(t, filepath.Join(second.xfs, "lower", files[0]), true)
	require.NoError(t, demandReadDirect(lower, 0, expected[0]))
	metadataPayload, err := os.ReadFile(filepath.Join(second.merged, metadataPath))
	require.NoError(t, err)
	require.Equal(t, "metadata-fixture\n", string(metadataPayload), "nested metadata must survive WAL reattachment")
	require.NoError(t, second.close())
	second.assertEncryptedReads(t)
	t.Logf("verified parallel demand, copy-up override, and WAL reattachment; dirty bytes: %d -> %d", first.dirtyBytes, second.dirtyBytes)
}

func demandReadPattern(file int) []byte {
	payload := make([]byte, demandReadFileBytes)
	for offset := 0; offset < len(payload); offset += 8 {
		binary.LittleEndian.PutUint64(payload[offset:], uint64(file)<<56|uint64(offset/8+1))
	}
	return payload
}

type demandReadEncryptor struct {
	objectstore.Encryptor
	mu    sync.Mutex
	calls map[[32]byte]int
}

func (e *demandReadEncryptor) Decrypt(payload []byte) ([]byte, error) {
	e.mu.Lock()
	e.calls[sha256.Sum256(payload)]++
	e.mu.Unlock()
	return e.Encryptor.Decrypt(payload)
}

type demandReadGate struct {
	mu      sync.Mutex
	targets map[rootfsblock.ObjectRange]bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *demandReadGate) unblock() { g.once.Do(func() { close(g.release) }) }

type demandReadSource struct {
	ctx          context.Context
	store        objectstore.ContextConditionalStore
	mu           sync.Mutex
	gate         *demandReadGate
	calls        map[rootfsblock.ObjectRange]int
	legacyCalls  atomic.Int64
	contextCalls atomic.Int64
}

func (s *demandReadSource) setGate(gate *demandReadGate) {
	s.mu.Lock()
	s.gate = gate
	s.mu.Unlock()
}

func (s *demandReadSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.legacyCalls.Add(1)
	return s.get(s.ctx, key, offset, length)
}

func (s *demandReadSource) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.contextCalls.Add(1)
	return s.get(ctx, key, offset, length)
}

func (s *demandReadSource) get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	object := rootfsblock.ObjectRange{Key: key, Offset: offset, Length: length}
	s.mu.Lock()
	s.calls[object]++
	gate := s.gate
	s.mu.Unlock()
	if gate != nil {
		gate.mu.Lock()
		seen, target := gate.targets[object]
		if target && !seen {
			gate.targets[object] = true
		}
		gate.mu.Unlock()
		if target && !seen {
			gate.entered <- struct{}{}
			select {
			case <-gate.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return s.store.GetContext(ctx, key, offset, length)
}

type demandReadMount struct {
	runtime       *LinuxRuntime
	devicePath    string
	xfs           string
	merged        string
	source        *demandReadSource
	unwraps       *demandReadEncryptor
	branch        *rootfsblock.Branch
	backend       *demandReadBackend
	device        *rootfsblock.KernelNBDDevice
	files         []*os.File
	readers       sync.WaitGroup
	dirtyBytes    int64
	closed        bool
	absenceWait   time.Duration
	absenceChecks int
	contextReader bool
}

// Instrument only the NBD-facing boundary. Branch still receives the concrete
// *rootfsblock.Reader, preserving its verified clean-span coalescing path.
type demandReadBackend struct {
	*rootfsblock.Branch
	reads atomic.Int64
}

func (b *demandReadBackend) ReadAt(payload []byte, offset int64) (int, error) {
	b.reads.Add(1)
	return b.Branch.ReadAt(payload, offset)
}

func (m *demandReadMount) open(t *testing.T, path string, direct bool) *os.File {
	t.Helper()
	flags := unix.O_RDONLY | unix.O_CLOEXEC
	if direct {
		flags |= unix.O_DIRECT
	}
	fd, err := unix.Open(path, flags, 0)
	require.NoError(t, err)
	file := os.NewFile(uintptr(fd), path)
	m.files = append(m.files, file)
	return file
}

func (m *demandReadMount) close() error {
	if m.closed {
		return nil
	}
	var result error
	m.source.mu.Lock()
	gate := m.source.gate
	m.source.mu.Unlock()
	if gate != nil {
		gate.unblock()
	}
	// Wake any pressure admission before joining filesystem readers or NBD.
	// Cancellation alone cannot wake Branch's pressure condition variable.
	if m.branch != nil {
		result = errors.Join(result, m.branch.BeginRetirement())
	}
	m.readers.Wait()
	for _, file := range m.files {
		result = errors.Join(result, file.Close())
	}
	m.files = nil
	for _, path := range []string{m.merged, filepath.Join(m.xfs, "lower"), m.xfs} {
		mounted, err := mountinfo.Mounted(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("inspect mount %s: %w", path, err))
			continue
		}
		if !mounted {
			continue
		}
		result = errors.Join(result, syncPath(path))
		if err := unix.Unmount(path, 0); err != nil {
			// Detach the exact test mount on failure, but retain the original
			// error so a lazy detach can never count as a successful regression.
			result = errors.Join(result, fmt.Errorf("unmount %s: %w", path, err), unix.Unmount(path, unix.MNT_DETACH))
		}
	}
	if m.branch != nil {
		result = errors.Join(result, m.branch.Flush())
		m.dirtyBytes = m.branch.DirtyTailUsage().DirtyBytes
	}
	if m.device != nil {
		result = errors.Join(result, m.device.Close())
		m.device = nil
	}
	if m.branch != nil {
		result = errors.Join(result, m.branch.Close())
		m.branch = nil
	}
	// Kernel sysfs teardown can trail NBD_DO_IT/file close. Observe the same
	// complete absence proof until it succeeds; never treat Close alone as
	// authority to reattach or remove the fixture. This is test cleanup only,
	// independent of the unchanged request/readiness timeouts above.
	began := time.Now()
	absenceCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := demandReadWaitForAbsence(absenceCtx, func() error {
		m.absenceChecks++
		_, err := m.runtime.InspectCrashFence(m.devicePath, m.xfs, m.merged)
		return errors.Join(err, demandReadUnusedDevice(m.runtime, m.devicePath))
	})
	cancel()
	m.absenceWait += time.Since(began)
	result = errors.Join(result, err)
	m.closed = result == nil
	return result
}

func demandReadWaitForAbsence(ctx context.Context, inspect func() error) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := inspect()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestDemandReadAbsencePollingRequiresActualProof(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pending := errors.New("exact device remains owned")
		checks := 0
		err := demandReadWaitForAbsence(t.Context(), func() error {
			checks++
			if checks < 3 {
				return pending
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, checks)
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		err = demandReadWaitForAbsence(ctx, func() error { return pending })
		require.ErrorIs(t, err, pending)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func demandReadUnusedDevice(runtime *LinuxRuntime, devicePath string) error {
	if _, err := runtime.inspectCrashFenceDevice(devicePath); err != nil {
		return err
	}
	// Check size even on kernels that retain a zero-valued PID attribute.
	size, err := os.ReadFile(filepath.Join(runtime.sysBlockRoot, filepath.Base(devicePath), "size"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(size)) != "0" {
		return fmt.Errorf("selected NBD device %s retains nonzero size %q", devicePath, size)
	}
	return nil
}

func (m *demandReadMount) assertEncryptedReads(t *testing.T) {
	t.Helper()
	require.Positive(t, m.backend.reads.Load(), "kernel NBD must call the actual Branch read backend")
	if m.contextReader {
		require.Positive(t, m.source.contextCalls.Load(), "current reader must use context-aware transport")
		require.Zero(t, m.source.legacyCalls.Load(), "legacy Get must not hide a lost reader lifetime")
	}
	m.unwraps.mu.Lock()
	defer m.unwraps.mu.Unlock()
	m.source.mu.Lock()
	defer m.source.mu.Unlock()
	require.NotEmpty(t, m.unwraps.calls, "real RSA unwrap and AEAD reads must occur")
	for _, count := range m.unwraps.calls {
		require.Equal(t, 1, count, "immutable header/AEAD cache must reuse each unwrapped key")
	}
	var reads int
	for _, count := range m.source.calls {
		reads += count
	}
	require.Greater(t, reads, len(m.unwraps.calls), "multiple demanded ranges must reuse cached object headers")
	t.Logf("%s: %d kernel READs, %d verified range GETs, %d unique encrypted key unwraps",
		filepath.Base(filepath.Dir(m.xfs)), m.backend.reads.Load(), reads, len(m.unwraps.calls))
	t.Logf("exact absence proof: checks=%d elapsed=%s", m.absenceChecks, m.absenceWait)
}

func demandReadDirect(file *os.File, offset int64, expected []byte) (result error) {
	// Anonymous mmap supplies page alignment for O_DIRECT without depending on
	// the Go allocator. Direct reads avoid page-cache hits masking NBD demand.
	payload, err := unix.Mmap(-1, 0, len(expected), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, unix.Munmap(payload)) }()
	n, err := file.ReadAt(payload, offset)
	if err != nil || n != len(expected) {
		return fmt.Errorf("read %s offset %d: got %d bytes: %w", file.Name(), offset, n, errors.Join(err, io.ErrUnexpectedEOF))
	}
	if !bytes.Equal(payload, expected) {
		return fmt.Errorf("data mismatch in %s at offset %d", file.Name(), offset)
	}
	return nil
}

func demandReadWriteOverride(path string, payload []byte, offset int64) (result error) {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	n, err := file.WriteAt(payload, offset)
	if err != nil || n != len(payload) {
		return errors.Join(err, io.ErrShortWrite)
	}
	return file.Sync()
}

func demandReadRootPage(t *testing.T, ctx context.Context, store objectstore.ContextConditionalStore, descriptor rootfsblock.Descriptor) rootfsblock.MappingPage {
	t.Helper()
	return demandReadMappingPage(t, ctx, store, descriptor.MappingRoot.Object)
}

func demandReadMappingPage(t *testing.T, ctx context.Context, store objectstore.ContextConditionalStore, object rootfsblock.ObjectRange) rootfsblock.MappingPage {
	t.Helper()
	require.NoError(t, object.Validate(rootfsblock.MaxMappingRootBytes))
	body, err := store.GetContext(ctx, object.Key, object.Offset, object.StoredLength())
	require.NoError(t, err)
	payload, readErr := io.ReadAll(io.LimitReader(body, object.StoredLength()+1))
	require.NoError(t, errors.Join(readErr, body.Close()))
	require.EqualValues(t, object.StoredLength(), len(payload))
	if object.Encoding != "" {
		require.Equal(t, rootfsblock.RangeEncodingZstd, object.Encoding)
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(rootfsblock.CompressedDataRangeBytes),
			zstd.WithDecoderMaxMemory(rootfsblock.MaxMappingRootBytes), zstd.WithDecodeAllCapLimit(true))
		require.NoError(t, err)
		defer decoder.Close()
		payload, err = decoder.DecodeAll(payload, make([]byte, 0, int(object.Length)))
		require.NoError(t, err)
	}
	require.EqualValues(t, object.Length, len(payload))
	require.Equal(t, object.Checksum, digest.FromBytes(payload).String())
	page, err := rootfsblock.DecodeMappingPage(payload)
	require.NoError(t, err)
	return page
}

func demandReadMappedRange(t *testing.T, ctx context.Context, store objectstore.ContextConditionalStore, page rootfsblock.MappingPage, physical uint64) rootfsblock.ObjectRange {
	t.Helper()
	block := physical / rootfsblock.LogicalBlockSize
	for _, entry := range page.Entries {
		if block >= entry.LogicalStart && block-entry.LogicalStart < uint64(entry.BlockCount) {
			if entry.Kind == rootfsblock.MappingEntryChild {
				child := demandReadMappingPage(t, ctx, store, entry.Object)
				require.Equal(t, page.Level, child.Level+1)
				require.Equal(t, entry.LogicalStart, child.StartBlock)
				require.EqualValues(t, entry.BlockCount, child.BlockCount)
				return demandReadMappedRange(t, ctx, store, child, physical)
			}
			require.Equal(t, rootfsblock.MappingEntryData, entry.Kind)
			object := entry.Object
			if page.Version == rootfsblock.CompressedFormatVersion {
				require.Equal(t, rootfsblock.RangeEncodingZstd, object.Encoding,
					"the patterned fixture must exercise actual compressed file data")
			}
			// Gate actual source transport identity. Encoded physical lengths
			// cannot be compared with decoded logical byte lengths.
			return rootfsblock.ObjectRange{Key: object.Key, Offset: object.Offset, Length: object.StoredLength()}
		}
	}
	t.Fatalf("fixture physical offset %d is not backed by a checksummed data range", physical)
	return rootfsblock.ObjectRange{}
}

func demandReadPhysicalOffset(t *testing.T, file *os.File, offset, length uint64) uint64 {
	t.Helper()
	// Linux FS_IOC_FIEMAP (_IOWR('f', 11, struct fiemap)) with one extent.
	// This metadata query never reads or prewarms tenant file contents.
	var mapping struct {
		Start, Length                  uint64
		Flags, Mapped, Count, Reserved uint32
		Extent                         struct {
			Logical, Physical, Length uint64
			Reserved64                [2]uint64
			Flags                     uint32
			Reserved                  [3]uint32
		}
	}
	mapping.Start, mapping.Length, mapping.Count = offset, length, 1
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), 0xc020660b, uintptr(unsafe.Pointer(&mapping)))
	require.Zero(t, errno, "FIEMAP fixture file: %s", file.Name())
	require.Equal(t, uint32(1), mapping.Mapped)
	// LAST and SHARED are harmless; reject delayed, unwritten, encoded, inline,
	// or otherwise unsuitable extents instead of guessing their disk location.
	require.Zero(t, mapping.Extent.Flags & ^uint32(0x1|0x2000))
	require.LessOrEqual(t, mapping.Extent.Logical, offset)
	require.GreaterOrEqual(t, mapping.Extent.Logical+mapping.Extent.Length, offset+length)
	return mapping.Extent.Physical + offset - mapping.Extent.Logical
}
