package runtimecheckpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

// CapturePeerInventory is a disposable hint about full chunks received before
// the final RootFS cut. It is not a manifest or execution authority. The source
// must compare these hashes with its final plan, and the destination must
// rehash every reused range before acknowledging the final image.
type CapturePeerInventory struct {
	ScopeDigest string `json:"scope_digest"`
	Files       []File `json:"files"`
}

func (i CapturePeerInventory) validate(scope CaptureScope, maxBytes int64) error {
	want, err := scope.Digest()
	if err != nil || want != i.ScopeDigest || maxBytes <= 0 || maxBytes > MaxImageBytes || len(i.Files) > MaxFiles {
		return fmt.Errorf("invalid capture peer inventory scope or limit")
	}
	if len(i.Files) != 0 {
		if err := validateImageFiles(i.Files, maxBytes); err != nil {
			return err
		}
		for _, file := range i.Files {
			if file.Size == 0 || file.Size%ChunkBytes != 0 {
				return fmt.Errorf("capture inventory requires full chunks")
			}
		}
	}
	payload, err := json.Marshal(i)
	if err != nil || len(payload) > MaxManifestBytes {
		return fmt.Errorf("capture peer inventory exceeds metadata bound")
	}
	return nil
}

// Encode validates a bounded inventory before sending it over the node channel.
func (i CapturePeerInventory) Encode(scope CaptureScope, maxBytes int64) ([]byte, error) {
	if err := i.validate(scope, maxBytes); err != nil {
		return nil, err
	}
	return json.Marshal(i)
}

// DecodeCapturePeerInventory rejects unknown, duplicate and noncanonical metadata
// before it can influence which source ranges are omitted from a final stream.
func DecodeCapturePeerInventory(payload []byte, scope CaptureScope, maxBytes int64) (CapturePeerInventory, error) {
	var inventory CapturePeerInventory
	if len(payload) == 0 || len(payload) > MaxManifestBytes {
		return inventory, fmt.Errorf("invalid capture inventory size")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inventory); err != nil {
		return CapturePeerInventory{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return CapturePeerInventory{}, fmt.Errorf("trailing capture inventory data")
	}
	canonical, err := inventory.Encode(scope, maxBytes)
	if err != nil {
		return CapturePeerInventory{}, err
	}
	if !bytes.Equal(payload, canonical) {
		return CapturePeerInventory{}, fmt.Errorf("noncanonical capture inventory")
	}
	return inventory, nil
}

// CapturePeerTimings separates waiting for source bytes, local hash checks and
// durable sync. These observations grant no authority and are not latency SLOs.
type CapturePeerTimings struct{ Read, Hash, Sync time.Duration }

type captureTimedReader struct {
	io.Reader
	duration *time.Duration
}

func (r captureTimedReader) Read(p []byte) (int, error) {
	started := time.Now()
	n, err := r.Reader.Read(p)
	*r.duration += time.Since(started)
	return n, err
}

// CapturePeerCache owns a newly created private destination directory. Its
// caller must first journal the exact source/destination authorization and
// reserve the existing XFS staging quota. Methods serialize ownership, but the
// caller must cancel and join the growing stream before inventory/finalization,
// primary handover or cleanup. A crash discards this optimization: the owning
// journal must remove the partial directory before reopening a fresh cache.
// Finalization overwrites the same files, avoiding a second full image copy.
type CapturePeerCache struct {
	mu              sync.Mutex
	scope           CaptureScope
	directory       string
	root            *os.Root
	maxBytes        int64
	framesRemaining int64
	sizes           map[string]int64
	chunks          map[string]map[int64]Chunk
	finalizing      bool
	handles         map[string]*os.File
	writeErr        error
	timings         CapturePeerTimings
	writeback       func(*os.File, int64, int64) error
	syncFile        func(*os.File) error
}

func NewCapturePeerCache(ctx context.Context, scope CaptureScope, directory string, maxBytes int64,
	admit func(int64, uint64) error) (*CapturePeerCache, error) {
	if _, err := scope.Digest(); err != nil {
		return nil, err
	}
	if maxBytes < ChunkBytes || maxBytes > MaxImageBytes || maxBytes%4096 != 0 || admit == nil ||
		!filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, fmt.Errorf("capture peer requires bounded private staging admission")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := admit(maxBytes, MaxFiles*8); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return nil, err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return nil, err
	}
	return &CapturePeerCache{scope: scope, directory: directory, root: root, maxBytes: maxBytes, framesRemaining: maxBytes / ChunkBytes,
		sizes: make(map[string]int64), chunks: make(map[string]map[int64]Chunk), handles: make(map[string]*os.File), writeback: capturePeerWriteback, syncFile: func(f *os.File) error { return f.Sync() }}, nil
}

// Timings is read after the owning stream has returned; it waits for stream
// custody like Close and Inventory rather than racing active counters.
func (c *CapturePeerCache) Timings() CapturePeerTimings {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timings
}

// Close releases the directory handle, not regional or journal custody. It
// deliberately leaves files for the existing authorized cleanup protocol.
func (c *CapturePeerCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.root == nil {
		return nil
	}
	var err error
	for name, file := range c.handles {
		err = errors.Join(err, file.Close())
		delete(c.handles, name)
	}
	err = errors.Join(err, c.root.Close())
	c.root = nil
	return err
}

func (c *CapturePeerCache) put(ctx context.Context, name string, offset int64, data []byte) error {
	return c.putVerified(ctx, name, offset, data, digest.FromBytes(data).String())
}

// ReceiveGrowing owns this buffer and has already checked its wire digest.
// Reuse that check for the tentative inventory instead of hashing every byte
// a second time; final repair still independently rehashes retained disk data.
func (c *CapturePeerCache) putVerified(ctx context.Context, name string, offset int64, data []byte, hash string) error {
	if c.root == nil || c.finalizing || c.writeErr != nil {
		return fmt.Errorf("capture peer cache is closed for tentative writes")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validPath(name) || offset < 0 || offset%ChunkBytes != 0 || offset > c.maxBytes-ChunkBytes || len(data) != ChunkBytes {
		return fmt.Errorf("invalid tentative capture range")
	}
	sizes := make(map[string]int64, len(c.sizes)+1)
	for name, size := range c.sizes {
		sizes[name] = size
	}
	sizes[name] = max(sizes[name], offset+ChunkBytes)
	var total int64
	directories := map[string]bool{".": true}
	for name, size := range sizes {
		total += size
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, found := sizes[parent]; found {
				return fmt.Errorf("capture file overlaps a parent")
			}
			directories[parent] = true
		}
	}
	if total+int64(len(directories))*4096 > c.maxBytes || len(sizes) > MaxFiles || len(sizes)+len(directories) > MaxFiles*8 {
		return fmt.Errorf("tentative capture exceeds staging admission")
	}
	// Charge growth before filesystem mutation. Failed partial writes retain
	// their charge and can never be used as acknowledged reusable chunks.
	c.sizes = sizes
	output, err := c.openFile(name)
	if err != nil {
		c.writeErr = err
		return err
	}
	if c.chunks[name] == nil {
		c.chunks[name] = make(map[int64]Chunk)
	}
	delete(c.chunks[name], offset)
	if _, err := output.WriteAt(data, offset); err != nil {
		c.writeErr = err
		return err
	}
	if err := c.writeback(output, offset, int64(len(data))); err != nil {
		c.writeErr = err
		return err
	}
	c.chunks[name][offset] = Chunk{Digest: hash, Size: ChunkBytes}
	return nil
}

// Keep each original write descriptor through final fsync. Reopening after
// background writeback could otherwise miss an error already recorded before
// the new descriptor's error cursor. The path/inode match also rejects swaps.
func (c *CapturePeerCache) openFile(name string) (*os.File, error) {
	if file := c.handles[name]; file != nil {
		actual, err := c.root.Lstat(name)
		if err != nil {
			return nil, err
		}
		retained, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if !actual.Mode().IsRegular() || !os.SameFile(actual, retained) {
			return nil, fmt.Errorf("capture cache file identity changed")
		}
		return file, nil
	}
	if len(c.handles) >= MaxFiles {
		return nil, fmt.Errorf("capture descriptor budget exhausted")
	}
	if err := c.root.MkdirAll(path.Dir(name), 0o700); err != nil {
		return nil, err
	}
	file, err := c.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	c.handles[name] = file
	return file, nil
}

// Inventory returns owned metadata for the contiguous verified prefix of each
// file. Holes and interrupted writes are cache misses. No disk durability or
// completed source capture is inferred from these tentative hashes.
func (c *CapturePeerCache) Inventory(ctx context.Context) (CapturePeerInventory, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return CapturePeerInventory{}, err
	}
	if c.root == nil || c.finalizing {
		return CapturePeerInventory{}, fmt.Errorf("capture inventory unavailable")
	}
	scope, _ := c.scope.Digest()
	inventory := CapturePeerInventory{ScopeDigest: scope, Files: []File{}}
	for name, chunks := range c.chunks {
		file := File{Path: name}
		for offset := int64(0); ; offset += ChunkBytes {
			chunk, found := chunks[offset]
			if !found {
				break
			}
			file.Chunks = append(file.Chunks, chunk)
			file.Size += ChunkBytes
		}
		if file.Size > 0 {
			inventory.Files = append(inventory.Files, file)
		}
	}
	sort.Slice(inventory.Files, func(i, j int) bool { return inventory.Files[i].Path < inventory.Files[j].Path })
	return inventory, inventory.validate(c.scope, c.maxBytes)
}

// ReceiveFinal repairs the tentative image in place against the exact final
// reference. A successful result proves only complete durable local receipt;
// regional publication and destination execution authority remain separate.
// Any failure retains custody and forbids additional tentative writes. The
// caller may discard the cache and retry the ordinary complete-image path.
func (c *CapturePeerCache) ReceiveFinal(ctx context.Context, expected Binding, ref Reference, input io.Reader) (Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.root == nil || c.finalizing || c.writeErr != nil || !c.scope.matches(expected) {
		return Manifest{}, fmt.Errorf("capture finalization changed source or custody")
	}
	c.finalizing = true
	input = captureTimedReader{Reader: input, duration: &c.timings.Read}
	manifest, err := readPeerManifest(ctx, expected, ref, input, c.maxBytes, captureFinalMagic)
	if err != nil {
		return Manifest{}, err
	}
	bytes, _, err := manifest.StagingFootprint()
	if err != nil || bytes > c.maxBytes {
		return Manifest{}, fmt.Errorf("final capture exceeds staging admission")
	}
	// Refuse unexpected paths instead of silently deleting data. A changed
	// producer inventory is a cache miss, handled by authorized fallback cleanup.
	existing, err := (&Store{maxBytes: c.maxBytes}).inspectImageFiles(ctx, c.root, true)
	if err != nil {
		return Manifest{}, err
	}
	final := make(map[string]File, len(manifest.Files))
	for _, file := range manifest.Files {
		final[file.Path] = file
	}
	// Bound directory growth too: early directory names must all belong to
	// the final image, including any empty directories left by an interrupted write.
	directories := map[string]bool{".": true}
	for _, file := range manifest.Files {
		for parent := path.Dir(file.Path); parent != "."; parent = path.Dir(parent) {
			directories[parent] = true
		}
	}
	if err := fs.WalkDir(c.root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() && !directories[name] {
			return fmt.Errorf("tentative directory is absent from final inventory")
		}
		return nil
	}); err != nil {
		return Manifest{}, err
	}
	for _, file := range existing {
		want, found := final[file.Path]
		if !found {
			return Manifest{}, fmt.Errorf("tentative file is absent from final inventory")
		}
		if file.Size > want.Size {
			output, err := c.openFile(file.Path)
			if err != nil {
				return Manifest{}, err
			}
			if err := output.Truncate(want.Size); err != nil {
				return Manifest{}, err
			}
		}
	}
	buffer := make([]byte, ChunkBytes)
	for _, file := range manifest.Files {
		if err := c.receiveFinalFile(ctx, file, input, buffer); err != nil {
			return Manifest{}, err
		}
	}
	// A sender may finish source verification after releasing reuse frames.
	// Truncation at that boundary must fail even on a clean local-reader EOF.
	var complete [len(captureFinalComplete)]byte
	if _, err := io.ReadFull(input, complete[:]); err != nil || string(complete[:]) != captureFinalComplete {
		return Manifest{}, fmt.Errorf("incomplete final capture verification")
	}
	var extra [1]byte
	if n, err := io.ReadFull(input, extra[:]); n != 0 || err != io.EOF {
		return Manifest{}, fmt.Errorf("incomplete or overlong final capture stream")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	syncStarted := time.Now()
	defer func() { c.timings.Sync += time.Since(syncStarted) }()
	if err := syncDirectories(c.root, manifest.Files); err != nil {
		return Manifest{}, err
	}
	parent, err := os.Open(filepath.Dir(c.directory))
	if err != nil {
		return Manifest{}, err
	}
	err = parent.Sync()
	closeErr := parent.Close()
	if err != nil {
		return Manifest{}, err
	}
	if closeErr != nil {
		return Manifest{}, closeErr
	}
	return manifest, ctx.Err()
}

func (c *CapturePeerCache) receiveFinalFile(ctx context.Context, file File, input io.Reader, buffer []byte) error {
	output, err := c.openFile(file.Path)
	if err != nil {
		return err
	}
	var offset int64
	reused := false
	for _, chunk := range file.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		var mode [1]byte
		if _, err := io.ReadFull(input, mode[:]); err != nil {
			return err
		}
		data := buffer[:int(chunk.Size)]
		switch mode[0] {
		case 0:
			if _, err := io.ReadFull(input, data); err != nil {
				return err
			}
		case 1:
			if chunk.Size != ChunkBytes {
				return fmt.Errorf("short capture chunk cannot be reused")
			}
			reused = true
		default:
			return fmt.Errorf("invalid capture reuse mode")
		}
		if mode[0] == 0 {
			hashStarted := time.Now()
			actual := digest.FromBytes(data).String()
			c.timings.Hash += time.Since(hashStarted)
			if actual != chunk.Digest {
				return fmt.Errorf("final capture chunk digest mismatch")
			}
		}
		if mode[0] == 0 {
			if _, err := output.WriteAt(data, offset); err != nil {
				return err
			}
		}
		offset += chunk.Size
	}
	if err := output.Truncate(file.Size); err != nil {
		return err
	}
	// Reused ranges are verified in parallel only after the final file shape
	// is known. Missing payloads were checked before writing; this final scan
	// independently validates every cached byte, including any disk changes.
	if reused {
		hashStarted := time.Now()
		_, err := scanImageFile(ctx, c.root, file, nil)
		c.timings.Hash += time.Since(hashStarted)
		if err != nil {
			return err
		}
	}
	syncStarted := time.Now()
	err = c.syncFile(output)
	c.timings.Sync += time.Since(syncStarted)
	return err
}
