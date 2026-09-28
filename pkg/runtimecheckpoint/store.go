package runtimecheckpoint

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"golang.org/x/sync/errgroup"
)

// Bound image transfer memory and object-store pressure independently of image
// size. Upload keeps four 8 MiB chunks in flight; download keeps eight.
const (
	publicationConcurrency = 4
	downloadConcurrency    = 8
)

// Reference is persisted by the regional transaction only after all image
// chunks and the manifest have been published. The binding digest scopes all
// objects to one team, sandbox, operation and exact source incarnation.
type Reference struct {
	BindingDigest  string `json:"binding_digest"`
	ManifestDigest string `json:"manifest_digest"`
}

func (r Reference) ValidateFor(binding Binding) error {
	want, err := binding.Digest()
	if err != nil {
		return err
	}
	if r.BindingDigest != want || validateDigest(r.ManifestDigest) != nil {
		return fmt.Errorf("checkpoint reference does not match expected binding")
	}
	return nil
}

type Store struct {
	objects   objectstore.ContextConditionalStore
	maxBytes  int64
	manifests manifestCache
	chunks    ChunkCache
}

// ChunkCache is disposable node-local storage for verified immutable image
// chunks. The regional manifest and lifecycle receipt remain authoritative.
// Implementations verify digest and size on every hit and bound eviction.
type ChunkCache interface {
	GetCheckpointChunk(digest string, size int64) ([]byte, bool)
	PutCheckpointChunk(digest string, payload []byte)
}

// ChunkCloner optionally copies a cache hit into an unexposed private image
// file using filesystem range cloning. The implementation verifies the cloned
// immutable source bytes before cloning the same inode. False falls back to
// ordinary cache reading and then regional storage; errors abort preparation.
type ChunkCloner interface {
	CloneCheckpointChunk(context.Context, string, int64, *os.File, int64) (bool, error)
}

// DownloadStats accounts for verified image chunks; manifest reads are small
// and remain regional. Counts belong to one transfer, not a shared node delta.
type DownloadStats struct {
	CacheChunks    int64
	CacheBytes     int64
	ClonedChunks   int64
	ClonedBytes    int64
	RegionalChunks int64
	RegionalBytes  int64
}

// New requires an already-configured regional store, normally constructed by
// rootfsobjectstore.Create so checkpoint memory receives envelope encryption.
// maxBytes bounds accepted image bytes. The caller separately enforces disk
// admission while capturing; this upload check cannot bound producer writes.
// Publication and local verification use at most publicationConcurrency chunk
// buffers and never load the full image. Regional downloads use at most
// downloadConcurrency chunk buffers; ordered peer streams use one.
func New(objects objectstore.Store, maxBytes int64) (*Store, error) {
	return NewWithChunkCache(objects, maxBytes, nil)
}

// NewWithChunkCache reuses the node's RootFS disk LRU for memory image chunks.
// A cache failure never grants restore authority or blocks regional fallback.
func NewWithChunkCache(objects objectstore.Store, maxBytes int64, chunks ChunkCache) (*Store, error) {
	if !objectstore.SupportsContextConditionalCreate(objects) || maxBytes <= 0 || maxBytes > MaxImageBytes {
		return nil, fmt.Errorf("checkpoint store needs bounded context-aware immutable storage")
	}
	return &Store{objects: objects.(objectstore.ContextConditionalStore), maxBytes: maxBytes, chunks: chunks}, nil
}

// Publish uploads an immutable, completed runsc image. The caller must hold
// source checkpoint custody throughout this operation. Publishing the same
// binding with different bytes fails instead of replacing an earlier image.
// Successful publication does not by itself authorize a destination restore.
func (s *Store) Publish(ctx context.Context, binding Binding, directory string) (Reference, error) {
	bindingDigest, err := binding.Digest()
	if err != nil {
		return Reference{}, err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return Reference{}, err
	}
	defer root.Close()
	files, err := s.imageFiles(ctx, root)
	if err != nil {
		return Reference{}, err
	}
	manifest := Manifest{Version: ManifestVersion, Binding: binding}
	for _, file := range files {
		captured, err := s.publishFile(ctx, root, file, bindingDigest)
		if err != nil {
			return Reference{}, err
		}
		manifest.Files = append(manifest.Files, captured)
	}
	payload, err := manifest.Encode(s.maxBytes)
	if err != nil {
		return Reference{}, err
	}
	if err := s.putImmutable(ctx, manifestKey(bindingDigest), payload); err != nil {
		return Reference{}, fmt.Errorf("publish checkpoint manifest: %w", err)
	}
	return Reference{BindingDigest: bindingDigest, ManifestDigest: digest.FromBytes(payload).String()}, nil
}

// Download verifies the regionally committed reference and exact expected
// binding before creating a new private image directory. Every chunk is
// checked before writing. A failed transfer retains a partial directory for
// node recovery; it must never be passed to runsc or mistaken for a completed
// transfer. The caller journals success only after this method returns nil.
func (s *Store) Download(ctx context.Context, expected Binding, ref Reference, directory string) (Manifest, error) {
	manifest, _, err := s.download(ctx, expected, ref, directory, nil)
	return manifest, err
}

// DownloadWithAdmission verifies the immutable manifest, then checks its disk
// footprint before creating a directory or reading image chunks. The callback
// must use node-owned authority, never a size supplied by an unverified caller.
func (s *Store) DownloadWithAdmission(ctx context.Context, expected Binding, ref Reference, directory string, admit func(int64, uint64) error) (Manifest, error) {
	manifest, _, err := s.DownloadWithAdmissionStats(ctx, expected, ref, directory, admit)
	return manifest, err
}

// DownloadWithAdmissionStats exposes per-restore cache use for node timing logs.
func (s *Store) DownloadWithAdmissionStats(ctx context.Context, expected Binding, ref Reference, directory string, admit func(int64, uint64) error) (Manifest, DownloadStats, error) {
	if admit == nil {
		return Manifest{}, DownloadStats{}, fmt.Errorf("checkpoint staging admission is required")
	}
	return s.download(ctx, expected, ref, directory, admit)
}

func (s *Store) download(ctx context.Context, expected Binding, ref Reference, directory string, admit func(int64, uint64) error) (Manifest, DownloadStats, error) {
	manifest, err := s.loadManifest(ctx, expected, ref)
	if err != nil {
		return Manifest{}, DownloadStats{}, err
	}
	prefix, err := manifest.chunkPrefix()
	if err != nil {
		return Manifest{}, DownloadStats{}, err
	}
	var cacheChunks, cacheBytes, clonedChunks, clonedBytes, regionalChunks, regionalBytes atomic.Int64
	var cloneChunk func(context.Context, Chunk, *os.File, int64) (bool, error)
	if cloner, ok := s.chunks.(ChunkCloner); ok {
		cloneChunk = func(ctx context.Context, chunk Chunk, output *os.File, offset int64) (bool, error) {
			cloned, err := cloner.CloneCheckpointChunk(ctx, chunk.Digest, chunk.Size, output, offset)
			if cloned {
				cacheChunks.Add(1)
				cacheBytes.Add(chunk.Size)
				clonedChunks.Add(1)
				clonedBytes.Add(chunk.Size)
			}
			return cloned, err
		}
	}
	result, err := materializeImage(ctx, manifest, directory, admit, downloadConcurrency, cloneChunk, func(ctx context.Context, chunk Chunk) ([]byte, error) {
		if s.chunks != nil {
			if payload, ok := s.chunks.GetCheckpointChunk(chunk.Digest, chunk.Size); ok {
				cacheChunks.Add(1)
				cacheBytes.Add(chunk.Size)
				return payload, nil
			}
		}
		payload, err := s.readObject(ctx, prefix+strings.TrimPrefix(chunk.Digest, "sha256:"), chunk.Size)
		if err == nil {
			regionalChunks.Add(1)
			regionalBytes.Add(int64(len(payload)))
			// The shared disk cache rechecks the digest before atomically
			// admitting the range. A damaged or evicted entry repairs itself on
			// the first regional fallback instead of missing on every resume.
			s.cacheChunk(chunk, payload)
		}
		return payload, err
	})
	return result, DownloadStats{cacheChunks.Load(), cacheBytes.Load(), clonedChunks.Load(), clonedBytes.Load(), regionalChunks.Load(), regionalBytes.Load()}, err
}

// Both regional downloads and peer streams use the same admission, private
// file creation, chunk verification and crash-durable completion boundary.
func materializeImage(ctx context.Context, manifest Manifest, directory string, admit func(int64, uint64) error, concurrency int,
	cloneChunk func(context.Context, Chunk, *os.File, int64) (bool, error),
	readChunk func(context.Context, Chunk) ([]byte, error)) (Manifest, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return Manifest{}, fmt.Errorf("checkpoint destination must be an absolute canonical path")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if admit != nil {
		bytes, inodes, err := manifest.StagingFootprint()
		if err != nil {
			return Manifest{}, err
		}
		if err := admit(bytes, inodes); err != nil {
			return Manifest{}, fmt.Errorf("admit checkpoint destination: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return Manifest{}, fmt.Errorf("reserve checkpoint destination: %w", err)
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return Manifest{}, err
	}
	defer root.Close()
	for _, file := range manifest.Files {
		if err := materializeFile(ctx, root, file, concurrency, cloneChunk, readChunk); err != nil {
			return Manifest{}, err
		}
	}
	// Sync all image directories after their contents. The destination parent
	// is also persisted so a crash cannot lose the published directory entry.
	if err := syncDirectories(root, manifest.Files); err != nil {
		return Manifest{}, err
	}
	parent, err := os.Open(filepath.Dir(directory))
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
	return manifest, nil
}

// VerifyLocal rechecks exact file inventory and every immutable chunk before a
// recovered destination may reuse an image. Journal existence is not evidence
// that staging survived a disk failure or that its contents remain unchanged.
// Reuse the bounded publication scanner so large images do not serialize their
// independent hash checks on the restore admission path.
func (s *Store) VerifyLocal(ctx context.Context, expected Binding, ref Reference, directory string) (Manifest, error) {
	manifest, err := s.loadManifest(ctx, expected, ref)
	if err != nil {
		return Manifest{}, err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return Manifest{}, err
	}
	defer root.Close()
	files, err := s.imageFiles(ctx, root)
	if err != nil {
		return Manifest{}, err
	}
	if len(files) != len(manifest.Files) {
		return Manifest{}, fmt.Errorf("checkpoint file inventory changed")
	}
	for i, file := range manifest.Files {
		if files[i].Path != file.Path || files[i].Size != file.Size {
			return Manifest{}, fmt.Errorf("checkpoint file identity changed")
		}
		if _, err := scanImageFile(ctx, root, file, nil); err != nil {
			return Manifest{}, err
		}
	}
	return manifest, nil
}

func (s *Store) loadManifest(ctx context.Context, expected Binding, ref Reference) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	bindingDigest, err := expected.Digest()
	if err != nil {
		return Manifest{}, err
	}
	if ref.BindingDigest != bindingDigest || validateDigest(ref.ManifestDigest) != nil {
		return Manifest{}, fmt.Errorf("checkpoint reference does not match expected binding")
	}
	if payload, ok := s.manifests.get(ref); ok {
		// Decode a fresh value: callers may mutate their returned chunk slices.
		// The cache only holds bytes checked against this exact immutable ref.
		return Decode([]byte(payload), s.maxBytes)
	}
	payload, err := s.readObject(ctx, manifestKey(bindingDigest), MaxManifestBytes)
	if err != nil {
		return Manifest{}, err
	}
	if digest.FromBytes(payload).String() != ref.ManifestDigest {
		return Manifest{}, fmt.Errorf("checkpoint manifest digest mismatch")
	}
	manifest, err := Decode(payload, s.maxBytes)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Binding != expected {
		return Manifest{}, fmt.Errorf("checkpoint manifest belongs to another source")
	}
	s.manifests.put(ref, string(payload))
	return manifest, nil
}

func (s *Store) imageFiles(ctx context.Context, root *os.Root) ([]File, error) {
	return s.inspectImageFiles(ctx, root, false)
}

func (s *Store) inspectImageFiles(ctx context.Context, root *os.Root, allowEmpty bool) ([]File, error) {
	var files []File
	var total int64
	entries := 0
	err := fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		if entries > MaxFiles*8 {
			return fmt.Errorf("checkpoint directory exceeds entry limit")
		}
		if name != "." && !validPath(name) {
			return fmt.Errorf("checkpoint contains an invalid path")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || len(files) >= MaxFiles || info.Size() < 0 || info.Size() > s.maxBytes-total {
			return fmt.Errorf("checkpoint contains a special file or exceeds image limits")
		}
		total += info.Size()
		files = append(files, File{Path: name, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !allowEmpty && (len(files) == 0 || total == 0) {
		return nil, fmt.Errorf("checkpoint image is empty")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func (s *Store) publishFile(ctx context.Context, root *os.Root, file File, bindingDigest string) (File, error) {
	return scanImageFile(ctx, root, file, func(ctx context.Context, chunk Chunk, payload []byte) error {
		if err := s.putImmutable(ctx, chunkKey(bindingDigest, chunk.Digest), payload); err != nil {
			return err
		}
		s.cacheChunk(chunk, payload)
		return nil
	})
}

func (s *Store) cacheChunk(chunk Chunk, payload []byte) {
	if s.chunks != nil && int64(len(payload)) == chunk.Size {
		s.chunks.PutCheckpointChunk(chunk.Digest, payload)
	}
}

// scanImageFile checks a retained local file with bounded parallel reads. A
// supplied chunk inventory is an expected plan, never permission to trust the
// current file bytes. The callback runs only after verifying that expectation.
func scanImageFile(ctx context.Context, root *os.Root, file File, publish func(context.Context, Chunk, []byte) error) (File, error) {
	input, err := root.Open(file.Path)
	if err != nil {
		return File{}, err
	}
	defer input.Close()
	before, err := input.Stat()
	if err != nil {
		return File{}, err
	}
	if !before.Mode().IsRegular() || before.Size() != file.Size {
		return File{}, fmt.Errorf("checkpoint file changed before publication")
	}
	count := int((file.Size + ChunkBytes - 1) / ChunkBytes)
	expected := file.Chunks
	if expected != nil && len(expected) != count {
		return File{}, fmt.Errorf("checkpoint file changed planned chunk inventory")
	}
	if count > 0 {
		file.Chunks = make([]Chunk, count)
	}
	workers := min(count, publicationConcurrency)
	group, workerCtx := errgroup.WithContext(ctx)
	for worker := range workers {
		group.Go(func() error {
			buffer := make([]byte, min(file.Size, int64(ChunkBytes)))
			for index := worker; index < count; index += workers {
				if err := workerCtx.Err(); err != nil {
					return err
				}
				offset := int64(index) * ChunkBytes
				chunk := buffer[:min(file.Size-offset, int64(ChunkBytes))]
				if _, err := input.ReadAt(chunk, offset); err != nil {
					return err
				}
				described := Chunk{Digest: digest.FromBytes(chunk).String(), Size: int64(len(chunk))}
				if expected != nil && expected[index] != described {
					return fmt.Errorf("checkpoint file changed planned chunk content")
				}
				if publish != nil {
					if err := publish(workerCtx, described, chunk); err != nil {
						return err
					}
				}
				// Each worker owns disjoint manifest positions, preserving file
				// order regardless of network completion order.
				file.Chunks[index] = described
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return File{}, err
	}
	if err := ctx.Err(); err != nil {
		return File{}, err
	}
	var extra [1]byte
	if n, err := input.ReadAt(extra[:], file.Size); n != 0 || err != io.EOF {
		return File{}, fmt.Errorf("checkpoint file grew during publication")
	}
	after, err := input.Stat()
	if err != nil {
		return File{}, err
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return File{}, fmt.Errorf("checkpoint file changed during publication")
	}
	return file, nil
}

func materializeFile(ctx context.Context, root *os.Root, file File, concurrency int,
	cloneChunk func(context.Context, Chunk, *os.File, int64) (bool, error),
	readChunk func(context.Context, Chunk) ([]byte, error)) error {
	if err := root.MkdirAll(path.Dir(file.Path), 0o700); err != nil {
		return err
	}
	output, err := root.OpenFile(file.Path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer output.Close()
	if err := output.Truncate(file.Size); err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	var cloneMu sync.Mutex
	for index, chunk := range file.Chunks {
		if err := groupCtx.Err(); err != nil {
			break
		}
		offset := int64(index) * ChunkBytes
		group.Go(func() error {
			if cloneChunk != nil {
				// Filesystem range cloning uses one destination inode. Serializing
				// those short local operations
				// avoids XFS extent-lock contention between parallel chunks;
				// regional misses still download through the existing pool.
				cloneMu.Lock()
				cloned, err := cloneChunk(groupCtx, chunk, output, offset)
				cloneMu.Unlock()
				if err != nil || cloned {
					return err
				}
			}
			payload, err := readChunk(groupCtx, chunk)
			if err != nil {
				return err
			}
			if int64(len(payload)) != chunk.Size || digest.FromBytes(payload).String() != chunk.Digest {
				return fmt.Errorf("checkpoint chunk size or digest mismatch")
			}
			written, err := output.WriteAt(payload, offset)
			if err != nil {
				return err
			}
			if written != len(payload) {
				return io.ErrShortWrite
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := output.Sync(); err != nil {
		return err
	}
	return output.Close()
}

func (s *Store) putImmutable(ctx context.Context, key string, payload []byte) error {
	created, err := s.objects.PutIfAbsentContext(ctx, key, bytes.NewReader(payload))
	if err != nil || created {
		return err
	}
	existing, err := s.readObject(ctx, key, int64(len(payload)))
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, payload) {
		return fmt.Errorf("immutable checkpoint object collision")
	}
	return nil
}

func (s *Store) readObject(ctx context.Context, key string, limit int64) ([]byte, error) {
	return readCheckpointObject(ctx, s.objects, key, limit)
}

type checkpointObjectReader interface {
	GetContext(context.Context, string, int64, int64) (io.ReadCloser, error)
}

func readCheckpointObject(ctx context.Context, objects checkpointObjectReader, key string, limit int64) ([]byte, error) {
	reader, err := objects.GetContext(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("checkpoint object exceeds expected size")
	}
	return data, nil
}

func openPrivateDirectory(directory string) (*os.Root, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, fmt.Errorf("checkpoint image path must be absolute and canonical")
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("checkpoint image must be a private directory, not a symlink")
	}
	return os.OpenRoot(directory)
}

func manifestKey(binding string) string {
	return imagePrefix(binding) + "manifest.json"
}

func chunkKey(binding, chunk string) string {
	return imagePrefix(binding) + "chunks/" + strings.TrimPrefix(chunk, "sha256:")
}

func imagePrefix(binding string) string {
	return "runtime-checkpoints/v1/" + strings.TrimPrefix(binding, "sha256:") + "/"
}

func syncDirectories(root *os.Root, files []File) error {
	directories := map[string]bool{".": true}
	for _, file := range files {
		for parent := path.Dir(file.Path); parent != "."; parent = path.Dir(parent) {
			directories[parent] = true
		}
	}
	ordered := make([]string, 0, len(directories))
	for directory := range directories {
		ordered = append(ordered, directory)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(ordered)))
	for _, directory := range ordered {
		file, err := root.Open(directory)
		if err != nil {
			return err
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
