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

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
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
	objects  objectstore.ContextConditionalStore
	maxBytes int64
}

// New requires an already-configured regional store, normally constructed by
// rootfsobjectstore.Create so checkpoint memory receives envelope encryption.
// maxBytes bounds accepted image bytes. The caller separately enforces disk
// admission while capturing; this upload check cannot bound producer writes.
// Transfers keep memory proportional to ChunkBytes and never load the full image.
func New(objects objectstore.Store, maxBytes int64) (*Store, error) {
	if !objectstore.SupportsContextConditionalCreate(objects) || maxBytes <= 0 || maxBytes > MaxImageBytes {
		return nil, fmt.Errorf("checkpoint store needs bounded context-aware immutable storage")
	}
	return &Store{objects: objects.(objectstore.ContextConditionalStore), maxBytes: maxBytes}, nil
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
	buffer := make([]byte, ChunkBytes)
	for _, file := range files {
		captured, err := s.publishFile(ctx, root, file, bindingDigest, buffer)
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
	return s.download(ctx, expected, ref, directory, nil)
}

// DownloadWithAdmission verifies the immutable manifest, then checks its disk
// footprint before creating a directory or reading image chunks. The callback
// must use node-owned authority, never a size supplied by an unverified caller.
func (s *Store) DownloadWithAdmission(ctx context.Context, expected Binding, ref Reference, directory string, admit func(int64, uint64) error) (Manifest, error) {
	if admit == nil {
		return Manifest{}, fmt.Errorf("checkpoint staging admission is required")
	}
	return s.download(ctx, expected, ref, directory, admit)
}

func (s *Store) download(ctx context.Context, expected Binding, ref Reference, directory string, admit func(int64, uint64) error) (Manifest, error) {
	manifest, err := s.loadManifest(ctx, expected, ref)
	if err != nil {
		return Manifest{}, err
	}
	bindingDigest := ref.BindingDigest
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
		if err := s.downloadFile(ctx, root, bindingDigest, file); err != nil {
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
	buffer := make([]byte, ChunkBytes)
	for i, file := range manifest.Files {
		if files[i].Path != file.Path || files[i].Size != file.Size {
			return Manifest{}, fmt.Errorf("checkpoint file identity changed")
		}
		input, err := root.Open(file.Path)
		if err != nil {
			return Manifest{}, err
		}
		verifyErr := func() error {
			for _, chunk := range file.Chunks {
				if err := ctx.Err(); err != nil {
					return err
				}
				payload := buffer[:int(chunk.Size)]
				if _, err := io.ReadFull(input, payload); err != nil {
					return err
				}
				if digest.FromBytes(payload).String() != chunk.Digest {
					return fmt.Errorf("checkpoint local chunk digest mismatch")
				}
			}
			var extra [1]byte
			if n, err := input.Read(extra[:]); n != 0 || err != io.EOF {
				return fmt.Errorf("checkpoint local file size changed")
			}
			return nil
		}()
		closeErr := input.Close()
		if verifyErr != nil {
			return Manifest{}, verifyErr
		}
		if closeErr != nil {
			return Manifest{}, closeErr
		}
	}
	return manifest, nil
}

func (s *Store) loadManifest(ctx context.Context, expected Binding, ref Reference) (Manifest, error) {
	bindingDigest, err := expected.Digest()
	if err != nil {
		return Manifest{}, err
	}
	if ref.BindingDigest != bindingDigest || validateDigest(ref.ManifestDigest) != nil {
		return Manifest{}, fmt.Errorf("checkpoint reference does not match expected binding")
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
	return manifest, nil
}

func (s *Store) imageFiles(ctx context.Context, root *os.Root) ([]File, error) {
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
	if len(files) == 0 || total == 0 {
		return nil, fmt.Errorf("checkpoint image is empty")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func (s *Store) publishFile(ctx context.Context, root *os.Root, file File, bindingDigest string, buffer []byte) (File, error) {
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
	for remaining := file.Size; remaining > 0; {
		if err := ctx.Err(); err != nil {
			return File{}, err
		}
		chunk := buffer[:min(remaining, int64(ChunkBytes))]
		if _, err := io.ReadFull(input, chunk); err != nil {
			return File{}, err
		}
		d := digest.FromBytes(chunk).String()
		if err := s.putImmutable(ctx, chunkKey(bindingDigest, d), chunk); err != nil {
			return File{}, err
		}
		file.Chunks = append(file.Chunks, Chunk{Digest: d, Size: int64(len(chunk))})
		remaining -= int64(len(chunk))
	}
	var extra [1]byte
	if n, err := input.Read(extra[:]); n != 0 || err != io.EOF {
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

func (s *Store) downloadFile(ctx context.Context, root *os.Root, bindingDigest string, file File) error {
	if err := root.MkdirAll(path.Dir(file.Path), 0o700); err != nil {
		return err
	}
	output, err := root.OpenFile(file.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer output.Close()
	for _, chunk := range file.Chunks {
		payload, err := s.readObject(ctx, chunkKey(bindingDigest, chunk.Digest), chunk.Size)
		if err != nil {
			return err
		}
		if int64(len(payload)) != chunk.Size || digest.FromBytes(payload).String() != chunk.Digest {
			return fmt.Errorf("checkpoint chunk size or digest mismatch")
		}
		if _, err := output.Write(payload); err != nil {
			return err
		}
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
	reader, err := s.objects.GetContext(ctx, key, 0, -1)
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
