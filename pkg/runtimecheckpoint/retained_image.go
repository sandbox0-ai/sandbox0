package runtimecheckpoint

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
)

// RetainedImageLimits bounds disposable complete images, independently of the
// shared chunk cache. Production callers must place Directory inside their
// kernel-enforced migration staging quota, on the capture filesystem.
type RetainedImageLimits struct {
	Directory string
	Bytes     int64
	Inodes    uint64
	Entries   int
	Lifetime  time.Duration
}

// Retaining the capture's inode also retains its reclaimable page cache. A
// reflink into a new inode cannot do that. This is only a locality hint: every
// restore still loads the regional manifest and hashes all linked bytes.
type retainedImageCache struct {
	mu     sync.Mutex
	limits RetainedImageLimits
	root   *os.Root
	lock   *os.File
	closed bool
}

type retainedImageEntry struct {
	name   string
	bytes  int64
	inodes uint64
	used   time.Time
}

// ConfigureRetainedImages opens a dedicated, exclusively cache-owned namespace.
// Existing contents in that namespace are disposable; never use a custody root.
func (s *Store) ConfigureRetainedImages(limits RetainedImageLimits) error {
	s.retainedMu.Lock()
	defer s.retainedMu.Unlock()
	if s.retainedImages != nil {
		if s.retainedImages.limits == limits {
			return nil
		}
		return fmt.Errorf("retained image cache already configured")
	}
	if !filepath.IsAbs(limits.Directory) || filepath.Clean(limits.Directory) != limits.Directory || limits.Bytes <= 0 || limits.Inodes < 4 || limits.Entries < 1 || limits.Entries > 64 || limits.Lifetime <= 0 {
		return fmt.Errorf("retained image cache requires bounded private storage")
	}
	if err := os.Mkdir(limits.Directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	root, err := openPrivateDirectory(limits.Directory)
	if err != nil {
		return err
	}
	lock, err := root.Open(".")
	if err != nil {
		root.Close()
		return err
	}
	c := &retainedImageCache{limits: limits, root: root, lock: lock}
	if err := c.withLock(context.Background(), func() error {
		entries, err := c.entries()
		if err != nil {
			return err
		}
		return c.trim(entries, 0, 0, 0)
	}); err != nil {
		lock.Close()
		root.Close()
		return err
	}
	s.retainedImages = c
	return nil
}

func (s *Store) retainedCache() *retainedImageCache {
	s.retainedMu.Lock()
	defer s.retainedMu.Unlock()
	return s.retainedImages
}

// EvictRetainedImages releases only cache-owned links under quota pressure.
// Captures, journal custody and already-linked restore directories stay intact.
func (s *Store) EvictRetainedImages(ctx context.Context) error {
	if c := s.retainedCache(); c != nil {
		return c.withLock(ctx, func() error {
			entries, err := c.entries()
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err := c.root.RemoveAll(entry.name); err != nil {
					return err
				}
			}
			return c.lock.Sync()
		})
	}
	return nil
}

func retainedImageName(ref Reference) string {
	return "image-" + strings.TrimPrefix(ref.BindingDigest, "sha256:") + "-" + strings.TrimPrefix(ref.ManifestDigest, "sha256:")
}

func retainedImageFootprint(manifest Manifest, payload []byte) (int64, uint64, error) {
	bytes, inodes, err := manifest.StagingFootprint()
	// Entry directory, image directory and a bounded manifest file. Counting
	// linked image bytes conservatively keeps retention a small quota share.
	return bytes + int64((len(payload)+4095)/4096+2)*4096, inodes + 3, err
}

func (c *retainedImageCache) withLock(ctx context.Context, f func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return os.ErrClosed
	}
	current, err := os.Lstat(c.limits.Directory)
	pinned, statErr := c.lock.Stat()
	if err != nil || statErr != nil || !current.IsDir() || current.Mode().Perm() != 0o700 || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(current, pinned) {
		return fmt.Errorf("retained image cache directory changed")
	}
	// Shared by overlapping live ctld generations. Do not hold a lifetime
	// ownership lock which would disable caching after a live binary rollout.
	lockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := lockRetainedImages(lockCtx, c.lock); err != nil {
		return err
	}
	defer unlockRetainedImages(c.lock)
	if err := ctx.Err(); err != nil {
		return err
	}
	return f()
}

// The cache namespace is exclusively owned here. Rooted removal never follows
// symlinks outside it; partial writes and invalid metadata are disposable.
func (c *retainedImageCache) entries() ([]retainedImageEntry, error) {
	dir, err := c.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(129)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > 128 {
		return nil, fmt.Errorf("retained image namespace exceeds inventory bound")
	}
	entries := make([]retainedImageEntry, 0, len(names))
	for _, name := range names {
		info, err := c.root.Lstat(name)
		if err != nil {
			return nil, err
		}
		manifest, payload, readErr := c.readManifest(name)
		if !info.IsDir() || info.Mode().Perm() != 0o700 || readErr != nil || time.Since(info.ModTime()) >= c.limits.Lifetime {
			if err := c.root.RemoveAll(name); err != nil {
				return nil, err
			}
			continue
		}
		bytes, inodes, err := retainedImageFootprint(manifest, payload)
		if err != nil {
			return nil, err
		}
		entries = append(entries, retainedImageEntry{name, bytes, inodes, info.ModTime()})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].used.Before(entries[j].used) })
	return entries, nil
}

func (c *retainedImageCache) readManifest(name string) (Manifest, []byte, error) {
	before, err := c.root.Lstat(path.Join(name, "manifest.json"))
	if err != nil {
		return Manifest{}, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > MaxManifestBytes {
		return Manifest{}, nil, fmt.Errorf("invalid retained manifest file")
	}
	file, err := c.root.Open(path.Join(name, "manifest.json"))
	if err != nil {
		return Manifest{}, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(before, info) || info.Size() > MaxManifestBytes {
		return Manifest{}, nil, fmt.Errorf("invalid retained manifest file")
	}
	payload, err := io.ReadAll(io.LimitReader(file, MaxManifestBytes+1))
	if err != nil {
		return Manifest{}, nil, err
	}
	manifest, err := Decode(payload, MaxImageBytes)
	if err != nil {
		return Manifest{}, nil, err
	}
	binding, err := manifest.Binding.Digest()
	if err != nil || name != retainedImageName(Reference{binding, digest.FromBytes(payload).String()}) {
		return Manifest{}, nil, fmt.Errorf("retained manifest identity changed")
	}
	return manifest, payload, nil
}

func (c *retainedImageCache) trim(entries []retainedImageEntry, bytes int64, inodes uint64, count int) error {
	for _, entry := range entries {
		bytes += entry.bytes
		inodes += entry.inodes
		count++
	}
	for _, entry := range entries {
		if bytes <= c.limits.Bytes && inodes <= c.limits.Inodes && count <= c.limits.Entries {
			break
		}
		if err := c.root.RemoveAll(entry.name); err != nil {
			return err
		}
		bytes -= entry.bytes
		inodes -= entry.inodes
		count--
	}
	return nil
}

func (s *Store) retainPublishedImage(ctx context.Context, ref Reference, manifest Manifest, payload []byte, source *os.Root) {
	c := s.retainedCache()
	if c == nil {
		return
	}
	// Publication already succeeded. Cache admission is best effort and cannot
	// change the durable receipt, even on quota exhaustion or link failure.
	_ = c.withLock(ctx, func() error {
		bytes, inodes, err := retainedImageFootprint(manifest, payload)
		if err != nil || bytes > c.limits.Bytes || inodes > c.limits.Inodes {
			return err
		}
		entries, err := c.entries()
		if err != nil {
			return err
		}
		name := retainedImageName(ref)
		for _, entry := range entries {
			if entry.name == name {
				return nil
			}
		}
		if err := c.trim(entries, bytes, inodes, 1); err != nil {
			return err
		}
		// Allocate through the pinned root, including crash-disposable partials.
		tmp := "partial-" + rand.Text()
		if err := c.root.Mkdir(tmp, 0o700); err != nil {
			return err
		}
		defer c.root.RemoveAll(tmp)
		if err := c.root.Mkdir(path.Join(tmp, "image"), 0o700); err != nil {
			return err
		}
		image, err := c.root.OpenRoot(path.Join(tmp, "image"))
		if err != nil {
			return err
		}
		defer image.Close()
		if err := linkRetainedImage(ctx, source, image, manifest, true); err != nil {
			return err
		}
		if err := syncDirectories(image, manifest.Files); err != nil {
			return err
		}
		output, err := c.root.OpenFile(path.Join(tmp, "manifest.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		_, writeErr := output.Write(payload)
		syncErr := output.Sync()
		closeErr := output.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			return err
		}
		dir, err := c.root.Open(tmp)
		if err != nil {
			return err
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			return err
		}
		if err := c.root.Rename(tmp, name); err != nil {
			return err
		}
		return c.lock.Sync()
	})
}

func linkRetainedImage(ctx context.Context, source, destination *os.Root, manifest Manifest, capture bool) error {
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := destination.MkdirAll(path.Dir(file.Path), 0o700); err != nil {
			return err
		}
		input, err := source.Open(file.Path)
		if err != nil {
			return err
		}
		info, err := input.Stat()
		if err == nil && (!info.Mode().IsRegular() || info.Size() != file.Size) {
			err = fmt.Errorf("retained image file identity changed")
		}
		// Stock runsc creates capture files with 0644 before umask. The
		// stopped-source custodian narrows the same inode before retention;
		// restores do not accept a cache whose private mode has changed.
		if err == nil && capture {
			err = input.Chmod(0o600)
		}
		if err == nil && !capture && info.Mode().Perm() != 0o600 {
			err = fmt.Errorf("retained image file permissions changed")
		}
		if err == nil {
			err = linkRetainedFile(input, destination, file.Path)
		}
		if err == nil && capture {
			err = input.Sync()
		}
		input.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// A miss cleans up only a destination created by this attempt. Admission and
// pre-existing destinations are fatal, preserving the journal recovery rules.
func (s *Store) downloadRetainedImage(ctx context.Context, ref Reference, manifest Manifest, directory string, admit func(int64, uint64) error) (hit bool, resultErr error) {
	c := s.retainedCache()
	if c == nil {
		return false, nil
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return false, fmt.Errorf("checkpoint destination must be an absolute canonical path")
	}
	if admit != nil {
		bytes, inodes, err := manifest.StagingFootprint()
		if err != nil {
			return false, err
		}
		// Outside the cache lock: admission may itself evict cached images.
		if err := admit(bytes, inodes); err != nil {
			return false, fmt.Errorf("admit checkpoint destination: %w", err)
		}
	}
	var parent, destination *os.Root
	created, linked := false, false
	base := filepath.Base(directory)
	defer func() {
		if destination != nil {
			destination.Close()
		}
		if parent != nil {
			if created && !hit {
				if err := parent.RemoveAll(base); err != nil {
					resultErr = err
				}
			}
			parent.Close()
		}
	}()
	var fatal error
	name := retainedImageName(ref)
	err := c.withLock(ctx, func() error {
		entries, err := c.entries()
		if err != nil {
			return err
		}
		if err := c.trim(entries, 0, 0, 0); err != nil {
			return err
		}
		_, payload, err := c.readManifest(name)
		if err != nil {
			return err
		}
		if digest.FromBytes(payload).String() != ref.ManifestDigest {
			return fmt.Errorf("retained image reference changed")
		}
		source, err := c.root.OpenRoot(path.Join(name, "image"))
		if err != nil {
			_ = c.root.RemoveAll(name)
			return err
		}
		defer source.Close()
		files, err := s.imageFiles(ctx, source)
		if err == nil && len(files) != len(manifest.Files) {
			err = fmt.Errorf("retained image inventory changed")
		}
		if err == nil {
			for i, file := range files {
				if file.Path != manifest.Files[i].Path || file.Size != manifest.Files[i].Size {
					err = fmt.Errorf("retained image file identity changed")
					break
				}
			}
		}
		if err != nil {
			_ = c.root.RemoveAll(name)
			return err
		}
		parent, err = os.OpenRoot(filepath.Dir(directory))
		if err != nil {
			fatal = err
			return err
		}
		if err := parent.Mkdir(base, 0o700); err != nil {
			fatal = err
			return err
		}
		created = true
		destination, err = parent.OpenRoot(base)
		if err != nil {
			return err
		}
		if err := linkRetainedImage(ctx, source, destination, manifest, false); err != nil {
			_ = c.root.RemoveAll(name)
			return err
		}
		now := time.Now()
		_ = c.root.Chtimes(name, now, now)
		linked = true
		return nil
	})
	if fatal != nil {
		return false, fatal
	}
	if err != nil && ctx.Err() != nil {
		return false, ctx.Err()
	}
	if !linked {
		return false, nil
	}
	// The destination now owns its links. Hashing must not serialize unrelated
	// restores behind the shared cache lock. Eviction cannot remove these links;
	// concurrent link-count changes merely invalidate the optional watch proof.
	guard := newImageVerificationGuard()
	kept := false
	defer func() {
		if !kept {
			guard.close()
		}
	}()
	for _, file := range manifest.Files {
		input, err := destination.Open(file.Path)
		if err != nil {
			return false, err
		}
		guard.seal(file.Path, input)
		_, scanErr := scanImageFile(ctx, destination, file, nil)
		var syncErr error
		if scanErr == nil {
			syncErr = input.Sync()
		}
		closeErr := input.Close()
		if scanErr != nil {
			_ = c.withLock(ctx, func() error { return c.root.RemoveAll(name) })
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, nil
		}
		if err := errors.Join(syncErr, closeErr); err != nil {
			return false, err
		}
	}
	if err := syncDirectories(destination, manifest.Files); err != nil {
		return false, err
	}
	parentFD, err := parent.Open(".")
	if err != nil {
		return false, err
	}
	err = parentFD.Sync()
	parentFD.Close()
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if guard.finish(destination, manifest) {
		s.verifiedImages.put(ref, directory, guard)
		kept = true
	}
	return true, nil
}

func (c *retainedImageCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.lock.Close()
		c.root.Close()
	}
}
