// Package rootfsexport converts a complete rootfs v3 Head into one OCI layer.
// Export is an asynchronous template-publication path and is never used by
// pause or resume.
package rootfsexport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sys/unix"
)

type Result struct {
	Object rootfshead.Object
	DiffID string
}

// Export materializes the complete persistent overlay represented by reference
// as a deterministic gzip-compressed OCI layer and stores it in the team's CAS.
func Export(ctx context.Context, store objectstore.Store, teamID string, reference rootfshead.HeadReference) (Result, error) {
	if store == nil {
		return Result{}, fmt.Errorf("rootfs export object store is required")
	}
	if err := reference.Validate(); err != nil {
		return Result{}, err
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return Result{}, err
	}
	if err := rootfshead.ValidateObjectScope(prefix, reference.Manifest); err != nil {
		return Result{}, err
	}

	loader := objectLoader{ctx: ctx, store: store, prefix: prefix}
	headPayload, err := loader.readMetadata(reference.Manifest)
	if err != nil {
		return Result{}, err
	}
	head, err := rootfshead.DecodeHead(bytes.NewReader(headPayload))
	if err != nil {
		return Result{}, err
	}
	if head.HeadID != reference.HeadID {
		return Result{}, fmt.Errorf("rootfs export Head id %q does not match reference %q", head.HeadID, reference.HeadID)
	}

	temporary, err := os.CreateTemp("", "sandbox0-rootfs-export-*.tar.gz")
	if err != nil {
		return Result{}, fmt.Errorf("create rootfs export temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()

	compressedDigester := digest.Canonical.Digester()
	gzipWriter, err := gzip.NewWriterLevel(io.MultiWriter(temporary, compressedDigester.Hash()), gzip.BestSpeed)
	if err != nil {
		return Result{}, fmt.Errorf("create rootfs export compressor: %w", err)
	}
	gzipWriter.ModTime = time.Unix(0, 0).UTC()
	gzipWriter.OS = 255
	uncompressedDigester := digest.Canonical.Digester()
	tarWriter := tar.NewWriter(io.MultiWriter(gzipWriter, uncompressedDigester.Hash()))

	archive := archiveWriter{
		ctx:        ctx,
		objects:    loader,
		tar:        tarWriter,
		activeDirs: make(map[string]struct{}),
		hardlinks:  make(map[string]hardlinkSource),
	}
	writeErr := archive.writeDirectory(head.Root, ".")
	closeTarErr := tarWriter.Close()
	closeGZIPErr := gzipWriter.Close()
	if err := errors.Join(writeErr, closeTarErr, closeGZIPErr); err != nil {
		return Result{}, fmt.Errorf("write rootfs OCI export: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	info, err := temporary.Stat()
	if err != nil {
		return Result{}, fmt.Errorf("inspect rootfs export: %w", err)
	}
	compressedDigest := compressedDigester.Digest()
	objectKey, err := rootfshead.ObjectKey(prefix, rootfshead.ExportLayerMediaType, compressedDigest.String())
	if err != nil {
		return Result{}, err
	}
	object := rootfshead.Object{
		Key:       objectKey,
		Digest:    compressedDigest.String(),
		Size:      info.Size(),
		MediaType: rootfshead.ExportLayerMediaType,
	}
	if err := putImmutable(ctx, store, object, temporary); err != nil {
		return Result{}, err
	}
	return Result{Object: object, DiffID: uncompressedDigester.Digest().String()}, nil
}

type objectLoader struct {
	ctx    context.Context
	store  objectstore.Store
	prefix string
}

func (l objectLoader) readMetadata(object rootfshead.Object) ([]byte, error) {
	if object.Size > rootfshead.MaxMetadataObjectBytes {
		return nil, fmt.Errorf("rootfs metadata object %s is too large: %d", object.Key, object.Size)
	}
	return l.read(object)
}

func (l objectLoader) read(object rootfshead.Object) ([]byte, error) {
	if err := l.ctx.Err(); err != nil {
		return nil, err
	}
	if err := rootfshead.ValidateObjectScope(l.prefix, object); err != nil {
		return nil, err
	}
	reader, err := l.store.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, fmt.Errorf("read rootfs export object %s: %w", object.Key, err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(contextReader{ctx: l.ctx, reader: reader}, object.Size+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("read rootfs export object %s: %w", object.Key, errors.Join(readErr, closeErr))
	}
	if int64(len(payload)) != object.Size || digest.FromBytes(payload).String() != object.Digest {
		return nil, fmt.Errorf("rootfs export object %s failed size or digest validation", object.Key)
	}
	return payload, l.ctx.Err()
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(payload []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(payload)
}

type hardlinkSource struct {
	path  string
	entry rootfshead.Entry
}

type archiveWriter struct {
	ctx        context.Context
	objects    objectLoader
	tar        *tar.Writer
	activeDirs map[string]struct{}
	hardlinks  map[string]hardlinkSource
}

func (w *archiveWriter) writeDirectory(entry rootfshead.Entry, archivePath string) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if entry.Kind != rootfshead.EntryDirectory || entry.Directory == nil {
		return fmt.Errorf("rootfs export path %q is not a directory", archivePath)
	}
	if _, ok := w.activeDirs[entry.Directory.Key]; ok {
		return fmt.Errorf("rootfs export directory graph contains a cycle at %q", archivePath)
	}
	w.activeDirs[entry.Directory.Key] = struct{}{}
	defer delete(w.activeDirs, entry.Directory.Key)

	header, err := headerForEntry(entry, archivePath)
	if err != nil {
		return err
	}
	if archivePath != "." {
		header.Name = strings.TrimSuffix(header.Name, "/") + "/"
	}
	if err := w.writeHeader(header); err != nil {
		return err
	}
	if entry.Opaque {
		opaquePath := path.Join(archivePath, ".wh..wh..opq")
		if err := w.writeHeader(whiteoutHeader(opaquePath, entry)); err != nil {
			return err
		}
	}

	return w.forEachDirectoryEntry(entry, func(child rootfshead.Entry) error {
		childPath := path.Join(archivePath, child.Name)
		switch child.Kind {
		case rootfshead.EntryDirectory:
			if err := w.writeDirectory(child, childPath); err != nil {
				return err
			}
		case rootfshead.EntryFile:
			if err := w.writeFile(child, childPath); err != nil {
				return err
			}
		case rootfshead.EntryWhiteout:
			whiteoutPath := path.Join(archivePath, ".wh."+child.Name)
			if err := w.writeHeader(whiteoutHeader(whiteoutPath, child)); err != nil {
				return err
			}
		case rootfshead.EntrySymlink, rootfshead.EntryChar, rootfshead.EntryBlock, rootfshead.EntryFIFO:
			header, err := headerForEntry(child, childPath)
			if err != nil {
				return err
			}
			if err := w.writeHeader(header); err != nil {
				return err
			}
		default:
			return fmt.Errorf("rootfs export path %q has unsupported kind %q", childPath, child.Kind)
		}
		return nil
	})
}

// forEachDirectoryEntry preserves deterministic bucket and entry order while
// bounding decoded metadata to one directory shard at a time. Lexical order is
// unnecessary for OCI layers and would require materializing very large
// directories in manager memory.
func (w *archiveWriter) forEachDirectoryEntry(directory rootfshead.Entry, visit func(rootfshead.Entry) error) error {
	indexPayload, err := w.objects.readMetadata(*directory.Directory)
	if err != nil {
		return err
	}
	index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(indexPayload))
	if err != nil {
		return err
	}
	for _, shardReference := range index.Shards {
		shardPayload, err := w.objects.readMetadata(shardReference.Object)
		if err != nil {
			return err
		}
		shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(shardPayload))
		if err != nil {
			return err
		}
		if shard.Bucket != shardReference.Bucket {
			return fmt.Errorf("rootfs export directory shard bucket %d does not match index bucket %d", shard.Bucket, shardReference.Bucket)
		}
		for _, entry := range shard.Entries {
			if strings.HasPrefix(entry.Name, ".wh.") {
				return fmt.Errorf("rootfs export path %q uses OCI-reserved whiteout name", entry.Name)
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *archiveWriter) writeFile(entry rootfshead.Entry, archivePath string) error {
	if entry.File == nil {
		return fmt.Errorf("rootfs export file %q has no manifest", archivePath)
	}
	if source, ok := w.hardlinks[entry.Inode]; entry.Nlink > 1 && ok {
		if !sameHardlinkMetadata(source.entry, entry) || *source.entry.File != *entry.File {
			return fmt.Errorf("rootfs export inode %q has inconsistent hardlink metadata", entry.Inode)
		}
		header, err := headerForEntry(entry, archivePath)
		if err != nil {
			return err
		}
		header.Typeflag = tar.TypeLink
		header.Linkname = source.path
		header.Size = 0
		header.Xattrs = nil
		return w.writeHeader(header)
	}

	manifestPayload, err := w.objects.readMetadata(*entry.File)
	if err != nil {
		return err
	}
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(manifestPayload))
	if err != nil {
		return err
	}
	if manifest.Size != entry.Size || manifest.Size > math.MaxInt64 {
		return fmt.Errorf("rootfs export file %q size metadata is inconsistent", archivePath)
	}
	header, err := headerForEntry(entry, archivePath)
	if err != nil {
		return err
	}
	header.Size = int64(manifest.Size)
	if err := w.writeHeader(header); err != nil {
		return err
	}
	if entry.Nlink > 1 {
		w.hardlinks[entry.Inode] = hardlinkSource{path: archivePath, entry: entry}
	}

	var logicalOffset uint64
	for _, extent := range manifest.Extents {
		if extent.Offset > logicalOffset {
			if err := writeZeros(w.ctx, w.tar, extent.Offset-logicalOffset); err != nil {
				return err
			}
		}
		chunk, err := w.objects.read(extent.Object)
		if err != nil {
			return err
		}
		start := extent.ObjectOffset
		end := start + extent.Length
		if end < start || end > uint64(len(chunk)) {
			return fmt.Errorf("rootfs export file %q extent exceeds chunk %s", archivePath, extent.Object.Key)
		}
		if err := writeAll(w.ctx, w.tar, chunk[start:end]); err != nil {
			return err
		}
		logicalOffset = extent.Offset + extent.Length
	}
	if manifest.Size > logicalOffset {
		return writeZeros(w.ctx, w.tar, manifest.Size-logicalOffset)
	}
	return nil
}

func (w *archiveWriter) writeHeader(header *tar.Header) error {
	if err := w.tar.WriteHeader(header); err != nil {
		return fmt.Errorf("write rootfs export header %q: %w", header.Name, err)
	}
	return nil
}

func headerForEntry(entry rootfshead.Entry, archivePath string) (*tar.Header, error) {
	if err := entry.Validate(archivePath == "."); err != nil {
		return nil, err
	}
	header := &tar.Header{
		Name:       archivePath,
		Mode:       int64(entry.Mode & 0o7777),
		Uid:        int(entry.UID),
		Gid:        int(entry.GID),
		ModTime:    entry.ModTime.Time(),
		AccessTime: entry.AccessTime.Time(),
		ChangeTime: entry.ChangeTime.Time(),
		Format:     tar.FormatPAX,
	}
	if len(entry.XAttrs) > 0 {
		header.Xattrs = make(map[string]string, len(entry.XAttrs))
		for _, xattr := range entry.XAttrs {
			header.Xattrs[xattr.Name] = string(xattr.Value)
		}
	}
	switch entry.Kind {
	case rootfshead.EntryDirectory:
		header.Typeflag = tar.TypeDir
	case rootfshead.EntryFile:
		header.Typeflag = tar.TypeReg
	case rootfshead.EntrySymlink:
		header.Typeflag = tar.TypeSymlink
		header.Linkname = entry.Target
	case rootfshead.EntryChar:
		header.Typeflag = tar.TypeChar
		header.Devmajor = int64(unix.Major(uint64(entry.Rdev)))
		header.Devminor = int64(unix.Minor(uint64(entry.Rdev)))
	case rootfshead.EntryBlock:
		header.Typeflag = tar.TypeBlock
		header.Devmajor = int64(unix.Major(uint64(entry.Rdev)))
		header.Devminor = int64(unix.Minor(uint64(entry.Rdev)))
	case rootfshead.EntryFIFO:
		header.Typeflag = tar.TypeFifo
	default:
		return nil, fmt.Errorf("rootfs export path %q has unsupported kind %q", archivePath, entry.Kind)
	}
	return header, nil
}

func whiteoutHeader(archivePath string, source rootfshead.Entry) *tar.Header {
	return &tar.Header{
		Name:       archivePath,
		Mode:       0,
		Typeflag:   tar.TypeReg,
		Size:       0,
		Uid:        int(source.UID),
		Gid:        int(source.GID),
		ModTime:    source.ModTime.Time(),
		AccessTime: source.AccessTime.Time(),
		ChangeTime: source.ChangeTime.Time(),
		Format:     tar.FormatPAX,
	}
}

func sameHardlinkMetadata(left, right rootfshead.Entry) bool {
	if left.Mode != right.Mode || left.UID != right.UID || left.GID != right.GID ||
		left.Nlink != right.Nlink || left.Size != right.Size || left.Blocks != right.Blocks ||
		left.Rdev != right.Rdev || left.ModTime != right.ModTime || left.AccessTime != right.AccessTime ||
		left.ChangeTime != right.ChangeTime || len(left.XAttrs) != len(right.XAttrs) {
		return false
	}
	for position := range left.XAttrs {
		if left.XAttrs[position].Name != right.XAttrs[position].Name ||
			!bytes.Equal(left.XAttrs[position].Value, right.XAttrs[position].Value) {
			return false
		}
	}
	return true
}

func writeZeros(ctx context.Context, writer io.Writer, length uint64) error {
	var zeroes [32 << 10]byte
	for length > 0 {
		amount := min(length, uint64(len(zeroes)))
		if err := writeAll(ctx, writer, zeroes[:amount]); err != nil {
			return err
		}
		length -= amount
	}
	return nil
}

func writeAll(ctx context.Context, writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func putImmutable(ctx context.Context, store objectstore.Store, object rootfshead.Object, temporary *os.File) error {
	_, err := store.Head(object.Key)
	if err == nil {
		// Head may expose the physical ciphertext size. The object key binds the
		// plaintext digest, and consumers validate the descriptor when reading.
		return ctx.Err()
	}
	if !objectstore.IsNotFound(err) {
		return fmt.Errorf("inspect rootfs export object %s: %w", object.Key, err)
	}
	if _, err := temporary.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind rootfs export: %w", err)
	}
	if err := store.Put(object.Key, contextReader{ctx: ctx, reader: temporary}); err != nil {
		return fmt.Errorf("store rootfs export object %s: %w", object.Key, err)
	}
	return ctx.Err()
}
