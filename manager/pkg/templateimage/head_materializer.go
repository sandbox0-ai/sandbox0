package templateimage

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"golang.org/x/sys/unix"
)

const materializedHeadLayerPrefix = "materialized-rootfs-head-"

type materializedHeadLayer struct {
	Layer
	path string
}

func (m *materializedHeadLayer) OpenAt(offset int64) (io.ReadCloser, error) {
	if m == nil || strings.TrimSpace(m.path) == "" {
		return nil, fmt.Errorf("materialized rootfs head layer is unavailable")
	}
	if offset < 0 || offset > m.Size {
		return nil, fmt.Errorf("materialized rootfs head offset %d exceeds size %d", offset, m.Size)
	}
	file, err := os.Open(m.path)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &limitedReadCloser{Reader: io.LimitReader(file, m.Size-offset), Closer: file}, nil
}

func (m *materializedHeadLayer) Close() error {
	if m == nil || strings.TrimSpace(m.path) == "" {
		return nil
	}
	err := os.Remove(m.path)
	m.path = ""
	return err
}

// materializeHeadLayer is an intentionally offline compatibility bridge for
// template builds. Lifecycle pause and resume never call it; they retain the
// sharded metadata head and lazy chunk reads.
func materializeHeadLayer(ctx context.Context, objects ObjectReader, reference rootfshead.HeadReference) (_ *materializedHeadLayer, resultErr error) {
	if objects == nil {
		return nil, fmt.Errorf("rootfs object reader is required")
	}
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	payload, err := readHeadObject(ctx, objects, reference.Manifest, rootfshead.HeadMediaType)
	if err != nil {
		return nil, fmt.Errorf("read rootfs head: %w", err)
	}
	head, err := rootfshead.DecodeHead(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if head.HeadID != reference.HeadID {
		return nil, fmt.Errorf("rootfs head id %q does not match reference %q", head.HeadID, reference.HeadID)
	}

	output, err := os.CreateTemp("", materializedHeadLayerPrefix+"*.tar")
	if err != nil {
		return nil, fmt.Errorf("create materialized rootfs layer: %w", err)
	}
	outputPath := output.Name()
	defer func() {
		if resultErr != nil {
			_ = output.Close()
			_ = os.Remove(outputPath)
		}
	}()
	if err := output.Chmod(0o600); err != nil {
		return nil, err
	}
	digester := digest.Canonical.Digester()
	tarWriter := tar.NewWriter(io.MultiWriter(output, digester.Hash()))
	materializer := &headTarMaterializer{
		ctx:       ctx,
		objects:   objects,
		writer:    tarWriter,
		hardlinks: make(map[string]string),
	}
	if err := materializer.writeRoot(head.Root); err != nil {
		_ = tarWriter.Close()
		return nil, err
	}
	if err := tarWriter.Close(); err != nil {
		return nil, fmt.Errorf("close materialized rootfs layer: %w", err)
	}
	if err := output.Close(); err != nil {
		return nil, fmt.Errorf("close materialized rootfs layer file: %w", err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		return nil, err
	}
	digestValue := digester.Digest()
	return &materializedHeadLayer{
		Layer: Layer{
			ID:        materializedHeadLayerPrefix + reference.HeadID,
			ObjectKey: materializedHeadLayerPrefix + reference.HeadID,
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digestValue.String(),
			DiffID:    digestValue.String(),
			Size:      info.Size(),
		},
		path: outputPath,
	}, nil
}

type headTarMaterializer struct {
	ctx       context.Context
	objects   ObjectReader
	writer    *tar.Writer
	hardlinks map[string]string
}

func (m *headTarMaterializer) writeRoot(root rootfshead.Entry) error {
	if err := m.writeEntry(".", root); err != nil {
		return err
	}
	return m.writeDirectory("", root.Directory)
}

func (m *headTarMaterializer) writeDirectory(parent string, object *rootfshead.Object) error {
	if object == nil {
		return fmt.Errorf("rootfs directory %q has no index", parent)
	}
	payload, err := readHeadObject(m.ctx, m.objects, *object, rootfshead.DirectoryIndexMediaType)
	if err != nil {
		return fmt.Errorf("read rootfs directory %q: %w", parent, err)
	}
	index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	var entries []rootfshead.Entry
	for _, shardReference := range index.Shards {
		payload, err := readHeadObject(m.ctx, m.objects, shardReference.Object, rootfshead.DirectoryShardMediaType)
		if err != nil {
			return fmt.Errorf("read rootfs directory shard %q/%d: %w", parent, shardReference.Bucket, err)
		}
		shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
		if err != nil {
			return err
		}
		entries = append(entries, shard.Entries...)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	for _, entry := range entries {
		if err := m.ctx.Err(); err != nil {
			return err
		}
		entryPath := path.Join(parent, entry.Name)
		if entry.Kind == rootfshead.EntryWhiteout {
			if err := m.writeWhiteout(parent, entry.Name); err != nil {
				return err
			}
			continue
		}
		if err := m.writeEntry(entryPath, entry); err != nil {
			return err
		}
		if entry.Kind == rootfshead.EntryDirectory {
			if rootfsEntryOpaque(entry) {
				if err := m.writeOpaqueWhiteout(entryPath); err != nil {
					return err
				}
			}
			if err := m.writeDirectory(entryPath, entry.Directory); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *headTarMaterializer) writeEntry(name string, entry rootfshead.Entry) error {
	header := &tar.Header{
		Name:       name,
		Mode:       int64(entry.Mode & 0o7777),
		Uid:        int(entry.UID),
		Gid:        int(entry.GID),
		ModTime:    entry.ModTime.Time(),
		AccessTime: entry.AccessTime.Time(),
		ChangeTime: entry.ChangeTime.Time(),
		Format:     tar.FormatPAX,
	}
	header.Xattrs = make(map[string]string)
	for _, attr := range entry.XAttrs {
		if attr.Name == "trusted.overlay.opaque" || attr.Name == "user.overlay.opaque" {
			continue
		}
		header.Xattrs[attr.Name] = string(attr.Value)
	}
	if len(header.Xattrs) == 0 {
		header.Xattrs = nil
	}
	switch entry.Kind {
	case rootfshead.EntryDirectory:
		header.Typeflag = tar.TypeDir
		if name != "." {
			header.Name += "/"
		}
	case rootfshead.EntryFile:
		if entry.File == nil {
			return fmt.Errorf("rootfs file %q has no manifest", name)
		}
		if entry.Nlink > 1 {
			if first := m.hardlinks[entry.Inode]; first != "" {
				header.Typeflag = tar.TypeLink
				header.Linkname = first
				return m.writer.WriteHeader(header)
			}
			m.hardlinks[entry.Inode] = name
		}
		header.Typeflag = tar.TypeReg
		header.Size = int64(entry.Size)
		if err := m.writer.WriteHeader(header); err != nil {
			return err
		}
		return m.writeFile(*entry.File, entry.Size)
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
		return fmt.Errorf("unsupported rootfs entry kind %q for %q", entry.Kind, name)
	}
	return m.writer.WriteHeader(header)
}

func (m *headTarMaterializer) writeFile(object rootfshead.Object, size uint64) error {
	payload, err := readHeadObject(m.ctx, m.objects, object, rootfshead.FileMediaType)
	if err != nil {
		return err
	}
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	if manifest.Size != size {
		return fmt.Errorf("rootfs file manifest size %d does not match entry size %d", manifest.Size, size)
	}
	var position uint64
	for _, extent := range manifest.Extents {
		if extent.Offset > position {
			if err := writeZeroes(m.writer, extent.Offset-position); err != nil {
				return err
			}
		}
		chunk, err := readHeadObject(m.ctx, m.objects, extent.Object, rootfshead.ChunkMediaType)
		if err != nil {
			return err
		}
		start := extent.ObjectOffset
		end := start + extent.Length
		if end > uint64(len(chunk)) {
			return fmt.Errorf("rootfs file extent exceeds chunk payload")
		}
		if _, err := m.writer.Write(chunk[start:end]); err != nil {
			return err
		}
		position = extent.Offset + extent.Length
	}
	if position < size {
		return writeZeroes(m.writer, size-position)
	}
	return nil
}

func (m *headTarMaterializer) writeWhiteout(parent, name string) error {
	return m.writer.WriteHeader(&tar.Header{
		Name:     path.Join(parent, ".wh."+name),
		Mode:     0,
		Size:     0,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	})
}

func (m *headTarMaterializer) writeOpaqueWhiteout(directory string) error {
	return m.writer.WriteHeader(&tar.Header{
		Name:     path.Join(directory, ".wh..wh..opq"),
		Mode:     0,
		Size:     0,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	})
}

func rootfsEntryOpaque(entry rootfshead.Entry) bool {
	for _, attr := range entry.XAttrs {
		if (attr.Name == "trusted.overlay.opaque" || attr.Name == "user.overlay.opaque") &&
			(string(attr.Value) == "y" || string(attr.Value) == "x") {
			return true
		}
	}
	return false
}

func readHeadObject(ctx context.Context, objects ObjectReader, object rootfshead.Object, mediaType string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := object.Validate(mediaType); err != nil {
		return nil, err
	}
	reader, err := objects.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	payload, err := io.ReadAll(io.LimitReader(reader, object.Size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) != object.Size {
		return nil, fmt.Errorf("rootfs object %q size is %d, expected %d", object.Key, len(payload), object.Size)
	}
	digestValue, err := digest.Parse(object.Digest)
	if err != nil {
		return nil, err
	}
	if actual := digest.FromBytes(payload); actual != digestValue {
		return nil, fmt.Errorf("rootfs object %q digest is %s, expected %s", object.Key, actual, digestValue)
	}
	return payload, ctx.Err()
}

func writeZeroes(writer io.Writer, size uint64) error {
	var zeroes [32 << 10]byte
	for size > 0 {
		length := min(size, uint64(len(zeroes)))
		if _, err := writer.Write(zeroes[:length]); err != nil {
			return err
		}
		size -= length
	}
	return nil
}
