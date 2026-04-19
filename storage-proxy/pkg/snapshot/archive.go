package snapshot

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
)

type ExportSnapshotRequest struct {
	VolumeID   string
	SnapshotID string
	TeamID     string
}

type archiveSessionFactory func(context.Context, *db.SandboxVolume) (*snapshotArchiveSession, error)

type snapshotArchiveSession struct {
	meta   snapshotArchiveMeta
	reader snapshotArchiveReader
	close  func() error
}

type snapshotArchiveMeta interface {
	GetAttr(fsmeta.Context, fsmeta.Ino, *fsmeta.Attr) syscall.Errno
	Readdir(fsmeta.Context, fsmeta.Ino, uint8, *[]*fsmeta.Entry) syscall.Errno
	ReadLink(fsmeta.Context, fsmeta.Ino, *[]byte) syscall.Errno
}

type snapshotArchiveReader interface {
	ReadFile(context.Context, fsmeta.Ino, uint64, io.Writer) error
}

func (m *Manager) ExportSnapshotArchive(ctx context.Context, req *ExportSnapshotRequest, w io.Writer) error {
	if req == nil {
		return fmt.Errorf("export snapshot request is nil")
	}
	_, _, session, rootInode, rootAttr, err := m.openSnapshotArchiveSession(ctx, req.VolumeID, req.SnapshotID, req.TeamID)
	if err != nil {
		return err
	}
	if session.close != nil {
		defer session.close()
	}

	gzipWriter := gzip.NewWriter(w)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	return writeSnapshotArchiveTree(ctx, tarWriter, session, rootInode, "", rootAttr)
}

func writeSnapshotArchiveTree(ctx context.Context, tw *tar.Writer, session *snapshotArchiveSession, inode fsmeta.Ino, relPath string, attr *fsmeta.Attr) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch attr.Typ {
	case fsmeta.TypeDirectory:
		if relPath != "" {
			header := snapshotTarHeader(relPath+"/", attr, tar.TypeDir)
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
		}

		var entries []*fsmeta.Entry
		if errno := session.meta.Readdir(fsmeta.Background(), inode, 1, &entries); errno != 0 {
			return fmt.Errorf("readdir inode %d: %w", inode, syscall.Errno(errno))
		}
		sort.Slice(entries, func(i, j int) bool {
			return string(entries[i].Name) < string(entries[j].Name)
		})
		for _, entry := range entries {
			name := string(entry.Name)
			if name == "." || name == ".." || name == "" {
				continue
			}
			nextPath := name
			if relPath != "" {
				nextPath = path.Join(relPath, name)
			}
			entryAttr := entry.Attr
			if entryAttr == nil {
				entryAttr = &fsmeta.Attr{}
				if errno := session.meta.GetAttr(fsmeta.Background(), entry.Inode, entryAttr); errno != 0 {
					return fmt.Errorf("getattr inode %d: %w", entry.Inode, syscall.Errno(errno))
				}
			}
			if err := writeSnapshotArchiveTree(ctx, tw, session, entry.Inode, nextPath, entryAttr); err != nil {
				return err
			}
		}
		return nil
	case fsmeta.TypeFile:
		header := snapshotTarHeader(relPath, attr, tar.TypeReg)
		header.Size = int64(attr.Length)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		return session.reader.ReadFile(ctx, inode, attr.Length, tw)
	case fsmeta.TypeSymlink:
		var target []byte
		if errno := session.meta.ReadLink(fsmeta.Background(), inode, &target); errno != 0 {
			return fmt.Errorf("readlink inode %d: %w", inode, syscall.Errno(errno))
		}
		header := snapshotTarHeader(relPath, attr, tar.TypeSymlink)
		header.Linkname = string(target)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported snapshot inode type %d at %q", attr.Typ, relPath)
	}
}

func snapshotTarHeader(name string, attr *fsmeta.Attr, typeFlag byte) *tar.Header {
	return &tar.Header{
		Name:     cleanArchivePath(name),
		Mode:     int64(attr.Mode),
		ModTime:  time.Unix(attr.Mtime, int64(attr.Mtimensec)).UTC(),
		Uid:      int(attr.Uid),
		Gid:      int(attr.Gid),
		Typeflag: typeFlag,
	}
}

func cleanArchivePath(name string) string {
	hasTrailingSlash := strings.HasSuffix(name, "/")
	cleaned := path.Clean("/" + strings.TrimSpace(name))
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." {
		return ""
	}
	if hasTrailingSlash && cleaned != "" {
		return cleaned + "/"
	}
	return cleaned
}
