package portal

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"golang.org/x/sys/unix"
)

const (
	rootFSStateDirName = ".sandbox0"
	rootFSUpperTarName = "upper.tar"
)

func restoreRootFSUpperDir(ctx context.Context, engine *s0fs.Engine, upperDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if engine == nil {
		return fmt.Errorf("sandbox rootfs s0fs engine is required")
	}
	if strings.TrimSpace(upperDir) == "" {
		return fmt.Errorf("sandbox rootfs upperdir is required")
	}
	if err := resetDirectory(upperDir, 0o755); err != nil {
		return fmt.Errorf("reset sandbox rootfs upperdir: %w", err)
	}
	data, err := readS0FSFile(engine, rootFSStateDirName, rootFSUpperTarName)
	if errors.Is(err, s0fs.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read sandbox rootfs upperdir state: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := extractTarToDir(bytes.NewReader(data), upperDir); err != nil {
		return fmt.Errorf("extract sandbox rootfs upperdir state: %w", err)
	}
	return nil
}

func syncRootFSUpperToS0FS(ctx context.Context, engine *s0fs.Engine, upperDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if engine == nil || strings.TrimSpace(upperDir) == "" {
		return nil
	}
	if _, err := os.Stat(upperDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	data, err := tarDirectory(upperDir)
	if err != nil {
		return err
	}
	if err := writeS0FSFile(engine, rootFSStateDirName, rootFSUpperTarName, data, 0o600); err != nil {
		return err
	}
	return engine.Fsync(s0fs.RootInode)
}

func resetDirectory(path string, mode os.FileMode) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return os.MkdirAll(path, mode)
}

func readS0FSFile(engine *s0fs.Engine, dirName, fileName string) ([]byte, error) {
	dir, err := engine.Lookup(s0fs.RootInode, dirName)
	if err != nil {
		return nil, err
	}
	if dir.Type != s0fs.TypeDirectory {
		return nil, fmt.Errorf("%s is not a directory", dirName)
	}
	file, err := engine.Lookup(dir.Inode, fileName)
	if err != nil {
		return nil, err
	}
	if file.Type != s0fs.TypeFile {
		return nil, fmt.Errorf("%s/%s is not a file", dirName, fileName)
	}
	return engine.Read(file.Inode, 0, file.Size)
}

func writeS0FSFile(engine *s0fs.Engine, dirName, fileName string, data []byte, mode uint32) error {
	dir, err := ensureS0FSDir(engine, s0fs.RootInode, dirName, 0o700)
	if err != nil {
		return err
	}
	file, err := engine.Lookup(dir.Inode, fileName)
	switch {
	case err == nil:
		if file.Type != s0fs.TypeFile {
			return fmt.Errorf("%s/%s is not a file", dirName, fileName)
		}
		if err := engine.Truncate(file.Inode, 0); err != nil {
			return err
		}
	case errors.Is(err, s0fs.ErrNotFound):
		file, err = engine.CreateFile(dir.Inode, fileName, mode)
		if err != nil {
			return err
		}
	default:
		return err
	}
	if mode != 0 {
		if err := engine.SetMode(file.Inode, mode); err != nil {
			return err
		}
	}
	if len(data) > 0 {
		if _, err := engine.Write(file.Inode, 0, data); err != nil {
			return err
		}
	}
	return nil
}

func ensureS0FSDir(engine *s0fs.Engine, parent uint64, name string, mode uint32) (*s0fs.Node, error) {
	node, err := engine.Lookup(parent, name)
	switch {
	case err == nil:
		if node.Type != s0fs.TypeDirectory {
			return nil, fmt.Errorf("%s is not a directory", name)
		}
		return node, nil
	case errors.Is(err, s0fs.ErrNotFound):
		return engine.Mkdir(parent, name, mode)
	default:
		return nil, err
	}
}

func tarDirectory(root string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		linkTarget := ""
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return err
		}
		header.Name = rel
		if info.IsDir() {
			header.Name += "/"
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			header.Uid = int(stat.Uid)
			header.Gid = int(stat.Gid)
			header.AccessTime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
			header.ChangeTime = time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func extractTarToDir(reader io.Reader, root string) error {
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		target, err := safeTarTarget(root, header.Name)
		if err != nil {
			return err
		}
		mode := os.FileMode(header.Mode)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, mode.Perm()); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := writeExtractedFile(target, tr, mode.Perm()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			linkTarget, err := safeTarTarget(root, header.Linkname)
			if err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return err
			}
		case tar.TypeFifo:
			_ = os.Remove(target)
			if err := unix.Mkfifo(target, uint32(mode.Perm())); err != nil {
				return err
			}
		case tar.TypeChar, tar.TypeBlock:
			_ = os.Remove(target)
			nodeMode := uint32(mode.Perm())
			if header.Typeflag == tar.TypeChar {
				nodeMode |= unix.S_IFCHR
			} else {
				nodeMode |= unix.S_IFBLK
			}
			dev := int(unix.Mkdev(uint32(header.Devmajor), uint32(header.Devminor)))
			if err := unix.Mknod(target, nodeMode, dev); err != nil {
				return err
			}
		default:
			continue
		}
		applyTarMetadata(target, header)
	}
}

func safeTarTarget(root, name string) (string, error) {
	clean := filepath.Clean(string(filepath.Separator) + name)
	rel := strings.TrimPrefix(clean, string(filepath.Separator))
	if rel == "" || rel == "." {
		return "", fmt.Errorf("invalid tar entry %q", name)
	}
	target := filepath.Join(root, rel)
	rootWithSep := filepath.Clean(root) + string(filepath.Separator)
	if target != filepath.Clean(root) && !strings.HasPrefix(target, rootWithSep) {
		return "", fmt.Errorf("tar entry escapes root: %q", name)
	}
	return target, nil
}

func writeExtractedFile(path string, reader io.Reader, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, reader)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func applyTarMetadata(path string, header *tar.Header) {
	if header == nil {
		return
	}
	_ = os.Lchown(path, header.Uid, header.Gid)
	if header.Typeflag != tar.TypeSymlink {
		_ = os.Chmod(path, os.FileMode(header.Mode).Perm())
	}
	modTime := header.ModTime
	if modTime.IsZero() {
		return
	}
	_ = os.Chtimes(path, modTime, modTime)
}
