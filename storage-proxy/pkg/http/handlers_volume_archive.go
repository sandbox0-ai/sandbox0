package http

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	pathpkg "path"
	"strconv"
	"strings"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type volumeArchiveUploadResult struct {
	Files     int   `json:"files"`
	Dirs      int   `json:"dirs"`
	Symlinks  int   `json:"symlinks"`
	Bytes     int64 `json:"bytes"`
	Overwrote int   `json:"overwrote"`
}

const (
	volumeArchiveWriteChunkSize      = 256 * 1024
	volumeArchiveCheckpointBytes     = 4 * 1024 * 1024
	volumeArchiveCheckpointMutations = 512
)

func (s *Server) handleVolumeFileArchiveUpload(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}
	targetPath := r.URL.Query().Get("path")
	if targetPath == "" {
		targetPath = "/"
	}
	overwrite := parseBoolDefault(r.URL.Query().Get("overwrite"), false)

	s.withSharedVolumeFileRequest(w, r, volumeID, func(lockedReq *http.Request) {
		ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, lockedReq, volumeID)
		if handled {
			return
		}
		defer cleanup()

		result, err := s.importVolumeArchive(ctx, volumeID, targetPath, lockedReq, overwrite)
		if err != nil {
			s.writeVolumeFileError(w, err)
			return
		}
		if result.Files > 0 || result.Dirs > 0 || result.Symlinks > 0 || result.Overwrote > 0 {
			if err := s.finalizeVolumeFileMutation(ctx, volumeID); err != nil {
				s.writeVolumeFileError(w, err)
				return
			}
		}
		_ = spec.WriteSuccess(w, http.StatusCreated, result)
	})
}

func (s *Server) importVolumeArchive(ctx context.Context, volumeID, targetPath string, r *http.Request, overwrite bool) (*volumeArchiveUploadResult, error) {
	root, err := cleanVolumePath(targetPath, true)
	if err != nil {
		return nil, err
	}
	reader, closeFn, err := archiveReader(r)
	if err != nil {
		return nil, err
	}
	if closeFn != nil {
		defer closeFn()
	}
	tr := tar.NewReader(reader)
	result := &volumeArchiveUploadResult{}
	checkpoint := volumeArchiveCheckpoint{}
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, errInvalidPath
		}
		if header == nil {
			continue
		}
		rel, err := cleanArchiveEntryPath(header.Name)
		if err != nil {
			return nil, err
		}
		if rel == "" {
			continue
		}
		logicalPath := joinVolumePath(root, rel)
		switch header.Typeflag {
		case tar.TypeDir:
			changed, err := s.mkdirVolumePathWithMode(ctx, volumeID, logicalPath, true, archiveMode(header.Mode, 0o755))
			if err != nil {
				return nil, err
			}
			if changed {
				result.Dirs++
			}
			if err := s.maybeCheckpointVolumeArchive(ctx, volumeID, result, &checkpoint); err != nil {
				return nil, err
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size > maxVolumeFileSize {
				return nil, errFileTooLarge
			}
			changed, replaced, written, err := s.writeVolumePathWithMode(ctx, volumeID, logicalPath, tr, header.Size, archiveMode(header.Mode, 0o644), overwrite)
			if err != nil {
				return nil, err
			}
			if changed {
				result.Files++
				result.Bytes += written
			}
			if replaced {
				result.Overwrote++
			}
			if err := s.maybeCheckpointVolumeArchive(ctx, volumeID, result, &checkpoint); err != nil {
				return nil, err
			}
		case tar.TypeSymlink:
			changed, replaced, err := s.symlinkVolumePath(ctx, volumeID, logicalPath, header.Linkname, overwrite)
			if err != nil {
				return nil, err
			}
			if changed {
				result.Symlinks++
			}
			if replaced {
				result.Overwrote++
			}
			if err := s.maybeCheckpointVolumeArchive(ctx, volumeID, result, &checkpoint); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: unsupported tar entry type %d", errInvalidPath, header.Typeflag)
		}
	}
	return result, nil
}

type volumeArchiveCheckpoint struct {
	bytes     int64
	mutations int
}

func (s *Server) maybeCheckpointVolumeArchive(ctx context.Context, volumeID string, result *volumeArchiveUploadResult, checkpoint *volumeArchiveCheckpoint) error {
	if result == nil || checkpoint == nil {
		return nil
	}
	mutations := result.Files + result.Dirs + result.Symlinks
	if result.Bytes-checkpoint.bytes < volumeArchiveCheckpointBytes && mutations-checkpoint.mutations < volumeArchiveCheckpointMutations {
		return nil
	}
	if err := s.finalizeVolumeFileMutation(ctx, volumeID); err != nil {
		return err
	}
	checkpoint.bytes = result.Bytes
	checkpoint.mutations = mutations
	return nil
}

func archiveReader(r *http.Request) (io.Reader, func(), error) {
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format == "tar" {
		return r.Body, nil, nil
	}
	gz, err := gzip.NewReader(r.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid gzip archive", errInvalidPath)
	}
	return gz, func() { _ = gz.Close() }, nil
}

func cleanArchiveEntryPath(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	if strings.HasPrefix(name, "/") {
		return "", errInvalidPath
	}
	parts := strings.Split(name, "/")
	for _, part := range parts {
		if part == ".." {
			return "", errInvalidPath
		}
	}
	cleaned := pathpkg.Clean(name)
	if cleaned == "." {
		return "", nil
	}
	return cleaned, nil
}

func archiveMode(mode int64, fallback uint32) uint32 {
	if mode <= 0 {
		return fallback
	}
	return uint32(mode) & 0o777
}

func parseBoolDefault(raw string, fallback bool) bool {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func (s *Server) mkdirVolumePathWithMode(ctx context.Context, volumeID, raw string, recursive bool, mode uint32) (bool, error) {
	resolved, err := s.lookupVolumePath(ctx, volumeID, raw, false)
	if err != nil {
		return false, err
	}
	if resolved.Exists {
		if attrModeIsDir(resolved.Attr.Mode) && recursive {
			return false, nil
		}
		if attrModeIsDir(resolved.Attr.Mode) {
			return false, errPathAlreadyExists
		}
		return false, errPathNotDir
	}

	parent, base, err := s.ensureVolumeParent(ctx, volumeID, raw, recursive)
	if err != nil {
		return false, err
	}
	_, err = s.fileRPC.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Parent:   parent,
		Name:     base,
		Mode:     mode,
		Umask:    0,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		if fserror.CodeOf(err) == fserror.AlreadyExists && recursive {
			return false, nil
		}
		return false, translateVolumeRPCError(err)
	}
	return true, nil
}

func (s *Server) writeVolumePathWithMode(ctx context.Context, volumeID, raw string, reader io.Reader, size int64, mode uint32, overwrite bool) (bool, bool, int64, error) {
	resolved, err := s.lookupVolumePath(ctx, volumeID, raw, false)
	if err != nil {
		return false, false, 0, err
	}
	replaced := false
	if resolved.Exists {
		if !overwrite {
			return false, false, 0, errPathAlreadyExists
		}
		if err := s.removeVolumePath(ctx, volumeID, resolved); err != nil {
			return false, false, 0, err
		}
		replaced = true
	}

	parent, base, err := s.ensureVolumeParent(ctx, volumeID, raw, true)
	if err != nil {
		return false, false, 0, err
	}
	nodeResp, err := s.fileRPC.Create(ctx, &pb.CreateRequest{
		VolumeId: volumeID,
		Parent:   parent,
		Name:     base,
		Mode:     mode,
		Flags:    uint32(syscall.O_WRONLY),
		Umask:    0,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return false, false, 0, translateVolumeRPCError(err)
	}
	handleID := nodeResp.HandleId
	defer func() {
		if handleID != 0 {
			_, _ = s.fileRPC.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    nodeResp.Inode,
				HandleId: handleID,
				Actor:    volumeFileActor(ctx),
			})
		}
	}()

	buf := make([]byte, volumeArchiveWriteChunkSize)
	var written int64
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if written+int64(n) > maxVolumeFileSize || written+int64(n) > size {
				return false, replaced, written, errFileTooLarge
			}
			resp, err := s.fileRPC.Write(ctx, &pb.WriteRequest{
				VolumeId: volumeID,
				Inode:    nodeResp.Inode,
				Offset:   written,
				Data:     append([]byte(nil), buf[:n]...),
				HandleId: handleID,
				Actor:    volumeFileActor(ctx),
			})
			if err != nil {
				return false, replaced, written, translateVolumeRPCError(err)
			}
			if resp != nil && resp.BytesWritten >= 0 && resp.BytesWritten != int64(n) {
				return false, replaced, written, io.ErrShortWrite
			}
			written += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return false, replaced, written, readErr
		}
	}
	if written != size {
		return false, replaced, written, io.ErrUnexpectedEOF
	}
	if size == 0 {
		if _, err := s.fileRPC.Write(ctx, &pb.WriteRequest{
			VolumeId: volumeID,
			Inode:    nodeResp.Inode,
			Offset:   0,
			Data:     nil,
			HandleId: handleID,
			Actor:    volumeFileActor(ctx),
		}); err != nil {
			return false, replaced, written, translateVolumeRPCError(err)
		}
	}
	return true, replaced, written, nil
}

func (s *Server) symlinkVolumePath(ctx context.Context, volumeID, raw, target string, overwrite bool) (bool, bool, error) {
	if strings.TrimSpace(target) == "" {
		return false, false, errInvalidPath
	}
	resolved, err := s.lookupVolumePath(ctx, volumeID, raw, false)
	if err != nil {
		return false, false, err
	}
	replaced := false
	if resolved.Exists {
		if !overwrite {
			return false, false, errPathAlreadyExists
		}
		if err := s.removeVolumePath(ctx, volumeID, resolved); err != nil {
			return false, false, err
		}
		replaced = true
	}
	parent, base, err := s.ensureVolumeParent(ctx, volumeID, raw, true)
	if err != nil {
		return false, false, err
	}
	_, err = s.fileRPC.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId: volumeID,
		Parent:   parent,
		Name:     base,
		Target:   target,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return false, replaced, translateVolumeRPCError(err)
	}
	return true, replaced, nil
}
