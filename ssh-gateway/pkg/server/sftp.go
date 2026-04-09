package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	procdfile "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
)

type sftpSubsystemRequest struct {
	Subsystem string
}

type sftpHandlers struct {
	server  *Server
	target  *SessionTarget
	overlay *pendingWriteOverlay
}

func (s *Server) serveSFTP(channel io.ReadWriteCloser, target *SessionTarget) error {
	handlers := &sftpHandlers{
		server:  s,
		target:  target,
		overlay: newPendingWriteOverlay(),
	}
	requestServer := sftp.NewRequestServer(channel, sftp.Handlers{
		FileGet:  handlers,
		FilePut:  handlers,
		FileCmd:  handlers,
		FileList: handlers,
	}, sftp.WithStartDirectory("/"))
	return requestServer.Serve()
}

func (h *sftpHandlers) Fileread(req *sftp.Request) (io.ReaderAt, error) {
	data, err := h.server.readFile(req.Context(), h.target, req.Filepath)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func (h *sftpHandlers) Filewrite(req *sftp.Request) (io.WriterAt, error) {
	filePath := normalizeSFTPPath(req.Filepath)
	stat, err := h.server.statFile(req.Context(), h.target, filePath)
	if err != nil {
		if !errors.Is(err, sftp.ErrSSHFxNoSuchFile) {
			return nil, err
		}
	}
	if stat != nil && isDirType(stat.Type) {
		return nil, &sftp.StatusError{Code: 24}
	}
	var initial []byte
	if stat != nil && !isDirType(stat.Type) {
		initial, err = h.server.readFile(req.Context(), h.target, filePath)
		if err != nil {
			return nil, err
		}
	}
	if flags := req.Pflags(); flags.Trunc {
		initial = nil
	}
	writer := &pendingFileWriter{
		ctx:     req.Context(),
		overlay: h.overlay,
		server:  h.server,
		target:  h.target,
		path:    filePath,
		buffer:  append([]byte(nil), initial...),
		state:   newPendingWriteState(filePath, stat),
		closed:  false,
	}
	h.overlay.register(writer)
	return writer, nil
}

func (h *sftpHandlers) Filecmd(req *sftp.Request) error {
	switch req.Method {
	case "Rename", "PosixRename":
		return h.server.movePath(req.Context(), h.target, req.Filepath, req.Target)
	case "Rmdir", "Remove":
		return h.server.removePath(req.Context(), h.target, req.Filepath)
	case "Mkdir":
		return h.server.makeDir(req.Context(), h.target, req.Filepath, true)
	case "Setstat":
		return nil
	default:
		return &sftp.StatusError{Code: uint32(sftp.ErrSSHFxOpUnsupported)}
	}
}

func (h *sftpHandlers) Filelist(req *sftp.Request) (sftp.ListerAt, error) {
	switch req.Method {
	case "List":
		dirPath := normalizeSFTPPath(req.Filepath)
		entries, err := h.server.listDir(req.Context(), h.target, dirPath)
		if err != nil {
			return nil, err
		}
		entries = h.withPendingDirectoryEntries(dirPath, entries)
		infos := make([]os.FileInfo, 0, len(entries))
		for _, entry := range entries {
			infos = append(infos, newVirtualFileInfo(entry))
		}
		return listerAt(infos), nil
	case "Stat":
		entry, err := h.statPath(req.Context(), req.Filepath)
		if err != nil {
			return nil, err
		}
		return listerAt([]os.FileInfo{newVirtualFileInfo(*entry)}), nil
	case "Readlink":
		entry, err := h.server.statFile(req.Context(), h.target, normalizeSFTPPath(req.Filepath))
		if err != nil {
			return nil, err
		}
		if !entry.IsLink {
			return nil, sftp.ErrSSHFxNoSuchFile
		}
		return listerAt([]os.FileInfo{virtualFileInfo{name: entry.LinkTarget, modTime: entry.ModTime, sys: entry}}), nil
	default:
		return nil, &sftp.StatusError{Code: uint32(sftp.ErrSSHFxOpUnsupported)}
	}
}

func (h *sftpHandlers) Lstat(req *sftp.Request) (sftp.ListerAt, error) {
	entry, err := h.statPath(req.Context(), req.Filepath)
	if err != nil {
		return nil, err
	}
	return listerAt([]os.FileInfo{newVirtualFileInfo(*entry)}), nil
}

func (h *sftpHandlers) RealPath(filePath string) string {
	return normalizeSFTPPath(filePath)
}

func (h *sftpHandlers) statPath(ctx context.Context, filePath string) (*procdStatResponse, error) {
	normalized := normalizeSFTPPath(filePath)
	if pending, ok := h.overlay.stat(normalized); ok {
		return pending, nil
	}
	return h.server.statFile(ctx, h.target, normalized)
}

func (h *sftpHandlers) withPendingDirectoryEntries(dirPath string, entries []procdStatResponse) []procdStatResponse {
	return h.overlay.mergeDirectoryEntries(dirPath, entries)
}

type pendingWriteOverlay struct {
	mu      sync.RWMutex
	writers map[string]*pendingFileWriter
}

func newPendingWriteOverlay() *pendingWriteOverlay {
	return &pendingWriteOverlay{}
}

func (o *pendingWriteOverlay) register(writer *pendingFileWriter) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.writers == nil {
		o.writers = make(map[string]*pendingFileWriter)
	}
	o.writers[writer.path] = writer
}

func (o *pendingWriteOverlay) unregister(writer *pendingFileWriter) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if existing, ok := o.writers[writer.path]; ok && existing == writer {
		delete(o.writers, writer.path)
	}
}

func (o *pendingWriteOverlay) stat(filePath string) (*procdStatResponse, bool) {
	o.mu.RLock()
	writer := o.writers[filePath]
	o.mu.RUnlock()
	if writer == nil {
		return nil, false
	}
	entry := writer.snapshotStat()
	return &entry, true
}

func (o *pendingWriteOverlay) mergeDirectoryEntries(dirPath string, entries []procdStatResponse) []procdStatResponse {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if len(o.writers) == 0 {
		return entries
	}

	merged := make([]procdStatResponse, 0, len(entries)+len(o.writers))
	pending := make(map[string]procdStatResponse)
	for filePath, writer := range o.writers {
		if path.Dir(filePath) != dirPath {
			continue
		}
		pending[filePath] = writer.snapshotStat()
	}
	if len(pending) == 0 {
		return entries
	}

	for _, entry := range entries {
		filePath := normalizeSFTPPath(entry.Path)
		if overlay, ok := pending[filePath]; ok {
			merged = append(merged, overlay)
			delete(pending, filePath)
			continue
		}
		merged = append(merged, entry)
	}
	for _, entry := range pending {
		merged = append(merged, entry)
	}
	return merged
}

type pendingFileWriter struct {
	ctx     context.Context
	overlay *pendingWriteOverlay
	server  *Server
	target  *SessionTarget
	path    string

	mu     sync.Mutex
	buffer []byte
	state  pendingWriteState
	closed bool
}

func (w *pendingFileWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	end := int(off) + len(p)
	if end > len(w.buffer) {
		next := make([]byte, end)
		copy(next, w.buffer)
		w.buffer = next
	}
	copy(w.buffer[int(off):end], p)
	w.state.Size = int64(len(w.buffer))
	w.state.ModTime = time.Now()
	return len(p), nil
}

func (w *pendingFileWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	data := append([]byte(nil), w.buffer...)
	w.mu.Unlock()
	defer w.overlay.unregister(w)
	return w.server.writeFile(w.ctx, w.target, w.path, data)
}

func (w *pendingFileWriter) snapshotStat() procdStatResponse {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state.Size = int64(len(w.buffer))
	if w.state.ModTime.IsZero() {
		w.state.ModTime = time.Now()
	}
	return w.state.toProcdStatResponse()
}

type pendingWriteState struct {
	Path    string
	Name    string
	Type    string
	Mode    string
	ModTime time.Time
	Size    int64
}

func newPendingWriteState(filePath string, stat *procdStatResponse) pendingWriteState {
	state := pendingWriteState{
		Path:    filePath,
		Name:    path.Base(strings.TrimSuffix(filePath, "/")),
		Type:    string(procdfile.FileTypeFile),
		Mode:    "0644",
		ModTime: time.Now(),
	}
	if state.Name == "." || state.Name == "/" {
		state.Name = ""
	}
	if stat == nil {
		return state
	}
	if stat.Name != "" {
		state.Name = stat.Name
	}
	if stat.Type != "" {
		state.Type = stat.Type
	}
	if stat.Mode != "" {
		state.Mode = stat.Mode
	}
	if !stat.ModTime.IsZero() {
		state.ModTime = stat.ModTime
	}
	state.Size = stat.Size
	return state
}

func (s pendingWriteState) toProcdStatResponse() procdStatResponse {
	return procdStatResponse{
		Name:    s.Name,
		Path:    s.Path,
		Type:    s.Type,
		Size:    s.Size,
		Mode:    s.Mode,
		ModTime: s.ModTime,
	}
}

func normalizeSFTPPath(filePath string) string {
	cleaned := path.Clean("/" + strings.TrimSpace(strings.TrimPrefix(filePath, "/")))
	if cleaned == "." {
		return "/"
	}
	return cleaned
}

type listerAt []os.FileInfo

func (l listerAt) ListAt(dst []os.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(dst, l[offset:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

type virtualFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     any
}

func newVirtualFileInfo(entry procdStatResponse) os.FileInfo {
	mode := os.FileMode(parseFileMode(entry.Mode))
	if isDirType(entry.Type) {
		mode |= os.ModeDir
	}
	if entry.IsLink {
		mode |= os.ModeSymlink
	}
	name := entry.Name
	if name == "" {
		name = path.Base(strings.TrimSuffix(entry.Path, "/"))
	}
	return virtualFileInfo{name: name, size: entry.Size, mode: mode, modTime: entry.ModTime, isDir: isDirType(entry.Type), sys: entry}
}

func (v virtualFileInfo) Name() string       { return v.name }
func (v virtualFileInfo) Size() int64        { return v.size }
func (v virtualFileInfo) Mode() os.FileMode  { return v.mode }
func (v virtualFileInfo) ModTime() time.Time { return v.modTime }
func (v virtualFileInfo) IsDir() bool        { return v.isDir }
func (v virtualFileInfo) Sys() any           { return v.sys }
