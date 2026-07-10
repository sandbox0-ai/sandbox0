package portal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal/nodefs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"golang.org/x/sys/unix"
)

const (
	rootFSStateVersion       = 1
	rootFSStateDirectoryName = ".sandbox0-ctld-rootfs"
	rootFSStateFileName      = "orphans.json"
	rootFSOrphanDirectory    = "orphans"
	rootFSMaxStateBytes      = 16 << 20
)

type rootFSHostIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Mode   uint32 `json:"mode"`
}

type rootFSOpenHandle struct {
	openCount uint64
	directory bool
	readFile  *os.File
	writeFile *os.File
}

type rootFSOrphanRecord struct {
	Inode     uint64             `json:"inode"`
	Identity  rootFSHostIdentity `json:"identity"`
	OpenCount uint64             `json:"open_count"`
	Directory bool               `json:"directory"`
	Retain    bool               `json:"retain"`
}

type rootFSPersistedState struct {
	Version int                           `json:"version"`
	Orphans map[uint64]rootFSOrphanRecord `json:"orphans"`
}

// initializeState rebuilds NodeIDs from stable host inode numbers. Only
// open-unlink state is durable; Lookup, Open, and ordinary Release never write.
func (s *rootFSBackedSession) initializeState() error {
	rootInfo, err := os.Stat(s.root)
	if err != nil {
		return fmt.Errorf("stat rootfs backing directory: %w", err)
	}
	if !rootInfo.IsDir() {
		return fmt.Errorf("rootfs backing path %q is not a directory", s.root)
	}
	if err := ensurePrivateDirectory(s.stateDirectoryPath()); err != nil {
		return fmt.Errorf("prepare rootfs state directory: %w", err)
	}
	if err := ensurePrivateDirectory(s.orphanDirectoryPath()); err != nil {
		return fmt.Errorf("prepare rootfs orphan directory: %w", err)
	}
	if err := s.scanNamespaceLocked(rootInfo); err != nil {
		return err
	}

	state, err := s.readState()
	if errors.Is(err, os.ErrNotExist) {
		s.orphans = make(map[uint64]rootFSOrphanRecord)
		s.handles = make(map[uint64]*rootFSOpenHandle)
		for path, inode := range s.inodeByPath {
			if isRootFSInternalPath(path) {
				if err := s.retainUntrackedOrphanLocked(inode); err != nil {
					return err
				}
			}
		}
		return s.persistStateLocked()
	}
	if err != nil {
		return err
	}
	s.recovered = true
	if err := s.loadOrphansLocked(state); err != nil {
		return err
	}
	changed := false
	for path, inode := range s.inodeByPath {
		if !isRootFSInternalPath(path) {
			continue
		}
		if _, ok := s.orphans[inode]; !ok {
			if err := s.retainUntrackedOrphanLocked(inode); err != nil {
				return err
			}
			changed = true
		}
	}
	for inode, orphan := range s.orphans {
		path := rootFSOrphanPath(inode)
		if !s.pathMatchesIdentityLocked(path, inode, orphan.Identity) {
			return fmt.Errorf("rootfs orphan state for inode %d has no matching host file", inode)
		}
		if orphan.OpenCount != 0 {
			s.handles[inode] = &rootFSOpenHandle{openCount: orphan.OpenCount, directory: orphan.Directory}
		}
	}
	if changed {
		return s.persistStateLocked()
	}
	return nil
}

func (s *rootFSBackedSession) scanNamespaceLocked(rootInfo os.FileInfo) error {
	rootIdentity, err := rootFSIdentityFromInfo(rootInfo)
	if err != nil {
		return err
	}
	s.inodeByPath = map[string]uint64{rootFSBackedSessionRoot: s0fs.RootInode}
	s.pathByInode = map[uint64]string{s0fs.RootInode: rootFSBackedSessionRoot}
	s.identityByInode = map[uint64]rootFSHostIdentity{s0fs.RootInode: rootIdentity}
	s.handles = make(map[uint64]*rootFSOpenHandle)

	err = filepath.WalkDir(s.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		rel = cleanRootFSBackedRel(filepath.ToSlash(rel))
		if rel == rootFSStateDirectoryName {
			return filepath.SkipDir
		}
		if rel == rootFSBackedSessionRoot {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		_, err = s.registerPathLocked(rel, info)
		return err
	})
	if err != nil {
		return fmt.Errorf("scan rootfs namespace: %w", err)
	}
	entries, err := os.ReadDir(s.orphanDirectoryPath())
	if err != nil {
		return fmt.Errorf("scan rootfs orphan directory: %w", err)
	}
	for _, entry := range entries {
		inode, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil || inode == 0 {
			return fmt.Errorf("invalid rootfs orphan name %q", entry.Name())
		}
		path := rootFSOrphanPath(inode)
		info, err := os.Lstat(s.hostPath(path))
		if err != nil {
			return err
		}
		registered, err := s.registerPathLocked(path, info)
		if err != nil {
			return err
		}
		if registered != inode {
			return fmt.Errorf("rootfs orphan %q contains host inode %d", path, registered)
		}
	}
	return nil
}

func (s *rootFSBackedSession) registerPathLocked(rel string, info os.FileInfo) (uint64, error) {
	identity, err := rootFSIdentityFromInfo(info)
	if err != nil {
		return 0, err
	}
	inode := identity.Inode
	if inode <= s0fs.RootInode || inode > nodefs.MaxBackendLocalID {
		return 0, fmt.Errorf("rootfs host inode %d for %q is outside nodefs local id range", inode, rel)
	}
	if existing, ok := s.identityByInode[inode]; ok && existing != identity {
		return 0, fmt.Errorf("rootfs host inode %d has conflicting identities", inode)
	}
	s.identityByInode[inode] = identity
	s.inodeByPath[rel] = inode
	if current, ok := s.pathByInode[inode]; !ok || isRootFSInternalPath(current) && !isRootFSInternalPath(rel) {
		s.pathByInode[inode] = rel
	}
	return inode, nil
}

func (s *rootFSBackedSession) readState() (*rootFSPersistedState, error) {
	info, err := os.Stat(s.stateFilePath())
	if err != nil {
		return nil, err
	}
	if info.Size() > rootFSMaxStateBytes {
		return nil, fmt.Errorf("rootfs orphan state is too large: %d bytes", info.Size())
	}
	data, err := os.ReadFile(s.stateFilePath())
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(s.stateFilePath(), 0o600); err != nil {
		return nil, err
	}
	var state rootFSPersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode rootfs orphan state: %w", err)
	}
	return &state, nil
}

func (s *rootFSBackedSession) loadOrphansLocked(state *rootFSPersistedState) error {
	if state == nil || state.Version != rootFSStateVersion || state.Orphans == nil {
		return fmt.Errorf("invalid rootfs orphan state version or map")
	}
	s.orphans = state.Orphans
	for inode, orphan := range s.orphans {
		if inode <= s0fs.RootInode || inode > nodefs.MaxBackendLocalID || orphan.Inode != inode {
			return fmt.Errorf("invalid rootfs orphan inode %d", inode)
		}
	}
	return nil
}

// persistStateLocked is only called for rare orphan lifecycle transitions.
func (s *rootFSBackedSession) persistStateLocked() error {
	data, err := json.Marshal(&rootFSPersistedState{Version: rootFSStateVersion, Orphans: s.orphans})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(s.stateDirectoryPath(), ".orphans-*.tmp")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(path)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(path, s.stateFilePath()); err != nil {
		return err
	}
	return syncDirectory(s.stateDirectoryPath())
}

func (s *rootFSBackedSession) commitOrphansLocked(operation string) error {
	if err := s.persistStateLocked(); err != nil {
		s.initErr = fmt.Errorf("persist rootfs orphan state after %s: %w", operation, err)
		return s.initErr
	}
	return nil
}

func (s *rootFSBackedSession) ensureOpenLocked() error {
	if s.initErr != nil {
		return fserror.New(fserror.FailedPrecondition, fmt.Sprintf("rootfs-backed portal state is unavailable: %v", s.initErr))
	}
	if s.closed {
		return fserror.New(fserror.FailedPrecondition, "rootfs-backed portal is closed")
	}
	return nil
}

func (s *rootFSBackedSession) inodeForExistingPathLocked(rel string, info os.FileInfo) (uint64, error) {
	rel = cleanRootFSBackedRel(rel)
	if isRootFSInternalPath(rel) {
		return 0, fserror.New(fserror.PermissionDenied, "reserved rootfs path")
	}
	identity, err := rootFSIdentityFromInfo(info)
	if err != nil {
		return 0, err
	}
	if old, ok := s.inodeByPath[rel]; ok && old != identity.Inode {
		s.dropPathLocked(rel)
	}
	return s.registerPathLocked(rel, info)
}

func (s *rootFSBackedSession) openHandleLocked(inode uint64, directory bool, file *os.File, flags int) uint64 {
	handle := s.handles[inode]
	if handle == nil {
		handle = &rootFSOpenHandle{directory: directory}
		s.handles[inode] = handle
	}
	handle.openCount++
	cacheRootFSFile(handle, file, flags)
	return inode
}

// removePathLocked preserves a final name if an FH is open or this session was
// recovered and the kernel may still hold an FH unknown to user space.
func (s *rootFSBackedSession) removePathLocked(rel string, directory bool) (*pb.Empty, error) {
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	if info.IsDir() != directory {
		if directory {
			return nil, mapRootFSBackedError(syscall.ENOTDIR)
		}
		return nil, mapRootFSBackedError(syscall.EISDIR)
	}
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		return nil, err
	}
	if other := s.otherVisiblePathLocked(inode, rel); other != "" {
		if err := os.Remove(s.hostPath(rel)); err != nil {
			return nil, mapRootFSBackedError(err)
		}
		s.dropPathLocked(rel)
		return &pb.Empty{}, nil
	}
	handle := s.handles[inode]
	if !s.hasHandlesForInodeLocked(inode) && !s.recovered {
		if err := os.Remove(s.hostPath(rel)); err != nil {
			return nil, mapRootFSBackedError(err)
		}
		closeRootFSHandleFiles(handle)
		delete(s.handles, inode)
		s.dropPathLocked(rel)
		return &pb.Empty{}, nil
	}

	orphanPath := rootFSOrphanPath(inode)
	if err := renameRootFSNoReplace(s.hostPath(rel), s.hostPath(orphanPath)); err != nil {
		return nil, mapRootFSBackedError(err)
	}
	delete(s.inodeByPath, rel)
	s.inodeByPath[orphanPath] = inode
	s.pathByInode[inode] = orphanPath
	identity, _ := rootFSIdentityFromInfo(info)
	openCount := uint64(0)
	if handle != nil {
		openCount = handle.openCount
	}
	s.orphans[inode] = rootFSOrphanRecord{
		Inode: inode, Identity: identity, OpenCount: openCount, Directory: directory, Retain: s.recovered,
	}
	if err := s.commitOrphansLocked("unlink"); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) releaseHandleLocked(inode uint64, directory bool) (*pb.Empty, error) {
	handle := s.handles[inode]
	if handle == nil {
		return &pb.Empty{}, nil
	}
	if handle.directory != directory {
		return nil, fserror.New(fserror.InvalidArgument, fmt.Sprintf("handle %d has the wrong type", inode))
	}
	if handle.openCount > 0 {
		handle.openCount--
	}
	orphan, orphaned := s.orphans[inode]
	if !orphaned || orphan.Retain {
		if handle.openCount == 0 {
			closeRootFSHandleFiles(handle)
			delete(s.handles, inode)
		}
		return &pb.Empty{}, nil
	}
	if handle.openCount != 0 {
		orphan.OpenCount = handle.openCount
		s.orphans[inode] = orphan
		return &pb.Empty{}, s.commitOrphansLocked("orphan release")
	}
	delete(s.handles, inode)
	closeRootFSHandleFiles(handle)
	delete(s.orphans, inode)
	if err := s.commitOrphansLocked("final orphan release"); err != nil {
		return nil, err
	}
	path := rootFSOrphanPath(inode)
	if err := os.Remove(s.hostPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, mapRootFSBackedError(err)
	}
	s.dropPathLocked(path)
	return &pb.Empty{}, nil
}

func (s *rootFSBackedSession) retainUntrackedOrphanLocked(inode uint64) error {
	identity, ok := s.identityByInode[inode]
	if !ok {
		return fmt.Errorf("untracked rootfs orphan inode %d has no identity", inode)
	}
	info, err := os.Lstat(s.hostPath(rootFSOrphanPath(inode)))
	if err != nil {
		return err
	}
	s.orphans[inode] = rootFSOrphanRecord{Inode: inode, Identity: identity, Directory: info.IsDir(), Retain: true}
	return nil
}

func (s *rootFSBackedSession) pathMatchesIdentityLocked(path string, inode uint64, identity rootFSHostIdentity) bool {
	return s.inodeByPath[path] == inode && s.identityByInode[inode] == identity
}

func (s *rootFSBackedSession) hasHandlesForInodeLocked(inode uint64) bool {
	handle := s.handles[inode]
	return handle != nil && handle.openCount != 0
}

func (s *rootFSBackedSession) countPathsForInodeLocked(inode uint64) int {
	return len(s.pathsForInodeLocked(inode))
}

func (s *rootFSBackedSession) otherVisiblePathLocked(inode uint64, excluded string) string {
	for _, path := range s.pathsForInodeLocked(inode) {
		if path != excluded && !isRootFSInternalPath(path) {
			return path
		}
	}
	return ""
}

func (s *rootFSBackedSession) pathsForInodeLocked(inode uint64) []string {
	var paths []string
	for path, mapped := range s.inodeByPath {
		if mapped == inode {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths
}

func (s *rootFSBackedSession) dropPathLocked(rel string) {
	inode, ok := s.inodeByPath[rel]
	if !ok || inode == s0fs.RootInode {
		return
	}
	delete(s.inodeByPath, rel)
	if s.pathByInode[inode] != rel {
		return
	}
	paths := s.pathsForInodeLocked(inode)
	if len(paths) == 0 {
		delete(s.pathByInode, inode)
		delete(s.identityByInode, inode)
		return
	}
	s.pathByInode[inode] = preferredRootFSPath(paths)
}

func (s *rootFSBackedSession) dropPathTreeLocked(rel string) {
	var paths []string
	for path := range s.inodeByPath {
		if path == rel || strings.HasPrefix(path, rel+"/") {
			paths = append(paths, path)
		}
	}
	for _, path := range paths {
		s.dropPathLocked(path)
	}
}

func (s *rootFSBackedSession) renamePathTreeLocked(oldRel, newRel string) {
	for path, inode := range s.inodeByPath {
		if path != oldRel && !strings.HasPrefix(path, oldRel+"/") {
			continue
		}
		next := newRel + strings.TrimPrefix(path, oldRel)
		delete(s.inodeByPath, path)
		s.inodeByPath[next] = inode
		if s.pathByInode[inode] == path {
			s.pathByInode[inode] = next
		}
	}
}

func (s *rootFSBackedSession) stateDirectoryPath() string {
	return filepath.Join(s.root, rootFSStateDirectoryName)
}

func (s *rootFSBackedSession) stateFilePath() string {
	return filepath.Join(s.stateDirectoryPath(), rootFSStateFileName)
}

func (s *rootFSBackedSession) orphanDirectoryPath() string {
	return filepath.Join(s.stateDirectoryPath(), rootFSOrphanDirectory)
}

func (s *rootFSBackedSession) resendLedgerPath() string {
	return filepath.Join(s.stateDirectoryPath(), rootFSResendLedgerFileName)
}

func rootFSOrphanPath(inode uint64) string {
	return filepath.ToSlash(filepath.Join(rootFSStateDirectoryName, rootFSOrphanDirectory, strconv.FormatUint(inode, 10)))
}

func isRootFSInternalPath(rel string) bool {
	return rel == rootFSStateDirectoryName || strings.HasPrefix(rel, rootFSStateDirectoryName+"/")
}

func preferredRootFSPath(paths []string) string {
	for _, path := range paths {
		if !isRootFSInternalPath(path) {
			return path
		}
	}
	if len(paths) != 0 {
		return paths[0]
	}
	return ""
}

func rootFSIdentityFromInfo(info os.FileInfo) (rootFSHostIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return rootFSHostIdentity{}, fmt.Errorf("rootfs backing filesystem did not return stat identity")
	}
	return rootFSHostIdentity{Device: uint64(stat.Dev), Inode: stat.Ino, Mode: stat.Mode & syscall.S_IFMT}, nil
}

func renameRootFSNoReplace(oldPath, newPath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	if !errors.Is(err, syscall.ENOSYS) && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	if _, statErr := os.Lstat(newPath); !errors.Is(statErr, os.ErrNotExist) {
		if statErr == nil {
			return os.ErrExist
		}
		return statErr
	}
	return os.Rename(oldPath, newPath)
}

func ensurePrivateDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a private directory", path)
	}
	return os.Chmod(path, 0o700)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func cacheRootFSFile(handle *rootFSOpenHandle, file *os.File, flags int) {
	if file == nil || handle.directory {
		if file != nil {
			_ = file.Close()
		}
		return
	}
	used := false
	switch flags & unix.O_ACCMODE {
	case os.O_WRONLY:
		if handle.writeFile == nil {
			handle.writeFile = file
			used = true
		}
	case os.O_RDWR:
		if handle.readFile == nil {
			handle.readFile = file
			used = true
		}
		if handle.writeFile == nil {
			handle.writeFile = file
			used = true
		}
	default:
		if handle.readFile == nil {
			handle.readFile = file
			used = true
		}
	}
	if !used {
		_ = file.Close()
	}
}

func closeRootFSHandleFiles(handle *rootFSOpenHandle) {
	if handle == nil {
		return
	}
	if handle.readFile != nil {
		_ = handle.readFile.Close()
	}
	if handle.writeFile != nil && handle.writeFile != handle.readFile {
		_ = handle.writeFile.Close()
	}
	handle.readFile = nil
	handle.writeFile = nil
}
