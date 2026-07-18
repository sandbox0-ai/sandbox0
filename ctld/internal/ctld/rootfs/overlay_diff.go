package rootfs

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/sysx"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"golang.org/x/sys/unix"
)

var defaultRootFSSnapshotExcludedPaths = []string{
	"/procd",
}

func (r *ContainerdRuntime) createOverlayUpperDiff(ctx context.Context, client containerdClient, info ctldapi.RootFSInfo, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, bool, error) {
	if strings.TrimSpace(info.Snapshotter) != "overlayfs" {
		return ctldapi.RootFSDiffDescriptor{}, nil, false, nil
	}
	upperdir, err := r.activeOverlayUpperdir(ctx, client, info)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, true, err
	}
	desc, reader, err := writeOverlayUpperDiff(ctx, upperdir, excludedPaths, portalPaths)
	return desc, reader, true, err
}

func (r *ContainerdRuntime) activeOverlayUpperdir(ctx context.Context, client containerdClient, info ctldapi.RootFSInfo) (string, error) {
	if strings.TrimSpace(info.Snapshotter) != "overlayfs" {
		return "", fmt.Errorf("%w: rootfs baseline requires overlayfs snapshotter", ErrBadRequest)
	}
	snapshotter := client.SnapshotService(info.Snapshotter)
	if snapshotter == nil {
		return "", fmt.Errorf("overlayfs snapshotter is not configured")
	}
	snapshotInfo, err := snapshotter.Stat(ctx, info.SnapshotKey)
	if err != nil {
		return "", fmt.Errorf("inspect overlayfs snapshot: %w", err)
	}
	if snapshotInfo.Kind != snapshots.KindActive {
		return "", fmt.Errorf("%w: rootfs snapshot %s is not active", ErrBadRequest, info.SnapshotKey)
	}
	mounts, err := snapshotter.Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return "", fmt.Errorf("inspect overlayfs mounts: %w", err)
	}
	upperdir, ok := overlayUpperdir(mounts)
	if !ok {
		return "", fmt.Errorf("%w: overlayfs upperdir is not available", ErrBadRequest)
	}
	mountedUpperdir, err := r.mountedContainerdDataPath(upperdir)
	if err != nil {
		return "", err
	}
	return mountedUpperdir, nil
}

func overlayUpperdir(mounts []mount.Mount) (string, bool) {
	for _, m := range mounts {
		if m.Type != "overlay" && m.Type != "fuse-overlayfs" {
			continue
		}
		for _, option := range m.Options {
			if strings.HasPrefix(option, "upperdir=") {
				upperdir := strings.TrimSpace(strings.TrimPrefix(option, "upperdir="))
				return upperdir, upperdir != ""
			}
		}
	}
	return "", false
}

func (r *ContainerdRuntime) mountedContainerdDataPath(hostPath string) (string, error) {
	hostPath = filepath.Clean(strings.TrimSpace(hostPath))
	if hostPath == "." || hostPath == "" {
		return "", fmt.Errorf("overlayfs upperdir is empty")
	}

	candidates := []string{hostPath}
	if r != nil {
		if mapped, ok := rebasePath(hostPath, r.containerdHostDataRoot, r.containerdDataRoot); ok {
			candidates = append([]string{mapped}, candidates...)
		}
	}
	seen := make(map[string]struct{}, len(candidates))
	var lastErr error
	for _, candidate := range candidates {
		candidate = filepath.Clean(candidate)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		st, err := os.Stat(candidate)
		if err == nil && st.IsDir() {
			return candidate, nil
		}
		if err == nil {
			lastErr = fmt.Errorf("%s is not a directory", candidate)
			continue
		}
		lastErr = err
	}
	return "", fmt.Errorf("overlayfs upperdir %s is not readable from ctld: %w", hostPath, lastErr)
}

func rebasePath(path, fromRoot, toRoot string) (string, bool) {
	path = filepath.Clean(strings.TrimSpace(path))
	fromRoot = filepath.Clean(strings.TrimSpace(fromRoot))
	toRoot = filepath.Clean(strings.TrimSpace(toRoot))
	if path == "" || fromRoot == "" || toRoot == "" || fromRoot == "." || toRoot == "." {
		return "", false
	}
	if path != fromRoot && !strings.HasPrefix(path, fromRoot+string(filepath.Separator)) {
		return "", false
	}
	rel, err := filepath.Rel(fromRoot, path)
	if err != nil {
		return "", false
	}
	return filepath.Join(toRoot, rel), true
}

func writeOverlayUpperDiff(ctx context.Context, upperdir string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	filter := newRootFSPathFilter(rootFSExcludedPathsWithPortals(excludedPaths, portalPaths))
	desc, reader, err := writeOverlayDiffTar(upperdir, filter, func(changeFn fs.ChangeFunc) error {
		return walkOverlayUpper(ctx, upperdir, filter, changeFn)
	})
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return appendPortalRootFSToDiff(desc, reader, portalPaths)
}

func writeOverlayUpperDiffFromBaseline(ctx context.Context, baselineDir, upperdir string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	filter := newRootFSPathFilter(rootFSExcludedPathsWithPortals(excludedPaths, portalPaths))
	desc, reader, err := writeOverlayDiffTar(upperdir, filter, func(changeFn fs.ChangeFunc) error {
		return fs.Changes(ctx, baselineDir, upperdir, func(kind fs.ChangeKind, path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if kind == fs.ChangeKindDelete {
				if filter.Excludes(path) {
					return nil
				}
				return changeFn(kind, path, nil, nil)
			}
			if filter.Excludes(path) {
				return nil
			}
			if isOverlayWhiteout(info) {
				return changeFn(fs.ChangeKindDelete, path, nil, nil)
			}
			if info != nil && info.IsDir() {
				sourcePath := filepath.Join(upperdir, strings.TrimPrefix(path, string(filepath.Separator)))
				opaque, err := isOverlayOpaqueDir(sourcePath)
				if err != nil {
					return err
				}
				if opaque {
					if err := changeFn(fs.ChangeKindDelete, filepath.Join(path, ".wh..opq"), nil, nil); err != nil {
						return err
					}
				}
			}
			return changeFn(kind, path, info, nil)
		})
	})
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return appendPortalRootFSToDiff(desc, reader, portalPaths)
}

func writeOverlayDiffTar(source string, filter rootFSPathFilter, walkChanges func(fs.ChangeFunc) error) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	tmp, err := os.CreateTemp("", "sandbox0-rootfs-overlay-diff-*.tar")
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	digester := digest.Canonical.Digester()
	writer := io.MultiWriter(tmp, digester.Hash())
	cw := archive.NewChangeWriter(writer, source)
	if err := walkChanges(filter.ChangeFunc(cw.HandleChange)); err != nil {
		_ = cw.Close()
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	if err := cw.Close(); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	stat, err := tmp.Stat()
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}

	removeOnError = false
	diffID := digester.Digest().String()
	return ctldapi.RootFSDiffDescriptor{
		MediaType: ocispec.MediaTypeImageLayer,
		Digest:    diffID,
		DiffID:    diffID,
		Size:      stat.Size(),
	}, removeOnCloseFile{File: tmp}, nil
}

func walkOverlayUpper(ctx context.Context, upperdir string, filter rootFSPathFilter, changeFn fs.ChangeFunc) error {
	return filepath.Walk(upperdir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(upperdir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		changePath := string(filepath.Separator) + rel
		if filter.Excludes(changePath) {
			if info != nil && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if isOverlayWhiteout(info) {
			return changeFn(fs.ChangeKindDelete, changePath, nil, nil)
		}
		if info.IsDir() {
			opaque, err := isOverlayOpaqueDir(path)
			if err != nil {
				return err
			}
			if opaque {
				if err := changeFn(fs.ChangeKindDelete, filepath.Join(changePath, ".wh..opq"), nil, nil); err != nil {
					return err
				}
			}
		}
		return changeFn(fs.ChangeKindAdd, changePath, info, nil)
	})
}

func isOverlayWhiteout(info os.FileInfo) bool {
	if info == nil || info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	dev := uint64(stat.Rdev)
	return unix.Major(dev) == 0 && unix.Minor(dev) == 0
}

func isOverlayOpaqueDir(path string) (bool, error) {
	for _, attr := range []string{"trusted.overlay.opaque", "user.overlay.opaque"} {
		value, err := sysx.LGetxattr(path, attr)
		if err == nil {
			return len(value) == 1 && value[0] == 'y', nil
		}
		if err == unix.ENODATA || err == unix.ENOTSUP || err == unix.EOPNOTSUPP {
			continue
		}
		return false, fmt.Errorf("read overlay opaque xattr %s on %s: %w", attr, path, err)
	}
	return false, nil
}

type rootFSPathFilter struct {
	excluded       []string
	opaquePreserve []string
}

func newRootFSPathFilter(extraPaths []string) rootFSPathFilter {
	seen := make(map[string]struct{}, len(defaultRootFSSnapshotExcludedPaths)+len(extraPaths))
	excluded := make([]string, 0, len(defaultRootFSSnapshotExcludedPaths)+len(extraPaths))
	var opaquePreserve []string
	add := func(value string, preserveOpaque bool) {
		clean := cleanRootFSPath(value)
		if clean == "/" {
			return
		}
		if _, ok := seen[clean]; !ok {
			seen[clean] = struct{}{}
			excluded = append(excluded, clean)
		}
		if preserveOpaque {
			opaquePreserve = append(opaquePreserve, clean)
		}
	}
	for _, value := range defaultRootFSSnapshotExcludedPaths {
		add(value, false)
	}
	for _, value := range extraPaths {
		add(value, true)
	}
	return rootFSPathFilter{excluded: excluded, opaquePreserve: opaquePreserve}
}

func (f rootFSPathFilter) Excludes(changePath string) bool {
	clean := cleanRootFSPath(changePath)
	for _, excluded := range f.excluded {
		if clean == excluded || strings.HasPrefix(clean, excluded+"/") {
			return true
		}
	}
	return false
}

func (f rootFSPathFilter) ExcludesTarHeader(headerName string) bool {
	if f.Excludes(headerName) {
		return true
	}
	if target, opaque, ok := rootFSTarWhiteoutTargetPath(headerName); ok {
		if opaque {
			return f.AffectsOpaquePreservedPath(target)
		}
		return f.Excludes(target)
	}
	return false
}

func (f rootFSPathFilter) ChangeFunc(next fs.ChangeFunc) fs.ChangeFunc {
	return func(kind fs.ChangeKind, changePath string, info os.FileInfo, err error) error {
		if err != nil {
			return next(kind, changePath, info, err)
		}
		if f.Excludes(changePath) {
			return nil
		}
		if target, opaque, ok := rootFSChangeWhiteoutTargetPath(changePath); ok {
			if opaque && f.AffectsOpaquePreservedPath(target) {
				return nil
			}
			if !opaque && f.Excludes(target) {
				return nil
			}
		}
		return next(kind, changePath, info, err)
	}
}

func (f rootFSPathFilter) AffectsOpaquePreservedPath(dir string) bool {
	clean := cleanRootFSPath(dir)
	for _, preserved := range f.opaquePreserve {
		if clean == preserved || strings.HasPrefix(preserved, clean+"/") {
			return true
		}
	}
	return false
}

func shouldFilterRootFSDiffTar(desc ctldapi.RootFSDiffDescriptor) bool {
	mediaType := strings.TrimSpace(desc.MediaType)
	return mediaType == "" || mediaType == ocispec.MediaTypeImageLayer
}

func filterRootFSDiffTar(desc ctldapi.RootFSDiffDescriptor, reader io.Reader, excludedPaths []string) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	if !shouldFilterRootFSDiffTar(desc) {
		return desc, noopReadSeekCloser{Reader: reader}, nil
	}
	filter := newRootFSPathFilter(excludedPaths)

	tmp, err := os.CreateTemp("", "sandbox0-rootfs-filtered-diff-*.tar")
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	digester := digest.Canonical.Digester()
	writer := io.MultiWriter(tmp, digester.Hash())
	tarWriter := tar.NewWriter(writer)
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		if filter.ExcludesTarHeader(header.Name) {
			continue
		}

		headerCopy := *header
		if err := tarWriter.WriteHeader(&headerCopy); err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		if header.Size > 0 {
			if _, err := io.Copy(tarWriter, tarReader); err != nil {
				_ = tarWriter.Close()
				return ctldapi.RootFSDiffDescriptor{}, nil, err
			}
		}
	}
	if err := tarWriter.Close(); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	stat, err := tmp.Stat()
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}

	desc.Digest = digester.Digest().String()
	desc.DiffID = desc.Digest
	desc.Size = stat.Size()
	if strings.TrimSpace(desc.MediaType) == "" {
		desc.MediaType = ocispec.MediaTypeImageLayer
	}
	removeOnError = false
	return desc, removeOnCloseFile{File: tmp}, nil
}

func filterRootFSDiffTarForSave(desc ctldapi.RootFSDiffDescriptor, reader io.Reader, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	filteredDesc, filteredReader, err := filterRootFSDiffTar(desc, reader, rootFSExcludedPathsWithPortals(excludedPaths, portalPaths))
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return appendPortalRootFSToDiff(filteredDesc, filteredReader, portalPaths)
}

func filterRootFSDiffTarForApply(desc ctldapi.RootFSDiffDescriptor, reader io.Reader, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	portalPaths = filterRootFSPortalPaths(portalPaths, excludedPaths)
	if !shouldFilterRootFSDiffTar(desc) {
		return desc, noopReadSeekCloser{Reader: reader}, nil
	}
	filter := newRootFSPathFilter(rootFSExcludedPathsWithPortals(excludedPaths, portalPaths))

	tmp, err := os.CreateTemp("", "sandbox0-rootfs-apply-filtered-diff-*.tar")
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	digester := digest.Canonical.Digester()
	writer := io.MultiWriter(tmp, digester.Hash())
	tarWriter := tar.NewWriter(writer)
	tarReader := tar.NewReader(reader)
	restored := make(map[string]struct{}, len(portalPaths))
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		if portal, rel, ok := matchRootFSPortalHeader(header.Name, portalPaths); ok {
			if err := restoreRootFSPortalHeader(tarReader, header, portal, rel, restored); err != nil {
				_ = tarWriter.Close()
				return ctldapi.RootFSDiffDescriptor{}, nil, err
			}
			continue
		}
		if filter.ExcludesTarHeader(header.Name) {
			continue
		}

		headerCopy := *header
		if err := tarWriter.WriteHeader(&headerCopy); err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		if header.Size > 0 {
			if _, err := io.Copy(tarWriter, tarReader); err != nil {
				_ = tarWriter.Close()
				return ctldapi.RootFSDiffDescriptor{}, nil, err
			}
		}
	}
	if err := tarWriter.Close(); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	stat, err := tmp.Stat()
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}

	desc.Digest = digester.Digest().String()
	desc.DiffID = desc.Digest
	desc.Size = stat.Size()
	if strings.TrimSpace(desc.MediaType) == "" {
		desc.MediaType = ocispec.MediaTypeImageLayer
	}
	removeOnError = false
	return desc, removeOnCloseFile{File: tmp}, nil
}

func appendPortalRootFSToDiff(desc ctldapi.RootFSDiffDescriptor, reader io.ReadSeekCloser, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	portalPaths = filterRootFSPortalPaths(portalPaths, nil)
	if len(portalPaths) == 0 {
		return desc, reader, nil
	}
	defer reader.Close()

	tmp, err := os.CreateTemp("", "sandbox0-rootfs-portal-diff-*.tar")
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	digester := digest.Canonical.Digester()
	writer := io.MultiWriter(tmp, digester.Hash())
	tarWriter := tar.NewWriter(writer)
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		headerCopy := *header
		if err := tarWriter.WriteHeader(&headerCopy); err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		if header.Size > 0 {
			if _, err := io.Copy(tarWriter, tarReader); err != nil {
				_ = tarWriter.Close()
				return ctldapi.RootFSDiffDescriptor{}, nil, err
			}
		}
	}
	for _, portal := range portalPaths {
		if err := writeRootFSPortalTree(tarWriter, portal); err != nil {
			_ = tarWriter.Close()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
	}
	if err := tarWriter.Close(); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	stat, err := tmp.Stat()
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}

	desc.Digest = digester.Digest().String()
	desc.DiffID = desc.Digest
	desc.Size = stat.Size()
	if strings.TrimSpace(desc.MediaType) == "" {
		desc.MediaType = ocispec.MediaTypeImageLayer
	}
	removeOnError = false
	return desc, removeOnCloseFile{File: tmp}, nil
}

func writeRootFSPortalTree(tw *tar.Writer, portal ctldapi.RootFSPortalPath) error {
	mountPath := cleanRootFSPath(portal.MountPath)
	backingPath := filepath.Clean(strings.TrimSpace(portal.BackingPath))
	if mountPath == "/" || backingPath == "" || backingPath == "." {
		return nil
	}
	if err := os.MkdirAll(backingPath, 0o755); err != nil {
		return fmt.Errorf("create portal backing dir %s: %w", backingPath, err)
	}
	return filepath.WalkDir(backingPath, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(backingPath, current)
		if err != nil {
			return err
		}
		name := rootFSPortalTarName(mountPath, rel, info.IsDir())
		var linkTarget string
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(current)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return err
		}
		header.Name = name
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(current)
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
}

func restoreRootFSPortalHeader(reader io.Reader, header *tar.Header, portal ctldapi.RootFSPortalPath, rel string, restored map[string]struct{}) error {
	backingPath := filepath.Clean(strings.TrimSpace(portal.BackingPath))
	if backingPath == "" || backingPath == "." {
		return nil
	}
	if _, ok := restored[backingPath]; !ok {
		if err := os.RemoveAll(backingPath); err != nil {
			return fmt.Errorf("clear portal backing dir %s: %w", backingPath, err)
		}
		if err := os.MkdirAll(backingPath, 0o755); err != nil {
			return fmt.Errorf("create portal backing dir %s: %w", backingPath, err)
		}
		restored[backingPath] = struct{}{}
	}
	target, err := rootFSPortalRestorePath(backingPath, rel)
	if err != nil {
		return err
	}
	mode := os.FileMode(header.Mode)
	switch header.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, mode); err != nil {
			return err
		}
		return applyRootFSTarMetadata(target, header, false)
	case tar.TypeReg, tar.TypeRegA:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(file, reader)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return applyRootFSTarMetadata(target, header, false)
	case tar.TypeSymlink:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.RemoveAll(target); err != nil {
			return err
		}
		if err := os.Symlink(header.Linkname, target); err != nil {
			return err
		}
		return applyRootFSTarMetadata(target, header, true)
	case tar.TypeLink:
		linkPortal, linkRel, ok := matchRootFSPortalHeader(header.Linkname, []ctldapi.RootFSPortalPath{portal})
		if !ok {
			return nil
		}
		linkTarget, err := rootFSPortalRestorePath(linkPortal.BackingPath, linkRel)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.RemoveAll(target); err != nil {
			return err
		}
		if err := os.Link(linkTarget, target); err != nil {
			return err
		}
		return applyRootFSTarMetadata(target, header, false)
	case tar.TypeFifo:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := unix.Mkfifo(target, uint32(mode)); err != nil && !os.IsExist(err) {
			return err
		}
		return applyRootFSTarMetadata(target, header, false)
	default:
		return nil
	}
}

func applyRootFSTarMetadata(target string, header *tar.Header, symlink bool) error {
	if header == nil {
		return nil
	}
	if symlink {
		_ = os.Lchown(target, header.Uid, header.Gid)
		return nil
	}
	_ = os.Chown(target, header.Uid, header.Gid)
	if header.Mode != 0 {
		_ = os.Chmod(target, os.FileMode(header.Mode))
	}
	mtime := header.ModTime
	if !mtime.IsZero() {
		_ = os.Chtimes(target, mtime, mtime)
	}
	return nil
}

func rootFSPortalRestorePath(backingPath, rel string) (string, error) {
	rel = filepath.Clean(filepath.FromSlash(strings.TrimPrefix(cleanRootFSPath(rel), "/")))
	if rel == "." {
		return backingPath, nil
	}
	target := filepath.Join(backingPath, rel)
	cleanBacking := filepath.Clean(backingPath)
	cleanTarget := filepath.Clean(target)
	if cleanTarget != cleanBacking && !strings.HasPrefix(cleanTarget, cleanBacking+string(filepath.Separator)) {
		return "", fmt.Errorf("portal restore path escapes backing dir")
	}
	return cleanTarget, nil
}

func rootFSPortalTarName(mountPath, rel string, isDir bool) string {
	mountPath = strings.TrimPrefix(cleanRootFSPath(mountPath), "/")
	rel = filepath.ToSlash(filepath.Clean(rel))
	if rel == "." || rel == "" {
		if isDir && !strings.HasSuffix(mountPath, "/") {
			return mountPath + "/"
		}
		return mountPath
	}
	name := path.Join(mountPath, rel)
	if isDir && !strings.HasSuffix(name, "/") {
		name += "/"
	}
	return name
}

func matchRootFSPortalHeader(headerName string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSPortalPath, string, bool) {
	clean := cleanRootFSPath(headerName)
	for _, portal := range portalPaths {
		mountPath := cleanRootFSPath(portal.MountPath)
		if mountPath == "/" {
			continue
		}
		if clean == mountPath {
			return portal, "/", true
		}
		if strings.HasPrefix(clean, mountPath+"/") {
			return portal, strings.TrimPrefix(clean, mountPath), true
		}
	}
	return ctldapi.RootFSPortalPath{}, "", false
}

func filterRootFSPortalPaths(paths []ctldapi.RootFSPortalPath, excludedPaths []string) []ctldapi.RootFSPortalPath {
	if len(paths) == 0 {
		return nil
	}
	filter := newRootFSPathFilter(excludedPaths)
	seen := make(map[string]struct{}, len(paths))
	out := make([]ctldapi.RootFSPortalPath, 0, len(paths))
	for _, portal := range paths {
		mountPath := cleanRootFSPath(portal.MountPath)
		backingPath := filepath.Clean(strings.TrimSpace(portal.BackingPath))
		if mountPath == "/" || backingPath == "" || backingPath == "." || filter.Excludes(mountPath) {
			continue
		}
		key := mountPath + "\x00" + backingPath
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		portal.MountPath = mountPath
		portal.BackingPath = backingPath
		out = append(out, portal)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MountPath == out[j].MountPath {
			return out[i].BackingPath < out[j].BackingPath
		}
		return out[i].MountPath < out[j].MountPath
	})
	return out
}

func rootFSExcludedPathsWithPortals(excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) []string {
	if len(portalPaths) == 0 {
		return excludedPaths
	}
	out := append([]string(nil), excludedPaths...)
	for _, portal := range filterRootFSPortalPaths(portalPaths, excludedPaths) {
		out = append(out, portal.MountPath)
	}
	return out
}

func (f rootFSPathFilter) RemoveAll(root string) error {
	for _, excluded := range f.excluded {
		rel := strings.TrimPrefix(excluded, "/")
		if rel == "" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
			return fmt.Errorf("remove excluded rootfs path %s: %w", excluded, err)
		}
	}
	return nil
}

func rootFSTarWhiteoutTargetPath(headerName string) (string, bool, bool) {
	clean := cleanRootFSPath(headerName)
	base := path.Base(clean)
	if base == ".wh..wh..opq" {
		return path.Dir(clean), true, true
	}
	return rootFSFileWhiteoutTargetPath(clean)
}

func rootFSChangeWhiteoutTargetPath(changePath string) (string, bool, bool) {
	clean := cleanRootFSPath(changePath)
	base := path.Base(clean)
	if base == ".wh..opq" {
		return path.Dir(clean), true, true
	}
	return rootFSFileWhiteoutTargetPath(clean)
}

func rootFSFileWhiteoutTargetPath(clean string) (string, bool, bool) {
	base := path.Base(clean)
	if !strings.HasPrefix(base, ".wh.") {
		return "", false, false
	}
	name := strings.TrimPrefix(base, ".wh.")
	if name == "" {
		return "", false, false
	}
	return path.Join(path.Dir(clean), name), false, true
}

func cleanRootFSPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	return path.Clean("/" + strings.TrimPrefix(value, "/"))
}

type removeOnCloseFile struct {
	*os.File
}

func (f removeOnCloseFile) Close() error {
	name := f.Name()
	err := f.File.Close()
	removeErr := os.Remove(name)
	if err != nil {
		return err
	}
	if removeErr != nil && !os.IsNotExist(removeErr) {
		return removeErr
	}
	return nil
}

type noopReadSeekCloser struct {
	io.Reader
}

func (noopReadSeekCloser) Seek(int64, int) (int64, error) {
	return 0, fmt.Errorf("seek is not supported")
}

func (noopReadSeekCloser) Close() error {
	return nil
}
