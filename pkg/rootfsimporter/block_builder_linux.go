//go:build linux

// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootfsimporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// Build imports, formats, verifies, and publishes one OCI base filesystem.
// Local unpacked and XFS data are removed on every return path. A successful
// result does not make the artifact ready until the caller durably records the
// object inventory and commits its regional PostgreSQL transaction.
func (b BlockBuilder) Build(
	ctx context.Context,
	request BuildRequest,
) (result BuildResult, resultErr error) {
	if b.Unpacker == nil || b.Filesystem == nil || b.Publisher == nil {
		return BuildResult{}, fmt.Errorf("OCI unpacker, filesystem builder, and immutable publisher are required")
	}
	if request.LogicalSizeBytes < rootfsartifact.MinimumLogicalSizeBytes ||
		request.LogicalSizeBytes > rootfsartifact.MaximumLogicalSizeBytes ||
		request.LogicalSizeBytes%rootfsblock.LogicalBlockSize != 0 {
		return BuildResult{}, fmt.Errorf(
			"logical size must be between %d and %d and aligned to %d bytes",
			rootfsartifact.MinimumLogicalSizeBytes, rootfsartifact.MaximumLogicalSizeBytes,
			rootfsblock.LogicalBlockSize,
		)
	}
	blockOptions, err := rootfsblock.NormalizeBuildOptions(request.BlockOptions)
	if err != nil {
		return BuildResult{}, fmt.Errorf("RootFS block build options: %w", err)
	}
	imported, err := b.Unpacker.Import(ctx, request.Image)
	if err != nil {
		return BuildResult{}, fmt.Errorf("import OCI root filesystem: %w", err)
	}
	rootPath, err := validateOwnedImportedRoot(request.Image.WorkRoot, imported.RootPath)
	if err != nil {
		return BuildResult{}, err
	}
	imagePath := rootPath + ".xfs"
	imageOwned := false
	defer func() {
		var imageErr error
		if imageOwned {
			imageErr = removeOwnedImage(imagePath)
		}
		cleanupErr := errors.Join(imageErr, os.RemoveAll(rootPath))
		if cleanupErr != nil {
			result = BuildResult{}
			resultErr = errors.Join(resultErr, fmt.Errorf("remove OCI block build staging: %w", cleanupErr))
		}
	}()
	if err := validateImportedEvidence(request.Image, imported); err != nil {
		return BuildResult{}, err
	}
	if _, err := os.Lstat(imagePath); err == nil {
		return BuildResult{}, fmt.Errorf("OCI block image path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return BuildResult{}, fmt.Errorf("inspect OCI block image path: %w", err)
	}
	imageOwned = true
	if err := b.Filesystem.Build(ctx, rootPath, imagePath, request.LogicalSizeBytes); err != nil {
		if errors.Is(err, rootfsartifact.ErrXFSImageStillMounted) {
			imageOwned = false
		}
		return BuildResult{}, fmt.Errorf("build XFS base image: %w", err)
	}
	image, err := openVerifiedFilesystemImage(imagePath, request.LogicalSizeBytes)
	if err != nil {
		return BuildResult{}, err
	}
	built, buildErr := rootfsblock.BuildMaterializedGeneration(
		ctx,
		image,
		request.LogicalSizeBytes,
		b.Publisher,
		blockOptions,
	)
	closeErr := image.Close()
	if err := errors.Join(buildErr, closeErr); err != nil {
		return BuildResult{}, fmt.Errorf("publish OCI block generation: %w", err)
	}
	descriptorDigest := digest.FromBytes(built.Payload)
	baseBlockRoot, err := digest.Parse(built.Descriptor.MappingRoot.RootDigest)
	if err != nil {
		return BuildResult{}, fmt.Errorf("parse generated base block root: %w", err)
	}
	result = BuildResult{
		SourceOCIRef: imported.Reference, SourceOCIDigest: imported.SourceDigest,
		ManifestDigest: imported.ManifestDigest, ConfigDigest: imported.ConfigDigest,
		Platform:         imported.Platform,
		LayerDigests:     append([]digest.Digest(nil), imported.LayerDigests...),
		DiffIDs:          append([]digest.Digest(nil), imported.DiffIDs...),
		ProcdDigest:      imported.ProcdDigest,
		UnpackedBytes:    imported.UnpackedBytes,
		Files:            imported.Files,
		LogicalSizeBytes: request.LogicalSizeBytes,
		DescriptorDigest: descriptorDigest, BaseBlockRoot: baseBlockRoot,
		Descriptor: built.Descriptor, DescriptorBytes: append([]byte(nil), built.Payload...),
		Objects: built.Objects, Bytes: built.Bytes,
		References: append([]rootfsblock.ObjectReference(nil), built.References...),
	}
	if err := result.Validate(); err != nil {
		return BuildResult{}, err
	}
	return result, nil
}

func validateOwnedImportedRoot(workRoot, importedRoot string) (string, error) {
	cleanWorkRoot := filepath.Clean(strings.TrimSpace(workRoot))
	cleanImportedRoot := filepath.Clean(strings.TrimSpace(importedRoot))
	if workRoot != cleanWorkRoot || importedRoot != cleanImportedRoot ||
		!filepath.IsAbs(cleanWorkRoot) || !filepath.IsAbs(cleanImportedRoot) ||
		filepath.Dir(cleanImportedRoot) != cleanWorkRoot ||
		!strings.HasPrefix(filepath.Base(cleanImportedRoot), "oci-rootfs-") ||
		len(filepath.Base(cleanImportedRoot)) == len("oci-rootfs-") {
		return "", fmt.Errorf("OCI importer returned a root outside its operation work directory")
	}
	info, err := os.Lstat(cleanImportedRoot)
	if err != nil {
		return "", fmt.Errorf("stat imported OCI root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return "", fmt.Errorf("imported OCI root must be a mode 0700 directory without symlinks")
	}
	return cleanImportedRoot, nil
}

func validateImportedEvidence(request ocirootfs.Request, result ocirootfs.Result) error {
	expectedSource, err := pinnedSourceDigest(request.Reference)
	if err != nil {
		return err
	}
	if result.SourceDigest != expectedSource {
		return fmt.Errorf("imported OCI source digest %s does not match request %s", result.SourceDigest, expectedSource)
	}
	resultSource, err := pinnedSourceDigest(result.Reference)
	if err != nil || resultSource != expectedSource {
		return fmt.Errorf("imported OCI reference does not bind the requested source digest")
	}
	expectedPlatform := platforms.Normalize(request.Platform)
	if request.Platform.OS != "linux" || request.Platform.OS != expectedPlatform.OS ||
		request.Platform.Architecture != expectedPlatform.Architecture ||
		request.Platform.OSVersion != "" || len(request.Platform.OSFeatures) != 0 {
		return fmt.Errorf("requested OCI platform is not canonical Linux")
	}
	if result.Platform.OS != expectedPlatform.OS || result.Platform.Architecture != expectedPlatform.Architecture ||
		result.Platform.Variant != expectedPlatform.Variant || result.Platform.OSVersion != "" || len(result.Platform.OSFeatures) != 0 {
		return fmt.Errorf("imported OCI platform does not match the requested platform")
	}
	if result.ProcdDigest != request.ExpectedProcdDigest {
		return fmt.Errorf("imported procd digest %s does not match request %s", result.ProcdDigest, request.ExpectedProcdDigest)
	}
	for _, item := range []struct {
		name  string
		value digest.Digest
	}{
		{name: "source", value: result.SourceDigest},
		{name: "manifest", value: result.ManifestDigest},
		{name: "config", value: result.ConfigDigest},
		{name: "procd", value: result.ProcdDigest},
	} {
		if err := validateArtifactSHA256Digest(item.value); err != nil {
			return fmt.Errorf("imported %s digest: %w", item.name, err)
		}
	}
	if len(result.LayerDigests) == 0 || len(result.LayerDigests) != len(result.DiffIDs) {
		return fmt.Errorf("imported OCI layer and DiffID evidence is incomplete")
	}
	for index := range result.LayerDigests {
		if err := validateArtifactSHA256Digest(result.LayerDigests[index]); err != nil {
			return fmt.Errorf("imported layer %d digest: %w", index, err)
		}
		if err := validateArtifactSHA256Digest(result.DiffIDs[index]); err != nil {
			return fmt.Errorf("imported layer %d DiffID: %w", index, err)
		}
	}
	if result.UnpackedBytes < 0 || result.Files < 0 {
		return fmt.Errorf("imported OCI counters must not be negative")
	}
	return nil
}

func openVerifiedFilesystemImage(path string, logicalSize int64) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open XFS base image: %w", err)
	}
	image := os.NewFile(uintptr(fd), path)
	if image == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("wrap XFS base image descriptor")
	}
	info, err := image.Stat()
	if err != nil {
		_ = image.Close()
		return nil, fmt.Errorf("stat XFS base image: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
		info.Size() != logicalSize || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
		_ = image.Close()
		return nil, fmt.Errorf("XFS base image metadata does not match the immutable build contract")
	}
	return image, nil
}

func removeOwnedImage(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
