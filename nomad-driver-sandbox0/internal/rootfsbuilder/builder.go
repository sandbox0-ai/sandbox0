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

package rootfsbuilder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// Options defines the inputs for one immutable base RootFS generation.
type Options struct {
	SourceRoot   string
	ImagePath    string
	LogicalSize  int64
	RootFSID     string
	ObjectPrefix string
	Runner       rootfsartifact.CommandRunner
}

// Build creates an XFS base artifact, publishes its block objects, and returns
// a durable GenerationDescriptor suitable for rootfssession.Manager.Ensure.
func Build(ctx context.Context, store objectstore.ConditionalStore, options Options) (rootfshandoff.GenerationDescriptor, error) {
	if store == nil {
		return rootfshandoff.GenerationDescriptor{}, fmt.Errorf("conditional object store is required")
	}
	options.RootFSID = strings.TrimSpace(options.RootFSID)
	options.ObjectPrefix = strings.Trim(strings.TrimSpace(options.ObjectPrefix), "/")
	if options.RootFSID == "" || options.ObjectPrefix == "" {
		return rootfshandoff.GenerationDescriptor{}, fmt.Errorf("rootfs id and object prefix are required")
	}
	if options.LogicalSize == 0 {
		options.LogicalSize = rootfsartifact.MinimumLogicalSizeBytes
	}
	sourceDigest, err := DigestDirectory(options.SourceRoot)
	if err != nil {
		return rootfshandoff.GenerationDescriptor{}, err
	}
	if err := (rootfsartifact.XFSBuilder{Runner: options.Runner}).Build(
		ctx, options.SourceRoot, options.ImagePath, options.LogicalSize,
	); err != nil {
		return rootfshandoff.GenerationDescriptor{}, err
	}
	image, err := os.Open(options.ImagePath)
	if err != nil {
		return rootfshandoff.GenerationDescriptor{}, fmt.Errorf("open XFS artifact: %w", err)
	}
	defer image.Close()
	result, err := rootfsblock.BuildMaterializedGeneration(ctx, image, options.LogicalSize,
		rootfsblock.ObjectStorePublisher{Store: store},
		rootfsblock.BuildOptions{ObjectPrefix: options.ObjectPrefix},
	)
	if err != nil {
		return rootfshandoff.GenerationDescriptor{}, fmt.Errorf("publish block generation: %w", err)
	}
	blockRoot := result.Descriptor.MappingRoot.RootDigest
	artifactDigest := digest.FromBytes(result.Payload).String()
	generationID := "base-" + artifactDigest
	descriptor := rootfshandoff.GenerationDescriptor{
		Version:      rootfshandoff.GenerationDescriptorVersion,
		GenerationID: generationID, FilesystemID: options.RootFSID,
		SourceOCIDigest: sourceDigest, BaseArtifactDigest: artifactDigest,
		BaseBlockRoot: blockRoot, CurrentBlockHead: blockRoot,
		WriterEpoch: 0, FormatGeneration: 1, DurabilityState: rootfsblock.DurabilityS3,
		LocatorVersion: 1, Descriptor: result.Payload,
	}
	if err := descriptor.Validate(); err != nil {
		return rootfshandoff.GenerationDescriptor{}, fmt.Errorf("validate generated descriptor: %w", err)
	}
	return descriptor, nil
}

// DigestDirectory produces a deterministic source digest for a development artifact.
// Production image imports must replace this with the OCI manifest digest.
func DigestDirectory(root string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("source root must be absolute")
	}
	hasher := sha256.New()
	if err := digestDirectory(root, root, hasher); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func digestDirectory(root, current string, hasher io.Writer) error {
	entries, err := os.ReadDir(current)
	if err != nil {
		return fmt.Errorf("read %s: %w", current, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		path := filepath.Join(current, entry.Name())
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(hasher, "path=%s;mode=%s;size=%d", filepath.ToSlash(relative), info.Mode().String(), info.Size())
		switch {
		case entry.Type()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(hasher, ";symlink=%s", target)
		case entry.IsDir():
			if err := digestDirectory(root, path, hasher); err != nil {
				return err
			}
		case entry.Type().IsRegular():
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			fileHasher := sha256.New()
			_, copyErr := io.Copy(fileHasher, file)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				return fmt.Errorf("hash %s: %w", path, copyErr)
			}
			fmt.Fprintf(hasher, ";sha256=%s", hex.EncodeToString(fileHasher.Sum(nil)))
		default:
			return fmt.Errorf("unsupported source file type %s at %s", info.Mode(), path)
		}
		fmt.Fprintln(hasher)
	}
	return nil
}
