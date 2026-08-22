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

// Package rootfsimporter builds immutable block artifacts from verified OCI
// images without owning regional operation or ready-artifact state.
package rootfsimporter

import (
	"context"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// OCIUnpacker produces one operation-owned, verified OCI root directory.
// BlockBuilder takes ownership of RootPath after a successful call.
type OCIUnpacker interface {
	Import(context.Context, ocirootfs.Request) (ocirootfs.Result, error)
}

// FilesystemImageBuilder creates the immutable filesystem image consumed by
// the block-map builder.
type FilesystemImageBuilder interface {
	Build(context.Context, string, string, int64) error
}

// BlockBuilder turns a verified OCI root into immutable block objects. It
// deliberately stops before durable operation journaling and PostgreSQL ready
// artifact registration; those are regional control-plane responsibilities.
type BlockBuilder struct {
	Unpacker   OCIUnpacker
	Filesystem FilesystemImageBuilder
	Publisher  rootfsblock.ImmutableObjectPublisher
}

// BuildRequest binds filesystem construction and object publication
// to one already digest-pinned OCI import request.
type BuildRequest struct {
	Image            ocirootfs.Request
	LogicalSizeBytes int64
	BlockOptions     rootfsblock.BuildOptions
}

// BuildResult contains the non-secret attestation and complete object
// inventory required by a later immutable ready-artifact transaction.
type BuildResult struct {
	SourceOCIRef     string
	SourceOCIDigest  digest.Digest
	ManifestDigest   digest.Digest
	ConfigDigest     digest.Digest
	Platform         ocispec.Platform
	LayerDigests     []digest.Digest
	DiffIDs          []digest.Digest
	ProcdDigest      digest.Digest
	UnpackedBytes    int64
	Files            int
	LogicalSizeBytes int64
	// DescriptorDigest identifies the block descriptor only. The final ready
	// artifact identity must additionally bind platform and procd compatibility.
	DescriptorDigest digest.Digest
	BaseBlockRoot    digest.Digest
	Descriptor       rootfsblock.Descriptor
	DescriptorBytes  []byte
	Objects          int
	Bytes            int64
	References       []rootfsblock.ObjectReference
}
