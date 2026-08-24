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

package rootfsimportworker

import (
	"context"
	"fmt"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

// DurableBuilderConfig composes the privileged local importer with a fenced
// PostgreSQL pre-PUT journal and a conditional immutable object publisher.
type DurableBuilderConfig struct {
	Store      Store
	Unpacker   rootfsimporter.OCIUnpacker
	Filesystem rootfsimporter.FilesystemImageBuilder
	Publisher  rootfsblock.ImmutableObjectPublisher
	WorkRoot   string
	ProcdPath  string
}

// DurableBuilder converts a leased operation without owning lease renewal or
// the final ready CAS.
type DurableBuilder struct {
	store      Store
	unpacker   rootfsimporter.OCIUnpacker
	filesystem rootfsimporter.FilesystemImageBuilder
	publisher  rootfsblock.ImmutableObjectPublisher
	workRoot   string
	procdPath  string
	build      func(context.Context, rootfsimporter.BlockBuilder, rootfsimporter.BuildRequest) (rootfsimporter.BuildResult, error)
}

// NewDurableBuilder creates the production operation builder.
func NewDurableBuilder(config DurableBuilderConfig) (*DurableBuilder, error) {
	if config.Store == nil || config.Unpacker == nil || config.Filesystem == nil || config.Publisher == nil {
		return nil, fmt.Errorf("rootfs import store, unpacker, filesystem builder, and publisher are required")
	}
	if config.WorkRoot == "" || config.ProcdPath == "" {
		return nil, fmt.Errorf("rootfs import work root and procd path are required")
	}
	return &DurableBuilder{
		store: config.Store, unpacker: config.Unpacker, filesystem: config.Filesystem,
		publisher: config.Publisher, workRoot: config.WorkRoot, procdPath: config.ProcdPath,
		build: func(ctx context.Context, builder rootfsimporter.BlockBuilder, request rootfsimporter.BuildRequest) (rootfsimporter.BuildResult, error) {
			return builder.Build(ctx, request)
		},
	}, nil
}

// Build implements OperationBuilder.
func (b *DurableBuilder) Build(
	ctx context.Context,
	operation *sandboxstore.RootFSImportOperation,
	lease sandboxstore.RootFSImportLease,
) (rootfsimporter.BuildResult, error) {
	if b == nil || b.store == nil || b.unpacker == nil || b.filesystem == nil || b.publisher == nil || b.build == nil {
		return rootfsimporter.BuildResult{}, fmt.Errorf("durable rootfs import builder is not configured")
	}
	if operation == nil || operation.ID != lease.OperationID || operation.State != sandboxstore.RootFSImportStateBuilding {
		return rootfsimporter.BuildResult{}, fmt.Errorf("rootfs import operation does not match its lease")
	}
	spec, err := rootfsimporter.NormalizeOperationSpec(operation.Spec)
	if err != nil || spec != operation.Spec {
		return rootfsimporter.BuildResult{}, fmt.Errorf("rootfs import operation has a non-canonical durable specification")
	}
	sourceDigest, err := rootfsimporter.PinnedSourceDigest(spec.SourceOCIRef)
	if err != nil || sourceDigest.String() != operation.SourceOCIDigest {
		return rootfsimporter.BuildResult{}, fmt.Errorf("rootfs import operation source digest does not match its immutable reference")
	}
	procdDigest, err := digest.Parse(spec.ProcdDigest)
	if err != nil {
		return rootfsimporter.BuildResult{}, fmt.Errorf("parse rootfs import procd digest: %w", err)
	}
	journal := publicationJournal{store: b.store, lease: lease}
	blockBuilder := rootfsimporter.BlockBuilder{
		Unpacker: b.unpacker, Filesystem: b.filesystem,
		Publisher: rootfsimporter.JournaledPublisher{
			OperationID: operation.ID, Journal: journal, Publisher: b.publisher,
		},
	}
	return b.build(ctx, blockBuilder, rootfsimporter.BuildRequest{
		Image: ocirootfs.Request{
			Reference: spec.SourceOCIRef,
			Platform: ocispec.Platform{
				OS: spec.Platform.OS, Architecture: spec.Platform.Architecture, Variant: spec.Platform.Variant,
			},
			WorkRoot: b.workRoot, ProcdPath: b.procdPath, ExpectedProcdDigest: procdDigest,
		},
		LogicalSizeBytes: spec.LogicalSizeBytes,
		BlockOptions:     spec.BlockOptions,
	})
}

type publicationJournal struct {
	store Store
	lease sandboxstore.RootFSImportLease
}

func (j publicationJournal) PrepareObject(
	ctx context.Context,
	operationID string,
	reference rootfsblock.ObjectReference,
) error {
	if j.store == nil || operationID != j.lease.OperationID {
		return fmt.Errorf("rootfs import publication journal operation does not match its lease")
	}
	return j.store.PrepareRootFSImportObject(ctx, j.lease, reference)
}

func (j publicationJournal) MarkObjectPublished(
	ctx context.Context,
	operationID string,
	reference rootfsblock.ObjectReference,
) error {
	if j.store == nil || operationID != j.lease.OperationID {
		return fmt.Errorf("rootfs import publication journal operation does not match its lease")
	}
	return j.store.MarkRootFSImportObjectPublished(ctx, j.lease, reference)
}

var _ OperationBuilder = (*DurableBuilder)(nil)
var _ rootfsimporter.ObjectPublicationJournal = publicationJournal{}
