package rootfsimportworker

import (
	"context"
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

type unusedUnpacker struct{}

func (unusedUnpacker) Import(context.Context, ocirootfs.Request) (ocirootfs.Result, error) {
	panic("unexpected unpack")
}

type unusedFilesystem struct{}

func (unusedFilesystem) Build(context.Context, string, string, int64) error {
	panic("unexpected filesystem build")
}

type recordingPublisher struct{ calls int }

func (p *recordingPublisher) PutImmutable(context.Context, string, []byte) error {
	p.calls++
	return nil
}

func TestDurableBuilderDerivesExactRequestAndJournalsBeforePut(t *testing.T) {
	store := &fakeStore{}
	publisher := &recordingPublisher{}
	builder, err := NewDurableBuilder(DurableBuilderConfig{
		Store: store, Unpacker: unusedUnpacker{}, Filesystem: unusedFilesystem{}, Publisher: publisher,
		WorkRoot: "/var/lib/sandbox0/rootfs-import", ProcdPath: "/opt/sandbox0/bin/procd",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := testOperation(1)
	lease, err := operation.Lease()
	if err != nil {
		t.Fatal(err)
	}
	builder.build = func(
		ctx context.Context,
		blockBuilder rootfsimporter.BlockBuilder,
		request rootfsimporter.BuildRequest,
	) (rootfsimporter.BuildResult, error) {
		if request.Image.Reference != operation.Spec.SourceOCIRef ||
			request.Image.Platform.OS != "linux" || request.Image.Platform.Architecture != "amd64" ||
			request.Image.WorkRoot != "/var/lib/sandbox0/rootfs-import" ||
			request.Image.ProcdPath != "/opt/sandbox0/bin/procd" ||
			request.Image.ExpectedProcdDigest.String() != operation.Spec.ProcdDigest ||
			request.LogicalSizeBytes != operation.Spec.LogicalSizeBytes ||
			request.BlockOptions != operation.Spec.BlockOptions {
			t.Fatalf("unexpected build request: %#v", request)
		}
		payload := []byte("immutable-pack")
		checksum := digest.FromBytes(payload)
		key := operation.Spec.BlockOptions.ObjectPrefix + "/packs/sha256/" + checksum.Encoded()
		if err := blockBuilder.Publisher.PutImmutable(ctx, key, payload); err != nil {
			t.Fatal(err)
		}
		return rootfsimporter.BuildResult{}, nil
	}
	if _, err := builder.Build(context.Background(), operation, lease); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 || len(store.prepareRefs) != 1 || len(store.publishedRefs) != 1 {
		t.Fatalf("publisher=%d prepared=%d published=%d", publisher.calls, len(store.prepareRefs), len(store.publishedRefs))
	}
	prepared := store.prepareRefs[0]
	if prepared != store.publishedRefs[0] || prepared.Kind != rootfsblock.ObjectKindDataPack {
		t.Fatalf("unexpected journal references: %#v %#v", prepared, store.publishedRefs[0])
	}
}

func TestDurableBuilderRejectsOperationLeaseMismatch(t *testing.T) {
	store := &fakeStore{}
	builder, err := NewDurableBuilder(DurableBuilderConfig{
		Store: store, Unpacker: unusedUnpacker{}, Filesystem: unusedFilesystem{}, Publisher: &recordingPublisher{},
		WorkRoot: "/var/lib/sandbox0/rootfs-import", ProcdPath: "/opt/sandbox0/bin/procd",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := testOperation(1)
	lease, err := operation.Lease()
	if err != nil {
		t.Fatal(err)
	}
	lease.OperationID = "rootfs.import.other"
	if _, err := builder.Build(context.Background(), operation, lease); err == nil {
		t.Fatal("expected operation/lease mismatch")
	}
}
