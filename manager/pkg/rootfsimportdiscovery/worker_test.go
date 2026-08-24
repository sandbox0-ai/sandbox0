package rootfsimportdiscovery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
)

type fakeSources struct {
	items []templatestore.ImageSource
}

func (f *fakeSources) ListImageSourcesForRootFSImport(
	_ context.Context,
	cursor templatestore.ImageSourceCursor,
	limit int,
) ([]templatestore.ImageSource, error) {
	items := append([]templatestore.ImageSource(nil), f.items...)
	sort.Slice(items, func(i, j int) bool { return cursorKey(items[i].Cursor) < cursorKey(items[j].Cursor) })
	out := make([]templatestore.ImageSource, 0, limit)
	for _, item := range items {
		if cursorKey(item.Cursor) <= cursorKey(cursor) {
			continue
		}
		out = append(out, item)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func cursorKey(cursor templatestore.ImageSourceCursor) string {
	return cursor.Scope + "\x00" + cursor.TeamID + "\x00" + cursor.TemplateID
}

type fakeImports struct {
	ready map[string]bool
	begun []*sandboxstore.BeginRootFSImportRequest
}

func artifactKey(source string, platform sandboxstore.RootFSArtifactPlatform, format int, size int64) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d\x00%d",
		source, platform.OS, platform.Architecture, platform.Variant, format, size)
}

func (f *fakeImports) GetReadyRootFSBaseArtifact(
	_ context.Context,
	source string,
	platform sandboxstore.RootFSArtifactPlatform,
	requirements sandboxstore.ReadyRootFSArtifactRequirements,
) (*sandboxstore.RootFSBaseArtifact, error) {
	if f.ready[artifactKey(source, platform, requirements.FormatGeneration, requirements.LogicalSizeBytes)] {
		return &sandboxstore.RootFSBaseArtifact{ArtifactDigest: "sha256:" + strings.Repeat("e", 64)}, nil
	}
	return nil, sandboxstore.ErrRootFSBaseArtifactNotFound
}

func (f *fakeImports) BeginRootFSImport(
	_ context.Context,
	request *sandboxstore.BeginRootFSImportRequest,
) (*sandboxstore.RootFSImportOperation, error) {
	copy := *request
	f.begun = append(f.begun, &copy)
	return &sandboxstore.RootFSImportOperation{
		ID: request.OperationID, Spec: request.Spec, State: sandboxstore.RootFSImportStatePending,
	}, nil
}

func TestWorkerEnsuresExactSizeForEveryUniquePlatform(t *testing.T) {
	image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
	amd64 := sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"}
	arm64 := sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"}
	digest, err := rootfsimporter.PinnedSourceDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	imports := &fakeImports{ready: map[string]bool{
		artifactKey(digest.String(), amd64, 1, 8<<30): true,
	}}
	worker, err := New(Config{
		Sources: &fakeSources{items: []templatestore.ImageSource{{
			Cursor: templatestore.ImageSourceCursor{Scope: "team", TeamID: "team-1", TemplateID: "default"},
			Image:  image,
		}}},
		Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{arm64, amd64, arm64},
		FormatGeneration: 1,
		ProcdProtocol:    "sandbox0.procd.v1",
		ProcdDigest:      "sha256:" + strings.Repeat("b", 64),
		PageSize:         10,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunOnce(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Templates != 1 || result.Requirements != 2 || result.Ready != 1 ||
		result.Ensured != 1 || result.Failed != 0 || !result.Wrapped {
		t.Fatalf("result = %#v", result)
	}
	if len(imports.begun) != 1 {
		t.Fatalf("imports begun = %d, want 1", len(imports.begun))
	}
	request := imports.begun[0]
	if request.Spec.LogicalSizeBytes != 8<<30 || request.Spec.Platform.Architecture != "arm64" ||
		!strings.HasPrefix(request.OperationID, "template-import:") || len(request.OperationID) != 80 {
		t.Fatalf("import request = %#v", request)
	}

	_, err = worker.RunOnce(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(imports.begun) != 2 || imports.begun[0].OperationID != imports.begun[1].OperationID {
		t.Fatalf("idempotent operation IDs = %#v", imports.begun)
	}
}

func TestWorkerAdvancesPastMalformedHistoricalTemplate(t *testing.T) {
	sources := &fakeSources{items: []templatestore.ImageSource{
		{Cursor: templatestore.ImageSourceCursor{Scope: "public", TemplateID: "bad"}, Image: "mutable:latest"},
		{Cursor: templatestore.ImageSourceCursor{Scope: "public", TemplateID: "good"}, Image: "registry.example/good@sha256:" + strings.Repeat("c", 64), EphemeralStorage: "1Gi"},
	}}
	imports := &fakeImports{ready: make(map[string]bool)}
	worker, err := New(Config{
		Sources: sources, Imports: imports,
		Platforms:        []sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}},
		FormatGeneration: 1, ProcdProtocol: "sandbox0.procd.v1",
		ProcdDigest: "sha256:" + strings.Repeat("d", 64), PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := worker.RunOnce(t.Context())
	if err == nil || first.Failed != 1 || first.Wrapped {
		t.Fatalf("first pass = %#v, %v", first, err)
	}
	second, err := worker.RunOnce(t.Context())
	if err != nil || second.Ensured != 1 || len(imports.begun) != 1 {
		t.Fatalf("second pass = %#v, %v; begun=%d", second, err, len(imports.begun))
	}
}
