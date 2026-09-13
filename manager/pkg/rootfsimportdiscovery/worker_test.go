package rootfsimportdiscovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
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
	ready         map[string]bool
	readyRanges   map[string]int
	readyRefs     map[string]string
	readyPolicies map[string]string
	lookups       []sandboxstore.ReadyRootFSArtifactRequirements
	begun         []*sandboxstore.BeginRootFSImportRequest
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
	f.lookups = append(f.lookups, requirements)
	key := artifactKey(source, platform, requirements.FormatGeneration, requirements.LogicalSizeBytes)
	if f.ready[key] && requirements.ImportDataLayoutPolicy == f.readyPolicies[key] && (requirements.ImportDataRangeBytes == 0 || requirements.ImportDataRangeBytes == f.readyRanges[key]) &&
		(requirements.SourceOCIRef == "" || requirements.SourceOCIRef == f.readyRefs[key]) {
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
	}, readyRefs: map[string]string{artifactKey(digest.String(), amd64, 1, 8<<30): image}}
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

func TestWorkerSelectsExactImageImportGeometry(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		configured, readyRange              int
		otherReference, missing, wantImport bool
	}{
		{name: "default_reuses_8m", readyRange: 8 << 20},
		{name: "default_reuses_1m", readyRange: 1 << 20},
		{name: "default_builds_8m", missing: true, wantImport: true},
		{name: "strict_1m_rejects_8m", configured: 1 << 20, readyRange: 8 << 20, wantImport: true},
		{name: "strict_1m_reuses_1m", configured: 1 << 20, readyRange: 1 << 20},
		{name: "strict_8m_rejects_1m", configured: 8 << 20, readyRange: 1 << 20, wantImport: true},
		{name: "strict_reference", configured: 1 << 20, readyRange: 1 << 20, otherReference: true, wantImport: true},
		{name: "default_reference", readyRange: 8 << 20, otherReference: true, wantImport: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
			platform := sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"}
			sourceDigest, err := rootfsimporter.PinnedSourceDigest(image)
			if err != nil {
				t.Fatal(err)
			}
			key := artifactKey(sourceDigest.String(), platform, 1, 16<<30)
			readyRef := image
			if tc.otherReference {
				readyRef = "other.example/runtime@" + sourceDigest.String()
			}
			imports := &fakeImports{
				ready: map[string]bool{key: !tc.missing}, readyRanges: map[string]int{key: tc.readyRange},
				readyRefs: map[string]string{key: readyRef},
			}
			cfg := Config{
				Sources: &fakeSources{items: []templatestore.ImageSource{{Cursor: templatestore.ImageSourceCursor{Scope: "public", TemplateID: "image"}, Image: image, EphemeralStorage: "16Gi"}}},
				Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{platform},
				FormatGeneration: 1, ProcdProtocol: "sandbox0.procd.v3", ProcdDigest: "sha256:" + strings.Repeat("b", 64),
				BlockOptions: rootfsblock.BuildOptions{DataRangeBytes: tc.configured},
			}
			worker, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			result, err := worker.RunOnce(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(imports.lookups) != 1 || imports.lookups[0].SourceOCIRef != image || imports.lookups[0].ImportDataRangeBytes != tc.configured {
				t.Fatalf("artifact lookups=%+v", imports.lookups)
			}
			if !tc.wantImport {
				if result.Ready != 1 || result.Ensured != 0 || len(imports.begun) != 0 {
					t.Fatalf("unexpected import: result=%+v operations=%+v", result, imports.begun)
				}
				return
			}
			if result.Ready != 0 || result.Ensured != 1 || len(imports.begun) != 1 {
				t.Fatalf("missing exact import: result=%+v operations=%+v", result, imports.begun)
			}
			wantRange := tc.configured
			if wantRange == 0 {
				wantRange = rootfsblock.DefaultDataRangeBytes
			}
			got := imports.begun[0].Spec
			if got.SourceOCIRef != image || got.BlockOptions.DataRangeBytes != wantRange || got.BlockOptions.PackBytes != rootfsblock.DefaultPackBytes || got.BlockOptions.PageEntries != rootfsblock.DefaultPageEntries {
				t.Fatalf("import did not preserve canonical geometry and reference: %+v", got)
			}
			_, err = worker.RunOnce(t.Context())
			if err != nil || len(imports.begun) != 2 || imports.begun[0].OperationID != imports.begun[1].OperationID {
				t.Fatalf("non-idempotent import: %v %+v", err, imports.begun)
			}
		})
	}
}

func TestWorkerRejectsInvalidImportGeometryBeforeScanning(t *testing.T) {
	for _, dataRange := range []int{-1, (1 << 20) + 1, 3 << 20, rootfsblock.MaxDataRangeBytes + rootfsblock.LogicalBlockSize} {
		t.Run(fmt.Sprint(dataRange), func(t *testing.T) {
			imports := &fakeImports{}
			worker, err := New(Config{
				Sources: &fakeSources{}, Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}},
				FormatGeneration: 1, BlockOptions: rootfsblock.BuildOptions{DataRangeBytes: dataRange},
			})
			if err == nil || worker != nil || len(imports.lookups) != 0 || len(imports.begun) != 0 {
				t.Fatalf("invalid geometry accepted: %v", err)
			}
		})
	}
}

func TestWorkerRejectsInvalidFormatBindingBeforeScanning(t *testing.T) {
	for _, test := range []struct{ generation, version, dataRange int }{
		{-1, 0, 0}, {10005, 0, 0}, {1, 2, 0}, {2, 1, 0}, {2, 3, 0}, {2, 0, 1 << 20},
	} {
		t.Run(fmt.Sprint(test), func(t *testing.T) {
			imports := &fakeImports{}
			worker, err := New(Config{
				Sources: &fakeSources{}, Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}},
				FormatGeneration: test.generation, BlockOptions: rootfsblock.BuildOptions{FormatVersion: test.version, DataRangeBytes: test.dataRange},
			})
			if err == nil || worker != nil || len(imports.lookups) != 0 || len(imports.begun) != 0 {
				t.Fatalf("invalid format accepted: %v", err)
			}
		})
	}
}

type readyOperationImports struct {
	*fakeImports
	publishOnBegin bool
	nilAfterBegin  bool
	errAfterBegin  error
}

func (s *readyOperationImports) GetReadyRootFSBaseArtifact(ctx context.Context, source string, platform sandboxstore.RootFSArtifactPlatform, requirements sandboxstore.ReadyRootFSArtifactRequirements) (*sandboxstore.RootFSBaseArtifact, error) {
	if len(s.begun) > 0 && (s.nilAfterBegin || s.errAfterBegin != nil) {
		s.lookups = append(s.lookups, requirements)
		return nil, s.errAfterBegin
	}
	return s.fakeImports.GetReadyRootFSBaseArtifact(ctx, source, platform, requirements)
}

func (s *readyOperationImports) BeginRootFSImport(ctx context.Context, request *sandboxstore.BeginRootFSImportRequest) (*sandboxstore.RootFSImportOperation, error) {
	operation, err := s.fakeImports.BeginRootFSImport(ctx, request)
	if err != nil {
		return nil, err
	}
	operation.State = sandboxstore.RootFSImportStateReady
	operation.ArtifactDigest = "sha256:" + strings.Repeat("e", 64)
	if s.publishOnBegin {
		pinned, err := rootfsimporter.PinnedSourceDigest(request.Spec.SourceOCIRef)
		if err != nil {
			return nil, err
		}
		platform := sandboxstore.RootFSArtifactPlatform{OS: request.Spec.Platform.OS, Architecture: request.Spec.Platform.Architecture, Variant: request.Spec.Platform.Variant}
		key := artifactKey(pinned.String(), platform, request.Spec.FormatGeneration, request.Spec.LogicalSizeBytes)
		s.ready[key] = true
		s.readyRanges[key] = request.Spec.BlockOptions.DataRangeBytes
		s.readyRefs[key] = request.Spec.SourceOCIRef
	}
	return operation, nil
}

func TestWorkerReadyOperationRequiresSelectableArtifact(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		readyRange                                    int
		publishOnBegin, nilAfterBegin, otherReference bool
		errAfterBegin                                 error
	}{
		{name: "unknown_provenance"},
		{name: "wrong_geometry", readyRange: 8 << 20},
		{name: "wrong_reference", readyRange: 1 << 20, otherReference: true},
		{name: "nil_artifact", nilAfterBegin: true},
		{name: "selector_unavailable", errAfterBegin: errors.New("metadata temporarily unavailable")},
		{name: "publication_raced_lookup", publishOnBegin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
			platform := sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"}
			pinned, err := rootfsimporter.PinnedSourceDigest(image)
			if err != nil {
				t.Fatal(err)
			}
			key := artifactKey(pinned.String(), platform, 1, 16<<30)
			readyRef := image
			if tc.otherReference {
				readyRef = "mirror.example/runtime@" + pinned.String()
			}
			imports := &readyOperationImports{
				fakeImports:    &fakeImports{ready: map[string]bool{key: true}, readyRanges: map[string]int{key: tc.readyRange}, readyRefs: map[string]string{key: readyRef}},
				publishOnBegin: tc.publishOnBegin, nilAfterBegin: tc.nilAfterBegin, errAfterBegin: tc.errAfterBegin,
			}
			worker, err := New(Config{
				Sources: &fakeSources{items: []templatestore.ImageSource{{Cursor: templatestore.ImageSourceCursor{Scope: "public", TemplateID: "image"}, Image: image, EphemeralStorage: "16Gi"}}},
				Imports: imports, Platforms: []sandboxstore.RootFSArtifactPlatform{platform},
				FormatGeneration: 1, ProcdProtocol: "sandbox0.procd.v3", ProcdDigest: "sha256:" + strings.Repeat("b", 64),
				BlockOptions: rootfsblock.BuildOptions{DataRangeBytes: 1 << 20},
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := worker.RunOnce(t.Context())
			if len(imports.begun) != 1 || len(imports.lookups) != 2 {
				t.Fatalf("ready operation was not reselected: lookups=%d imports=%d", len(imports.lookups), len(imports.begun))
			}
			if imports.lookups[0] != imports.lookups[1] || imports.lookups[1].ImportDataRangeBytes != 1<<20 || imports.lookups[1].SourceOCIRef != image {
				t.Fatalf("reselection changed exact requirements: %+v", imports.lookups)
			}
			if result.Ensured != 0 {
				t.Fatalf("ready operation incorrectly counted as pending: %+v", result)
			}
			if tc.publishOnBegin {
				if err != nil || result.Ready != 1 || result.Failed != 0 {
					t.Fatalf("matching artifact not ready: %+v %v", result, err)
				}
				return
			}
			if err == nil || result.Ready != 0 || result.Failed != 1 || !strings.Contains(err.Error(), "has no selectable artifact") {
				t.Fatalf("unselectable ready operation was silently accepted: %+v %v", result, err)
			}
			// A subsequent verified metadata repair becomes usable through the
			// ordinary selector, without resetting or replacing the operation.
			imports.readyRanges[key], imports.readyRefs[key] = 1<<20, image
			imports.nilAfterBegin, imports.errAfterBegin = false, nil
			result, err = worker.RunOnce(t.Context())
			if err != nil || result.Ready != 1 || result.Failed != 0 || len(imports.begun) != 1 {
				t.Fatalf("matching repaired artifact not reused: %+v %v", result, err)
			}
		})
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
