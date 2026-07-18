package templateimage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	managerregistry "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestPublisherPublishComposesPlatformImageAndStreamsRootFSLayers(t *testing.T) {
	t.Parallel()

	baseLayer := []byte("base-amd64-layer")
	baseLayerDesc := descriptorFromBytes(ocispec.MediaTypeImageLayerGzip, baseLayer)
	baseDiffID := digest.FromString("base-amd64-uncompressed")
	baseConfig := ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Config: ocispec.ImageConfig{
			Env:        []string{"BASE_ENV=preserved"},
			Entrypoint: []string{"/bin/base"},
			Cmd:        []string{"serve"},
			WorkingDir: "/workspace",
		},
		RootFS: ocispec.RootFS{
			Type:    "layers",
			DiffIDs: []digest.Digest{baseDiffID},
		},
		History: []ocispec.History{{CreatedBy: "base build"}},
	}
	baseConfigBytes := mustJSON(t, baseConfig)
	baseConfigDesc := descriptorFromBytes(ocispec.MediaTypeImageConfig, baseConfigBytes)
	baseConfigDesc.Annotations = map[string]string{"base-config": "preserved"}
	baseConfigDesc.URLs = []string{"https://example.invalid/old-config"}
	baseConfigDesc.Data = append([]byte(nil), baseConfigBytes...)
	amd64Manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    baseConfigDesc,
		Layers: []ocispec.Descriptor{{
			MediaType:   baseLayerDesc.MediaType,
			Digest:      baseLayerDesc.Digest,
			Size:        baseLayerDesc.Size,
			Annotations: map[string]string{"base-layer": "preserved"},
		}},
		Annotations: map[string]string{"base-manifest": "preserved"},
	}
	amd64ManifestBytes := mustJSON(t, amd64Manifest)
	amd64ManifestDesc := descriptorFromBytes(ocispec.MediaTypeImageManifest, amd64ManifestBytes)

	armConfig := baseConfig
	armConfig.Architecture = "arm64"
	armConfigBytes := mustJSON(t, armConfig)
	armConfigDesc := descriptorFromBytes(ocispec.MediaTypeImageConfig, armConfigBytes)
	armManifest := amd64Manifest
	armManifest.Config = armConfigDesc
	armManifestBytes := mustJSON(t, armManifest)
	armManifestDesc := descriptorFromBytes(ocispec.MediaTypeImageManifest, armManifestBytes)

	index := ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{
			withPlatform(armManifestDesc, ocispec.Platform{OS: "linux", Architecture: "arm64"}),
			withPlatform(amd64ManifestDesc, ocispec.Platform{OS: "linux", Architecture: "amd64"}),
		},
	}
	indexBytes := mustJSON(t, index)
	indexDesc := descriptorFromBytes(ocispec.MediaTypeImageIndex, indexBytes)

	uncompressedLayer := tarWithWhiteout(t)
	compressedLayer := gzipBytes(t, []byte("second uncompressed filesystem diff"))
	uncompressedDiffID := digest.FromBytes(uncompressedLayer)
	compressedDiffID := digest.FromString("second uncompressed filesystem diff")
	objects := &fakeObjectReader{objects: map[string][]byte{
		"rootfs/layer-1": uncompressedLayer,
		"rootfs/layer-2": compressedLayer,
	}}

	source := &fakeResolver{
		root: indexDesc,
		fetcher: &fakeFetcher{blobs: map[digest.Digest][]byte{
			indexDesc.Digest:            indexBytes,
			amd64ManifestDesc.Digest:    amd64ManifestBytes,
			amd64Manifest.Config.Digest: baseConfigBytes,
			baseLayerDesc.Digest:        baseLayer,
			armManifestDesc.Digest:      armManifestBytes,
			armManifest.Config.Digest:   armConfigBytes,
		}},
	}
	targetPusher := newFakePusher()
	target := &fakeResolver{pusher: targetPusher}
	credentialProvider := &fakeCredentialProvider{credential: &managerregistry.Credential{
		Provider:     "builtin",
		PushRegistry: "127.0.0.1:30500/t-team",
		PullRegistry: "registry.manager.svc:5000/t-team",
		Username:     "builder",
		Password:     "secret",
	}}
	publisher, err := NewPublisher(objects, credentialProvider, managerconfig.RegistryConfig{Provider: "builtin"})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	var resolverCalls []resolverOptions
	publisher.newResolver = func(opts resolverOptions) remotes.Resolver {
		resolverCalls = append(resolverCalls, opts)
		if opts.purpose == resolverPurposeTarget {
			return target
		}
		return source
	}

	createdAt := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	req := BuildRequest{
		BuildID:         "20D888E6-EAED-4E83-9A76-270BC3BF1503",
		TeamID:          "team-1",
		TemplateID:      "Development Workspace",
		SourceSandboxID: "sandbox-1",
		BaseImageRef:    "base-image:latest",
		BaseImageDigest: indexDesc.Digest.String(),
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		CreatedAt:       createdAt,
		Layers: []Layer{
			{
				ID:        "layer-1",
				ObjectKey: "rootfs/layer-1",
				MediaType: ocispec.MediaTypeImageLayer,
				Digest:    digest.FromBytes(uncompressedLayer).String(),
				DiffID:    uncompressedDiffID.String(),
				Size:      int64(len(uncompressedLayer)),
			},
			{
				ID:        "layer-2",
				ObjectKey: "rootfs/layer-2",
				MediaType: ocispec.MediaTypeImageLayerGzip,
				Digest:    digest.FromBytes(compressedLayer).String(),
				DiffID:    compressedDiffID.String(),
				Size:      int64(len(compressedLayer)),
			},
		},
	}
	result, err := publisher.Publish(context.Background(), req)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got, want := source.resolvedRef, "docker.io/library/base-image@"+indexDesc.Digest.String(); got != want {
		t.Fatalf("source Resolve() ref = %q, want %q", got, want)
	}
	if len(resolverCalls) != 2 {
		t.Fatalf("resolver calls = %d, want 2", len(resolverCalls))
	}
	if !resolverCalls[0].plainHTTP || resolverCalls[0].purpose != resolverPurposeTarget {
		t.Fatalf("target resolver options = %#v, want builtin PlainHTTP", resolverCalls[0])
	}
	if resolverCalls[1].plainHTTP {
		t.Fatalf("public base resolver unexpectedly uses PlainHTTP")
	}
	username, password, err := resolverCalls[0].credentials("registry.manager.svc:5000")
	if err != nil || username != "builder" || password != "secret" {
		t.Fatalf("target credentials = %q/%q, %v", username, password, err)
	}
	if strings.Contains(targetPusher.reference, "127.0.0.1") {
		t.Fatalf("server-side push used external localhost endpoint: %s", targetPusher.reference)
	}
	if !strings.HasPrefix(targetPusher.reference, "registry.manager.svc:5000/t-team/") {
		t.Fatalf("target pusher reference = %q", targetPusher.reference)
	}
	if !strings.Contains(targetPusher.reference, ":build-20d888e6-eaed-4e83-9a76-270bc3bf1503") {
		t.Fatalf("target pusher reference does not use stable build tag: %q", targetPusher.reference)
	}
	if !strings.HasPrefix(result.PullReference, "registry.manager.svc:5000/t-team/") ||
		!strings.HasSuffix(result.PullReference, "@"+result.ManifestDigest.String()) {
		t.Fatalf("PullReference = %q, want internal digest-pinned reference", result.PullReference)
	}

	if len(targetPusher.order) == 0 || targetPusher.order[len(targetPusher.order)-1] != result.ManifestDigest {
		t.Fatalf("manifest was not pushed last: %#v", targetPusher.order)
	}
	manifestBytes, ok := targetPusher.blobs[result.ManifestDigest]
	if !ok {
		t.Fatalf("published manifest %s not found", result.ManifestDigest)
	}
	var publishedManifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &publishedManifest); err != nil {
		t.Fatalf("decode published manifest: %v", err)
	}
	if got, want := len(publishedManifest.Layers), 3; got != want {
		t.Fatalf("published layer count = %d, want %d", got, want)
	}
	wantLayerDigests := []digest.Digest{
		baseLayerDesc.Digest,
		digest.FromBytes(uncompressedLayer),
		digest.FromBytes(compressedLayer),
	}
	for i, want := range wantLayerDigests {
		if got := publishedManifest.Layers[i].Digest; got != want {
			t.Fatalf("published layer %d digest = %s, want %s", i, got, want)
		}
	}
	if _, leaked := publishedManifest.Layers[0].Annotations[distributionSourceLabel+".registry.manager.svc"]; leaked {
		t.Fatalf("temporary cross-repository mount annotation leaked into output manifest")
	}
	if got := publishedManifest.Annotations["base-manifest"]; got != "preserved" {
		t.Fatalf("base manifest annotation = %q, want preserved", got)
	}
	if got := publishedManifest.Config.Annotations["base-config"]; got != "preserved" {
		t.Fatalf("base config descriptor annotation = %q, want preserved", got)
	}
	if len(publishedManifest.Config.URLs) != 0 || len(publishedManifest.Config.Data) != 0 {
		t.Fatalf(
			"new config descriptor retained old digest data: urls=%#v data=%d bytes",
			publishedManifest.Config.URLs,
			len(publishedManifest.Config.Data),
		)
	}

	publishedConfigBytes := targetPusher.blobs[publishedManifest.Config.Digest]
	var publishedConfig ocispec.Image
	if err := json.Unmarshal(publishedConfigBytes, &publishedConfig); err != nil {
		t.Fatalf("decode published config: %v", err)
	}
	if got := publishedConfig.Config.Env; len(got) != 1 || got[0] != "BASE_ENV=preserved" {
		t.Fatalf("base config env was not preserved: %#v", got)
	}
	if got := publishedConfig.Config.Entrypoint; len(got) != 1 || got[0] != "/bin/base" {
		t.Fatalf("base entrypoint was not preserved: %#v", got)
	}
	if publishedConfig.OS != "linux" || publishedConfig.Architecture != "amd64" || publishedConfig.Variant != "" {
		t.Fatalf("published config platform = %#v, want captured linux/amd64", publishedConfig.Platform)
	}
	wantDiffIDs := []digest.Digest{baseDiffID, uncompressedDiffID, compressedDiffID}
	if len(publishedConfig.RootFS.DiffIDs) != len(wantDiffIDs) {
		t.Fatalf("config diff_id count = %d, want %d", len(publishedConfig.RootFS.DiffIDs), len(wantDiffIDs))
	}
	for i, want := range wantDiffIDs {
		if got := publishedConfig.RootFS.DiffIDs[i]; got != want {
			t.Fatalf("config diff_id %d = %s, want %s", i, got, want)
		}
	}
	if got := targetPusher.blobs[digest.FromBytes(uncompressedLayer)]; !bytes.Equal(got, uncompressedLayer) {
		t.Fatalf("uncompressed rootfs tar, including overlay whiteout, was modified")
	}
	if got := targetPusher.blobs[digest.FromBytes(compressedLayer)]; !bytes.Equal(got, compressedLayer) {
		t.Fatalf("compressed rootfs layer was modified")
	}
	if _, pushedWrongPlatform := targetPusher.blobs[armManifestDesc.Digest]; pushedWrongPlatform {
		t.Fatalf("arm64 manifest was pushed for amd64 source platform")
	}
	if got := objects.gets; len(got) != 2 || got[0].offset != 0 || got[1].offset != 0 {
		t.Fatalf("rootfs object reads = %#v, want one streaming read per layer", got)
	}
}

func TestPublisherUsesRealProviderTeamScopeExactlyOnce(t *testing.T) {
	t.Parallel()

	teamID := "team-contract"
	registryConfig := managerconfig.RegistryConfig{
		Provider:     "builtin",
		PushRegistry: "registry.public.example.com",
		PullRegistry: "registry.manager.svc:5000",
		Builtin: &managerconfig.RegistryBuiltinConfig{
			Username: "builder",
			Password: "secret",
		},
	}
	credentialProvider, err := managerregistry.NewProvider(registryConfig, nil, nil)
	if err != nil {
		t.Fatalf("registry.NewProvider() error = %v", err)
	}

	baseConfig := ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		RootFS:   ocispec.RootFS{Type: "layers"},
	}
	baseConfigBytes := mustJSON(t, baseConfig)
	baseConfigDesc := descriptorFromBytes(ocispec.MediaTypeImageConfig, baseConfigBytes)
	baseManifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    baseConfigDesc,
	}
	baseManifestBytes := mustJSON(t, baseManifest)
	baseManifestDesc := descriptorFromBytes(ocispec.MediaTypeImageManifest, baseManifestBytes)
	source := &fakeResolver{
		root: baseManifestDesc,
		fetcher: &fakeFetcher{blobs: map[digest.Digest][]byte{
			baseManifestDesc.Digest: baseManifestBytes,
			baseConfigDesc.Digest:   baseConfigBytes,
		}},
	}

	rootFSLayer := []byte("rootfs diff")
	objects := &fakeObjectReader{objects: map[string][]byte{"rootfs/layer": rootFSLayer}}
	targetPusher := newFakePusher()
	target := &fakeResolver{pusher: targetPusher}
	publisher, err := NewPublisher(objects, credentialProvider, registryConfig)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	publisher.newResolver = func(opts resolverOptions) remotes.Resolver {
		if opts.purpose == resolverPurposeTarget {
			return target
		}
		return source
	}

	result, err := publisher.Publish(context.Background(), BuildRequest{
		BuildID:         "20d888e6-eaed-4e83-9a76-270bc3bf1503",
		TeamID:          teamID,
		TemplateID:      "contract",
		SourceSandboxID: "sandbox-1",
		BaseImageRef:    "docker.io/library/base:latest",
		BaseImageDigest: baseManifestDesc.Digest.String(),
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Layers: []Layer{{
			ID:        "layer-1",
			ObjectKey: "rootfs/layer",
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(rootFSLayer).String(),
			DiffID:    digest.FromBytes(rootFSLayer).String(),
			Size:      int64(len(rootFSLayer)),
		}},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	teamPrefix := naming.TeamImageRepositoryPrefix(teamID)
	wantRepository := "registry.manager.svc:5000/" + teamPrefix + "/template-contract"
	wantPushTag := wantRepository + ":build-20d888e6-eaed-4e83-9a76-270bc3bf1503"
	if targetPusher.reference != wantPushTag {
		t.Fatalf("pusher reference = %q, want %q", targetPusher.reference, wantPushTag)
	}
	wantPullReference := wantRepository + "@" + result.ManifestDigest.String()
	if result.PullReference != wantPullReference {
		t.Fatalf("PullReference = %q, want %q", result.PullReference, wantPullReference)
	}
	if strings.Count(targetPusher.reference, "/"+teamPrefix+"/") != 1 {
		t.Fatalf("pusher reference has duplicated or missing team scope: %q", targetPusher.reference)
	}
}

func TestComposeImageIsDeterministicAndRequiresDiffID(t *testing.T) {
	t.Parallel()

	base := &resolvedBase{
		manifest: ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
		},
		config: ocispec.Image{
			RootFS: ocispec.RootFS{Type: "layers"},
		},
	}
	layerBytes := []byte("rootfs")
	req := BuildRequest{
		BuildID:         "build-1",
		TeamID:          "team-1",
		TemplateID:      "template-1",
		BaseImageRef:    "busybox",
		BaseImageDigest: digest.FromString("base").String(),
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		CreatedAt:       time.Unix(100, 0).UTC(),
		Layers: []Layer{{
			ObjectKey: "layer",
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(layerBytes).String(),
			DiffID:    digest.FromBytes(layerBytes).String(),
			Size:      int64(len(layerBytes)),
		}},
	}
	config1, configDesc1, manifest1, manifestDesc1, err := composeImage(req, base)
	if err != nil {
		t.Fatalf("composeImage() error = %v", err)
	}
	config2, configDesc2, manifest2, manifestDesc2, err := composeImage(req, base)
	if err != nil {
		t.Fatalf("composeImage() second error = %v", err)
	}
	if !bytes.Equal(config1, config2) || configDesc1.Digest != configDesc2.Digest ||
		configDesc1.Size != configDesc2.Size || !bytes.Equal(manifest1, manifest2) ||
		manifestDesc1.Digest != manifestDesc2.Digest || manifestDesc1.Size != manifestDesc2.Size {
		t.Fatalf("composeImage() is not deterministic across retry")
	}

	req.Layers[0].DiffID = ""
	if err := req.validate(false); err == nil || !strings.Contains(err.Error(), "diff_id") {
		t.Fatalf("validate() error = %v, want missing diff_id error", err)
	}
}

func TestResolveLayerDiffIDsSupportsLegacyUncompressedAndGzipLayers(t *testing.T) {
	t.Parallel()

	uncompressed := []byte("legacy uncompressed tar")
	plain := []byte("legacy compressed tar")
	compressed := gzipBytes(t, plain)
	objects := &fakeObjectReader{objects: map[string][]byte{
		"compressed": compressed,
	}}
	layers, err := ResolveLayerDiffIDs(context.Background(), objects, []Layer{
		{
			ObjectKey: "uncompressed",
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(uncompressed).String(),
			Size:      int64(len(uncompressed)),
		},
		{
			ObjectKey: "compressed",
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Digest:    digest.FromBytes(compressed).String(),
			Size:      int64(len(compressed)),
		},
	})
	if err != nil {
		t.Fatalf("ResolveLayerDiffIDs() error = %v", err)
	}
	if got, want := layers[0].DiffID, digest.FromBytes(uncompressed).String(); got != want {
		t.Fatalf("uncompressed DiffID = %s, want %s", got, want)
	}
	if got, want := layers[1].DiffID, digest.FromBytes(plain).String(); got != want {
		t.Fatalf("compressed DiffID = %s, want %s", got, want)
	}
	if got := len(objects.gets); got != 1 || objects.gets[0].key != "compressed" {
		t.Fatalf("object reads = %#v, want only compressed legacy layer", objects.gets)
	}
}

func TestResolveLayerDiffIDsRejectsCompressionThatDoesNotMatchMediaType(t *testing.T) {
	t.Parallel()

	payload := []byte("not gzip")
	objects := &fakeObjectReader{objects: map[string][]byte{"layer": payload}}
	_, err := ResolveLayerDiffIDs(context.Background(), objects, []Layer{{
		ObjectKey: "layer",
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Digest:    digest.FromBytes(payload).String(),
		Size:      int64(len(payload)),
	}})
	if err == nil || !strings.Contains(err.Error(), "compression does not match") {
		t.Fatalf("ResolveLayerDiffIDs() error = %v, want compression mismatch", err)
	}
}

func TestValidateBaseConfigRejectsPlatformVariantMismatch(t *testing.T) {
	t.Parallel()

	config := ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "arm", Variant: "v6"},
		RootFS:   ocispec.RootFS{Type: "layers"},
	}
	manifest := ocispec.Manifest{Layers: []ocispec.Descriptor{}}
	err := validateBaseConfig(config, manifest, ocispec.Platform{OS: "linux", Architecture: "arm", Variant: "v7"})
	if err == nil || !strings.Contains(err.Error(), "variant") {
		t.Fatalf("validateBaseConfig() error = %v, want platform variant mismatch", err)
	}
}

func TestPublicationEndpointsExternalUsesPushAndPullEndpoints(t *testing.T) {
	t.Parallel()

	push, pull, plainHTTP, err := publicationEndpoints(&managerregistry.Credential{
		Provider:     "aws",
		PushRegistry: "123.dkr.ecr.example.com/t-team",
		PullRegistry: "mirror.internal.example.com/t-team",
	})
	if err != nil {
		t.Fatalf("publicationEndpoints() error = %v", err)
	}
	if push != "123.dkr.ecr.example.com/t-team" || pull != "mirror.internal.example.com/t-team" || plainHTTP {
		t.Fatalf("publicationEndpoints() = %q, %q, %v", push, pull, plainHTTP)
	}
}

func TestRegistryEndpointsWithPathsPreserveRepositoryAndCompareByAuthority(t *testing.T) {
	t.Parallel()

	push, pull, plainHTTP, err := publicationEndpoints(&managerregistry.Credential{
		Provider:     "builtin",
		PushRegistry: "https://public.example.com/tenant/",
		PullRegistry: "http://registry.manager.svc:5000/t-team/",
	})
	if err != nil {
		t.Fatalf("publicationEndpoints() error = %v", err)
	}
	if push != "registry.manager.svc:5000/t-team" || pull != push || !plainHTTP {
		t.Fatalf("publicationEndpoints() = %q, %q, %v", push, pull, plainHTTP)
	}
	if got, want := joinImageReference(push, "template-a:build-1"), "registry.manager.svc:5000/t-team/template-a:build-1"; got != want {
		t.Fatalf("joinImageReference() = %q, want %q", got, want)
	}
	if !sameRegistryHost("https://registry.manager.svc:5000/source", push) {
		t.Fatal("sameRegistryHost() should compare registry authority independently from endpoint paths")
	}
	if got, want := registryHostname(push), "registry.manager.svc"; got != want {
		t.Fatalf("registryHostname() = %q, want %q", got, want)
	}
}

func TestSourceCredentialsPreferPullConfigForSameRegistryDifferentBaseRepository(t *testing.T) {
	t.Parallel()

	baseDigest := digest.FromString("private-base").String()
	_, baseHost, baseRepository, err := normalizeBaseReference(
		"123.dkr.ecr.example.com/base-images/runtime:stable",
		baseDigest,
	)
	if err != nil {
		t.Fatalf("normalizeBaseReference() error = %v", err)
	}
	if baseRepository != "base-images/runtime" {
		t.Fatalf("base repository = %q, want base-images/runtime", baseRepository)
	}

	publisher := &Publisher{
		registry: managerconfig.RegistryConfig{
			PullCredentialsFile: "/config/pull-dockerconfigjson",
		},
		readFile: func(path string) ([]byte, error) {
			if path != "/config/pull-dockerconfigjson" {
				return nil, fmt.Errorf("unexpected credentials path %q", path)
			}
			return []byte(`{
				"auths": {
					"https://123.dkr.ecr.example.com/v2/": {
						"username": "base-pull-user",
						"password": "base-pull-password"
					}
				}
			}`), nil
		},
	}
	targetCredentials := &managerregistry.Credential{
		Provider:     "aws",
		PushRegistry: "123.dkr.ecr.example.com/t-team",
		PullRegistry: "123.dkr.ecr.example.com/t-team",
		Username:     "repository-scoped-push-user",
		Password:     "repository-scoped-push-password",
	}
	sourceAuth, err := publisher.sourceCredentials(baseHost, targetCredentials)
	if err != nil {
		t.Fatalf("sourceCredentials() error = %v", err)
	}
	username, password, err := sourceAuth("123.dkr.ecr.example.com")
	if err != nil {
		t.Fatalf("source auth callback error = %v", err)
	}
	if username != "base-pull-user" || password != "base-pull-password" {
		t.Fatalf(
			"source credentials = %q/%q, want configured base pull credentials",
			username,
			password,
		)
	}

	publisher.registry.PullCredentialsFile = ""
	sourceAuth, err = publisher.sourceCredentials(baseHost, targetCredentials)
	if err != nil {
		t.Fatalf("sourceCredentials() fallback error = %v", err)
	}
	username, password, err = sourceAuth("123.dkr.ecr.example.com")
	if err != nil {
		t.Fatalf("source auth fallback callback error = %v", err)
	}
	if username != targetCredentials.Username || password != targetCredentials.Password {
		t.Fatalf(
			"source credential fallback = %q/%q, want target credentials",
			username,
			password,
		)
	}
}

func TestOutputRepositoryIsSingleRegistrySafeComponent(t *testing.T) {
	t.Parallel()

	repository := outputRepository("My Template/With Spaces And A Very Long User Supplied Name That Must Be Hashed Safely")
	if repository == "" || len(repository) > 63 {
		t.Fatalf("outputRepository() = %q with length %d", repository, len(repository))
	}
	if strings.Contains(repository, "/") || repository != strings.ToLower(repository) {
		t.Fatalf("outputRepository() = %q, want lowercase single component", repository)
	}
}

func TestRestartableReaderReopensAtUploadOffset(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789")
	var offsets []int64
	reader, err := newRestartableReader(context.Background(), int64(len(payload)), func(offset int64) (io.ReadCloser, error) {
		offsets = append(offsets, offset)
		return io.NopCloser(bytes.NewReader(payload[offset:])), nil
	})
	if err != nil {
		t.Fatalf("newRestartableReader() error = %v", err)
	}
	defer reader.Close()

	first := make([]byte, 4)
	if _, err := io.ReadFull(reader, first); err != nil {
		t.Fatalf("initial read error = %v", err)
	}
	if _, err := reader.Seek(6, io.SeekStart); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	remaining, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("resumed read error = %v", err)
	}
	if got, want := string(remaining), "6789"; got != want {
		t.Fatalf("resumed bytes = %q, want %q", got, want)
	}
	if got, want := fmt.Sprint(offsets), "[0 6]"; got != want {
		t.Fatalf("open offsets = %s, want %s", got, want)
	}
}

type fakeCredentialProvider struct {
	credential *managerregistry.Credential
	err        error
	requests   []managerregistry.PushCredentialsRequest
}

func (f *fakeCredentialProvider) GetPushCredentials(_ context.Context, req managerregistry.PushCredentialsRequest) (*managerregistry.Credential, error) {
	f.requests = append(f.requests, req)
	return f.credential, f.err
}

type objectGet struct {
	key    string
	offset int64
	limit  int64
}

type fakeObjectReader struct {
	objects map[string][]byte
	gets    []objectGet
}

func (f *fakeObjectReader) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	f.gets = append(f.gets, objectGet{key: key, offset: offset, limit: limit})
	payload, ok := f.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %q not found", key)
	}
	if offset < 0 || offset > int64(len(payload)) {
		return nil, fmt.Errorf("invalid offset %d", offset)
	}
	end := int64(len(payload))
	if limit >= 0 && offset+limit < end {
		end = offset + limit
	}
	return io.NopCloser(&smallChunkReader{reader: bytes.NewReader(payload[offset:end])}), nil
}

type smallChunkReader struct {
	reader *bytes.Reader
}

func (r *smallChunkReader) Read(p []byte) (int, error) {
	if len(p) > 3 {
		p = p[:3]
	}
	return r.reader.Read(p)
}

type fakeResolver struct {
	root        ocispec.Descriptor
	fetcher     remotes.Fetcher
	pusher      *fakePusher
	resolvedRef string
}

func (r *fakeResolver) Resolve(_ context.Context, ref string) (string, ocispec.Descriptor, error) {
	r.resolvedRef = ref
	return ref, r.root, nil
}

func (r *fakeResolver) Fetcher(context.Context, string) (remotes.Fetcher, error) {
	if r.fetcher == nil {
		return nil, fmt.Errorf("fetcher unavailable")
	}
	return r.fetcher, nil
}

func (r *fakeResolver) Pusher(_ context.Context, ref string) (remotes.Pusher, error) {
	if r.pusher == nil {
		return nil, fmt.Errorf("pusher unavailable")
	}
	r.pusher.reference = ref
	return r.pusher, nil
}

type fakeFetcher struct {
	blobs map[digest.Digest][]byte
}

func (f *fakeFetcher) Fetch(_ context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	payload, ok := f.blobs[desc.Digest]
	if !ok {
		return nil, fmt.Errorf("blob %s not found", desc.Digest)
	}
	return io.NopCloser(&smallChunkReader{reader: bytes.NewReader(payload)}), nil
}

type fakePusher struct {
	mu          sync.Mutex
	reference   string
	blobs       map[digest.Digest][]byte
	descriptors map[digest.Digest]ocispec.Descriptor
	order       []digest.Digest
}

func newFakePusher() *fakePusher {
	return &fakePusher{
		blobs:       make(map[digest.Digest][]byte),
		descriptors: make(map[digest.Digest]ocispec.Descriptor),
	}
}

func (p *fakePusher) Push(_ context.Context, desc ocispec.Descriptor) (content.Writer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.blobs[desc.Digest]; ok {
		return nil, fmt.Errorf("blob %s: %w", desc.Digest, errdefs.ErrAlreadyExists)
	}
	return &fakeContentWriter{pusher: p, desc: desc}, nil
}

type fakeContentWriter struct {
	pusher    *fakePusher
	desc      ocispec.Descriptor
	buffer    bytes.Buffer
	closed    bool
	committed bool
}

func (w *fakeContentWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.buffer.Write(p)
}

func (w *fakeContentWriter) Close() error {
	w.closed = true
	return nil
}

func (w *fakeContentWriter) Digest() digest.Digest {
	return digest.FromBytes(w.buffer.Bytes())
}

func (w *fakeContentWriter) Commit(_ context.Context, size int64, expected digest.Digest, _ ...content.Opt) error {
	payload := append([]byte(nil), w.buffer.Bytes()...)
	if int64(len(payload)) != size {
		return fmt.Errorf("commit size = %d, want %d", len(payload), size)
	}
	if got := digest.FromBytes(payload); got != expected {
		return fmt.Errorf("commit digest = %s, want %s", got, expected)
	}
	w.pusher.mu.Lock()
	defer w.pusher.mu.Unlock()
	w.pusher.blobs[expected] = payload
	w.pusher.descriptors[expected] = w.desc
	w.pusher.order = append(w.pusher.order, expected)
	w.committed = true
	w.closed = true
	return nil
}

func (w *fakeContentWriter) Status() (content.Status, error) {
	return content.Status{
		Ref:    w.desc.Digest.String(),
		Offset: int64(w.buffer.Len()),
		Total:  w.desc.Size,
	}, nil
}

func (w *fakeContentWriter) Truncate(size int64) error {
	if size != 0 {
		return fmt.Errorf("fake writer only supports truncate to zero")
	}
	w.buffer.Reset()
	return nil
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return payload
}

func withPlatform(desc ocispec.Descriptor, platform ocispec.Platform) ocispec.Descriptor {
	desc.Platform = &platform
	return desc
}

func tarWithWhiteout(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for name, content := range map[string]string{
		"workspace/file.txt":    "new content",
		"workspace/.wh.deleted": "",
	} {
		if err := writer.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}); err != nil {
			t.Fatalf("WriteHeader() error = %v", err)
		}
		if _, err := writer.Write([]byte(content)); err != nil {
			t.Fatalf("tar Write() error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("tar Close() error = %v", err)
	}
	return buffer.Bytes()
}

func gzipBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return buffer.Bytes()
}
