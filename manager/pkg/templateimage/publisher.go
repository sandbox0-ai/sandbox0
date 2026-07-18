package templateimage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	managerconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	managerregistry "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

const (
	maxIndexBytes    = 4 << 20
	maxManifestBytes = 8 << 20
	maxConfigBytes   = 16 << 20

	distributionSourceLabel = "containerd.io/distribution.source"
)

type resolverPurpose string

const (
	resolverPurposeSource resolverPurpose = "source"
	resolverPurposeTarget resolverPurpose = "target"
)

type resolverOptions struct {
	purpose     resolverPurpose
	plainHTTP   bool
	credentials func(host string) (string, string, error)
}

type resolverFactory func(opts resolverOptions) remotes.Resolver

// Publisher composes a source base image and persisted rootfs layers into one
// digest-pinned, single-platform OCI image.
type Publisher struct {
	objects     ObjectReader
	credentials CredentialProvider
	registry    managerconfig.RegistryConfig
	newResolver resolverFactory
	readFile    func(string) ([]byte, error)
}

// NewPublisher creates a template image publisher.
func NewPublisher(objects ObjectReader, credentials CredentialProvider, registry managerconfig.RegistryConfig) (*Publisher, error) {
	if objects == nil {
		return nil, fmt.Errorf("object reader is required")
	}
	if credentials == nil {
		return nil, fmt.Errorf("registry credential provider is required")
	}
	if strings.TrimSpace(registry.Provider) == "" {
		return nil, fmt.Errorf("registry provider is required")
	}
	return &Publisher{
		objects:     objects,
		credentials: credentials,
		registry:    registry,
		newResolver: func(opts resolverOptions) remotes.Resolver {
			return docker.NewResolver(docker.ResolverOptions{
				Credentials: opts.credentials,
				PlainHTTP:   opts.plainHTTP,
			})
		},
		readFile: os.ReadFile,
	}, nil
}

type resolvedBase struct {
	reference  string
	host       string
	repository string
	manifest   ocispec.Manifest
	config     ocispec.Image
	fetcher    remotes.Fetcher
}

// Publish streams the base image and captured rootfs layers to the configured
// registry. The output is digest-pinned even though an idempotent build tag is
// also written for operational inspection.
func (p *Publisher) Publish(ctx context.Context, req BuildRequest) (*Result, error) {
	if p == nil {
		return nil, fmt.Errorf("template image publisher is required")
	}
	if err := req.validate(true); err != nil {
		return nil, err
	}
	resolvedLayers, err := ResolveLayerDiffIDs(ctx, p.objects, req.Layers)
	if err != nil {
		return nil, err
	}
	req.Layers = resolvedLayers
	if err := req.validate(false); err != nil {
		return nil, err
	}

	repository := outputRepository(req.TemplateID)
	tag := "build-" + strings.ToLower(strings.TrimSpace(req.BuildID))
	targetImage := repository + ":" + tag
	credential, err := p.credentials.GetPushCredentials(ctx, managerregistry.PushCredentialsRequest{
		TeamID:      req.TeamID,
		TargetImage: targetImage,
	})
	if err != nil {
		return nil, fmt.Errorf("get registry credentials: %w", err)
	}
	if credential == nil {
		return nil, fmt.Errorf("registry credential provider returned no credentials")
	}

	pushRegistry, pullRegistry, plainHTTP, err := publicationEndpoints(
		credential,
		p.registry.InternalRegistry,
		req.TeamID,
	)
	if err != nil {
		return nil, err
	}
	pushTagRef := joinImageReference(pushRegistry, targetImage)
	pullRepositoryRef := joinImageReference(pullRegistry, repository)

	targetResolver := p.newResolver(resolverOptions{
		purpose:   resolverPurposeTarget,
		plainHTTP: plainHTTP,
		credentials: staticCredentials(
			pushRegistry,
			credential.Username,
			credential.Password,
		),
	})
	pusher, err := targetResolver.Pusher(ctx, pushTagRef)
	if err != nil {
		return nil, fmt.Errorf("create target registry pusher: %w", err)
	}

	baseRef, baseHost, baseRepository, err := normalizeBaseReference(req.BaseImageRef, req.BaseImageDigest)
	if err != nil {
		return nil, err
	}
	baseRef, baseHost, baseRepository, sourcePlainHTTP, err := publicationSourceReference(
		baseRef,
		baseHost,
		baseRepository,
		credential,
		pushRegistry,
		plainHTTP,
	)
	if err != nil {
		return nil, err
	}
	sourceAuth, err := p.sourceCredentials(baseHost, credential, pushRegistry)
	if err != nil {
		return nil, err
	}
	sourceResolver := p.newResolver(resolverOptions{
		purpose:     resolverPurposeSource,
		plainHTTP:   sourcePlainHTTP,
		credentials: sourceAuth,
	})
	base, err := resolveBase(ctx, sourceResolver, baseRef, baseHost, baseRepository, req.Platform)
	if err != nil {
		return nil, err
	}

	configBytes, configDesc, manifestBytes, manifestDesc, err := composeImage(req, base)
	if err != nil {
		return nil, err
	}

	targetHost := registryHostname(pushRegistry)
	for _, layer := range base.manifest.Layers {
		pushDesc := layer
		if sameRegistryHost(base.host, pushRegistry) {
			pushDesc.Annotations = cloneStringMap(layer.Annotations)
			if pushDesc.Annotations == nil {
				pushDesc.Annotations = make(map[string]string)
			}
			pushDesc.Annotations[distributionSourceLabel+"."+targetHost] = base.repository
		}
		err := pushBlob(ctx, pusher, pushDesc, remoteBlobOpener(ctx, base.fetcher, layer))
		if err != nil {
			return nil, fmt.Errorf("push base layer %s: %w", layer.Digest, err)
		}
	}
	for i, layer := range req.Layers {
		desc, err := rootFSLayerDescriptor(layer)
		if err != nil {
			return nil, err
		}
		err = pushBlob(ctx, pusher, desc, func(offset int64) (io.ReadCloser, error) {
			return p.objects.Get(layer.ObjectKey, offset, layer.Size-offset)
		})
		if err != nil {
			return nil, fmt.Errorf("push rootfs layer %d (%s): %w", i, desc.Digest, err)
		}
	}
	if err := pushBytes(ctx, pusher, configDesc, configBytes); err != nil {
		return nil, fmt.Errorf("push image config: %w", err)
	}
	// The manifest is intentionally last. A failed build can leave only
	// unreferenced blobs and never expose a partially composed image.
	if err := pushBytes(ctx, pusher, manifestDesc, manifestBytes); err != nil {
		return nil, fmt.Errorf("push image manifest: %w", err)
	}

	return &Result{
		PushReference:  joinImageReference(pushRegistry, repository) + "@" + manifestDesc.Digest.String(),
		PullReference:  pullRepositoryRef + "@" + manifestDesc.Digest.String(),
		ManifestDigest: manifestDesc.Digest,
		Platform:       platforms.Normalize(req.Platform),
	}, nil
}

func publicationEndpoints(
	credential *managerregistry.Credential,
	internalRegistry string,
	teamID string,
) (push, pull string, plainHTTP bool, err error) {
	push = strings.Trim(naming.NormalizeRegistryHost(credential.PushRegistry), "/")
	pull = strings.Trim(naming.NormalizeRegistryHost(credential.PullRegistry), "/")
	if pull == "" {
		pull = push
	}
	if strings.EqualFold(strings.TrimSpace(credential.Provider), "builtin") {
		serverRegistry := strings.Trim(naming.NormalizeRegistryHost(internalRegistry), "/")
		if serverRegistry != "" {
			push = naming.TeamScopedImageRegistry(serverRegistry, teamID)
		} else {
			// Preserve compatibility with configs where PullRegistry carried
			// the builtin service endpoint.
			push = pull
		}
		plainHTTP = true
	}
	if push == "" {
		return "", "", false, fmt.Errorf("registry push endpoint is required")
	}
	if pull == "" {
		return "", "", false, fmt.Errorf("registry pull endpoint is required")
	}
	return push, pull, plainHTTP, nil
}

// publicationSourceReference maps a previously advertised builtin image back
// to manager's authenticated internal registry alias for recursive builds.
func publicationSourceReference(
	ref string,
	host string,
	repository string,
	credential *managerregistry.Credential,
	serverPushRegistry string,
	plainHTTP bool,
) (string, string, string, bool, error) {
	sourcePlainHTTP := plainHTTP && sameRegistryHost(host, serverPushRegistry)
	if credential == nil ||
		!strings.EqualFold(strings.TrimSpace(credential.Provider), "builtin") ||
		(!sameRegistryHost(host, credential.PushRegistry) && !sameRegistryHost(host, credential.PullRegistry)) ||
		sameRegistryHost(host, serverPushRegistry) {
		return ref, host, repository, sourcePlainHTTP, nil
	}

	advertisedPrefix := ""
	relativeRepository := ""
	matchedAdvertisedPrefix := false
	for _, advertisedRegistry := range []string{credential.PullRegistry, credential.PushRegistry} {
		if !sameRegistryHost(host, advertisedRegistry) {
			continue
		}
		prefix := registryRepositoryPrefix(advertisedRegistry)
		relative, ok := trimRepositoryPrefix(repository, prefix)
		if ok && (!matchedAdvertisedPrefix || len(prefix) >= len(advertisedPrefix)) {
			matchedAdvertisedPrefix = true
			advertisedPrefix = prefix
			relativeRepository = relative
		}
	}
	if !matchedAdvertisedPrefix {
		return ref, host, repository, sourcePlainHTTP, nil
	}

	authority := registryAuthority(serverPushRegistry)
	if authority == "" {
		return "", "", "", false, fmt.Errorf("builtin server-side registry endpoint is invalid")
	}
	_, pinnedDigest, ok := strings.Cut(ref, "@")
	if !ok || strings.TrimSpace(pinnedDigest) == "" {
		return "", "", "", false, fmt.Errorf("pinned base image reference is invalid")
	}
	internalRepository := joinRepositoryPath(registryRepositoryPrefix(serverPushRegistry), relativeRepository)
	if internalRepository == "" {
		return "", "", "", false, fmt.Errorf("builtin server-side repository is invalid")
	}
	return authority + "/" + internalRepository + "@" + pinnedDigest, authority, internalRepository, plainHTTP, nil
}

func outputRepository(templateID string) string {
	return naming.TemplateNameForCluster(naming.ScopePublic, "", "template-"+strings.TrimSpace(templateID))
}

func joinImageReference(registry, image string) string {
	return strings.TrimRight(registry, "/") + "/" + strings.TrimLeft(image, "/")
}

func normalizeBaseReference(rawRef, rawDigest string) (pinned, host, repository string, err error) {
	named, err := distref.ParseNormalizedNamed(strings.TrimSpace(rawRef))
	if err != nil {
		return "", "", "", fmt.Errorf("parse base image reference: %w", err)
	}
	expected, err := digest.Parse(strings.TrimSpace(rawDigest))
	if err != nil {
		return "", "", "", fmt.Errorf("parse base image digest: %w", err)
	}
	if digested, ok := named.(distref.Digested); ok && digest.Digest(digested.Digest()) != expected {
		return "", "", "", fmt.Errorf("base image reference digest %s does not match captured digest %s", digested.Digest(), expected)
	}
	trimmed := distref.TrimNamed(named)
	host = distref.Domain(trimmed)
	repository = distref.Path(trimmed)
	return trimmed.Name() + "@" + expected.String(), host, repository, nil
}

func resolveBase(ctx context.Context, resolver remotes.Resolver, ref, host, repository string, platform ocispec.Platform) (*resolvedBase, error) {
	_, root, err := resolver.Resolve(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("resolve pinned base image %q: %w", ref, err)
	}
	expected := digest.Digest(strings.TrimSpace(strings.SplitN(ref, "@", 2)[1]))
	if root.Digest != expected {
		return nil, fmt.Errorf("resolved base image digest %s does not match captured digest %s", root.Digest, expected)
	}
	fetcher, err := resolver.Fetcher(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("create base image fetcher: %w", err)
	}

	manifestDesc, err := selectManifest(ctx, fetcher, root, platforms.Normalize(platform), 0)
	if err != nil {
		return nil, err
	}
	manifestBytes, err := fetchSmallBlob(ctx, fetcher, manifestDesc, maxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch base manifest %s: %w", manifestDesc.Digest, err)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode base manifest: %w", err)
	}
	if manifest.SchemaVersion != 2 {
		return nil, fmt.Errorf("base manifest schemaVersion must be 2")
	}
	if manifest.Config.Digest == "" {
		return nil, fmt.Errorf("base manifest has no config descriptor")
	}

	configBytes, err := fetchSmallBlob(ctx, fetcher, manifest.Config, maxConfigBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch base image config %s: %w", manifest.Config.Digest, err)
	}
	var imageConfig ocispec.Image
	if err := json.Unmarshal(configBytes, &imageConfig); err != nil {
		return nil, fmt.Errorf("decode base image config: %w", err)
	}
	if err := validateBaseConfig(imageConfig, manifest, platform); err != nil {
		return nil, err
	}

	return &resolvedBase{
		reference:  ref,
		host:       host,
		repository: repository,
		manifest:   manifest,
		config:     imageConfig,
		fetcher:    fetcher,
	}, nil
}

func selectManifest(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor, platform ocispec.Platform, depth int) (ocispec.Descriptor, error) {
	if depth > 4 {
		return ocispec.Descriptor{}, fmt.Errorf("base image index nesting exceeds limit")
	}
	if images.IsManifestType(desc.MediaType) {
		return desc, nil
	}
	if !images.IsIndexType(desc.MediaType) {
		return ocispec.Descriptor{}, fmt.Errorf("base image descriptor %s has unsupported media type %q", desc.Digest, desc.MediaType)
	}
	indexBytes, err := fetchSmallBlob(ctx, fetcher, desc, maxIndexBytes)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("fetch base image index %s: %w", desc.Digest, err)
	}
	var index ocispec.Index
	if err := json.Unmarshal(indexBytes, &index); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("decode base image index: %w", err)
	}
	matcher := platforms.Only(platform)
	var candidates []ocispec.Descriptor
	for _, candidate := range index.Manifests {
		if candidate.Platform != nil && matcher.Match(*candidate.Platform) {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		return ocispec.Descriptor{}, fmt.Errorf("base image has no manifest for platform %s", platforms.Format(platform))
	}
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && matcher.Less(*candidates[j].Platform, *candidates[j-1].Platform); j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}
	return selectManifest(ctx, fetcher, candidates[0], platform, depth+1)
}

func validateBaseConfig(config ocispec.Image, manifest ocispec.Manifest, platform ocispec.Platform) error {
	if config.RootFS.Type != "layers" {
		return fmt.Errorf("base image rootfs type %q is unsupported", config.RootFS.Type)
	}
	if len(config.RootFS.DiffIDs) != len(manifest.Layers) {
		return fmt.Errorf("base image config has %d diff_ids for %d layers", len(config.RootFS.DiffIDs), len(manifest.Layers))
	}
	for i, diffID := range config.RootFS.DiffIDs {
		if err := diffID.Validate(); err != nil {
			return fmt.Errorf("base image config diff_id %d is invalid: %w", i, err)
		}
	}
	for i, layer := range manifest.Layers {
		if !images.IsLayerType(layer.MediaType) {
			return fmt.Errorf("base image layer %d has unsupported media type %q", i, layer.MediaType)
		}
		if err := layer.Digest.Validate(); err != nil {
			return fmt.Errorf("base image layer %d digest is invalid: %w", i, err)
		}
		if layer.Size < 0 {
			return fmt.Errorf("base image layer %d size must not be negative", i)
		}
	}
	normalized := platforms.Normalize(platform)
	configPlatform := platforms.Normalize(config.Platform)
	if configPlatform.OS != "" && configPlatform.OS != normalized.OS {
		return fmt.Errorf("base image config os %q does not match captured platform %q", configPlatform.OS, normalized.OS)
	}
	if configPlatform.Architecture != "" && configPlatform.Architecture != normalized.Architecture {
		return fmt.Errorf("base image config architecture %q does not match captured platform %q", configPlatform.Architecture, normalized.Architecture)
	}
	if configPlatform.Variant != "" && configPlatform.Variant != normalized.Variant {
		return fmt.Errorf("base image config variant %q does not match captured platform %q", configPlatform.Variant, normalized.Variant)
	}
	return nil
}

func composeImage(req BuildRequest, base *resolvedBase) ([]byte, ocispec.Descriptor, []byte, ocispec.Descriptor, error) {
	config := base.config
	capturedPlatform := platforms.Normalize(req.Platform)
	config.OS = capturedPlatform.OS
	config.Architecture = capturedPlatform.Architecture
	config.Variant = capturedPlatform.Variant
	config.RootFS.DiffIDs = append([]digest.Digest(nil), base.config.RootFS.DiffIDs...)
	config.History = append([]ocispec.History(nil), base.config.History...)
	layers := append([]ocispec.Descriptor(nil), base.manifest.Layers...)

	for i, layer := range req.Layers {
		desc, err := rootFSLayerDescriptor(layer)
		if err != nil {
			return nil, ocispec.Descriptor{}, nil, ocispec.Descriptor{}, err
		}
		diffID, _ := digest.Parse(layer.DiffID)
		layers = append(layers, desc)
		config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, diffID)
		history := ocispec.History{
			CreatedBy: fmt.Sprintf("sandbox0 rootfs checkpoint layer %d", i+1),
			Comment:   "created from sandbox " + strings.TrimSpace(req.SourceSandboxID),
		}
		if !req.CreatedAt.IsZero() {
			createdAt := req.CreatedAt.UTC()
			history.Created = &createdAt
		}
		config.History = append(config.History, history)
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		return nil, ocispec.Descriptor{}, nil, ocispec.Descriptor{}, fmt.Errorf("encode image config: %w", err)
	}
	configDesc := descriptorFromBytes(ocispec.MediaTypeImageConfig, configBytes)
	// Descriptor annotations can describe the config semantically and remain
	// useful after composition. URLs and embedded Data refer to the old digest,
	// so they must not be copied to the newly generated config descriptor.
	configDesc.Annotations = cloneStringMap(base.manifest.Config.Annotations)
	manifest := ocispec.Manifest{
		Versioned:   specs.Versioned{SchemaVersion: 2},
		MediaType:   ocispec.MediaTypeImageManifest,
		Config:      configDesc,
		Layers:      layers,
		Annotations: cloneStringMap(base.manifest.Annotations),
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, ocispec.Descriptor{}, nil, ocispec.Descriptor{}, fmt.Errorf("encode image manifest: %w", err)
	}
	manifestDesc := descriptorFromBytes(ocispec.MediaTypeImageManifest, manifestBytes)
	return configBytes, configDesc, manifestBytes, manifestDesc, nil
}

func rootFSLayerDescriptor(layer Layer) (ocispec.Descriptor, error) {
	dgst, err := digest.Parse(strings.TrimSpace(layer.Digest))
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("parse rootfs layer digest: %w", err)
	}
	if !images.IsLayerType(layer.MediaType) {
		return ocispec.Descriptor{}, fmt.Errorf("rootfs layer %s has unsupported media type %q", layer.ID, layer.MediaType)
	}
	return ocispec.Descriptor{
		MediaType: layer.MediaType,
		Digest:    dgst,
		Size:      layer.Size,
	}, nil
}

// ResolveLayerDiffIDs fills missing DiffIDs from legacy rootfs layer rows.
// Uncompressed layers are already addressed by their DiffID. Compressed layers
// are decompressed and hashed as a stream, without buffering the layer.
func ResolveLayerDiffIDs(ctx context.Context, objects ObjectReader, layers []Layer) ([]Layer, error) {
	out := append([]Layer(nil), layers...)
	for i := range out {
		if strings.TrimSpace(out[i].DiffID) != "" {
			continue
		}
		desc, err := rootFSLayerDescriptor(out[i])
		if err != nil {
			return nil, err
		}
		expectedCompression, ok := layerCompression(desc.MediaType)
		if !ok {
			return nil, fmt.Errorf("rootfs layer %d has unsupported media type %q", i, desc.MediaType)
		}
		if expectedCompression == compression.Uncompressed {
			out[i].DiffID = desc.Digest.String()
			continue
		}
		if objects == nil {
			return nil, fmt.Errorf("rootfs layer %d has no diff_id and object reader is unavailable", i)
		}
		diffID, err := calculateCompressedLayerDiffID(ctx, objects, out[i], desc, expectedCompression)
		if err != nil {
			return nil, fmt.Errorf("calculate legacy rootfs layer %d diff_id: %w", i, err)
		}
		out[i].DiffID = diffID.String()
	}
	return out, nil
}

func layerCompression(mediaType string) (compression.Compression, bool) {
	switch mediaType {
	case ocispec.MediaTypeImageLayer,
		ocispec.MediaTypeImageLayerNonDistributable,
		images.MediaTypeDockerSchema2Layer,
		images.MediaTypeDockerSchema2LayerForeign:
		return compression.Uncompressed, true
	case ocispec.MediaTypeImageLayerGzip,
		ocispec.MediaTypeImageLayerNonDistributableGzip,
		images.MediaTypeDockerSchema2LayerGzip,
		images.MediaTypeDockerSchema2LayerForeignGzip:
		return compression.Gzip, true
	case ocispec.MediaTypeImageLayerZstd,
		ocispec.MediaTypeImageLayerNonDistributableZstd,
		images.MediaTypeDockerSchema2LayerZstd:
		return compression.Zstd, true
	default:
		return compression.Unknown, false
	}
}

func calculateCompressedLayerDiffID(
	ctx context.Context,
	objects ObjectReader,
	layer Layer,
	desc ocispec.Descriptor,
	expectedCompression compression.Compression,
) (digest.Digest, error) {
	reader, err := objects.Get(layer.ObjectKey, 0, layer.Size)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	compressedDigester := digest.Canonical.Digester()
	counted := &countingReader{
		reader: io.TeeReader(io.LimitReader(reader, layer.Size+1), compressedDigester.Hash()),
	}
	decompressed, err := compression.DecompressStream(counted)
	if err != nil {
		return "", fmt.Errorf("open compressed layer: %w", err)
	}
	defer decompressed.Close()
	if actualCompression := decompressed.GetCompression(); actualCompression != expectedCompression {
		return "", fmt.Errorf(
			"layer compression does not match media type %q: got %d, want %d",
			desc.MediaType,
			actualCompression,
			expectedCompression,
		)
	}
	diffDigester := digest.Canonical.Digester()
	if _, err := io.Copy(diffDigester.Hash(), &contextReader{ctx: ctx, reader: decompressed}); err != nil {
		return "", fmt.Errorf("decompress layer: %w", err)
	}
	if counted.read != layer.Size {
		return "", fmt.Errorf("compressed layer size mismatch: got %d, want %d", counted.read, layer.Size)
	}
	if got := compressedDigester.Digest(); got != desc.Digest {
		return "", fmt.Errorf("compressed layer digest mismatch: got %s, want %s", got, desc.Digest)
	}
	return diffDigester.Digest(), nil
}

type countingReader struct {
	reader io.Reader
	read   int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func descriptorFromBytes(mediaType string, data []byte) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(data),
		Size:      int64(len(data)),
	}
}

func fetchSmallBlob(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor, maxSize int64) ([]byte, error) {
	if desc.Size < 0 || desc.Size > maxSize {
		return nil, fmt.Errorf("descriptor size %d exceeds limit %d", desc.Size, maxSize)
	}
	reader, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	payload, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) != desc.Size {
		return nil, fmt.Errorf("descriptor size mismatch: got %d, want %d", len(payload), desc.Size)
	}
	if actual := digest.FromBytes(payload); actual != desc.Digest {
		return nil, fmt.Errorf("descriptor digest mismatch: got %s, want %s", actual, desc.Digest)
	}
	return payload, nil
}

func pushBytes(ctx context.Context, pusher remotes.Pusher, desc ocispec.Descriptor, payload []byte) error {
	return pushBlob(ctx, pusher, desc, func(offset int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload[offset:])), nil
	})
}

func pushBlob(ctx context.Context, pusher remotes.Pusher, desc ocispec.Descriptor, openAt openAtFunc) error {
	writer, err := pusher.Push(ctx, desc)
	if err != nil {
		if errdefs.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	defer writer.Close()
	reader, err := newRestartableReader(ctx, desc.Size, openAt)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := content.Copy(ctx, writer, reader, desc.Size, desc.Digest); err != nil {
		return err
	}
	return nil
}

func remoteBlobOpener(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor) openAtFunc {
	return func(offset int64) (io.ReadCloser, error) {
		reader, err := fetcher.Fetch(ctx, desc)
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			n, discardErr := io.CopyN(io.Discard, reader, offset)
			if discardErr != nil {
				reader.Close()
				return nil, fmt.Errorf("resume remote blob at %d after %d bytes: %w", offset, n, discardErr)
			}
		}
		return &limitedReadCloser{
			Reader: io.LimitReader(reader, desc.Size-offset),
			Closer: reader,
		}, nil
	}
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}

func staticCredentials(expectedHost, username, password string) func(string) (string, string, error) {
	return func(host string) (string, string, error) {
		if sameRegistryHost(host, expectedHost) {
			return username, password, nil
		}
		return "", "", nil
	}
}

func (p *Publisher) sourceCredentials(baseHost string, target *managerregistry.Credential, registryAliases ...string) (func(string) (string, string, error), error) {
	auths := map[string]dockerAuth{}
	if path := strings.TrimSpace(p.registry.PullCredentialsFile); path != "" {
		payload, err := p.readFile(path)
		if err != nil {
			return nil, fmt.Errorf("read registry pull credentials: %w", err)
		}
		var cfg dockerConfig
		if err := json.Unmarshal(payload, &cfg); err != nil {
			return nil, fmt.Errorf("decode registry pull credentials: %w", err)
		}
		auths = cfg.Auths
	}
	return func(host string) (string, string, error) {
		if auth, ok := lookupDockerAuth(auths, host); ok {
			username, password, err := auth.credentials()
			if err != nil {
				return "", "", err
			}
			return username, password, nil
		}
		if sameRegistryHost(host, target.PushRegistry) || sameRegistryHost(host, target.PullRegistry) {
			return target.Username, target.Password, nil
		}
		for _, alias := range registryAliases {
			if sameRegistryHost(host, alias) {
				return target.Username, target.Password, nil
			}
		}
		if sameRegistryHost(host, baseHost) {
			return "", "", nil
		}
		return "", "", nil
	}, nil
}

type dockerConfig struct {
	Auths map[string]dockerAuth `json:"auths"`
}

type dockerAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Auth     string `json:"auth"`
}

func (a dockerAuth) credentials() (string, string, error) {
	if a.Username != "" || a.Password != "" {
		return a.Username, a.Password, nil
	}
	if strings.TrimSpace(a.Auth) == "" {
		return "", "", nil
	}
	decoded, err := base64.StdEncoding.DecodeString(a.Auth)
	if err != nil {
		return "", "", fmt.Errorf("decode docker registry auth: %w", err)
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return "", "", fmt.Errorf("docker registry auth has invalid format")
	}
	return username, password, nil
}

func lookupDockerAuth(auths map[string]dockerAuth, host string) (dockerAuth, bool) {
	for key, auth := range auths {
		if sameRegistryHost(key, host) {
			return auth, true
		}
	}
	return dockerAuth{}, false
}

func sameRegistryHost(a, b string) bool {
	left := normalizedRegistryAuthority(a)
	right := normalizedRegistryAuthority(b)
	return left != "" && left == right
}

func normalizedRegistryAuthority(raw string) string {
	authority := registryAuthority(raw)
	switch authority {
	case "index.docker.io", "registry-1.docker.io":
		return "docker.io"
	default:
		return authority
	}
}

func parseRegistryURL(raw string) (*url.URL, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, fmt.Errorf("registry is empty")
	}
	if !strings.Contains(value, "://") {
		value = "dummy://" + strings.TrimLeft(value, "/")
	}
	return url.Parse(value)
}

func registryAuthority(raw string) string {
	parsed, err := parseRegistryURL(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Host)
}

func registryRepositoryPrefix(raw string) string {
	parsed, err := parseRegistryURL(raw)
	if err != nil {
		return ""
	}
	return strings.Trim(parsed.Path, "/")
}

func trimRepositoryPrefix(repository, prefix string) (string, bool) {
	repository = strings.Trim(repository, "/")
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return repository, true
	}
	if repository == prefix {
		return "", true
	}
	if strings.HasPrefix(repository, prefix+"/") {
		return strings.TrimPrefix(repository, prefix+"/"), true
	}
	return "", false
}

func joinRepositoryPath(prefix, repository string) string {
	prefix = strings.Trim(prefix, "/")
	repository = strings.Trim(repository, "/")
	switch {
	case prefix == "":
		return repository
	case repository == "":
		return prefix
	default:
		return prefix + "/" + repository
	}
}

func registryHostname(raw string) string {
	parsed, err := parseRegistryURL(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
