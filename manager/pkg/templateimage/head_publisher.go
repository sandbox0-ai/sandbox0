package templateimage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const rootFSHeadRepository = "rootfs-heads"

// PublishHead publishes one tiny annotated marker layer. The base image is
// resolved only for runtime configuration and identity; its filesystem is
// reused from the warm node's deterministic base snapshot.
func (p *Publisher) PublishHead(ctx context.Context, req HeadRequest) (*Result, error) {
	if p == nil {
		return nil, fmt.Errorf("rootfs head publisher is required")
	}
	if err := req.validate(); err != nil {
		return nil, err
	}
	annotation, err := rootfshead.EncodeHeadAnnotation(req.Reference)
	if err != nil {
		return nil, err
	}
	markerLayer, err := rootfshead.EncodeMarker(req.Reference)
	if err != nil {
		return nil, err
	}
	markerDesc := descriptorFromBytes(ocispec.MediaTypeImageLayer, markerLayer)
	markerDesc.Annotations = map[string]string{rootfshead.AnnotationHead: annotation}

	repository := rootFSHeadRepository
	tag := "head-" + digest.FromString(req.Reference.HeadID).Encoded()[:24]
	targetImage := repository + ":" + tag
	target, err := p.preparePublication(ctx, req.TeamID, targetImage, repository)
	if err != nil {
		return nil, err
	}
	base, err := p.resolvePublicationBase(ctx, target, req.BaseImageRef, req.BaseImageDigest, req.Platform)
	if err != nil {
		return nil, err
	}

	configBytes, configDesc, manifestBytes, manifestDesc, err := composeHeadImage(req, base, markerDesc)
	if err != nil {
		return nil, err
	}
	if err := pushBytes(ctx, target.pusher, markerDesc, markerLayer); err != nil {
		return nil, fmt.Errorf("push rootfs head marker: %w", err)
	}
	if err := pushBytes(ctx, target.pusher, configDesc, configBytes); err != nil {
		return nil, fmt.Errorf("push rootfs head config: %w", err)
	}
	if err := pushBytes(ctx, target.pusher, manifestDesc, manifestBytes); err != nil {
		return nil, fmt.Errorf("push rootfs head manifest: %w", err)
	}
	return &Result{
		PushReference:  joinImageReference(target.pushRegistry, repository) + "@" + manifestDesc.Digest.String(),
		PullReference:  target.pullRepositoryRef + "@" + manifestDesc.Digest.String(),
		ManifestDigest: manifestDesc.Digest,
		Platform:       req.Platform,
	}, nil
}

func composeHeadImage(req HeadRequest, base *resolvedBase, marker ocispec.Descriptor) ([]byte, ocispec.Descriptor, []byte, ocispec.Descriptor, error) {
	config := base.config
	config.OS = req.Platform.OS
	config.Architecture = req.Platform.Architecture
	config.Variant = req.Platform.Variant
	config.RootFS.DiffIDs = []digest.Digest{marker.Digest}
	config.History = nil
	history := ocispec.History{
		CreatedBy: "sandbox0 metadata-only persistent rootfs head",
		Comment:   "sealed from sandbox " + strings.TrimSpace(req.SandboxID),
	}
	if !req.CreatedAt.IsZero() {
		createdAt := req.CreatedAt.UTC()
		history.Created = &createdAt
	}
	config.History = append(config.History, history)
	configBytes, err := json.Marshal(config)
	if err != nil {
		return nil, ocispec.Descriptor{}, nil, ocispec.Descriptor{}, fmt.Errorf("encode rootfs head config: %w", err)
	}
	configDesc := descriptorFromBytes(ocispec.MediaTypeImageConfig, configBytes)
	configDesc.Annotations = cloneStringMap(base.manifest.Config.Annotations)
	manifest := ocispec.Manifest{
		Versioned:   specs.Versioned{SchemaVersion: 2},
		MediaType:   ocispec.MediaTypeImageManifest,
		Config:      configDesc,
		Layers:      []ocispec.Descriptor{marker},
		Annotations: cloneStringMap(base.manifest.Annotations),
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, ocispec.Descriptor{}, nil, ocispec.Descriptor{}, fmt.Errorf("encode rootfs head manifest: %w", err)
	}
	manifestDesc := descriptorFromBytes(ocispec.MediaTypeImageManifest, manifestBytes)
	return configBytes, configDesc, manifestBytes, manifestDesc, nil
}

func pushBaseLayers(ctx context.Context, pusher remotes.Pusher, base *resolvedBase, pushRegistry string) error {
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
		if err := pushBlob(ctx, pusher, pushDesc, remoteBlobOpener(ctx, base.fetcher, layer)); err != nil {
			return fmt.Errorf("push base layer %s: %w", layer.Digest, err)
		}
	}
	return nil
}
