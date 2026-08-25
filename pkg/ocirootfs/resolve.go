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

// Package ocirootfs resolves and safely applies one digest-pinned OCI image
// for production RootFS base artifact construction.
package ocirootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/platforms"
	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	defaultMaxIndexBytes       = 4 << 20
	defaultMaxManifestBytes    = 8 << 20
	defaultMaxConfigBytes      = 16 << 20
	defaultMaxIndexDescriptors = 1024
	defaultMaxIndexDepth       = 4
	defaultMaxLayers           = 512
	defaultMaxLayerBytes       = 8 << 30
	defaultMaxImageBytes       = 64 << 30
	maxDockerHealthcheckBytes  = 64 << 10
	maxDockerHealthcheckItems  = 1024
	maxDockerHealthcheckString = 4096
)

var platformPartPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

// Limits bounds all remote metadata and compressed image content before any
// layer is applied.
type Limits struct {
	MaxIndexBytes       int64
	MaxManifestBytes    int64
	MaxConfigBytes      int64
	MaxIndexDescriptors int
	MaxIndexDepth       int
	MaxLayers           int
	MaxLayerBytes       int64
	MaxImageBytes       int64
	MaxLayerUnpacked    int64
	MaxImageUnpacked    int64
	MaxFileBytes        int64
	MaxFiles            int
	MaxPathBytes        int
	MaxPAXRecords       int
	MaxPAXBytes         int
}

// Request identifies one immutable source image and the exact production
// procd artifact to inject into its unpacked root filesystem.
type Request struct {
	Reference           string
	Platform            ocispec.Platform
	WorkRoot            string
	ProcdPath           string
	ExpectedProcdDigest digest.Digest
}

// Result contains only non-secret immutable evidence from one successful
// import. RootPath remains owned by the caller and must be removed after block
// artifact construction.
type Result struct {
	Reference      string
	SourceDigest   digest.Digest
	ManifestDigest digest.Digest
	ConfigDigest   digest.Digest
	Platform       ocispec.Platform
	LayerDigests   []digest.Digest
	DiffIDs        []digest.Digest
	ProcdDigest    digest.Digest
	RootPath       string
	UnpackedBytes  int64
	Files          int
}

func normalizeLimits(value Limits) (Limits, error) {
	if value.MaxIndexBytes == 0 {
		value.MaxIndexBytes = defaultMaxIndexBytes
	}
	if value.MaxManifestBytes == 0 {
		value.MaxManifestBytes = defaultMaxManifestBytes
	}
	if value.MaxConfigBytes == 0 {
		value.MaxConfigBytes = defaultMaxConfigBytes
	}
	if value.MaxIndexDescriptors == 0 {
		value.MaxIndexDescriptors = defaultMaxIndexDescriptors
	}
	if value.MaxIndexDepth == 0 {
		value.MaxIndexDepth = defaultMaxIndexDepth
	}
	if value.MaxLayers == 0 {
		value.MaxLayers = defaultMaxLayers
	}
	if value.MaxLayerBytes == 0 {
		value.MaxLayerBytes = defaultMaxLayerBytes
	}
	if value.MaxImageBytes == 0 {
		value.MaxImageBytes = defaultMaxImageBytes
	}
	if value.MaxLayerUnpacked == 0 {
		value.MaxLayerUnpacked = 64 << 30
	}
	if value.MaxImageUnpacked == 0 {
		value.MaxImageUnpacked = 256 << 30
	}
	if value.MaxFileBytes == 0 {
		value.MaxFileBytes = 32 << 30
	}
	if value.MaxFiles == 0 {
		value.MaxFiles = 10_000_000
	}
	if value.MaxPathBytes == 0 {
		value.MaxPathBytes = 4096
	}
	if value.MaxPAXRecords == 0 {
		value.MaxPAXRecords = 128
	}
	if value.MaxPAXBytes == 0 {
		value.MaxPAXBytes = 256 << 10
	}
	for name, limit := range map[string]int64{
		"max_index_bytes": value.MaxIndexBytes, "max_manifest_bytes": value.MaxManifestBytes,
		"max_config_bytes": value.MaxConfigBytes, "max_layer_bytes": value.MaxLayerBytes,
		"max_image_bytes": value.MaxImageBytes, "max_layer_unpacked": value.MaxLayerUnpacked,
		"max_image_unpacked": value.MaxImageUnpacked, "max_file_bytes": value.MaxFileBytes,
	} {
		if limit <= 0 {
			return Limits{}, fmt.Errorf("%s must be positive", name)
		}
	}
	for name, limit := range map[string]int{
		"max_index_descriptors": value.MaxIndexDescriptors, "max_index_depth": value.MaxIndexDepth,
		"max_layers": value.MaxLayers, "max_files": value.MaxFiles,
		"max_path_bytes": value.MaxPathBytes, "max_pax_records": value.MaxPAXRecords,
		"max_pax_bytes": value.MaxPAXBytes,
	} {
		if limit <= 0 {
			return Limits{}, fmt.Errorf("%s must be positive", name)
		}
	}
	if value.MaxLayerBytes > value.MaxImageBytes || value.MaxLayerUnpacked > value.MaxImageUnpacked ||
		value.MaxFileBytes > value.MaxLayerUnpacked {
		return Limits{}, fmt.Errorf("per-layer and per-file limits must not exceed their image limits")
	}
	return value, nil
}

type resolvedImage struct {
	reference      string
	sourceDigest   digest.Digest
	manifestDigest digest.Digest
	configDigest   digest.Digest
	platform       ocispec.Platform
	manifest       ocispec.Manifest
	config         ocispec.Image
	fetcher        remotes.Fetcher
}

func resolveImage(
	ctx context.Context,
	resolver remotes.Resolver,
	reference string,
	platform ocispec.Platform,
	limits Limits,
) (*resolvedImage, error) {
	if resolver == nil {
		return nil, fmt.Errorf("OCI resolver is required")
	}
	pinned, sourceDigest, err := normalizePinnedReference(reference)
	if err != nil {
		return nil, err
	}
	platform, err = normalizePlatform(platform)
	if err != nil {
		return nil, err
	}
	_, root, err := resolver.Resolve(ctx, pinned)
	if err != nil {
		return nil, fmt.Errorf("resolve pinned OCI image %q: %w", pinned, err)
	}
	if root.Digest != sourceDigest {
		return nil, fmt.Errorf("resolved OCI digest %s does not match pinned digest %s", root.Digest, sourceDigest)
	}
	fetcher, err := resolver.Fetcher(ctx, pinned)
	if err != nil {
		return nil, fmt.Errorf("create OCI fetcher: %w", err)
	}
	if fetcher == nil {
		return nil, fmt.Errorf("OCI resolver returned a nil fetcher")
	}
	manifestDescriptor, err := selectManifest(ctx, fetcher, root, platform, limits, 0)
	if err != nil {
		return nil, err
	}
	manifestPayload, err := fetchVerifiedBytes(ctx, fetcher, manifestDescriptor, limits.MaxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch OCI manifest %s: %w", manifestDescriptor.Digest, err)
	}
	var manifest ocispec.Manifest
	if err := decodeStrictJSON(manifestPayload, &manifest); err != nil {
		return nil, fmt.Errorf("decode OCI manifest: %w", err)
	}
	if manifest.SchemaVersion != 2 {
		return nil, fmt.Errorf("OCI manifest schemaVersion must be 2")
	}
	if manifest.MediaType != "" && !images.IsManifestType(manifest.MediaType) {
		return nil, fmt.Errorf("OCI manifest declares unsupported media type %q", manifest.MediaType)
	}
	if manifest.Config.Digest == "" {
		return nil, fmt.Errorf("OCI manifest has no config descriptor")
	}
	if !images.IsConfigType(manifest.Config.MediaType) {
		return nil, fmt.Errorf("OCI config has unsupported media type %q", manifest.Config.MediaType)
	}
	if len(manifest.Layers) == 0 || len(manifest.Layers) > limits.MaxLayers {
		return nil, fmt.Errorf("OCI manifest must contain 1..%d layers", limits.MaxLayers)
	}
	configPayload, err := fetchVerifiedBytes(ctx, fetcher, manifest.Config, limits.MaxConfigBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch OCI config %s: %w", manifest.Config.Digest, err)
	}
	config, err := decodeImageConfig(configPayload)
	if err != nil {
		return nil, fmt.Errorf("decode OCI config: %w", err)
	}
	if err := validateImage(config, manifest, platform, limits); err != nil {
		return nil, err
	}
	return &resolvedImage{
		reference: pinned, sourceDigest: sourceDigest,
		manifestDigest: manifestDescriptor.Digest, configDigest: manifest.Config.Digest,
		platform: platform, manifest: manifest, config: config, fetcher: fetcher,
	}, nil
}

// Docker emits Healthcheck in otherwise valid OCI image configs. RootFS import
// does not consume execution health checks, but accepting this one bounded,
// strictly decoded extension avoids rejecting common Linux image filesystems.
type compatibleImageConfigDocument struct {
	Created *time.Time `json:"created,omitempty"`
	Author  string     `json:"author,omitempty"`
	ocispec.Platform
	Config  json.RawMessage   `json:"config,omitempty"`
	RootFS  ocispec.RootFS    `json:"rootfs"`
	History []ocispec.History `json:"history,omitempty"`
}

type compatibleDockerImageConfig struct {
	ocispec.ImageConfig
	Healthcheck json.RawMessage `json:"Healthcheck,omitempty"`
}

type dockerHealthcheck struct {
	Test          []string `json:"Test,omitempty"`
	Interval      int64    `json:"Interval,omitempty"`
	Timeout       int64    `json:"Timeout,omitempty"`
	StartPeriod   int64    `json:"StartPeriod,omitempty"`
	StartInterval int64    `json:"StartInterval,omitempty"`
	Retries       int      `json:"Retries,omitempty"`
}

func decodeImageConfig(payload []byte) (ocispec.Image, error) {
	var document compatibleImageConfigDocument
	if err := decodeStrictJSON(payload, &document); err != nil {
		return ocispec.Image{}, err
	}
	if len(document.Config) == 0 {
		document.Config = []byte("{}")
	}
	var config compatibleDockerImageConfig
	if err := decodeStrictJSON(document.Config, &config); err != nil {
		return ocispec.Image{}, err
	}
	var healthcheck *dockerHealthcheck
	if len(config.Healthcheck) > 0 && string(config.Healthcheck) != "null" {
		if len(config.Healthcheck) > maxDockerHealthcheckBytes {
			return ocispec.Image{}, fmt.Errorf("Docker healthcheck exceeds configured bounds")
		}
		healthcheck = &dockerHealthcheck{}
		if err := decodeStrictJSON(config.Healthcheck, healthcheck); err != nil {
			return ocispec.Image{}, err
		}
	}
	if err := validateDockerHealthcheck(healthcheck); err != nil {
		return ocispec.Image{}, err
	}
	return ocispec.Image{
		Created: document.Created, Author: document.Author, Platform: document.Platform,
		Config: config.ImageConfig, RootFS: document.RootFS, History: document.History,
	}, nil
}

func validateDockerHealthcheck(healthcheck *dockerHealthcheck) error {
	if healthcheck == nil {
		return nil
	}
	if len(healthcheck.Test) > maxDockerHealthcheckItems || healthcheck.Retries < 0 ||
		healthcheck.Retries > 1_000_000 || healthcheck.Interval < 0 || healthcheck.Timeout < 0 ||
		healthcheck.StartPeriod < 0 || healthcheck.StartInterval < 0 {
		return fmt.Errorf("Docker healthcheck exceeds configured bounds")
	}
	total := 0
	for _, item := range healthcheck.Test {
		if len(item) > maxDockerHealthcheckString {
			return fmt.Errorf("Docker healthcheck exceeds configured bounds")
		}
		total += len(item)
		if total > maxDockerHealthcheckBytes {
			return fmt.Errorf("Docker healthcheck exceeds configured bounds")
		}
	}
	return nil
}

func normalizePinnedReference(raw string) (string, digest.Digest, error) {
	trimmed := strings.TrimSpace(raw)
	if raw != trimmed {
		return "", "", fmt.Errorf("OCI image reference must not contain surrounding whitespace")
	}
	named, err := distref.ParseNormalizedNamed(trimmed)
	if err != nil {
		return "", "", fmt.Errorf("parse OCI image reference: %w", err)
	}
	digested, ok := named.(distref.Digested)
	if !ok {
		return "", "", fmt.Errorf("OCI image reference must be digest-pinned")
	}
	value := digest.Digest(digested.Digest())
	if err := validateSHA256Digest(value); err != nil {
		return "", "", fmt.Errorf("OCI image digest: %w", err)
	}
	return named.String(), value, nil
}

func normalizePlatform(value ocispec.Platform) (ocispec.Platform, error) {
	if value.OSVersion != "" || len(value.OSFeatures) != 0 {
		return ocispec.Platform{}, fmt.Errorf("OCI platform os.version and os.features are unsupported")
	}
	if value.OS != "linux" {
		return ocispec.Platform{}, fmt.Errorf("OCI platform operating system must be linux")
	}
	if !platformPartPattern.MatchString(value.Architecture) ||
		(value.Variant != "" && !platformPartPattern.MatchString(value.Variant)) {
		return ocispec.Platform{}, fmt.Errorf("OCI platform architecture or variant is not canonical")
	}
	normalized := platforms.Normalize(value)
	if !platformPartPattern.MatchString(normalized.Architecture) ||
		(normalized.Variant != "" && !platformPartPattern.MatchString(normalized.Variant)) {
		return ocispec.Platform{}, fmt.Errorf("OCI platform architecture or variant is not canonical")
	}
	if value.OS != normalized.OS || value.Architecture != normalized.Architecture {
		return ocispec.Platform{}, fmt.Errorf("OCI platform must use canonical %s", platforms.Format(normalized))
	}
	return normalized, nil
}

func selectManifest(
	ctx context.Context,
	fetcher remotes.Fetcher,
	descriptor ocispec.Descriptor,
	platform ocispec.Platform,
	limits Limits,
	depth int,
) (ocispec.Descriptor, error) {
	if depth > limits.MaxIndexDepth {
		return ocispec.Descriptor{}, fmt.Errorf("OCI index nesting exceeds %d", limits.MaxIndexDepth)
	}
	if images.IsManifestType(descriptor.MediaType) {
		return descriptor, nil
	}
	if !images.IsIndexType(descriptor.MediaType) {
		return ocispec.Descriptor{}, fmt.Errorf("OCI descriptor %s has unsupported media type %q", descriptor.Digest, descriptor.MediaType)
	}
	payload, err := fetchVerifiedBytes(ctx, fetcher, descriptor, limits.MaxIndexBytes)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("fetch OCI index %s: %w", descriptor.Digest, err)
	}
	var index ocispec.Index
	if err := decodeStrictJSON(payload, &index); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("decode OCI index: %w", err)
	}
	if index.SchemaVersion != 2 {
		return ocispec.Descriptor{}, fmt.Errorf("OCI index schemaVersion must be 2")
	}
	if index.MediaType != "" && !images.IsIndexType(index.MediaType) {
		return ocispec.Descriptor{}, fmt.Errorf("OCI index declares unsupported media type %q", index.MediaType)
	}
	if len(index.Manifests) == 0 || len(index.Manifests) > limits.MaxIndexDescriptors {
		return ocispec.Descriptor{}, fmt.Errorf("OCI index must contain 1..%d descriptors", limits.MaxIndexDescriptors)
	}
	matcher := platforms.Only(platform)
	candidates := make([]ocispec.Descriptor, 0, len(index.Manifests))
	for _, candidate := range index.Manifests {
		if candidate.Platform != nil && matcher.Match(*candidate.Platform) {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		return ocispec.Descriptor{}, fmt.Errorf("OCI image has no manifest for platform %s", platforms.Format(platform))
	}
	slices.SortStableFunc(candidates, func(left, right ocispec.Descriptor) int {
		if matcher.Less(*left.Platform, *right.Platform) {
			return -1
		}
		if matcher.Less(*right.Platform, *left.Platform) {
			return 1
		}
		return strings.Compare(left.Digest.String(), right.Digest.String())
	})
	return selectManifest(ctx, fetcher, candidates[0], platform, limits, depth+1)
}

func validateImage(config ocispec.Image, manifest ocispec.Manifest, platform ocispec.Platform, limits Limits) error {
	if config.RootFS.Type != "layers" {
		return fmt.Errorf("OCI rootfs type %q is unsupported", config.RootFS.Type)
	}
	if len(config.RootFS.DiffIDs) != len(manifest.Layers) {
		return fmt.Errorf("OCI config has %d diff_ids for %d layers", len(config.RootFS.DiffIDs), len(manifest.Layers))
	}
	configuredPlatform := platforms.Normalize(config.Platform)
	if config.OSVersion != "" || len(config.OSFeatures) != 0 {
		return fmt.Errorf("OCI config os.version and os.features are unsupported")
	}
	if configuredPlatform.OS != "" && configuredPlatform.OS != platform.OS ||
		configuredPlatform.Architecture != "" && configuredPlatform.Architecture != platform.Architecture ||
		configuredPlatform.Variant != "" && configuredPlatform.Variant != platform.Variant {
		return fmt.Errorf("OCI config platform does not match selected platform %s", platforms.Format(platform))
	}
	var compressedBytes int64
	for index, layer := range manifest.Layers {
		if _, err := expectedLayerCompression(layer.MediaType); err != nil {
			return fmt.Errorf("OCI layer %d has unsupported media type %q", index, layer.MediaType)
		}
		if err := validateSHA256Digest(layer.Digest); err != nil {
			return fmt.Errorf("OCI layer %d digest: %w", index, err)
		}
		if layer.Size <= 0 || layer.Size > limits.MaxLayerBytes || compressedBytes > limits.MaxImageBytes-layer.Size {
			return fmt.Errorf("OCI layer %d or cumulative compressed size exceeds configured bounds", index)
		}
		compressedBytes += layer.Size
		if err := validateSHA256Digest(config.RootFS.DiffIDs[index]); err != nil {
			return fmt.Errorf("OCI layer %d diff_id: %w", index, err)
		}
	}
	return nil
}

func expectedLayerCompression(mediaType string) (string, error) {
	switch mediaType {
	case ocispec.MediaTypeImageLayer, ocispec.MediaTypeImageLayerNonDistributable,
		images.MediaTypeDockerSchema2Layer, images.MediaTypeDockerSchema2LayerForeign:
		return "none", nil
	case ocispec.MediaTypeImageLayerGzip, ocispec.MediaTypeImageLayerNonDistributableGzip,
		images.MediaTypeDockerSchema2LayerGzip, images.MediaTypeDockerSchema2LayerForeignGzip:
		return "gzip", nil
	case ocispec.MediaTypeImageLayerZstd, ocispec.MediaTypeImageLayerNonDistributableZstd,
		images.MediaTypeDockerSchema2LayerZstd:
		return "zstd", nil
	default:
		return "", fmt.Errorf("unsupported OCI layer media type %q", mediaType)
	}
}

func fetchVerifiedBytes(
	ctx context.Context,
	fetcher remotes.Fetcher,
	descriptor ocispec.Descriptor,
	maxBytes int64,
) ([]byte, error) {
	if fetcher == nil {
		return nil, fmt.Errorf("OCI fetcher is required")
	}
	if err := validateSHA256Digest(descriptor.Digest); err != nil {
		return nil, err
	}
	if descriptor.Size <= 0 || descriptor.Size > maxBytes {
		return nil, fmt.Errorf("descriptor size %d exceeds 1..%d", descriptor.Size, maxBytes)
	}
	reader, err := fetcher.Fetch(ctx, descriptor)
	if err != nil {
		return nil, err
	}
	payload, err := io.ReadAll(io.LimitReader(reader, descriptor.Size+1))
	closeErr := reader.Close()
	if err := errors.Join(err, closeErr); err != nil {
		return nil, err
	}
	if int64(len(payload)) != descriptor.Size {
		return nil, fmt.Errorf("descriptor size is %d, expected %d", len(payload), descriptor.Size)
	}
	if actual := digest.FromBytes(payload); actual != descriptor.Digest {
		return nil, fmt.Errorf("descriptor digest is %s, expected %s", actual, descriptor.Digest)
	}
	return payload, nil
}

func decodeStrictJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("document must contain exactly one JSON value")
	}
	return nil
}

func validateSHA256Digest(value digest.Digest) error {
	if err := value.Validate(); err != nil || value.Algorithm() != digest.SHA256 || value.String() != strings.TrimSpace(value.String()) {
		return fmt.Errorf("value must be a canonical sha256 digest")
	}
	return nil
}
