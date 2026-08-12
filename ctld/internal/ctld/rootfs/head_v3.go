package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	distref "github.com/distribution/reference"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	criManagedImageLabel = "io.cri-containerd.image"
	criManagedImageValue = "managed"
	rootFSHeadImageLabel = "sandbox0.ai/rootfs-head-v3"
)

type materializerClient interface {
	containerdClient
	LeasesService() leases.Manager
	GetImage(context.Context, string) (containerd.Image, error)
}

func (r *ContainerdRuntime) ActiveUpperdir(ctx context.Context, info ctldapi.RootFSInfo) (string, error) {
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return "", err
	}
	defer closeClient()
	return r.activeOverlayUpperdir(ctx, client, info)
}

func (r *ContainerdRuntime) BaseIdentityAndConfig(
	ctx context.Context,
	info ctldapi.RootFSInfo,
	expected *rootfshead.BaseIdentity,
) (rootfshead.BaseIdentity, []byte, error) {
	if expected != nil {
		if err := expected.Validate(); err != nil {
			return rootfshead.BaseIdentity{}, nil, err
		}
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return rootfshead.BaseIdentity{}, nil, err
	}
	defer closeClient()
	if expected != nil {
		configData, err := publishedBaseConfig(ctx, client, info, *expected)
		if err != nil {
			return rootfshead.BaseIdentity{}, nil, err
		}
		return *expected, configData, nil
	}

	imageReference := strings.TrimSpace(info.BaseImageRef)
	record, config, configData, err := imageConfig(ctx, client, imageReference, platforms.DefaultStrict())
	if err != nil {
		return rootfshead.BaseIdentity{}, nil, err
	}
	if !platforms.DefaultStrict().Match(config.Platform) {
		return rootfshead.BaseIdentity{}, nil, fmt.Errorf("rootfs base image platform %s does not match node platform %s", platforms.Format(config.Platform), platforms.Format(platforms.DefaultSpec()))
	}
	if len(config.RootFS.DiffIDs) == 0 {
		return rootfshead.BaseIdentity{}, nil, fmt.Errorf("rootfs base image %s has no diff IDs", imageReference)
	}
	base := rootfshead.BaseIdentity{
		ImageReference: record.Name,
		ManifestDigest: record.Target.Digest.String(),
		ChainID:        identity.ChainID(config.RootFS.DiffIDs).String(),
		OS:             config.OS,
		Architecture:   config.Architecture,
		Variant:        config.Variant,
	}
	if err := base.Validate(); err != nil {
		return rootfshead.BaseIdentity{}, nil, err
	}
	if parent := strings.TrimSpace(info.SnapshotParent); parent != "" && parent != base.ChainID {
		return rootfshead.BaseIdentity{}, nil, fmt.Errorf("active rootfs parent %s does not match base chain %s", parent, base.ChainID)
	}
	return base, configData, nil
}

// publishedBaseConfig preserves the OCI config carried by the running Head
// marker and verifies the canonical base through snapshot metadata. A local
// image record target is not a portable base identity: registry pulls may
// retain an image index while Docker archive imports synthesize a manifest.
func publishedBaseConfig(
	ctx context.Context,
	client containerdClient,
	info ctldapi.RootFSInfo,
	expected rootfshead.BaseIdentity,
) ([]byte, error) {
	expectedPlatform := ocispec.Platform{
		OS:           expected.OS,
		Architecture: expected.Architecture,
		Variant:      expected.Variant,
	}
	record, config, configData, err := imageConfig(ctx, client, info.BaseImageRef, platforms.OnlyStrict(expectedPlatform))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(record.Labels[rootFSHeadImageLabel]) == "" {
		return nil, fmt.Errorf("runtime image %s is not a materialized rootfs Head", info.BaseImageRef)
	}
	if !platforms.OnlyStrict(expectedPlatform).Match(config.Platform) {
		return nil, fmt.Errorf("rootfs Head image platform %s does not match published base platform %s", platforms.Format(config.Platform), platforms.Format(expectedPlatform))
	}
	parent := strings.TrimSpace(info.SnapshotParent)
	if parent == "" {
		return nil, fmt.Errorf("rootfs Head runtime has no committed marker parent")
	}
	snapshotter := client.SnapshotService(info.Snapshotter)
	if snapshotter == nil {
		return nil, fmt.Errorf("rootfs snapshotter %q is unavailable", info.Snapshotter)
	}
	marker, err := snapshotter.Stat(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("inspect rootfs Head marker snapshot %s: %w", parent, err)
	}
	if marker.Kind != snapshots.KindCommitted {
		return nil, fmt.Errorf("rootfs Head marker snapshot %s is not committed", parent)
	}
	declaredBase := strings.TrimSpace(marker.Labels[rootfshead.LabelBaseChainID])
	if declaredBase != expected.ChainID {
		return nil, fmt.Errorf("rootfs Head marker base is %q, expected %q", declaredBase, expected.ChainID)
	}
	return configData, nil
}

func (r *ContainerdRuntime) MaterializeRootFSHead(
	ctx context.Context,
	reference rootfshead.HeadReference,
	base rootfshead.BaseIdentity,
	image rootfshead.ImageReference,
	targetImageName string,
	envelopePayload []byte,
	marker []byte,
) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	if err := image.Validate(); err != nil {
		return err
	}
	if err := base.Validate(); err != nil {
		return err
	}
	targetImageName = strings.TrimSpace(targetImageName)
	if targetImageName == "" {
		targetImageName = image.Name
	}
	envelope, err := rootfshead.DecodeImageEnvelope(envelopePayload)
	if err != nil {
		return err
	}
	composed := rootfshead.ComposedImage{
		Reference:       image,
		Envelope:        envelope,
		MarkerPayload:   marker,
		EnvelopePayload: envelopePayload,
	}
	prefix, err := rootfsstore.PrefixFromObject(reference.Manifest)
	if err != nil {
		return err
	}
	if err := rootfshead.ValidateComposedImage(prefix, reference, composed); err != nil {
		return err
	}
	rawClient, closeClient, err := r.client(ctx)
	if err != nil {
		return err
	}
	defer closeClient()
	client, ok := rawClient.(materializerClient)
	if !ok {
		return fmt.Errorf("containerd client does not support rootfs Head materialization")
	}
	ctx = namespaces.WithNamespace(ctx, r.namespace)
	snapshotter := client.SnapshotService(rootfshead.SnapshotterName)
	baseSnapshot, err := snapshotter.Stat(ctx, base.ChainID)
	if err != nil {
		return fmt.Errorf("canonical rootfs base snapshot %s is not available: %w", base.ChainID, err)
	}
	if baseSnapshot.Kind != snapshots.KindCommitted {
		return fmt.Errorf("canonical rootfs base snapshot %s is not committed", base.ChainID)
	}
	lease, err := client.LeasesService().Create(ctx, leases.WithRandomID(), leases.WithExpiration(5*time.Minute))
	if err != nil {
		return fmt.Errorf("create rootfs Head content lease: %w", err)
	}
	leaseCtx := leases.WithLease(ctx, lease.ID)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = client.LeasesService().Delete(cleanup, lease)
	}()
	var manifest ocispec.Manifest
	if err := json.Unmarshal(envelope.ManifestData, &manifest); err != nil {
		return err
	}
	blobs := []struct {
		name       string
		descriptor ocispec.Descriptor
		payload    []byte
	}{
		{name: "marker", descriptor: manifest.Layers[0], payload: marker},
		{name: "config", descriptor: envelope.Config, payload: envelope.ConfigData},
		{name: "manifest", descriptor: envelope.Manifest, payload: envelope.ManifestData},
	}
	for _, blob := range blobs {
		writeRef := "sandbox0-rootfs-v3-" + blob.name + "-" + blob.descriptor.Digest.Encoded()
		if err := content.WriteBlob(leaseCtx, client.ContentStore(), writeRef, bytes.NewReader(blob.payload), blob.descriptor); err != nil {
			return fmt.Errorf("write rootfs Head %s content: %w", blob.name, err)
		}
	}
	if err := images.WalkNotEmpty(leaseCtx, images.SetChildrenLabels(client.ContentStore(), images.ChildrenHandler(client.ContentStore())), envelope.Manifest); err != nil {
		return fmt.Errorf("protect rootfs Head image content: %w", err)
	}
	record := images.Image{
		Name:   targetImageName,
		Target: envelope.Manifest,
		Labels: map[string]string{criManagedImageLabel: criManagedImageValue, rootFSHeadImageLabel: reference.HeadID},
	}
	created, err := client.ImageService().Create(leaseCtx, record)
	if errdefs.IsAlreadyExists(err) {
		created, err = client.ImageService().Get(leaseCtx, targetImageName)
		if err == nil && created.Target.Digest != envelope.Manifest.Digest {
			return fmt.Errorf("local rootfs Head image %s has manifest %s, expected %s", targetImageName, created.Target.Digest, envelope.Manifest.Digest)
		}
		if err == nil {
			created.Labels = record.Labels
			created, err = client.ImageService().Update(leaseCtx, created, "labels")
		}
	}
	if err != nil {
		return fmt.Errorf("register local rootfs Head image: %w", err)
	}
	var config ocispec.Image
	if err := json.Unmarshal(envelope.ConfigData, &config); err != nil {
		return fmt.Errorf("decode rootfs Head image config: %w", err)
	}
	markerSnapshot := identity.ChainID(config.RootFS.DiffIDs).String()
	annotation, err := rootfshead.EncodeHeadAnnotation(reference)
	if err != nil {
		return err
	}
	activeMarker := "sandbox0-rootfs-v3-marker-" + manifest.Layers[0].Digest.Encoded() + "-" + lease.ID
	if err := ensureRootFSHeadSnapshot(leaseCtx, snapshotter, markerSnapshot, base.ChainID, annotation, activeMarker); err != nil {
		return fmt.Errorf("materialize rootfs Head snapshot: %w", err)
	}
	localImage, err := client.GetImage(leaseCtx, created.Name)
	if err != nil {
		return err
	}
	if err := localImage.Unpack(leaseCtx, rootfshead.SnapshotterName); err != nil {
		return fmt.Errorf("unpack local rootfs Head image: %w", err)
	}
	if err := r.waitForCRIImage(leaseCtx, targetImageName); err != nil {
		return err
	}
	return nil
}

// EnsureBaseImage verifies that the fixed platform carrier image exists and
// unpacks its canonical snapshot into the external snapshotter delegate.
func (r *ContainerdRuntime) EnsureBaseImage(ctx context.Context, imageReference string) (rootfshead.BaseIdentity, error) {
	info := ctldapi.RootFSInfo{BaseImageRef: strings.TrimSpace(imageReference)}
	base, _, err := r.BaseIdentityAndConfig(ctx, info, nil)
	if err != nil {
		return rootfshead.BaseIdentity{}, err
	}
	rawClient, closeClient, err := r.client(ctx)
	if err != nil {
		return rootfshead.BaseIdentity{}, err
	}
	defer closeClient()
	client, ok := rawClient.(materializerClient)
	if !ok {
		return rootfshead.BaseIdentity{}, fmt.Errorf("containerd client does not support carrier base unpack")
	}
	ctx = namespaces.WithNamespace(ctx, r.namespace)
	image, err := client.GetImage(ctx, base.ImageReference)
	if err != nil {
		return rootfshead.BaseIdentity{}, fmt.Errorf("load carrier base image: %w", err)
	}
	if err := image.Unpack(ctx, rootfshead.SnapshotterName); err != nil {
		return rootfshead.BaseIdentity{}, fmt.Errorf("unpack carrier base image: %w", err)
	}
	return base, nil
}

// waitForCRIImage closes the registration race between containerd's image
// store and the CRI image service observed by kubelet.
func (r *ContainerdRuntime) waitForCRIImage(ctx context.Context, image string) error {
	client, err := r.imageClient(ctx)
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, requestErr := client.ImageStatus(waitCtx, &runtimeapi.ImageStatusRequest{
			Image: &runtimeapi.ImageSpec{Image: image},
		})
		if requestErr == nil && response != nil && response.Image != nil {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if requestErr != nil {
				return fmt.Errorf("wait for local rootfs Head image in CRI: %w", requestErr)
			}
			return fmt.Errorf("wait for local rootfs Head image in CRI: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// ensureRootFSHeadSnapshot explicitly commits the marker snapshot with its
// durable base and Head metadata. Containerd does not inherit arbitrary OCI
// descriptor annotations into snapshot labels, but Unpack reliably reuses an
// existing committed ChainID.
func ensureRootFSHeadSnapshot(
	ctx context.Context,
	snapshotter snapshots.Snapshotter,
	name string,
	parent string,
	annotation string,
	activeKey string,
) (retErr error) {
	validate := func(info snapshots.Info) error {
		if info.Kind != snapshots.KindCommitted {
			return fmt.Errorf("rootfs Head snapshot %s is not committed", name)
		}
		if info.Parent != parent {
			return fmt.Errorf("rootfs Head snapshot %s has parent %q, expected %q", name, info.Parent, parent)
		}
		if strings.TrimSpace(info.Labels[rootfshead.AnnotationHead]) != annotation {
			return fmt.Errorf("rootfs Head snapshot %s has conflicting Head metadata", name)
		}
		if strings.TrimSpace(info.Labels[rootfshead.LabelBaseChainID]) != parent {
			return fmt.Errorf("rootfs Head snapshot %s has conflicting base metadata", name)
		}
		return nil
	}
	if existing, err := snapshotter.Stat(ctx, name); err == nil {
		return validate(existing)
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspect rootfs Head snapshot %s: %w", name, err)
	}

	prepared := false
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err := snapshotter.Remove(cleanupCtx, activeKey)
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	defer func() {
		if prepared {
			retErr = errors.Join(retErr, cleanup())
		}
	}()
	if _, err := snapshotter.Prepare(ctx, activeKey, parent, snapshots.WithLabels(map[string]string{
		rootfshead.AnnotationHead:   annotation,
		rootfshead.LabelBaseChainID: parent,
	})); err != nil {
		return fmt.Errorf("prepare rootfs Head marker snapshot: %w", err)
	}
	prepared = true
	if err := snapshotter.Commit(ctx, name, activeKey); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return fmt.Errorf("commit rootfs Head marker snapshot: %w", err)
		}
		if cleanupErr := cleanup(); cleanupErr != nil {
			return errors.Join(fmt.Errorf("rootfs Head snapshot %s was committed concurrently", name), cleanupErr)
		}
		prepared = false
	} else {
		prepared = false
	}
	existing, err := snapshotter.Stat(ctx, name)
	if err != nil {
		return fmt.Errorf("inspect committed rootfs Head snapshot %s: %w", name, err)
	}
	return validate(existing)
}

func imageConfig(
	ctx context.Context,
	client containerdClient,
	reference string,
	platform platforms.MatchComparer,
) (images.Image, ocispec.Image, []byte, error) {
	reference = strings.TrimSpace(reference)
	record, err := getImageRecord(ctx, client.ImageService(), reference)
	if err != nil {
		return images.Image{}, ocispec.Image{}, nil, fmt.Errorf("load rootfs base image %s: %w", reference, err)
	}
	configDescriptor, err := images.Config(ctx, client.ContentStore(), record.Target, platform)
	if err != nil {
		return images.Image{}, ocispec.Image{}, nil, fmt.Errorf("resolve rootfs base image config: %w", err)
	}
	payload, err := content.ReadBlob(ctx, client.ContentStore(), configDescriptor)
	if err != nil {
		return images.Image{}, ocispec.Image{}, nil, err
	}
	var config ocispec.Image
	if err := json.Unmarshal(payload, &config); err != nil {
		return images.Image{}, ocispec.Image{}, nil, err
	}
	return record, config, payload, nil
}

func getImageRecord(ctx context.Context, store images.Store, imageReference string) (images.Image, error) {
	candidates := []string{strings.TrimSpace(imageReference)}
	if named, err := distref.ParseNormalizedNamed(imageReference); err == nil {
		candidates = appendUniqueString(candidates, named.String())
		candidates = appendUniqueString(candidates, distref.TagNameOnly(named).String())
	}
	var lastErr error
	for _, candidate := range candidates {
		record, err := store.Get(ctx, candidate)
		if err == nil {
			return record, nil
		}
		lastErr = err
		if !errdefs.IsNotFound(err) {
			return images.Image{}, err
		}
	}
	return images.Image{}, lastErr
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" || slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}
