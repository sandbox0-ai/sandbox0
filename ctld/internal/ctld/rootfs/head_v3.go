package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
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
	imageReference := strings.TrimSpace(info.BaseImageRef)
	if expected != nil {
		if err := expected.Validate(); err != nil {
			return rootfshead.BaseIdentity{}, nil, err
		}
		imageReference = strings.TrimSpace(expected.ImageReference)
	}
	record, config, configData, err := r.imageConfig(ctx, imageReference)
	loadedExpectedImage := expected != nil
	if err != nil && expected != nil && strings.TrimSpace(info.BaseImageRef) != imageReference {
		loadedExpectedImage = false
		record, config, configData, err = r.imageConfig(ctx, info.BaseImageRef)
	}
	if err != nil {
		return rootfshead.BaseIdentity{}, nil, err
	}
	if expected != nil {
		if loadedExpectedImage && record.Target.Digest.String() != expected.ManifestDigest {
			return rootfshead.BaseIdentity{}, nil, fmt.Errorf("rootfs base manifest is %s, expected %s", record.Target.Digest, expected.ManifestDigest)
		}
		return *expected, configData, nil
	}
	if len(config.RootFS.DiffIDs) == 0 {
		return rootfshead.BaseIdentity{}, nil, fmt.Errorf("rootfs base image %s has no diff IDs", imageReference)
	}
	base := rootfshead.BaseIdentity{
		ImageReference: imageReference,
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

func (r *ContainerdRuntime) MaterializeRootFSHead(
	ctx context.Context,
	reference rootfshead.HeadReference,
	base rootfshead.BaseIdentity,
	image rootfshead.ImageReference,
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
		Name:   image.Name,
		Target: envelope.Manifest,
		Labels: map[string]string{criManagedImageLabel: criManagedImageValue, rootFSHeadImageLabel: reference.HeadID},
	}
	created, err := client.ImageService().Create(leaseCtx, record)
	if errdefs.IsAlreadyExists(err) {
		created, err = client.ImageService().Get(leaseCtx, image.Name)
		if err == nil && created.Target.Digest != envelope.Manifest.Digest {
			return fmt.Errorf("local rootfs Head image %s has manifest %s, expected %s", image.Name, created.Target.Digest, envelope.Manifest.Digest)
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
	return nil
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

func (r *ContainerdRuntime) imageConfig(ctx context.Context, reference string) (images.Image, ocispec.Image, []byte, error) {
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return images.Image{}, ocispec.Image{}, nil, err
	}
	defer closeClient()
	record, err := client.ImageService().Get(ctx, strings.TrimSpace(reference))
	if err != nil {
		return images.Image{}, ocispec.Image{}, nil, fmt.Errorf("load rootfs base image %s: %w", reference, err)
	}
	configDescriptor, err := images.Config(ctx, client.ContentStore(), record.Target, platforms.All)
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
