package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	criManagedImageLabel = "io.cri-containerd.image"
	criManagedImageValue = "managed"
	rootFSHeadImageLabel = "sandbox0.ai/rootfs-head"
)

// MaterializeRootFSHead installs a durable OCI envelope into the selected
// node's containerd and unpacks its marker through the sandbox0 snapshotter.
// No resolver or registry is involved.
func (r *ContainerdRuntime) MaterializeRootFSHead(
	ctx context.Context,
	head rootfshead.HeadReference,
	image rootfshead.ImageReference,
	envelope rootfshead.ImageEnvelope,
	baseSnapshotKey string,
) error {
	if err := rootfshead.ValidateImage(head, image, envelope); err != nil {
		return fmt.Errorf("validate rootfs head image: %w", err)
	}
	baseSnapshotKey = strings.TrimSpace(baseSnapshotKey)
	if baseSnapshotKey == "" {
		return fmt.Errorf("%w: rootfs base snapshot is required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return err
	}
	defer closeClient()
	if _, err := client.SnapshotService(rootfshead.SnapshotterName).Stat(ctx, baseSnapshotKey); err != nil {
		return fmt.Errorf("%w: rootfs base snapshot %s is unavailable: %v", ErrConflict, baseSnapshotKey, err)
	}

	leaseManager := client.LeasesService()
	lease, err := leaseManager.Create(ctx, leases.WithRandomID(), leases.WithExpiration(5*time.Minute))
	if err != nil {
		return fmt.Errorf("create rootfs head content lease: %w", err)
	}
	leaseContext := leases.WithLease(ctx, lease.ID)
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = leaseManager.Delete(cleanupContext, lease)
	}()

	markerData, err := rootfshead.EncodeMarker(head)
	if err != nil {
		return err
	}
	var manifest ocispec.Manifest
	if err := unmarshalOCIManifest(envelope.ManifestData, &manifest); err != nil {
		return err
	}
	blobs := []struct {
		name       string
		descriptor ocispec.Descriptor
		payload    []byte
	}{
		{name: "marker", descriptor: manifest.Layers[0], payload: markerData},
		{name: "config", descriptor: envelope.Config, payload: envelope.ConfigData},
		{name: "manifest", descriptor: envelope.Manifest, payload: envelope.ManifestData},
	}
	for _, blob := range blobs {
		ref := "sandbox0-rootfs-head-" + blob.name + "-" + blob.descriptor.Digest.Encoded()
		if err := content.WriteBlob(leaseContext, client.ContentStore(), ref, bytes.NewReader(blob.payload), blob.descriptor); err != nil {
			return fmt.Errorf("write rootfs head %s blob: %w", blob.name, err)
		}
	}

	record := images.Image{
		Name:   image.Name,
		Target: envelope.Manifest,
		Labels: map[string]string{
			criManagedImageLabel: criManagedImageValue,
			rootFSHeadImageLabel: head.HeadID,
		},
	}
	created, err := client.ImageService().Create(leaseContext, record)
	if errdefs.IsAlreadyExists(err) {
		created, err = client.ImageService().Get(leaseContext, image.Name)
		if err == nil && created.Target.Digest != envelope.Manifest.Digest {
			return fmt.Errorf("%w: local rootfs head image %s points to %s, expected %s", ErrConflict, image.Name, created.Target.Digest, envelope.Manifest.Digest)
		}
		if err == nil && (created.Labels[criManagedImageLabel] != criManagedImageValue || created.Labels[rootFSHeadImageLabel] != head.HeadID) {
			created.Labels = record.Labels
			created, err = client.ImageService().Update(leaseContext, created, "labels")
		}
	}
	if err != nil {
		return fmt.Errorf("register local rootfs head image: %w", err)
	}
	localImage, err := client.GetImage(leaseContext, created.Name)
	if err != nil {
		return fmt.Errorf("load local rootfs head image: %w", err)
	}
	if err := localImage.Unpack(leaseContext, rootfshead.SnapshotterName); err != nil {
		return fmt.Errorf("unpack local rootfs head image: %w", err)
	}
	if err := r.waitForCRIImage(leaseContext, image.Name); err != nil {
		return err
	}
	return nil
}

func (r *ContainerdRuntime) waitForCRIImage(ctx context.Context, image string) error {
	client, err := r.imageClient(ctx)
	if err != nil {
		return err
	}
	waitContext, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, requestErr := client.ImageStatus(waitContext, &runtimeapi.ImageStatusRequest{
			Image: &runtimeapi.ImageSpec{Image: image},
		})
		if requestErr == nil && response != nil && response.Image != nil {
			return nil
		}
		select {
		case <-waitContext.Done():
			if requestErr != nil {
				return fmt.Errorf("wait for local rootfs head image in CRI: %w", requestErr)
			}
			return fmt.Errorf("wait for local rootfs head image in CRI: %w", waitContext.Err())
		case <-ticker.C:
		}
	}
}

func unmarshalOCIManifest(payload []byte, manifest *ocispec.Manifest) error {
	if manifest == nil {
		return fmt.Errorf("rootfs head OCI manifest destination is nil")
	}
	if err := json.Unmarshal(payload, manifest); err != nil {
		return err
	}
	if len(manifest.Layers) != 1 {
		return fmt.Errorf("rootfs head OCI manifest has %d layers, expected 1", len(manifest.Layers))
	}
	return nil
}
