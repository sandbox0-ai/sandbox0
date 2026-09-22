package runtimecheckpoint

import (
	"context"
	"fmt"
	"io"

	"github.com/opencontainers/go-digest"
)

// LocalImagePlan identifies an immutable retained source cut before regional
// publication. Its reference is prospective: this value is neither a durable
// publication receipt nor authority to restore, fence or release either node.
// A caller may use it for authorized speculative transfer while PublishPlanned
// runs, but must separately commit successful regional publication before
// authorizing destination execution.
type LocalImagePlan struct {
	Manifest  Manifest  `json:"manifest"`
	Reference Reference `json:"reference"`
}

// LocalImageInventory is a bounded set of hashes from a stopped, retained
// image. It contains no runtime binding and grants no publication authority.
// Files remain private so a caller cannot mutate the cached inventory.
type LocalImageInventory struct{ files []File }

// InspectLocal can overlap independent RootFS sealing after durable execution
// capture. Publication and peer transfer still reread and verify every chunk.
// The caller must retain exclusive custody of the completed image through
// inspection and all later users; this method does not freeze a producer.
func (s *Store) InspectLocal(ctx context.Context, directory string) (LocalImageInventory, error) {
	return s.inspectLocal(ctx, directory, nil)
}

func (s *Store) inspectLocal(ctx context.Context, directory string, publish func(context.Context, Chunk, []byte) error) (LocalImageInventory, error) {
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return LocalImageInventory{}, err
	}
	defer root.Close()
	files, err := s.imageFiles(ctx, root)
	if err != nil {
		return LocalImageInventory{}, err
	}
	for i, file := range files {
		files[i], err = scanImageFile(ctx, root, file, publish)
		if err != nil {
			return LocalImageInventory{}, err
		}
	}
	return LocalImageInventory{files: files}, nil
}

// BindLocalInventory binds hashes only after the exact RootFS cut is known.
// The returned plan owns its slices and remains prospective until publication.
func (s *Store) BindLocalInventory(binding Binding, inventory LocalImageInventory) (LocalImagePlan, error) {
	return s.bindLocalInventory(binding, inventory, ManifestVersion)
}

func (s *Store) bindLocalInventory(binding Binding, inventory LocalImageInventory, version int) (LocalImagePlan, error) {
	bindingDigest, err := binding.Digest()
	if err != nil {
		return LocalImagePlan{}, err
	}
	manifest := Manifest{Version: version, Binding: binding, Files: inventory.files}
	payload, err := manifest.Encode(s.maxBytes)
	if err != nil {
		return LocalImagePlan{}, err
	}
	manifest, err = Decode(payload, s.maxBytes)
	if err != nil {
		return LocalImagePlan{}, err
	}
	return LocalImagePlan{Manifest: manifest, Reference: Reference{BindingDigest: bindingDigest, ManifestDigest: digest.FromBytes(payload).String()}}, nil
}

// PlanLocal hashes a completed local image without writing regional objects.
// The caller must retain exclusive source-image custody across planning,
// publication and every peer reader. This method does not freeze a running
// producer or make an unfinished checkpoint safe to publish.
func (s *Store) PlanLocal(ctx context.Context, binding Binding, directory string) (LocalImagePlan, error) {
	if _, err := binding.Digest(); err != nil {
		return LocalImagePlan{}, err
	}
	inventory, err := s.InspectLocal(ctx, directory)
	if err != nil {
		return LocalImagePlan{}, err
	}
	return s.BindLocalInventory(binding, inventory)
}

// snapshot binds the complete plan and owns its slices before starting worker
// goroutines. Callers must not mutate a plan concurrently with this call.
func (p LocalImagePlan) snapshot(expected Binding, maxBytes int64) (Manifest, []byte, error) {
	if p.Manifest.Binding != expected {
		return Manifest{}, nil, fmt.Errorf("checkpoint plan changed source binding")
	}
	if err := p.Reference.ValidateFor(expected); err != nil {
		return Manifest{}, nil, err
	}
	payload, err := p.Manifest.Encode(maxBytes)
	if err != nil {
		return Manifest{}, nil, err
	}
	if digest.FromBytes(payload).String() != p.Reference.ManifestDigest {
		return Manifest{}, nil, fmt.Errorf("checkpoint plan changed manifest")
	}
	manifest, err := Decode(payload, maxBytes)
	return manifest, payload, err
}

// PublishPlanned publishes only the bytes previously named by a local plan.
// Every chunk is reread and checked before upload; the manifest is created last.
// Successful peer transfer never substitutes for successful publication here.
func (s *Store) PublishPlanned(ctx context.Context, expected Binding, plan LocalImagePlan, directory string) (Reference, error) {
	manifest, payload, err := plan.snapshot(expected, s.maxBytes)
	if err != nil {
		return Reference{}, err
	}
	if manifest.Version != ManifestVersion {
		return Reference{}, fmt.Errorf("staged checkpoint publication requires its capture custodian")
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return Reference{}, err
	}
	defer root.Close()
	files, err := s.imageFiles(ctx, root)
	if err != nil {
		return Reference{}, err
	}
	if len(files) != len(manifest.Files) {
		return Reference{}, fmt.Errorf("checkpoint planned inventory changed")
	}
	for i, file := range files {
		if file.Path != manifest.Files[i].Path || file.Size != manifest.Files[i].Size {
			return Reference{}, fmt.Errorf("checkpoint planned file identity changed")
		}
	}
	for _, file := range manifest.Files {
		if _, err := s.publishFile(ctx, root, file, plan.Reference.BindingDigest); err != nil {
			return Reference{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Reference{}, err
	}
	if err := s.putImmutable(ctx, manifestKey(plan.Reference.BindingDigest), payload); err != nil {
		return Reference{}, fmt.Errorf("publish planned checkpoint manifest: %w", err)
	}
	return plan.Reference, nil
}

// WritePlannedPeerImage uses the same checked wire format as a published image,
// without waiting for the regional manifest. The enclosing node protocol must
// authorize the prospective reference for its exact destination, retain source
// custody through all readers, and keep preparation separate from execution.
func (s *Store) WritePlannedPeerImage(ctx context.Context, expected Binding, plan LocalImagePlan, directory string, output io.Writer) error {
	manifest, _, err := plan.snapshot(expected, s.maxBytes)
	if err != nil {
		return err
	}
	return writePeerImage(ctx, manifest, directory, output, s.maxBytes)
}
