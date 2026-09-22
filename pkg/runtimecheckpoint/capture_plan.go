package runtimecheckpoint

import (
	"context"
	"fmt"
)

// InspectAndStageLocal fills missing tentative chunks while inspecting a
// completed image. It never binds a RootFS cut or publishes a manifest. The
// caller must own stopped-source custody and join this bounded worker before
// publication or release. Failed uploads retain their existing budget charge.
func (s *CaptureStager) InspectAndStageLocal(ctx context.Context, directory string) (LocalImageInventory, error) {
	if err := s.acquire(ctx); err != nil {
		return LocalImageInventory{}, err
	}
	defer func() { <-s.gate }()
	if s.bound != "" {
		return LocalImageInventory{}, fmt.Errorf("capture is already bound for publication")
	}
	return s.store.inspectLocal(ctx, directory, s.stageVerifiedChunk)
}

// BindLocalInventory selects the final cut for a staged capture and constructs
// the same prospective reference used by peer transfer and regional publication.
// The caller owns a completed image and must retain it through both readers.
// This closes tentative admission, but does not publish a restorable manifest.
func (s *CaptureStager) BindLocalInventory(ctx context.Context, binding Binding, inventory LocalImageInventory) (LocalImagePlan, error) {
	if err := s.acquire(ctx); err != nil {
		return LocalImagePlan{}, err
	}
	defer func() { <-s.gate }()
	plan, err := s.store.bindLocalInventory(binding, inventory, StagedManifestVersion)
	if err != nil {
		return LocalImagePlan{}, err
	}
	if err := s.bindPublication(ctx, binding); err != nil {
		return LocalImagePlan{}, err
	}
	return plan, nil
}

// PlanLocal hashes a completed image when no retained inventory is available.
// A plan may be streamed to the authorized peer while PublishPlanned runs, but
// execution still requires the independent durable publication receipt.
func (s *CaptureStager) PlanLocal(ctx context.Context, binding Binding, directory string) (LocalImagePlan, error) {
	if !s.scope.matches(binding) {
		return LocalImagePlan{}, fmt.Errorf("final checkpoint changed its capture source")
	}
	inventory, err := s.store.InspectLocal(ctx, directory)
	if err != nil {
		return LocalImagePlan{}, err
	}
	return s.BindLocalInventory(ctx, binding, inventory)
}

// PublishPlanned reuses tentative uploads only when the current local bytes
// match the prospective peer plan. It never repairs a changed plan silently:
// the destination may already hold that exact reference. The manifest is last.
func (s *CaptureStager) PublishPlanned(ctx context.Context, binding Binding, plan LocalImagePlan, directory string) (Reference, error) {
	manifest, payload, err := plan.snapshot(binding, s.store.maxBytes)
	if err != nil {
		return Reference{}, err
	}
	if manifest.Version != StagedManifestVersion {
		return Reference{}, fmt.Errorf("capture publication requires a staged checkpoint plan")
	}
	if err := s.acquire(ctx); err != nil {
		return Reference{}, err
	}
	defer func() { <-s.gate }()
	if err := s.bindPublication(ctx, binding); err != nil {
		return Reference{}, err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return Reference{}, err
	}
	defer root.Close()
	files, err := s.store.imageFiles(ctx, root)
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
		if _, err := scanImageFile(ctx, root, file, s.stageVerifiedChunk); err != nil {
			return Reference{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Reference{}, err
	}
	if err := s.store.putImmutable(ctx, manifestKey(plan.Reference.BindingDigest), payload); err != nil {
		return Reference{}, fmt.Errorf("publish staged checkpoint manifest: %w", err)
	}
	return plan.Reference, nil
}
