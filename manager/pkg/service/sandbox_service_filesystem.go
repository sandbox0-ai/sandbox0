package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func (s *SandboxService) acquireClaimFilesystem(ctx context.Context, req *ClaimRequest, template *v1alpha1.SandboxTemplate) error {
	if s == nil || s.sandboxFilesystemStore == nil || req == nil {
		return nil
	}
	if strings.TrimSpace(req.FilesystemID) == "" {
		if err := s.ensureClaimFilesystem(req, template); err != nil {
			return err
		}
	}
	record, err := s.sandboxFilesystemStore.AcquireOwner(ctx, SandboxFilesystemAcquireRequest{
		FilesystemID:           req.FilesystemID,
		TeamID:                 req.TeamID,
		UserID:                 req.UserID,
		BaseImageRef:           req.FilesystemBaseImageRef,
		BaseImageDigest:        req.FilesystemBaseImageDigest,
		OwnerSandboxID:         req.SandboxID,
		OwnerRuntimeGeneration: req.RuntimeGeneration,
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrSandboxFilesystemBusy):
			return fmt.Errorf("%w: %v", ErrClaimConflict, err)
		case errors.Is(err, ErrSandboxFilesystemForbidden):
			return fmt.Errorf("%w: %v", ErrInvalidClaimRequest, err)
		default:
			return fmt.Errorf("acquire sandbox filesystem: %w", err)
		}
	}
	if record == nil {
		return nil
	}
	req.FilesystemID = record.FilesystemID
	if record.BaseImageRef != "" {
		req.FilesystemBaseImageRef = record.BaseImageRef
	}
	if record.BaseImageDigest != "" {
		req.FilesystemBaseImageDigest = record.BaseImageDigest
	}
	return nil
}

func (s *SandboxService) releaseSandboxFilesystemOwner(ctx context.Context, filesystemID, sandboxID string, runtimeGeneration int64) error {
	if s == nil || s.sandboxFilesystemStore == nil {
		return nil
	}
	filesystemID = strings.TrimSpace(filesystemID)
	sandboxID = strings.TrimSpace(sandboxID)
	if filesystemID == "" || sandboxID == "" {
		return nil
	}
	return s.sandboxFilesystemStore.ReleaseOwner(ctx, SandboxFilesystemReleaseRequest{
		FilesystemID:           filesystemID,
		OwnerSandboxID:         sandboxID,
		OwnerRuntimeGeneration: runtimeGeneration,
	})
}

func (s *SandboxService) releaseClaimFilesystemOwner(ctx context.Context, req *ClaimRequest) error {
	if req == nil {
		return nil
	}
	return s.releaseSandboxFilesystemOwner(ctx, req.FilesystemID, req.SandboxID, req.RuntimeGeneration)
}
