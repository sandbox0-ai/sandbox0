// Package migrationstaging verifies the kernel-enforced node staging budget.
// It never provisions filesystems, changes quotas, or deletes retained images.
package migrationstaging

import (
	"fmt"
	"path/filepath"

	"github.com/containerd/errdefs"
)

type Limits struct {
	ProjectID uint32
	Bytes     int64
	Inodes    uint64
}

func (l Limits) Validate() error {
	if l.ProjectID == 0 || l.Bytes < 1<<20 || l.Bytes > 1<<50 || l.Bytes%4096 != 0 || l.Inodes < 16 || l.Inodes > 16384 {
		return fmt.Errorf("migration staging requires a nonzero project, aligned 1 MiB–1 PiB byte limit and 16–16384 inode limit")
	}
	return nil
}

type snapshot struct {
	device, inode      uint64
	project            uint32
	inherit, enforced  bool
	hardBlocks, blocks uint64
	hardInodes, inodes uint64
	freeBytes          uint64
}

// Guard pins one private directory's identity and exact provisioned limits.
// Read-only checks do not release quota when an RPC ends or a file is unlinked;
// the kernel continues charging open files until their final reference closes.
type Guard struct {
	root   string
	limits Limits
	first  snapshot
}

func Open(root string, limits Limits) (*Guard, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return nil, fmt.Errorf("migration staging path must be absolute and canonical")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	state, err := inspect(root, limits.ProjectID)
	if err != nil {
		return nil, err
	}
	if err := validate(state, limits, false); err != nil {
		return nil, err
	}
	if err := verifyExistingTree(root, limits, state.device); err != nil {
		return nil, err
	}
	return &Guard{root: root, limits: limits, first: state}, nil
}

// Verify checks enforcement and identity even when the pool is full, allowing
// exact recovery to remove an incomplete destination before retrying download.
func (g *Guard) Verify(root string) error {
	return g.check(root, false)
}

func (g *Guard) Admit(root string) error {
	return g.check(root, true)
}

// AdmitBudget rejects a known image footprint before any image file is
// created. This is an admission observation, not a durable reservation: the
// caller must serialize competing writers and retain operation custody.
func (g *Guard) AdmitBudget(root string, bytes int64, inodes uint64) error {
	state, err := g.state(root)
	if err != nil {
		return err
	}
	return admitBudget(state, bytes, inodes)
}

func admitBudget(s snapshot, bytes int64, inodes uint64) error {
	if bytes <= 0 || bytes > 1<<50 || inodes == 0 || inodes > 16384 {
		return fmt.Errorf("invalid migration staging footprint: %w", errdefs.ErrInvalidArgument)
	}
	blocks := (uint64(bytes) + 511) / 512
	if s.blocks > s.hardBlocks || blocks > s.hardBlocks-s.blocks ||
		s.inodes > s.hardInodes || inodes > s.hardInodes-s.inodes || uint64(bytes) > s.freeBytes {
		return fmt.Errorf("migration image exceeds available staging capacity: %w", errdefs.ErrResourceExhausted)
	}
	return nil
}

func (g *Guard) check(root string, admission bool) error {
	state, err := g.state(root)
	if err != nil {
		return err
	}
	return validate(state, g.limits, admission)
}

func (g *Guard) state(root string) (snapshot, error) {
	if g == nil || root != g.root {
		return snapshot{}, errdefs.ErrFailedPrecondition
	}
	state, err := inspect(root, g.limits.ProjectID)
	if err != nil {
		return snapshot{}, err
	}
	if state.device != g.first.device || state.inode != g.first.inode {
		return snapshot{}, fmt.Errorf("migration staging directory was replaced: %w", errdefs.ErrFailedPrecondition)
	}
	return state, validate(state, g.limits, false)
}

func validate(s snapshot, l Limits, admission bool) error {
	if s.project != l.ProjectID || !s.inherit || !s.enforced || s.hardBlocks != uint64(l.Bytes/512) || s.hardInodes != l.Inodes {
		return fmt.Errorf("migration staging lacks its exact enforced project quota: %w", errdefs.ErrFailedPrecondition)
	}
	if admission && (s.blocks >= s.hardBlocks || s.inodes >= s.hardInodes || s.freeBytes == 0) {
		return fmt.Errorf("migration staging quota is exhausted: %w", errdefs.ErrResourceExhausted)
	}
	return nil
}
