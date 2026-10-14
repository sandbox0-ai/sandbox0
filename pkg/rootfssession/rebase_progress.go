package session

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
)

type rebaseProgressEngine interface {
	ApplyWithProgress(context.Context, rootfsrebase.WorkerRequest, string, string, string, []uint64, func(context.Context, uint64) error) (*rootfsrebase.ApplyResult, error)
}

type rebaseSyncRuntime interface {
	SyncXFS(devicePath, xfsRoot string) error
}

// The target is private to this operation. Intermediate descriptors are held
// by its regional upload custody, never published as filesystem heads. A crash
// discards this target and restarts from the immutable request, rather than
// attempting to resume a partially applied semantic merge.
type rebaseProgress struct {
	manager     *Manager
	current     *rebaseRecord
	branch      *rootfsblock.Branch
	base        rootfsblock.Descriptor
	lastWritten uint64
	objects     int
	bytes       int64
}

func (m *Manager) newRebaseProgress(current *rebaseRecord, branch *rootfsblock.Branch, base rootfsblock.Descriptor) *rebaseProgress {
	return &rebaseProgress{manager: m, current: current, branch: branch, base: base}
}

func (m *Manager) prepareRebaseTargetCapacity(ctx context.Context, source, target *rebaseResource, ceiling int64) error {
	host, ok := m.runtime.(XFSGrowthRuntime)
	if !ok {
		return nil // Synthetic hosts have no mounted filesystem geometry.
	}
	from, err := host.InspectXFS(source.DevicePath, source.XFSRoot)
	if err != nil {
		return fmt.Errorf("inspect rebase source capacity: %w", err)
	}
	to, err := host.InspectXFS(target.DevicePath, target.XFSRoot)
	if err != nil {
		return fmt.Errorf("inspect rebase target capacity: %w", err)
	}
	if _, err := XFSGrowthTarget(from, ceiling); err != nil {
		return err
	}
	if _, err := XFSGrowthTarget(to, ceiling); err != nil {
		return err
	}
	if from.SizeBytes <= to.SizeBytes {
		return nil
	}
	if from.SizeBytes%to.BlockBytes != 0 {
		return fmt.Errorf("rebase source capacity is not aligned to target XFS blocks")
	}
	_, err = host.GrowXFS(ctx, target.DevicePath, target.XFSRoot, from.SizeBytes)
	if err != nil {
		return fmt.Errorf("grow private rebase target to source capacity: %w", err)
	}
	return nil
}

func (p *rebaseProgress) advance(ctx context.Context, written uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	host, ok := p.manager.runtime.(rebaseSyncRuntime)
	if !ok {
		return nil
	}
	target := p.current.resource("target")
	if target == nil || p.branch == nil {
		return fmt.Errorf("private rebase target is missing")
	}
	// Apply bounds each data write to 1 MiB. Sync/cut well before the local
	// limit, leaving room for allocation and journal metadata flushed by XFS.
	limit := uint64(max(rootfsblock.LogicalBlockSize, min(p.manager.maxDirty/4, 8<<20)))
	usage := p.branch.DirtyTailUsage()
	node := p.manager.NodeDirtyTailUsage()
	pressure := node.MaxBytes-node.UsedBytes < 2<<20
	if written < p.lastWritten {
		return fmt.Errorf("private rebase progress moved backwards")
	}
	if written-p.lastWritten >= limit || uint64(usage.DirtyBytes) >= limit || pressure {
		if err := host.SyncXFS(target.DevicePath, target.XFSRoot); err != nil {
			return fmt.Errorf("sync private rebase target before cut: %w", err)
		}
		if err := p.commit(ctx, target); err != nil {
			return err
		}
		p.lastWritten = written
	}
	node = p.manager.NodeDirtyTailUsage()
	if pressure && node.MaxBytes-node.UsedBytes < 2<<20 {
		return fmt.Errorf("shared node budget cannot admit the next rebase write: %w", errdefs.ErrUnavailable)
	}
	if growth, ok := p.manager.runtime.(XFSGrowthRuntime); ok {
		usage, err := growth.InspectXFS(target.DevicePath, target.XFSRoot)
		if err != nil {
			return err
		}
		bytes, err := XFSGrowthTarget(usage, p.base.LogicalSizeBytes)
		if err != nil {
			return err
		}
		if bytes != 0 {
			if _, err := growth.GrowXFS(ctx, target.DevicePath, target.XFSRoot, bytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *rebaseProgress) commit(ctx context.Context, target *rebaseResource) error {
	checkpoint, err := p.branch.Checkpoint()
	if err != nil {
		return err
	}
	defer checkpoint.Close()
	if checkpoint.RecordCount() == 0 {
		return nil
	}
	built, err := rootfsblock.BuildIncrementalGenerationFromBlockReader(ctx, p.manager.source, p.base, checkpoint, p.manager.rebasePublisher(p.current.Request), rootfsblock.BuildOptions{})
	if err != nil {
		return fmt.Errorf("upload private rebase cut: %w", err)
	}
	cut := checkpoint.Cut()
	if err := checkpoint.Close(); err != nil {
		return err
	}
	reader, err := rootfsblock.NewReaderWithCacheContext(p.manager.lifetime, p.manager.source, built.Descriptor, p.manager.readCache)
	if err != nil {
		return err
	}
	oldPath := target.BranchPath
	nextPath := filepath.Join(filepath.Dir(oldPath), fmt.Sprintf("target-%d.wal", cut.Sequence))
	// Every PUT and its custody acknowledgement succeeded before this journal
	// callback authorizes unlinking any covered local record. Ambiguous journal
	// writes retain both WAL paths; cleanup detaches before deleting either.
	if err := p.branch.RotateCommitted(cut, reader, built.Descriptor.MappingRoot.RootDigest, nextPath, func(identity rootfsblock.BranchIdentity) error {
		if identity.SequenceBase != cut.Sequence || identity.BaseRootDigest != built.Descriptor.MappingRoot.RootDigest {
			return fmt.Errorf("private rebase installation differs from uploaded cut")
		}
		target.BranchPath, target.ObsoleteBranchPath = nextPath, oldPath
		p.current.WorkingTargetDescriptor = append([]byte(nil), built.Payload...)
		p.current.WorkingTargetSequence = cut.Sequence
		return p.manager.saveRebase(*p.current)
	}); err != nil {
		return fmt.Errorf("install private rebase cut: %w", err)
	}
	p.base = built.Descriptor
	p.objects += built.Objects
	p.bytes += built.Bytes
	return nil
}
