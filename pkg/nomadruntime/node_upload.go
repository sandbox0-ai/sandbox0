package nomadruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

type nodeUploadAuthority interface {
	RecordRootFSNodeUpload(context.Context, rootfshandoff.StageRequest, string, rootfsblock.ObjectReference, bool) error
}

type nodeRebaseUploadAuthority interface {
	RecordRootFSRebaseNodeUpload(context.Context, string, rootfsblock.ObjectReference, bool) error
}

type nodeJournalPublisher struct {
	rebase    bool
	authority rootFSWriterAuthority
	stage     rootfshandoff.StageRequest
	operation string
	publisher rootfsblock.ImmutableObjectPublisher
}

func (p nodeJournalPublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	kind := rootfsblock.ObjectKindMappingPage
	if strings.Contains(key, "/packs/") {
		kind = rootfsblock.ObjectKindDataPack
	}
	reference := rootfsblock.ObjectReference{Key: key, Kind: kind, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
	if err := p.record(ctx, reference, false); err != nil {
		return err
	}
	if err := p.publisher.PutImmutable(ctx, key, payload); err != nil {
		return err
	}
	return p.record(ctx, reference, true)
}

func (p nodeJournalPublisher) record(ctx context.Context, reference rootfsblock.ObjectReference, uploaded bool) error {
	if p.rebase {
		if authority, ok := p.authority.(nodeRebaseUploadAuthority); ok {
			return authority.RecordRootFSRebaseNodeUpload(ctx, p.operation, reference, uploaded)
		}
	} else if authority, ok := p.authority.(nodeUploadAuthority); ok {
		return authority.RecordRootFSNodeUpload(ctx, p.stage, p.operation, reference, uploaded)
	}
	return fmt.Errorf("node upload custody authority unavailable: %w", errdefs.ErrUnavailable)
}
