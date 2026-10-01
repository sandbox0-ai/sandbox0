package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// PreflightLiveHandoff runs before source commit, without opening the source's
// exclusive Bolt journal. It checks the exact tokenless records, immutable
// readers, sealed indexes and existing mounts while the source can still abort.
// The inherited primary lease remains held and source control admission joined.
func PreflightLiveHandoff(ctx context.Context, config Config, manifest LiveHandoff, files []*os.File) error {
	if manifest.Version != 1 || len(files) != 2*len(manifest.Sessions) {
		return fmt.Errorf("invalid RootFS preflight envelope")
	}
	validator, ok := config.Runtime.(interface {
		ValidateLiveDevice(string, string, string, string, int64) error
	})
	if !ok {
		return fmt.Errorf("host runtime cannot preflight connected RootFS devices")
	}
	seen := make(map[string]bool)
	cache, err := rootfsblock.NewReadCache(0)
	if err != nil {
		return err
	}
	defer cache.Close()
	for i, entry := range manifest.Sessions {
		if err := ctx.Err(); err != nil {
			return err
		}
		digest := sha256.Sum256(entry.Record)
		if entry.Parent == "" || seen[entry.Parent] || hex.EncodeToString(digest[:]) != entry.RecordDigest {
			return fmt.Errorf("RootFS preflight record changed or repeated")
		}
		seen[entry.Parent] = true
		var current record
		if err := json.Unmarshal(entry.Record, &current); err != nil {
			return err
		}
		paths := sessionPaths(config.BranchRoot, config.MountRoot, entry.Parent)
		if current.Version != sessionSchemaVersion || current.Parent != entry.Parent || current.State != stateReady || current.Stage == nil || current.Consumer == nil || current.Consumer.RenewalProtocol != 1 || current.Stage.Identity.WriterGrantToken != "" || current.BranchPath != paths.branch || current.XFSRoot != paths.xfs || current.MergedRoot != paths.merged {
			return fmt.Errorf("RootFS preflight durable binding is incompatible")
		}
		if err := current.Stage.ValidateDurableBinding(); err != nil {
			return err
		}
		descriptor, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
		if err != nil {
			return err
		}
		reader, err := rootfsblock.NewReaderWithCacheContext(ctx, config.Source, descriptor, cache)
		if err != nil {
			return err
		}
		branch, err := rootfsblock.OpenBranchWithOptions(current.BranchPath, rootfsblock.BranchIdentity{
			Version: rootfsblock.BranchFormatVersion, RootFSID: current.RootFSID,
			GenerationID: current.GenerationID, WriterEpoch: current.WriterEpoch,
			LogicalSizeBytes: int64(reader.Size()), BaseRootDigest: descriptor.MappingRoot.RootDigest,
		}, reader, rootfsblock.BranchOptions{LiveIndex: files[2*i+1]})
		if err != nil {
			return err
		}
		sequence, err := branch.DurableSequence()
		closeErr := branch.Close()
		if err != nil || closeErr != nil || sequence != entry.Sequence {
			return fmt.Errorf("RootFS preflight journal cut differs: %v, %v", err, closeErr)
		}
		if err := validator.ValidateLiveDevice(current.DevicePath, current.DeviceAllocationID, current.XFSRoot, current.MergedRoot, int64(reader.Size())); err != nil {
			return err
		}
	}
	return nil
}
