package rootfsmaintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

type RootFSObjectReader interface {
	Get(key string, offset, length int64) (io.ReadCloser, error)
}

const rootFSInventoryClaimTTL = 2 * time.Minute

func (c *Controller) processV3Inventory(ctx context.Context) error {
	if c == nil || c.store == nil || c.objectReader == nil {
		return nil
	}
	worker := c.workerID
	jobs, err := c.store.ClaimRootFSInventoryJobs(ctx, worker, c.cfg.BatchSize, rootFSInventoryClaimTTL)
	if err != nil {
		return err
	}
	var result error
	for _, job := range jobs {
		objects, inventoryErr := c.inventoryRootFSHeadWithHeartbeat(ctx, worker, job)
		if inventoryErr == nil {
			inventoryErr = c.store.CompleteRootFSInventoryJob(ctx, worker, job.HeadID, job.TeamID, objects)
		}
		if inventoryErr != nil {
			if failErr := c.store.FailRootFSInventoryJob(ctx, worker, job.HeadID, inventoryErr); failErr != nil {
				result = errors.Join(result, fmt.Errorf("rootfs v3 inventory %s failed: %v; record failure: %w", job.HeadID, inventoryErr, failErr))
				continue
			}
			result = errors.Join(result, fmt.Errorf("rootfs v3 inventory %s: %w", job.HeadID, inventoryErr))
		}
	}
	return result
}

func (c *Controller) inventoryRootFSHeadWithHeartbeat(ctx context.Context, worker string, job sandboxstore.RootFSInventoryJob) ([]rootfshead.Object, error) {
	jobCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(rootFSInventoryClaimTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-jobCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				renewed, err := c.store.RenewRootFSInventoryJob(jobCtx, worker, job.HeadID, rootFSInventoryClaimTTL)
				if err != nil && jobCtx.Err() != nil {
					done <- nil
					return
				}
				if err == nil && !renewed {
					err = fmt.Errorf("rootfs inventory claim for Head %s was lost", job.HeadID)
				}
				if err != nil {
					done <- err
					cancel()
					return
				}
			}
		}
	}()
	objects, inventoryErr := inventoryRootFSHead(jobCtx, c.objectReader, job)
	cancel()
	heartbeatErr := <-done
	if heartbeatErr != nil {
		return nil, heartbeatErr
	}
	return objects, inventoryErr
}

func inventoryRootFSHead(ctx context.Context, reader RootFSObjectReader, job sandboxstore.RootFSInventoryJob) ([]rootfshead.Object, error) {
	if reader == nil {
		return nil, fmt.Errorf("rootfs inventory object reader is required")
	}
	if err := job.Reference.Validate(); err != nil {
		return nil, err
	}
	if err := job.Image.Validate(); err != nil {
		return nil, err
	}
	prefix, err := rootfshead.TeamObjectPrefix(job.TeamID)
	if err != nil {
		return nil, err
	}
	objects := make([]rootfshead.Object, 0, 1024)
	positions := make(map[string]int, 1024)
	add := func(object rootfshead.Object) (bool, error) {
		if err := rootfshead.ValidateObjectScope(prefix, object); err != nil {
			return false, err
		}
		if position, ok := positions[object.Key]; ok {
			if objects[position] != object {
				return false, fmt.Errorf("rootfs inventory object %s has conflicting descriptors", object.Key)
			}
			return false, nil
		}
		positions[object.Key] = len(objects)
		objects = append(objects, object)
		return true, nil
	}
	for _, object := range []rootfshead.Object{job.Reference.Manifest, job.Image.Marker, job.Image.Envelope} {
		if _, err := add(object); err != nil {
			return nil, err
		}
	}
	headPayload, err := readInventoryMetadata(ctx, reader, job.Reference.Manifest)
	if err != nil {
		return nil, err
	}
	head, err := rootfshead.DecodeHead(bytes.NewReader(headPayload))
	if err != nil {
		return nil, err
	}
	if head.HeadID != job.HeadID {
		return nil, fmt.Errorf("rootfs inventory Head id %s does not match job %s", head.HeadID, job.HeadID)
	}

	directories := []rootfshead.Entry{head.Root}
	for len(directories) > 0 {
		directory := directories[len(directories)-1]
		directories = directories[:len(directories)-1]
		if directory.Directory == nil {
			return nil, fmt.Errorf("rootfs inventory directory %s has no index", directory.Name)
		}
		indexObject := *directory.Directory
		added, err := add(indexObject)
		if err != nil {
			return nil, err
		}
		if !added {
			continue
		}
		indexPayload, err := readInventoryMetadata(ctx, reader, indexObject)
		if err != nil {
			return nil, err
		}
		index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(indexPayload))
		if err != nil {
			return nil, err
		}
		for _, shardRef := range index.Shards {
			if _, err := add(shardRef.Object); err != nil {
				return nil, err
			}
			shardPayload, err := readInventoryMetadata(ctx, reader, shardRef.Object)
			if err != nil {
				return nil, err
			}
			shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(shardPayload))
			if err != nil {
				return nil, err
			}
			if shard.Bucket != shardRef.Bucket {
				return nil, fmt.Errorf("rootfs inventory shard bucket %d does not match index bucket %d", shard.Bucket, shardRef.Bucket)
			}
			for _, entry := range shard.Entries {
				switch entry.Kind {
				case rootfshead.EntryDirectory:
					directories = append(directories, entry)
				case rootfshead.EntryFile:
					if entry.File == nil {
						return nil, fmt.Errorf("rootfs inventory file %s has no manifest", entry.Name)
					}
					fileObject := *entry.File
					added, err := add(fileObject)
					if err != nil {
						return nil, err
					}
					if !added {
						continue
					}
					filePayload, err := readInventoryMetadata(ctx, reader, fileObject)
					if err != nil {
						return nil, err
					}
					manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(filePayload))
					if err != nil {
						return nil, err
					}
					if manifest.Size != entry.Size || manifest.Blocks != entry.Blocks {
						return nil, fmt.Errorf(
							"rootfs inventory file %s metadata mismatch: entry size/blocks=%d/%d manifest=%d/%d",
							entry.Name, entry.Size, entry.Blocks, manifest.Size, manifest.Blocks,
						)
					}
					for _, extent := range manifest.Extents {
						if _, err := add(extent.Object); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func readInventoryMetadata(ctx context.Context, reader RootFSObjectReader, object rootfshead.Object) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if object.Size > rootfshead.MaxMetadataObjectBytes {
		return nil, fmt.Errorf("rootfs metadata object %s is too large: %d", object.Key, object.Size)
	}
	content, err := reader.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, fmt.Errorf("read rootfs inventory object %s: %w", object.Key, err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(content, object.Size+1))
	closeErr := content.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("read rootfs inventory object %s: %w", object.Key, errors.Join(readErr, closeErr))
	}
	if int64(len(payload)) != object.Size || digest.FromBytes(payload).String() != object.Digest {
		return nil, fmt.Errorf("rootfs inventory object %s failed size or digest validation", object.Key)
	}
	return payload, ctx.Err()
}
