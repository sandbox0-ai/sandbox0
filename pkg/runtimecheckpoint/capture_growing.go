package runtimecheckpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

// UploadGrowing copies tentative full chunks while the checkpoint producer is
// writing. File length is only a hint to speculate: concurrent writes can yield
// mixed or preallocated bytes, which final publication must rehash and replace.
// Each file offset is tried once per worker lifetime, with bounded parallel
// buffers and the capture's durable distinct-object budget.
//
// The caller must already own regional upload admission and exclusive source
// custody. It cancels and joins this call before planning, publication, cleanup,
// or primary handover. Cancellation joins every upload and returns ctx.Err();
// it says nothing about producer completion or local durability. This method
// never creates the image directory, which belongs to runsc.
func (s *CaptureStager) UploadGrowing(ctx context.Context, directory string) error {
	return s.UploadGrowingWithPeer(ctx, directory, nil)
}

// UploadGrowingWithPeer shares each bounded source buffer with the regional
// uploader and an optional authenticated destination stream. The callback must
// finish using the borrowed bytes before returning and support cancellation.
// The owner cancels and joins both consumers before sealing or releasing custody.
func (s *CaptureStager) UploadGrowingWithPeer(ctx context.Context, directory string, peer func(context.Context, string, int64, []byte) error) error {
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-s.gate }()
	if s.bound != "" {
		return fmt.Errorf("capture is already bound for publication")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var root *os.Root
	defer func() {
		if root != nil {
			_ = root.Close()
		}
	}()
	offsets := make(map[string]int64)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if root == nil {
			var err error
			root, err = openPrivateDirectory(directory)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if root != nil {
			if err := s.uploadGrowingPass(ctx, root, offsets, peer); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *CaptureStager) uploadGrowingPass(ctx context.Context, root *os.Root, offsets map[string]int64, peer func(context.Context, string, int64, []byte) error) error {
	files, err := s.store.inspectImageFiles(ctx, root, true)
	if err != nil {
		return err
	}
	for _, file := range files {
		offset, seen := offsets[file.Path]
		if !seen && len(offsets) >= MaxFiles {
			return fmt.Errorf("growing capture exceeds retained file limit")
		}
		offsets[file.Path] = offset
		end := file.Size / ChunkBytes * ChunkBytes
		if end <= offset {
			continue
		}
		if err := s.uploadGrowingFile(ctx, root, file, offset, end, peer); err != nil {
			return err
		}
		offsets[file.Path] = end
	}
	return nil
}

func (s *CaptureStager) uploadGrowingFile(ctx context.Context, root *os.Root, file File, start, end int64, peer func(context.Context, string, int64, []byte) error) error {
	input, err := root.Open(file.Path)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < end {
		return fmt.Errorf("growing checkpoint file changed identity or shrank")
	}
	workers := min(int((end-start)/ChunkBytes), publicationConcurrency)
	group, workerCtx := errgroup.WithContext(ctx)
	for worker := range workers {
		group.Go(func() error {
			buffer := make([]byte, ChunkBytes)
			for offset := start + int64(worker)*ChunkBytes; offset < end; offset += int64(workers) * ChunkBytes {
				if err := workerCtx.Err(); err != nil {
					return err
				}
				if _, err := input.ReadAt(buffer, offset); err != nil {
					return err
				}
				if peer == nil {
					if _, err := s.stageChunk(workerCtx, buffer); err != nil {
						return err
					}
				} else {
					consumers, consumeCtx := errgroup.WithContext(workerCtx)
					consumers.Go(func() error { _, err := s.stageChunk(consumeCtx, buffer); return err })
					consumers.Go(func() error { return peer(consumeCtx, file.Path, offset, buffer) })
					if err := consumers.Wait(); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return group.Wait()
}
