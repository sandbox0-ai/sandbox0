//go:build linux

package gvisorcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const checkpointWritebackBatch = 16 << 20
const checkpointWritebackFileLimit = 256

type checkpointWritebackFile struct {
	file      *os.File
	info      os.FileInfo
	submitted int64
}

type checkpointWriteback struct {
	root  *os.Root
	files map[string]*checkpointWritebackFile
	flush func(*os.File) error
	sync  func(*os.File) error
}

// startCheckpointWriteback overlaps bounded file writeback with stock runsc's
// capture. Retaining each descriptor preserves writeback-error observation;
// opening fresh descriptors only after an early flush could miss an I/O error.
// The returned join always closes custody and propagates failures. It does not
// replace the driver's final complete-inventory and directory fsync boundary.
func startCheckpointWriteback(ctx context.Context, directory string) (func() error, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	worker := &checkpointWriteback{root: root, files: make(map[string]*checkpointWritebackFile),
		flush: func(f *os.File) error { return unix.Fdatasync(int(f.Fd())) },
		sync:  func(f *os.File) error { return f.Sync() },
	}
	return worker.start(ctx), nil
}

func (w *checkpointWriteback) start(parent context.Context) func() error {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := w.scan(ctx); err != nil {
					done <- err
					return
				}
			}
		}
	}()
	return func() error {
		cancel()
		err := <-done
		for _, f := range w.files {
			// Even after an interrupted capture, observe pending writeback errors on
			// the same descriptor before closing it. A failed capture remains failed.
			err = errors.Join(err, w.sync(f.file), f.file.Close())
		}
		return errors.Join(err, w.root.Close())
	}
}

func (w *checkpointWriteback) scan(ctx context.Context) error {
	dir, err := w.root.Open(".")
	if err != nil {
		return err
	}
	entries, err := dir.ReadDir(checkpointWritebackFileLimit + 1)
	closeErr := dir.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if len(entries) > checkpointWritebackFileLimit {
		return fmt.Errorf("checkpoint writeback file limit exceeded")
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("checkpoint writeback requires regular files")
		}
		tracked := w.files[entry.Name()]
		if tracked == nil {
			if len(w.files) >= checkpointWritebackFileLimit {
				return fmt.Errorf("checkpoint writeback inventory changed beyond limit")
			}
			file, err := w.root.OpenFile(entry.Name(), os.O_RDWR|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			actual, err := file.Stat()
			if err != nil || !actual.Mode().IsRegular() || !os.SameFile(info, actual) {
				return errors.Join(fmt.Errorf("checkpoint writeback file identity changed"), err, file.Close())
			}
			tracked = &checkpointWritebackFile{file: file, info: actual}
			w.files[entry.Name()] = tracked
		} else if !os.SameFile(info, tracked.info) {
			return fmt.Errorf("checkpoint writeback file was replaced")
		}
		if info.Size() < tracked.submitted {
			return fmt.Errorf("checkpoint writeback file was truncated")
		}
		end := info.Size() &^ int64(checkpointWritebackBatch-1)
		if end > tracked.submitted {
			if err := w.flush(tracked.file); err != nil {
				return err
			}
			tracked.submitted = end
		}
	}
	return nil
}
