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

// startCheckpointWritebackProbe only requests early writeback of complete
// megabyte ranges. It never certifies persistence: the caller still fsyncs every
// completed image file and its directory after joining this bounded helper.
func startCheckpointWritebackProbe(parent context.Context, directory string, mode string) func() error {
	if mode == "0" {
		return func() error { return nil }
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() {
		offsets := make(map[string]int64)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
			}
			err := requestCheckpointWriteback(directory, offsets, mode)
			if err != nil {
				done <- err
				return
			}
		}
	}()
	return func() error { cancel(); return <-done }
}

func requestCheckpointWriteback(directory string, offsets map[string]int64, mode string) error {
	root, err := os.OpenRoot(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, err := dir.ReadDir(257)
	closeErr := dir.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if len(entries) > 256 {
		return fmt.Errorf("checkpoint probe file limit exceeded")
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular checkpoint probe file")
		}
		file, err := root.OpenFile(entry.Name(), os.O_RDWR, 0)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err == nil && !info.Mode().IsRegular() {
			err = fmt.Errorf("nonregular checkpoint probe descriptor")
		}
		if err == nil {
			granularity := int64(1 << 20)
			if mode == "2" {
				granularity = 16 << 20
			}
			end := info.Size() &^ (granularity - 1)
			offset := offsets[entry.Name()]
			if end > offset {
				if mode == "2" {
					err = unix.Fdatasync(int(file.Fd()))
				} else {
					err = unix.SyncFileRange(int(file.Fd()), offset, end-offset, unix.SYNC_FILE_RANGE_WRITE)
				}
				if err == nil {
					offsets[entry.Name()] = end
				}
			}
		}
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}

func syncCheckpointProbeImage(directory string) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(257)
	if err != nil {
		return err
	}
	if len(entries) == 0 || len(entries) > 256 {
		return fmt.Errorf("invalid checkpoint probe file count")
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular checkpoint probe file")
		}
		file, err := root.Open(entry.Name())
		if err != nil {
			return err
		}
		err = file.Sync()
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return dir.Sync()
}
