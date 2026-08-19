// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type result struct {
	Mode            string `json:"mode"`
	Path            string `json:"path"`
	FileSize        int64  `json:"file_size"`
	Offset          int64  `json:"offset"`
	WriteBytes      int    `json:"write_bytes"`
	Value           byte   `json:"value"`
	BeforeBlocks512 int64  `json:"before_guest_blocks_512,omitempty"`
	AfterBlocks512  int64  `json:"after_guest_blocks_512"`
	WriteFsyncUS    int64  `json:"write_fsync_us,omitempty"`
	CompletedAt     string `json:"completed_at"`
}

func main() {
	mode := flag.String("mode", "mutate", "mutate or verify")
	path := flag.String("path", "/large-1g.bin", "large lower file path")
	expectedSize := flag.Int64("expected-size", 1<<30, "expected logical file size")
	offset := flag.Int64("offset", 512<<20, "write offset")
	length := flag.Int("length", 1, "write length")
	value := flag.Int("value", 0xee, "write byte value")
	beforeByte := flag.Int("before-byte", 32, "expected byte immediately before the changed range")
	afterByte := flag.Int("after-byte", 33, "expected byte immediately after the changed range")
	flag.Parse()
	if *value < 0 || *value > 255 || *beforeByte < 0 || *beforeByte > 255 || *afterByte < 0 || *afterByte > 255 {
		fatalf("invalid copy-up workload arguments")
	}
	output, err := run(*mode, *path, *expectedSize, *offset, *length, byte(*value), byte(*beforeByte), byte(*afterByte))
	if err != nil {
		fatalf("%s: %v", *mode, err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fatalf("encode result: %v", err)
	}
}

func run(mode, path string, expectedSize, offset int64, length int, value, beforeByte, afterByte byte) (result, error) {
	if expectedSize <= 0 || offset < 1 || length <= 0 || int64(length) > expectedSize-offset-1 {
		return result{}, fmt.Errorf("invalid range offset=%d length=%d size=%d", offset, length, expectedSize)
	}
	file, err := os.Open(path)
	if err != nil {
		return result{}, err
	}
	before, err := file.Stat()
	if err != nil {
		file.Close()
		return result{}, err
	}
	if before.Size() != expectedSize {
		file.Close()
		return result{}, fmt.Errorf("file size %d, want %d", before.Size(), expectedSize)
	}
	beforeBlocks, err := blocks512(before)
	if err != nil {
		file.Close()
		return result{}, err
	}
	if err := verifyBoundary(file, offset, length, beforeByte, afterByte); err != nil {
		file.Close()
		return result{}, err
	}
	output := result{
		Mode: mode, Path: path, FileSize: expectedSize, Offset: offset,
		WriteBytes: length, Value: value, BeforeBlocks512: beforeBlocks,
	}
	switch mode {
	case "mutate":
		if err := file.Close(); err != nil {
			return result{}, err
		}
		started := time.Now()
		file, err = os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return result{}, err
		}
		defer file.Close()
		written, err := file.WriteAt(bytes.Repeat([]byte{value}, length), offset)
		if err != nil {
			return result{}, err
		}
		if written != length {
			return result{}, io.ErrShortWrite
		}
		if err := file.Sync(); err != nil {
			return result{}, err
		}
		if err := unix.Syncfs(int(file.Fd())); err != nil {
			return result{}, err
		}
		output.WriteFsyncUS = time.Since(started).Microseconds()
	case "verify":
		defer file.Close()
	default:
		file.Close()
		return result{}, fmt.Errorf("unsupported mode %q", mode)
	}
	actual := make([]byte, length)
	if _, err := file.ReadAt(actual, offset); err != nil {
		return result{}, err
	}
	if !bytes.Equal(actual, bytes.Repeat([]byte{value}, length)) {
		return result{}, fmt.Errorf("changed range does not contain the expected value")
	}
	after, err := file.Stat()
	if err != nil {
		return result{}, err
	}
	output.AfterBlocks512, err = blocks512(after)
	output.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return output, err
}

func verifyBoundary(file *os.File, offset int64, length int, beforeByte, afterByte byte) error {
	for _, boundary := range []struct {
		position int64
		expected byte
	}{
		{position: offset - 1, expected: beforeByte},
		{position: offset + int64(length), expected: afterByte},
	} {
		var actual [1]byte
		if _, err := file.ReadAt(actual[:], boundary.position); err != nil {
			return err
		}
		if actual[0] != boundary.expected {
			return fmt.Errorf("boundary byte at %d is %#x, want %#x", boundary.position, actual[0], boundary.expected)
		}
	}
	return nil
}

func blocks512(info os.FileInfo) (int64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("unexpected stat type %T", info.Sys())
	}
	return stat.Blocks, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
