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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestRunFSXIsDeterministic(t *testing.T) {
	firstDigest, firstSize, err := runFSX(filepath.Join(t.TempDir(), "first"), 2_000)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, secondSize, err := runFSX(filepath.Join(t.TempDir(), "second"), 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest || firstSize != secondSize {
		t.Fatalf("deterministic FSX mismatch: (%s, %d) != (%s, %d)", firstDigest, firstSize, secondDigest, secondSize)
	}
}

func TestMinimalACLEncoding(t *testing.T) {
	payload := minimalACL(7, 5, 1)
	if got, want := len(payload), 36; got != want {
		t.Fatalf("ACL length %d, want %d", got, want)
	}
	if got := binary.LittleEndian.Uint32(payload[:4]); got != 2 {
		t.Fatalf("ACL version %d, want 2", got)
	}
	wantTags := []uint16{1, 4, 16, 32}
	wantPerms := []uint16{7, 5, 5, 1}
	for index := range wantTags {
		offset := 4 + index*8
		if got := binary.LittleEndian.Uint16(payload[offset : offset+2]); got != wantTags[index] {
			t.Fatalf("ACL tag %d at entry %d, want %d", got, index, wantTags[index])
		}
		if got := binary.LittleEndian.Uint16(payload[offset+2 : offset+4]); got != wantPerms[index] {
			t.Fatalf("ACL permission %d at entry %d, want %d", got, index, wantPerms[index])
		}
		if got := binary.LittleEndian.Uint32(payload[offset+4 : offset+8]); got != ^uint32(0) {
			t.Fatalf("ACL ID %d at entry %d, want undefined", got, index)
		}
	}
}

func TestExerciseInotify(t *testing.T) {
	if err := exerciseInotify(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAndVerifySmallFileTrees(t *testing.T) {
	root := t.TempDir()
	counts := manifest{ZeroFlatCount: 5, ZeroDeepCount: 7, FourKiBCount: 3, SixtyFourKiBCount: 2}
	if err := createSmallFiles(root, counts); err != nil {
		t.Fatal(err)
	}
	if err := verifySmallFiles(root, counts); err != nil {
		t.Fatal(err)
	}
	deepFile := filepath.Join(root, "zero-deep", "00", "00", "000000")
	if err := os.WriteFile(deepFile, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifySmallFiles(root, counts); err == nil {
		t.Fatal("verifySmallFiles accepted a corrupted deep-tree file")
	}
}

func TestVerifyFileDigest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload")
	payload := []byte("sandbox0-rootfs")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := digestFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyFileDigest(path, int64(len(payload)), digest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, len(payload)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyFileDigest(path, int64(len(payload)), digest); err == nil {
		t.Fatal("verifyFileDigest accepted changed contents")
	}
}
