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
	"os"
	"path/filepath"
	"testing"
)

func TestMutateAndVerifyCopyUpRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large")
	payload := bytes.Repeat([]byte{0x21}, 32<<10)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	mutated, err := run("mutate", path, int64(len(payload)), 8<<10, 4096, 0xee, 0x21, 0x21)
	if err != nil {
		t.Fatal(err)
	}
	if mutated.WriteFsyncUS < 0 {
		t.Fatalf("write duration = %d", mutated.WriteFsyncUS)
	}
	if _, err := run("verify", path, int64(len(payload)), 8<<10, 4096, 0xee, 0x21, 0x21); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual[:8<<10], payload[:8<<10]) || !bytes.Equal(actual[12<<10:], payload[12<<10:]) {
		t.Fatal("copy-up workload changed bytes outside the requested range")
	}
}

func TestVerifyRejectsChangedBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x21}, 32<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("verify", path, 32<<10, 8<<10, 1, 0xee, 0x21, 0x21); err == nil {
		t.Fatal("verify accepted an unchanged target range")
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0x22}, (8<<10)-1); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if _, err := run("mutate", path, 32<<10, 8<<10, 1, 0xee, 0x21, 0x21); err == nil {
		t.Fatal("mutate accepted a changed boundary")
	}
}

func TestMutateSupportsDifferentBoundaryBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large")
	payload := append(bytes.Repeat([]byte{0x20}, 8<<10), bytes.Repeat([]byte{0x21}, 8<<10)...)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("mutate", path, int64(len(payload)), 8<<10, 1, 0xee, 0x20, 0x21); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsOutOfBoundsRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x21}, 32<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		offset int64
		length int
	}{
		{offset: 0, length: 1},
		{offset: 1, length: 0},
		{offset: 32<<10 - 1, length: 1},
	} {
		if _, err := run("mutate", path, 32<<10, test.offset, test.length, 0xee, 0x21, 0x21); err == nil {
			t.Fatalf("run accepted offset=%d length=%d", test.offset, test.length)
		}
	}
}
