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

package gvisorcli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommandRunscCreateDoesNotWaitForInheritedStderr(t *testing.T) {
	tempDir := t.TempDir()
	runscPath := filepath.Join(tempDir, "runsc")
	script := `#!/bin/sh
case "$*" in
  *" create "*)
    sleep 30 >&2 &
    exit 0
    ;;
esac
exit 0
`
	if err := os.WriteFile(runscPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake runsc: %v", err)
	}
	config := Config{
		Path: runscPath, Root: filepath.Join(tempDir, "root"),
		Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true,
	}
	runner := New(config)

	done := make(chan error, 1)
	go func() {
		done <- runner.Create(context.Background(), filepath.Join(tempDir, "bundle"), "container")
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Create() waited for a long-lived child that inherited stderr")
	}
}

func TestCommandRunBoundsInheritedStderr(t *testing.T) {
	runner := newOutputRunsc(t, `
dd if=/dev/zero bs=65537 count=1 1>&2 2>/dev/null
exit 1
`)

	err := runner.Create(context.Background(), t.TempDir(), "container")
	if err == nil || !strings.Contains(err.Error(), "stderr exceeds 65536 bytes") {
		t.Fatalf("Create() error = %v", err)
	}
}
