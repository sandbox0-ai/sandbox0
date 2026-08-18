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

package driver

import (
	"context"
	"os"
	"path/filepath"
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
	config := defaultPluginConfig()
	config.RunscPath = runscPath
	config.RunscRoot = filepath.Join(tempDir, "root")
	runner := NewCommandRunsc(*config)

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
