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

package rootfsartifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestXFSBuilderUsesReflinkLayoutAndCleanUnmount(t *testing.T) {
	runner := &recordingRunner{}
	destination := filepath.Join(t.TempDir(), "base.xfs")
	if err := (XFSBuilder{Runner: runner}).Build(context.Background(), t.TempDir(), destination, MinimumLogicalSizeBytes); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatalf("stat image: %v", err)
	}
	if info.Size() != MinimumLogicalSizeBytes {
		t.Fatalf("image size = %d, want %d", info.Size(), MinimumLogicalSizeBytes)
	}
	if got, want := runner.names(), []string{"mkfs.xfs", "mount", "cp", "umount", "xfs_repair"}; !slices.Equal(got, want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
}

func TestXFSBuilderUnmountsAfterCopyFailure(t *testing.T) {
	runner := &recordingRunner{failName: "cp"}
	err := (XFSBuilder{Runner: runner}).Build(
		context.Background(), t.TempDir(), filepath.Join(t.TempDir(), "base.xfs"), MinimumLogicalSizeBytes,
	)
	if err == nil || !errors.Is(err, errCommandFailure) {
		t.Fatalf("Build() error = %v, want command failure", err)
	}
	if got, want := runner.names(), []string{"mkfs.xfs", "mount", "cp", "umount"}; !slices.Equal(got, want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
}

var errCommandFailure = errors.New("command failed")

type commandCall struct {
	name string
	args []string
}

type recordingRunner struct {
	calls    []commandCall
	failName string
}

func (r *recordingRunner) Run(_ context.Context, name string, args ...string) error {
	r.calls = append(r.calls, commandCall{name: name, args: append([]string(nil), args...)})
	if name == r.failName {
		return errCommandFailure
	}
	return nil
}

func (r *recordingRunner) names() []string {
	names := make([]string, 0, len(r.calls))
	for _, call := range r.calls {
		names = append(names, call.name)
	}
	return names
}
