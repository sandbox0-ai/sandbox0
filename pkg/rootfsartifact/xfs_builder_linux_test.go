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
	"strings"
	"testing"
)

func TestXFSBuilderUsesReflinkLayoutAndCleanUnmount(t *testing.T) {
	runner := &recordingRunner{}
	destination := filepath.Join(t.TempDir(), "base.xfs")
	source := t.TempDir()
	if err := (XFSBuilder{Runner: runner}).Build(context.Background(), source, destination, MinimumLogicalSizeBytes); err != nil {
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
	if got := runner.calls[2].args[3]; got != source+"/." {
		t.Fatalf("cp source = %q, want %q", got, source+"/.")
	}
}

func TestXFSBuilderUnmountsAfterCopyFailure(t *testing.T) {
	runner := &recordingRunner{failName: "cp"}
	destination := filepath.Join(t.TempDir(), "base.xfs")
	err := (XFSBuilder{Runner: runner}).Build(
		context.Background(), t.TempDir(), destination, MinimumLogicalSizeBytes,
	)
	if err == nil || !errors.Is(err, errCommandFailure) {
		t.Fatalf("Build() error = %v, want command failure", err)
	}
	if got, want := runner.names(), []string{"mkfs.xfs", "mount", "cp", "umount"}; !slices.Equal(got, want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial image stat error = %v, want not exist", statErr)
	}
}

func TestXFSBuilderRetainsImageWhenUnmountCannotBeRecovered(t *testing.T) {
	runner := &recordingRunner{failName: "umount"}
	destination := filepath.Join(t.TempDir(), "base.xfs")
	err := (XFSBuilder{Runner: runner}).Build(
		context.Background(), t.TempDir(), destination, MinimumLogicalSizeBytes,
	)
	if !errors.Is(err, ErrXFSImageStillMounted) {
		t.Fatalf("Build() error = %v, want ErrXFSImageStillMounted", err)
	}
	if _, statErr := os.Stat(destination); statErr != nil {
		t.Fatalf("retained image stat: %v", statErr)
	}
	if got, want := runner.names(), []string{"mkfs.xfs", "mount", "cp", "umount", "umount", "umount"}; !slices.Equal(got, want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
	if got := runner.calls[len(runner.calls)-1].args; !slices.Equal(got, []string{"-l", runner.calls[1].args[5]}) {
		t.Fatalf("lazy umount args = %v", got)
	}
}

func TestXFSBuilderRejectsSymlinkedSourceBeforeCreatingImage(t *testing.T) {
	realSource := t.TempDir()
	symlink := filepath.Join(t.TempDir(), "source")
	if err := os.Symlink(realSource, symlink); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "base.xfs")
	runner := &recordingRunner{}
	err := (XFSBuilder{Runner: runner}).Build(context.Background(), symlink, destination, MinimumLogicalSizeBytes)
	if err == nil || !strings.Contains(err.Error(), "must not traverse symlinks") {
		t.Fatalf("Build() error = %v, want symlink rejection", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("commands = %v, want none", runner.names())
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination stat error = %v, want not exist", statErr)
	}
}

func TestBoundedCommandOutputKeepsPrefix(t *testing.T) {
	output := &boundedCommandOutput{limit: 4}
	if n, err := output.Write([]byte("abcdef")); err != nil || n != 6 {
		t.Fatalf("first Write() = %d, %v", n, err)
	}
	if n, err := output.Write([]byte("gh")); err != nil || n != 2 {
		t.Fatalf("second Write() = %d, %v", n, err)
	}
	if got, want := output.String(), "abcd\n[output truncated]"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
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
