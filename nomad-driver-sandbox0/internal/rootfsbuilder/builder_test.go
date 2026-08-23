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

package rootfsbuilder

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
)

func TestBuildProducesDurableGenerationDescriptor(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "marker"), []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := objectstore.NewMemoryStore(t.Name()).(objectstore.ContextConditionalStore)
	image := filepath.Join(t.TempDir(), "base.xfs")
	descriptor, err := Build(context.Background(), store, Options{
		SourceRoot: source, ImagePath: image, RootFSID: "rootfs-1",
		ObjectPrefix: "test/rootfs", Runner: noOpRunner{},
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("descriptor validation: %v", err)
	}
	if descriptor.DurabilityState != "s3_materialized" || descriptor.WriterEpoch != 0 {
		t.Fatalf("descriptor = %+v, want materialized base generation", descriptor)
	}
	if info, err := os.Stat(image); err != nil || info.Size() != rootfsartifact.MinimumLogicalSizeBytes {
		t.Fatalf("image stat = %+v, %v", info, err)
	}
}

type noOpRunner struct{}

func (noOpRunner) Run(context.Context, string, ...string) error { return nil }
