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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/nomad/plugins/drivers"
	"golang.org/x/sys/unix"
)

func TestPrivilegedBusyBundleSurvivesNomadAllocationGC(t *testing.T) {
	if os.Getenv("SANDBOX0_PRIVILEGED_MOUNT_TEST") != "1" {
		t.Skip("requires an isolated Linux mount namespace with CAP_SYS_ADMIN")
	}
	base := t.TempDir()
	config := &drivers.TaskConfig{ID: "slot-1", AllocDir: filepath.Join(base, "alloc", "allocation-1")}
	bundle := driverBundleDir(config)
	root := filepath.Join(bundle, "rootfs")
	source := filepath.Join(base, "source")
	for _, path := range []string{config.AllocDir, root, source} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount("tmpfs", source, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}
	defer unix.Unmount(source, 0)
	if err := unix.Mount(source, root, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	defer unix.Unmount(root, 0)
	holder, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if err := os.RemoveAll(config.AllocDir); err != nil {
		t.Fatal(err)
	}
	if err := (systemMounter{}).Unmount(root); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("unmount during active consumer = %v, want EBUSY", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("Nomad GC hid the busy task root: %v", err)
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := (systemMounter{}).Unmount(root); err != nil {
		t.Fatalf("retry task root unmount: %v", err)
	}
}
