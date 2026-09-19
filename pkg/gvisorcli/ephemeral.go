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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// EphemeralTmpDirectory is the private OCI-bundle directory owned by runsc.
const EphemeralTmpDirectory = "runtime-tmp"

// PrepareEphemeralTmp gives stock runsc a private disk directory for its tmpfs
// filestore. The mount hint selects a file-backed tmpfs even with overlay2=none,
// preserving direct writes to the persistent RootFS. Only /tmp changes medium.
func PrepareEphemeralTmp(bundle string, spec *specs.Spec) error {
	for i := range spec.Mounts {
		mount := &spec.Mounts[i]
		if mount.Destination != "/tmp" {
			continue
		}
		if mount.Type != "tmpfs" {
			return errors.New("expected logical tmpfs mount for runtime /tmp")
		}
		dir := filepath.Join(bundle, EphemeralTmpDirectory)
		if !filepath.IsAbs(dir) {
			return errors.New("runtime tmp backing directory must be absolute")
		}
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create runtime tmp backing directory: %w", err)
		}
		if err := validateEphemeralTmpDirectory(dir); err != nil {
			return err
		}
		if err := requireDiskFilesystem(dir); err != nil {
			return err
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			return fmt.Errorf("runtime tmp backing directory must be empty: %s", dir)
		}
		if spec.Annotations == nil {
			spec.Annotations = make(map[string]string)
		}
		prefix := "dev.gvisor.spec.mount.sandbox0-tmp."
		spec.Annotations[prefix+"source"] = dir
		spec.Annotations[prefix+"type"] = "tmpfs"
		spec.Annotations[prefix+"share"] = "container"
		spec.Annotations[prefix+"options"] = strings.Join(mount.Options, ",")
		mount.Type, mount.Source = "bind", dir
		mount.Options = []string{"bind", "rw", "nosuid", "nodev"}
		return nil
	}
	return errors.New("runtime OCI spec has no /tmp mount")
}

func validateEphemeralTmpDirectory(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("runtime tmp backing directory is not a private directory: %s", dir)
	}
	return nil
}

// CleanupEphemeralTmp runs only after runsc absence is proven. Normally runsc
// deletes its own filestore; this also handles death between deleting its state
// and unlinking the file. Never recursively delete guest-controlled content.
// The caller must authenticate and validate the enclosing OCI bundle path.
func CleanupEphemeralTmp(bundle, containerID string) error {
	if containerID == "" || filepath.Base(containerID) != containerID || containerID == "." || containerID == ".." {
		return errors.New("invalid runtime tmp container identity")
	}
	dir := filepath.Join(bundle, EphemeralTmpDirectory)
	if err := validateEphemeralTmpDirectory(dir); errors.Is(err, os.ErrNotExist) {
		return nil // Legacy runtimes and already-cleaned bundles have no filestore.
	} else if err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Remove(".gvisor.filestore." + containerID); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove runtime tmp filestore: %w", err)
	}
	// An unexpected file belongs to an unverified runtime. Keep the directory
	// and refuse cleanup proof rather than deleting another incarnation's data.
	if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove empty runtime tmp directory: %w", err)
	}
	return nil
}
