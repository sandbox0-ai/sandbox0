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
	"fmt"

	"golang.org/x/sys/unix"
)

func requireDiskFilesystem(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect runtime tmp backing filesystem: %w", err)
	}
	if stat.Type == unix.TMPFS_MAGIC || stat.Type == unix.RAMFS_MAGIC {
		return fmt.Errorf("runtime tmp backing directory must use disk, not tmpfs/ramfs: %s", path)
	}
	return nil
}
