//go:build linux

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

package nomadruntime

import (
	"fmt"
	"path/filepath"
	"strings"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

func networkNamespaceIdentity(path string) (string, error) {
	return runtimePathIdentity(path, "netns-v1", "Nomad network namespace")
}

func stableMountIdentity(path string) (string, error) {
	return runtimePathIdentity(path, "mount-v1", "runtime slot stable mount")
}

func runtimePathIdentity(path, version, description string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return "", fmt.Errorf("%s path must be a non-root absolute path", description)
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("stat %s: %w", description, err)
	}
	return fmt.Sprintf("%s:%x:%x", version, uint64(stat.Dev), stat.Ino), nil
}

func networkChainName(containerID string) string {
	return protocol.NomadNetworkChainName(containerID)
}
