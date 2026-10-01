package gvisorcli

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// RestoreCheckpointMounts preserves the mount topology in a verified stock
// runsc checkpoint, while rebinding host sources to this destination's bundle.
// Call only after immutable image verification and before runsc create. Never
// use the captured host paths: they belong to a retired source incarnation.
func RestoreCheckpointMounts(imageDirectory string, destination *specs.Spec) error {
	root, err := os.OpenRoot(imageDirectory)
	if err != nil {
		return fmt.Errorf("open checkpoint mounts: %w", err)
	}
	defer root.Close()
	file, err := root.Open("checkpoint.img")
	if err != nil {
		return fmt.Errorf("open checkpoint state: %w", err)
	}
	defer file.Close()
	var header [16]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return fmt.Errorf("read checkpoint metadata header: %w", err)
	}
	// Qualified runsc release-20260914.0 uses a gVisorSF header followed by
	// a big-endian length and a JSON string map (statefile.MetadataUnsafe).
	size := binary.BigEndian.Uint64(header[8:])
	if string(header[:8]) != "gVisorSF" || size == 0 || size > 16<<20 {
		return fmt.Errorf("invalid checkpoint metadata header")
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(file, payload); err != nil {
		return fmt.Errorf("read checkpoint metadata: %w", err)
	}
	var metadata map[string]string
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return fmt.Errorf("decode checkpoint metadata: %w", err)
	}
	var containers map[string]*specs.Spec
	if err := json.Unmarshal([]byte(metadata["container_specs"]), &containers); err != nil || len(containers) != 1 {
		return fmt.Errorf("checkpoint must contain exactly one container spec")
	}
	var captured *specs.Spec
	for _, spec := range containers {
		captured = spec
	}
	if captured == nil || len(captured.Mounts) == 0 || destination == nil {
		return fmt.Errorf("checkpoint container mount spec is missing")
	}
	current := make(map[string]specs.Mount, len(destination.Mounts))
	for _, mount := range destination.Mounts {
		key := checkpointMountDestination(mount.Destination)
		if _, exists := current[key]; exists {
			return fmt.Errorf("duplicate destination mount %q", key)
		}
		current[key] = mount
	}
	mounts := make([]specs.Mount, 0, len(captured.Mounts))
	seen := make(map[string]bool, len(captured.Mounts))
	for _, mount := range captured.Mounts {
		key := checkpointMountDestination(mount.Destination)
		rebound, exists := current[key]
		if !filepath.IsAbs(mount.Destination) || key != filepath.Clean(key) || !exists || seen[key] || mount.Type != rebound.Type {
			return fmt.Errorf("cannot rebind checkpoint mount %q", mount.Destination)
		}
		seen[key] = true
		mount.Source = rebound.Source
		mounts = append(mounts, mount)
	}
	destination.Mounts = mounts
	return nil
}

func checkpointMountDestination(destination string) string {
	// Stock runsc resolves the standard /var/run -> /run guest symlink.
	// The driver-owned CA is authored under /var/run but captured under /run.
	if strings.HasPrefix(destination, "/var/run/") {
		return "/run/" + strings.TrimPrefix(destination, "/var/run/")
	}
	return destination
}
