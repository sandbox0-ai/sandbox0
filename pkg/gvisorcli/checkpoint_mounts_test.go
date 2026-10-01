package gvisorcli

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func checkpointMountImage(t *testing.T, spec *specs.Spec) string {
	t.Helper()
	containers, err := json.Marshal(map[string]*specs.Spec{"__no_name_0": spec})
	require.NoError(t, err)
	metadata, err := json.Marshal(map[string]string{"container_specs": string(containers)})
	require.NoError(t, err)
	header := make([]byte, 16)
	copy(header, "gVisorSF")
	binary.BigEndian.PutUint64(header[8:], uint64(len(metadata)))
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "checkpoint.img"), append(header, metadata...), 0600))
	return directory
}

func TestRestoreCheckpointMountsAcrossDriverUpgrade(t *testing.T) {
	for _, capturedCA := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_CA_mount", true: "with_CA_mount"}[capturedCA], func(t *testing.T) {
			captured := specs.Spec{Mounts: []specs.Mount{
				{Destination: "/tmp", Type: "bind", Source: "/retired/tmp", Options: []string{"bind", "rw"}},
				{Destination: "/procd", Type: "bind", Source: "/retired/procd", Options: []string{"bind", "ro"}},
			}}
			ca := specs.Mount{Destination: "/run/sandbox0/networking/mitm-ca.crt", Type: "bind", Source: "/retired/ca", Options: []string{"rbind", "ro", "noexec"}}
			if capturedCA {
				captured.Mounts = append(captured.Mounts, ca)
			}
			current := specs.Spec{Mounts: []specs.Mount{
				{Destination: "/tmp", Type: "bind", Source: "/destination/tmp", Options: []string{"bind", "rw"}},
				{Destination: "/procd", Type: "bind", Source: "/destination/procd", Options: []string{"bind", "ro"}},
				{Destination: "/var/run/sandbox0/networking/mitm-ca.crt", Type: "bind", Source: "/destination/ca", Options: []string{"rbind", "ro", "noexec"}},
			}}
			require.NoError(t, RestoreCheckpointMounts(checkpointMountImage(t, &captured), &current))
			require.Len(t, current.Mounts, len(captured.Mounts))
			for i, mount := range current.Mounts {
				require.Equal(t, captured.Mounts[i].Destination, mount.Destination)
				require.Equal(t, captured.Mounts[i].Options, mount.Options)
				require.Contains(t, mount.Source, "/destination/")
			}
		})
	}
}

func TestRestoreCheckpointMountsFailsClosed(t *testing.T) {
	for name, captured := range map[string]*specs.Spec{
		"missing_spec":       nil,
		"unknown_mount":      {Mounts: []specs.Mount{{Destination: "/host", Type: "bind", Source: "/etc"}}},
		"changed_mount_type": {Mounts: []specs.Mount{{Destination: "/tmp", Type: "tmpfs"}}},
		"duplicate_mount":    {Mounts: []specs.Mount{{Destination: "/tmp", Type: "bind"}, {Destination: "/tmp", Type: "bind"}}},
	} {
		t.Run(name, func(t *testing.T) {
			current := specs.Spec{Mounts: []specs.Mount{{Destination: "/tmp", Type: "bind", Source: "/destination/tmp"}}}
			before := append([]specs.Mount(nil), current.Mounts...)
			require.Error(t, RestoreCheckpointMounts(checkpointMountImage(t, captured), &current))
			require.Equal(t, before, current.Mounts)
		})
	}
	for _, payload := range [][]byte{nil, []byte("invalid header!!"), append([]byte("gVisorSF"), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)} {
		directory := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(directory, "checkpoint.img"), payload, 0600))
		require.Error(t, RestoreCheckpointMounts(directory, &specs.Spec{}))
	}
}
