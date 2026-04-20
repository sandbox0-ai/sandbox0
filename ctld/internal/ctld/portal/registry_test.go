package portal

import (
	"encoding/json"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func TestIsConflictingMountForCtldBindIgnoresStorageProxyOwners(t *testing.T) {
	mount := &db.VolumeMount{
		VolumeID:     "vol-1",
		ClusterID:    "cluster-a",
		PodID:        "storage-proxy-1",
		MountOptions: mustRegistryMountOptions(t, volume.MountOptions{OwnerKind: volume.OwnerKindStorageProxy, AccessMode: volume.AccessModeRWO}),
	}

	if isConflictingMountForCtldBind(mount, "cluster-a", "ctld-1") {
		t.Fatal("expected storage-proxy owner to be ignored during ctld bind")
	}
}

func TestIsConflictingMountForCtldBindBlocksCtldOwners(t *testing.T) {
	mount := &db.VolumeMount{
		VolumeID:     "vol-1",
		ClusterID:    "cluster-a",
		PodID:        "sandbox0-system/ctld-node-a",
		MountOptions: mustRegistryMountOptions(t, volume.MountOptions{OwnerKind: volume.OwnerKindCtld, AccessMode: volume.AccessModeRWO}),
	}

	if !isConflictingMountForCtldBind(mount, "cluster-a", "sandbox0-system/ctld-node-b") {
		t.Fatal("expected ctld owner to block a different ctld bind")
	}
}

func mustRegistryMountOptions(t *testing.T, opts volume.MountOptions) *json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("marshal mount options: %v", err)
	}
	msg := json.RawMessage(raw)
	return &msg
}
