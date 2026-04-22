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

func TestIsTransferSourceMountAllowsLegacyEmptyClusterID(t *testing.T) {
	mount := &db.VolumeMount{
		VolumeID:  "vol-1",
		ClusterID: "",
		PodID:     "sandbox0-system/ctld-node-a",
	}

	if !isTransferSourceMount(mount, "", "sandbox0-system/ctld-node-a") {
		t.Fatal("expected legacy empty cluster ID to match transfer source")
	}
	if !isTransferSourceMount(mount, "default", "sandbox0-system/ctld-node-a") {
		t.Fatal("expected normalized default cluster ID to match legacy empty cluster ID")
	}
	if isTransferSourceMount(mount, "", "sandbox0-system/ctld-node-b") {
		t.Fatal("expected different transfer source pod to be rejected")
	}
}

func TestFindBoundPortalForVolumeReturnsOtherPortal(t *testing.T) {
	portals := map[string]*portalMount{
		"portal-a": {mountPath: "/workspace/a", volumeID: "vol-1"},
		"portal-b": {mountPath: "/workspace/b", volumeID: "vol-2"},
	}

	pm := findBoundPortalForVolume(portals, "vol-1", "portal-b")
	if pm == nil || pm.mountPath != "/workspace/a" {
		t.Fatalf("findBoundPortalForVolume() = %+v, want /workspace/a", pm)
	}
}

func TestFindBoundPortalForVolumeIgnoresExcludedPortal(t *testing.T) {
	portals := map[string]*portalMount{
		"portal-a": {mountPath: "/workspace/a", volumeID: "vol-1"},
	}

	if pm := findBoundPortalForVolume(portals, "vol-1", "portal-a"); pm != nil {
		t.Fatalf("findBoundPortalForVolume() = %+v, want nil", pm)
	}
}

func TestValidateBindableAccessModeAllowsROX(t *testing.T) {
	accessMode, err := validateBindableAccessMode("ROX")
	if err != nil {
		t.Fatalf("validateBindableAccessMode(ROX) error = %v", err)
	}
	if accessMode != volume.AccessModeROX {
		t.Fatalf("validateBindableAccessMode(ROX) = %s, want %s", accessMode, volume.AccessModeROX)
	}
}

func TestValidateBindableAccessModeRejectsRWX(t *testing.T) {
	if _, err := validateBindableAccessMode("RWX"); err == nil {
		t.Fatal("validateBindableAccessMode(RWX) error = nil, want rejection")
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
