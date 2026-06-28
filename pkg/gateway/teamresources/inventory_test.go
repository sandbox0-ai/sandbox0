package teamresources

import "testing"

func TestInventoryTracksBlockingResources(t *testing.T) {
	inventory := &Inventory{TeamID: "team-1"}
	if inventory.HasBlockingResources() {
		t.Fatal("empty inventory should not block deletion")
	}

	inventory.AddBlocking("sandboxes", 2)
	inventory.AddBlocking("api_keys", 0)

	if !inventory.HasBlockingResources() {
		t.Fatal("inventory with blocking resources should block deletion")
	}
	if len(inventory.BlockingResources) != 1 {
		t.Fatalf("blocking resources = %d, want 1", len(inventory.BlockingResources))
	}
	if inventory.BlockingResources[0].Category != "sandboxes" || inventory.BlockingResources[0].Count != 2 {
		t.Fatalf("blocking resource = %#v, want sandboxes count 2", inventory.BlockingResources[0])
	}
}

func TestRepositoryBlockingQueriesCoverTeamScopedStores(t *testing.T) {
	repo := NewRepository(nil)
	got := map[string]bool{}
	for _, query := range repo.blockingQueries() {
		got[query.category] = true
	}

	want := []string{
		"api_keys",
		"ssh_public_keys",
		"scheduler_templates",
		"scheduler_template_allocations",
		"credential_sources",
		"credential_source_versions",
		"sandbox_egress_credential_bindings",
		"sandboxes",
		"sandbox_lifecycle_transactions",
		"sandbox_rootfs_states",
		"sandbox_rootfs_heads",
		"sandbox_rootfs_bindings",
		"rootfs_filesystems",
		"rootfs_snapshots",
		"rootfs_layers",
		"rootfs_objects",
		"rootfs_object_deletions",
		"sandbox_volumes",
		"sandbox_volume_snapshots",
		"sandbox_volume_mounts",
		"snapshot_coordinations",
		"snapshot_flush_responses",
		"sandbox_volume_owners",
		"sandbox_volume_s0fs_heads",
		"sandbox_volume_handoffs",
		"sandbox_volume_sync_replicas",
		"sandbox_volume_sync_journal",
		"sandbox_volume_sync_conflicts",
		"sandbox_volume_sync_requests",
		"sandbox_volume_sync_retention",
		"sandbox_volume_sync_namespace_policy",
		"team_quota_limits",
	}
	for _, category := range want {
		if !got[category] {
			t.Fatalf("missing blocking query category %q", category)
		}
	}
}

func TestRepositoryRetainedQueriesDocumentMeteringPolicy(t *testing.T) {
	repo := NewRepository(nil)
	got := map[string]bool{}
	for _, query := range repo.retainedQueries() {
		got[query.category] = true
	}

	want := []string{
		"usage_events",
		"usage_windows",
		"manager_sandbox_projection_state",
		"storage_projection_state",
	}
	for _, category := range want {
		if !got[category] {
			t.Fatalf("missing retained query category %q", category)
		}
	}
	if MeteringRetentionPolicy == "" {
		t.Fatal("metering retention policy must be explicit")
	}
}

func TestDiscoveryHelpers(t *testing.T) {
	schemas := compactSchemas([]string{"shared_gateway", "", "manager", "shared_gateway", " "})
	if len(schemas) != 2 || schemas[0] != "shared_gateway" || schemas[1] != "manager" {
		t.Fatalf("schemas = %#v, want shared_gateway and manager", schemas)
	}

	if !isIgnoredDiscoveredTable("shared_gateway", "team_members") {
		t.Fatal("team_members should be ignored because team deletion cascades membership")
	}
	if isIgnoredDiscoveredTable("manager", "sandboxes") {
		t.Fatal("sandboxes should not be ignored")
	}
	if got := discoveredCategory("manager", "custom_team_table"); got != "manager.custom_team_table" {
		t.Fatalf("category = %q, want manager.custom_team_table", got)
	}
}
