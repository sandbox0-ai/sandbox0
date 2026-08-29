package legacyackmigration

import "strings"

// Inventory is a non-secret summary of one source database snapshot. It is
// safe to persist as migration evidence.
type Inventory struct {
	ManagerSchemaVersion int64          `json:"manager_schema_version"`
	ActiveLifecycleTxns  int64          `json:"active_lifecycle_transactions"`
	LiveSandboxCount     int            `json:"live_sandbox_count"`
	SandboxStates        map[string]int `json:"sandbox_states"`
	TeamCount            int            `json:"team_count"`
	LayerCount           int            `json:"layer_count"`
	ReachableLayerCount  int            `json:"reachable_layer_count"`
	OrphanLayerCount     int            `json:"orphan_layer_count"`
	LayerBytes           int64          `json:"layer_bytes"`
	ReachableLayerBytes  int64          `json:"reachable_layer_bytes"`
	FilesystemCount      int            `json:"filesystem_count"`
	BindingCount         int            `json:"binding_count"`
	SnapshotCount        int            `json:"snapshot_count"`
	BaseImageCount       int            `json:"base_image_count"`
	InvalidRootCount     int            `json:"invalid_root_count"`
}

// BuildInventory classifies every layer reachable from retained filesystem
// and snapshot roots without assuming that the final freeze gate already
// passes.
func (c Catalog) BuildInventory() Inventory {
	result := Inventory{
		ManagerSchemaVersion: c.ManagerSchemaVersion,
		ActiveLifecycleTxns:  c.ActiveLifecycleTxns,
		LiveSandboxCount:     len(c.Sandboxes),
		SandboxStates:        make(map[string]int),
		LayerCount:           len(c.Layers),
		FilesystemCount:      len(c.Filesystems),
		BindingCount:         len(c.Bindings),
		SnapshotCount:        len(c.Snapshots),
	}
	teams := make(map[string]struct{})
	baseImages := make(map[string]struct{})
	for _, sandbox := range c.Sandboxes {
		result.SandboxStates[sandbox.DesiredState]++
		if sandbox.TeamID != "" {
			teams[sandbox.TeamID] = struct{}{}
		}
	}
	layers := make(map[string]Layer, len(c.Layers))
	for _, layer := range c.Layers {
		layers[layer.ID] = layer
		result.LayerBytes += layer.DiffSize
		if layer.TeamID != "" {
			teams[layer.TeamID] = struct{}{}
		}
		if digest := strings.TrimSpace(layer.BaseImageDigest); digest != "" {
			baseImages[digest] = struct{}{}
		}
	}
	reachable := make(map[string]struct{})
	roots := make([]string, 0, len(c.Filesystems)+len(c.Snapshots))
	for _, filesystem := range c.Filesystems {
		roots = append(roots, filesystem.HeadLayerID)
		if filesystem.TeamID != "" {
			teams[filesystem.TeamID] = struct{}{}
		}
	}
	for _, snapshot := range c.Snapshots {
		roots = append(roots, snapshot.HeadLayerID)
		if snapshot.TeamID != "" {
			teams[snapshot.TeamID] = struct{}{}
		}
	}
	for _, root := range roots {
		visiting := make(map[string]struct{})
		current := strings.TrimSpace(root)
		if current == "" {
			result.InvalidRootCount++
			continue
		}
		for current != "" {
			if _, cycle := visiting[current]; cycle {
				result.InvalidRootCount++
				break
			}
			visiting[current] = struct{}{}
			layer, ok := layers[current]
			if !ok {
				result.InvalidRootCount++
				break
			}
			reachable[current] = struct{}{}
			current = strings.TrimSpace(layer.ParentID)
		}
	}
	for layerID := range reachable {
		result.ReachableLayerBytes += layers[layerID].DiffSize
	}
	result.ReachableLayerCount = len(reachable)
	result.OrphanLayerCount = len(layers) - len(reachable)
	result.TeamCount = len(teams)
	result.BaseImageCount = len(baseImages)
	return result
}
