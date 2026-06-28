package teamresources

// ResourceCount reports how many team-owned resources exist in one category.
type ResourceCount struct {
	Category string `json:"category"`
	Count    int64  `json:"count"`
}

// Inventory is the team deletion preflight result.
type Inventory struct {
	TeamID            string          `json:"team_id"`
	BlockingResources []ResourceCount `json:"blocking_resources,omitempty"`
	RetainedResources []ResourceCount `json:"retained_resources,omitempty"`
	RetentionPolicy   string          `json:"retention_policy,omitempty"`
}

const MeteringRetentionPolicy = "historical metering records are retained for usage truth and audit and do not block team deletion"

// HasBlockingResources returns true when team deletion should be rejected.
func (i *Inventory) HasBlockingResources() bool {
	return i != nil && len(i.BlockingResources) > 0
}

// AddBlocking adds a blocking resource category when count is non-zero.
func (i *Inventory) AddBlocking(category string, count int64) {
	if i == nil || count <= 0 {
		return
	}
	i.BlockingResources = append(i.BlockingResources, ResourceCount{Category: category, Count: count})
}

// AddRetained adds a retained resource category when count is non-zero.
func (i *Inventory) AddRetained(category string, count int64) {
	if i == nil || count <= 0 {
		return
	}
	i.RetainedResources = append(i.RetainedResources, ResourceCount{Category: category, Count: count})
}
