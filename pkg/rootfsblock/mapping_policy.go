package rootfsblock

import "container/list"

// touchLocked reserves only a bounded fraction of the shared cache for recently
// used decoded mapping pages. Other data and oversized pages retain ordinary
// LRU behavior. Pages displaced from protection may use otherwise idle space.
// No payload is copied and total accounting includes both lists exactly once.
func (c *ReadCache) touchLocked(element *list.Element) {
	entry := element.Value.(rangeCacheEntry)
	if entry.page == nil || c.mappingBudget == 0 || entry.bytes > c.mappingBudget {
		c.order.MoveToFront(element)
		return
	}
	if entry.protected {
		c.mappingOrder.MoveToFront(element)
		return
	}
	c.order.Remove(element)
	entry.protected = true
	c.items[entry.key] = c.mappingOrder.PushFront(entry)
	c.mappingBytes += entry.bytes
	for c.mappingBytes > c.mappingBudget {
		oldest := c.mappingOrder.Back()
		demoted := oldest.Value.(rangeCacheEntry)
		c.mappingOrder.Remove(oldest)
		c.mappingBytes -= demoted.bytes
		demoted.protected = false
		c.items[demoted.key] = c.order.PushFront(demoted)
	}
}
