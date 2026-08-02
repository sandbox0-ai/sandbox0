package rootfshead

import "crypto/sha256"

// NameBucket selects a stable directory shard without leaking path names into
// object keys.
func NameBucket(name string) uint8 {
	sum := sha256.Sum256([]byte(name))
	return sum[0]
}
