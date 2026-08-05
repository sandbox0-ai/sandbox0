package rootfshead

import "crypto/sha256"

func NameBucket(name string) uint8 {
	sum := sha256.Sum256([]byte(name))
	return sum[0]
}
