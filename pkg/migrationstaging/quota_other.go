//go:build !linux

package migrationstaging

import "fmt"

func inspect(string, uint32) (snapshot, error) {
	return snapshot{}, fmt.Errorf("migration staging project quotas require Linux")
}

func verifyExistingTree(string, Limits, uint64) error {
	return fmt.Errorf("migration staging project quotas require Linux")
}
