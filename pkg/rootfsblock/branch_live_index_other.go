//go:build !linux

package rootfsblock

import (
	"fmt"
	"os"
)

func newLiveIndexFile(_ string) (*os.File, error) {
	return nil, fmt.Errorf("live branch index requires Linux sealed memory files")
}
func sealLiveIndex(_ *os.File) error         { return fmt.Errorf("live branch index requires Linux") }
func validateLiveIndexSeal(_ *os.File) error { return fmt.Errorf("live branch index requires Linux") }

func liveJournalIdentity(_ *os.File) (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("live branch index requires Linux")
}
