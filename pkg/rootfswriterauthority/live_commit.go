package rootfswriterauthority

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
)

// LiveCommitOperationID is stable across lost replies and daemon restarts. The
// grant's immutable binding supplies the epoch/node scope; sequences never reset
// when the local WAL rotates.
func LiveCommitOperationID(grantID string, sequence uint64) string {
	return "live/" + url.PathEscape(grantID) + "/" + strconv.FormatUint(sequence, 10)
}

func LiveCommitSequence(grantID, operationID string) (uint64, error) {
	prefix := "live/" + url.PathEscape(grantID) + "/"
	if strings.TrimSpace(grantID) == "" || !strings.HasPrefix(operationID, prefix) {
		return 0, fmt.Errorf("live operation does not match its grant")
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(operationID, prefix), 10, 64)
	if err != nil || sequence == 0 || sequence > math.MaxInt64 || operationID != LiveCommitOperationID(grantID, sequence) {
		return 0, fmt.Errorf("live cut sequence is not canonical")
	}
	return sequence, nil
}
