package rootfswriterauthority

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiveOperationBindsCanonicalMonotonicSequenceToGrant(t *testing.T) {
	for _, grant := range []string{"grant-a", "grant/with/slashes", "grant with spaces"} {
		for _, sequence := range []uint64{1, 2, math.MaxInt64} {
			operation := LiveCommitOperationID(grant, sequence)
			actual, err := LiveCommitSequence(grant, operation)
			require.NoError(t, err)
			require.Equal(t, sequence, actual)
			_, err = LiveCommitSequence("other-grant", operation)
			require.Error(t, err)
		}
	}
	for _, operation := range []string{"live/grant-a/0", "live/grant-a/01", "live/grant-a/+1", "live/grant-a/9223372036854775808", "live/grant-a/1/trailing", "live/other/1"} {
		_, err := LiveCommitSequence("grant-a", operation)
		require.Error(t, err, operation)
	}
}
