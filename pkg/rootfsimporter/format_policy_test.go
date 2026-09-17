package rootfsimporter

import (
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestImageImportFormatDefaultsAndRejectsUnknownPolicies(t *testing.T) {
	for _, configured := range []int{-1, 0, 1, 2, 3, 10005} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			format, err := ImageImportFormat(configured)
			if configured != 0 && configured != 2 {
				require.ErrorContains(t, err, "format_generation")
				require.Zero(t, format)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, format)
		})
	}
}

func TestNormalizeBlockOptionsUsesDurableFormat(t *testing.T) {
	for _, generation := range []int{1, 2, 3} {
		for _, explicit := range []int{0, 1, 2, 3} {
			t.Run(fmt.Sprintf("generation-%d-builder-%d", generation, explicit), func(t *testing.T) {
				normalized, err := NormalizeBlockOptions(generation, rootfsblock.BuildOptions{FormatVersion: explicit})
				if generation != 2 || (explicit != 0 && explicit != 2) {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 2, normalized.FormatVersion)
				require.Equal(t, 64<<10, normalized.DataRangeBytes)
				require.Equal(t, "rootfs/v2", normalized.ObjectPrefix)
				again, err := NormalizeBlockOptions(generation, normalized)
				require.NoError(t, err)
				require.Equal(t, normalized, again)
			})
		}
	}
	for _, size := range []int{-1, 65537, 1 << 20, 8 << 20} {
		_, err := NormalizeBlockOptions(2, rootfsblock.BuildOptions{DataRangeBytes: size})
		require.Error(t, err)
	}
	for _, size := range []int{4096, 16384, 65536} {
		normalized, err := NormalizeBlockOptions(2, rootfsblock.BuildOptions{DataRangeBytes: size})
		require.NoError(t, err)
		require.Equal(t, size, normalized.DataRangeBytes)
	}
}
