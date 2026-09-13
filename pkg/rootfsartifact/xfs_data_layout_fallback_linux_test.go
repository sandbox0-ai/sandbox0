//go:build linux

package rootfsartifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestXFSDataRangeBudgetFallbackUsesEntireLegacyGrid(t *testing.T) {
	const size = 3 * 64 << 10
	plan, err := xfsDataRangeFallback(t.Context(), size, 64<<10, fmt.Errorf("depth: %w", ErrXFSDataRangeLimit))
	require.NoError(t, err)
	require.Equal(t, XFSDataRangeBudgetFallback, plan.Fallback)
	require.Zero(t, plan.Stats, "a failed partial scan is not a complete metadata result")
	data := bytes.Repeat([]byte{0x72}, size)
	options := rootfsblock.BuildOptions{FormatVersion: 2}
	a, b := &xfsLayoutTestStore{objects: map[string][]byte{}}, &xfsLayoutTestStore{objects: map[string][]byte{}}
	want, err := rootfsblock.BuildMaterializedGeneration(t.Context(), bytes.NewReader(data), size, a, options)
	require.NoError(t, err)
	got, err := rootfsblock.BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(data), size, b, options, plan.Layout)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, a.objects, b.objects)
}

func TestXFSDataRangeFallbackNeverMasksUnsafeMetadataOrCancellation(t *testing.T) {
	for _, cause := range []error{errors.New("cross-device"), errors.New("overlap"), errors.New("short read"), context.Canceled} {
		plan, err := xfsDataRangeFallback(t.Context(), 1<<20, 64<<10, cause)
		require.ErrorIs(t, err, cause)
		require.Nil(t, plan.Layout)
		require.Empty(t, plan.Fallback)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plan, err := xfsDataRangeFallback(ctx, 1<<20, 64<<10, ErrXFSDataRangeLimit)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, plan.Layout)
}
