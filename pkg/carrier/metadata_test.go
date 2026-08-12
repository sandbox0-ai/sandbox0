package carrier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarkerImageBindsExactlyOneValidatedSlot(t *testing.T) {
	image, err := MarkerImage("s0-0123456789abcdef")
	require.NoError(t, err)
	assert.Equal(t, "sandbox0.local/rootfs-heads:s0-0123456789abcdef", image)
	require.NoError(t, ValidateMarkerImage("s0-0123456789abcdef", image))
	assert.Error(t, ValidateMarkerImage("s0-0123456789abcdef", image+"-other"))
}

func TestValidateSlotRejectsRegistryAndPathSyntax(t *testing.T) {
	for _, value := range []string{"short", "UPPERCASE-slot", "registry/name:tag", "../escape", "s0_value"} {
		t.Run(value, func(t *testing.T) {
			assert.Error(t, ValidateSlot(value))
		})
	}
}
