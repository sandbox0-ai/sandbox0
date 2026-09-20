//go:build !procd_bundle

package procdassets

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnbundledBuildCannotPublishReadiness(t *testing.T) {
	_, err := Digest()
	require.ErrorContains(t, err, "no bundled procd")
	_, err = Install("/must-not-be-created", "")
	require.ErrorContains(t, err, "no bundled procd")
}
