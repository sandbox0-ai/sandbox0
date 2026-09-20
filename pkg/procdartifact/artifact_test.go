package procdartifact

import (
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestArtifactRejectsLooseOrPlaceholderIdentity(t *testing.T) {
	good := Artifact{Digest: digest.FromString("binary").String(), Protocol: "sandbox0.procd.v3"}
	require.NoError(t, good.Validate())
	for _, value := range []Artifact{
		{Digest: "sha256:../procd", Protocol: good.Protocol},
		{Digest: "sha256:" + strings.Repeat("A", 64), Protocol: good.Protocol},
		{Digest: good.Digest}, {Digest: good.Digest, Protocol: " v3"},
		{Digest: PlaceholderDigest(), Protocol: good.Protocol},
	} {
		require.Error(t, value.Validate())
	}
	require.NotContains(t, rootFSPlaceholder, "exec ")
	require.Contains(t, rootFSPlaceholder, "exit 126")
}
