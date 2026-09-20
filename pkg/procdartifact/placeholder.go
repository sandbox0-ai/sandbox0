package procdartifact

import (
	_ "embed"
	"github.com/opencontainers/go-digest"
)

// The fixed fail-closed placeholder preserves the legacy RootFS attestation
// schema without embedding a platform daemon. Its digest never depends on the
// selected runtime version; existing embedded artifacts remain readable.
//
//go:embed rootfs-placeholder
var rootFSPlaceholder string

func PlaceholderDigest() string { return digest.FromString(rootFSPlaceholder).String() }
