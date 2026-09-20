//go:build procd_bundle

package procdassets

import _ "embed"

// executable is supplied by the build overlay; no generated binary enters Git.
//
//go:embed procd.bin
var executable string
