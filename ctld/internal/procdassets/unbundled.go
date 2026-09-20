//go:build !procd_bundle

package procdassets

// Ordinary go test builds need no payload. A deployable ctld must use build-ctld.sh.
const executable = ""
