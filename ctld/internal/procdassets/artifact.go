// Package procdassets owns the guest executable bundled with ctld.
package procdassets

import (
	"debug/elf"
	"fmt"
	"runtime"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/procdartifact"
)

// Digest validates the bundled executable before publishing node readiness.
func Digest() (string, error) {
	if executable == "" {
		return "", fmt.Errorf("ctld has no bundled procd; build with scripts/build-ctld.sh")
	}
	f, err := elf.NewFile(strings.NewReader(executable))
	if err != nil {
		return "", fmt.Errorf("invalid bundled procd: %w", err)
	}
	defer f.Close()
	expected := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}[runtime.GOARCH]
	if expected == 0 || f.Machine != expected || f.Class != elf.ELFCLASS64 || f.Data != elf.ELFDATA2LSB {
		return "", fmt.Errorf("bundled procd architecture does not match ctld")
	}
	return digest.FromString(executable).String(), nil
}

// Install is safe before primary election: both A/B slots only add immutable
// digest-addressed files. It never mutates another version or owns runtime state.
func Install(cache, expected string) (string, error) {
	actual, err := Digest()
	if err != nil {
		return "", err
	}
	if expected != "" && actual != expected {
		return "", fmt.Errorf("bundled procd differs from release digest")
	}
	return procdartifact.InstallEmbedded(cache, executable, actual)
}
