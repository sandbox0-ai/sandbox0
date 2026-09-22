package gvisorcli

import (
	"context"
	"debug/elf"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CPUProfileRunsc is optional and does not widen ordinary start/stop authority.
// Its measurement describes the current host, not a particular guest.
type CPUProfileRunsc interface {
	CPUProfile(context.Context) (protocol.MigrationCPUProfile, error)
}

// CPUCoverageRunsc samples every explicitly requested CPU. A successful
// observation still needs node-boot and runtime-launch bindings before use as
// migration eligibility. It is not part of ordinary claim compatibility.
type CPUCoverageRunsc interface {
	CPUCoverage(context.Context, string) (protocol.MigrationCPUObservation, error)
}

var _ CPUProfileRunsc = (*Command)(nil)
var _ CPUCoverageRunsc = (*Command)(nil)

func (r *Command) CPUFeatures(ctx context.Context) ([]string, error) {
	if ctx == nil {
		return nil, fmt.Errorf("CPU observation context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	output, err := r.output(ctx, "cpu-features")
	if err != nil {
		return nil, err
	}
	return protocol.ParseMigrationCPUFeatures(output)
}

// CPUProfile samples stock runsc's complete host feature names, including
// names omitted from /proc/cpuinfo, plus native architecture state layout.
// The executable must be native ELF and remain unchanged during observation.
func (r *Command) CPUProfile(ctx context.Context) (protocol.MigrationCPUProfile, error) {
	var profile protocol.MigrationCPUProfile
	if ctx == nil {
		return profile, fmt.Errorf("CPU observation context is required")
	}
	if err := ctx.Err(); err != nil {
		return profile, err
	}
	if runtime.GOOS != "linux" {
		return profile, fmt.Errorf("CPU observation requires a Linux host")
	}
	path, err := exec.LookPath(r.config.Path)
	if err != nil {
		return profile, err
	}
	// Use the inspected path for both commands rather than resolving PATH again.
	observer := *r
	observer.config.Path = path
	before, err := os.Stat(path)
	if err != nil {
		return profile, err
	}
	binary, err := elf.Open(path)
	if err != nil {
		return profile, fmt.Errorf("inspect native runsc executable: %w", err)
	}
	machine := binary.Machine
	closeErr := binary.Close()
	if closeErr != nil {
		return profile, closeErr
	}
	if runtime.GOARCH == "amd64" && machine != elf.EM_X86_64 || runtime.GOARCH == "arm64" && machine != elf.EM_AARCH64 {
		return profile, fmt.Errorf("runsc architecture does not match the observer")
	}
	cacheLine, layout, err := nativeCPUStateLayout()
	if err != nil {
		return profile, err
	}
	version, err := observer.Version(ctx)
	if err != nil {
		return profile, err
	}
	features, err := observer.CPUFeatures(ctx)
	if err != nil {
		return profile, err
	}
	after, err := os.Stat(path)
	if err != nil {
		return profile, err
	}
	if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return profile, fmt.Errorf("runsc changed during CPU observation")
	}
	profile = protocol.MigrationCPUProfile{Version: protocol.MigrationCPUProfileVersion, Architecture: runtime.GOARCH, RunscVersion: version, Features: features, CacheLineBytes: cacheLine, XStateLayoutDigest: layout}
	return profile, profile.Validate()
}
