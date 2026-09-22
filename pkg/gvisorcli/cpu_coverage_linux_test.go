package gvisorcli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

func coverageTestProfile() protocol.MigrationCPUProfile {
	return protocol.MigrationCPUProfile{Version: 1, Architecture: "arm64", RunscVersion: "runsc version test", Features: []string{"aes", "asimd", "fp"}}
}

func coverageTestMask(cpus ...int) unix.CPUSet {
	var mask unix.CPUSet
	for _, cpu := range cpus {
		mask.Set(cpu)
	}
	return mask
}

func TestCPUCoverageRequiresAllRequestedCPUsAndHomogeneousProfiles(t *testing.T) {
	for _, heterogeneous := range []bool{false, true} {
		t.Run(fmt.Sprintf("heterogeneous_%t", heterogeneous), func(t *testing.T) {
			original := coverageTestMask(0, 2, 5, 7)
			current := original
			var sampled []int
			observation, err := observeCPUCoverage(t.Context(), "2,5,7", cpuAffinityOps{
				get: func() (unix.CPUSet, error) { return current, nil },
				set: func(set unix.CPUSet) error { current = set; return nil },
			}, func(context.Context) (protocol.MigrationCPUProfile, error) {
				if current.Count() != 1 {
					return protocol.MigrationCPUProfile{}, errors.New("sample was not pinned")
				}
				for cpu := 0; cpu < cpuAffinityMaskBits; cpu++ {
					if current.IsSet(cpu) {
						sampled = append(sampled, cpu)
					}
				}
				profile := coverageTestProfile()
				if heterogeneous && current.IsSet(7) {
					profile.Features = []string{"asimd", "fp"}
				}
				return profile, nil
			})
			if current != original {
				t.Fatal("observer affinity was not restored")
			}
			if !slices.Equal(sampled, []int{2, 5, 7}) {
				t.Fatalf("sampled CPUs = %v", sampled)
			}
			if heterogeneous {
				if err == nil || observation.CPUSet != "" {
					t.Fatalf("accepted heterogeneous evidence: %+v %v", observation, err)
				}
			} else {
				if err != nil || observation.CPUSet != "2,5,7" {
					t.Fatalf("coverage: %+v %v", observation, err)
				}
				if err := observation.Covers("2,7"); err != nil {
					t.Fatal(err)
				}
				if err := observation.Covers("0,2,7"); err == nil {
					t.Fatal("observation included an unmeasured CPU")
				}
			}
		})
	}
}

func TestCPUCoverageRejectsInvalidOrUnavailableCPUSetBeforeSampling(t *testing.T) {
	for _, requested := range []string{"", "2,2", "2,0", "0-1024", "1024", "01", "4", "0,4"} {
		original := coverageTestMask(0, 2)
		current := original
		calls := 0
		_, err := observeCPUCoverage(t.Context(), requested, cpuAffinityOps{
			get: func() (unix.CPUSet, error) { return current, nil },
			set: func(set unix.CPUSet) error { current = set; return nil },
		}, func(context.Context) (protocol.MigrationCPUProfile, error) {
			calls++
			return coverageTestProfile(), nil
		})
		if err == nil || calls != 0 || current != original {
			t.Fatalf("unsafe coverage %q: %v, calls %d", requested, err, calls)
		}
	}
}

func TestCPUCoverageNeverReturnsPartialEvidence(t *testing.T) {
	for _, mode := range []string{"pin error", "pin ignored", "sample error", "cancel", "sample affinity changed", "restore error", "restore narrowed", "restore read error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := coverageTestMask(0, 2)
			current := original
			restored := false
			sampled := false
			observation, err := observeCPUCoverage(ctx, "0,2", cpuAffinityOps{
				get: func() (unix.CPUSet, error) {
					if restored && mode == "restore read error" {
						return unix.CPUSet{}, errors.New("unavailable")
					}
					return current, nil
				},
				set: func(set unix.CPUSet) error {
					if set == original {
						restored = true
						if mode == "restore error" {
							return errors.New("denied")
						}
						if mode == "restore narrowed" {
							current = coverageTestMask(0)
							return nil
						}
					} else {
						if mode == "pin error" {
							return errors.New("offline")
						}
						if mode == "pin ignored" {
							return nil
						}
					}
					current = set
					return nil
				},
			}, func(ctx context.Context) (protocol.MigrationCPUProfile, error) {
				sampled = true
				if mode == "sample error" {
					return protocol.MigrationCPUProfile{}, errors.New("unavailable")
				}
				if mode == "cancel" {
					cancel()
				}
				if mode == "sample affinity changed" {
					current = original
				}
				return coverageTestProfile(), nil
			})
			if err == nil || observation.CPUSet != "" || !restored {
				t.Fatalf("partial evidence: %+v %v restored=%t", observation, err, restored)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error: %v", err)
			}
			if strings.HasPrefix(mode, "pin ") && sampled {
				t.Fatal("sample ran without pinned affinity")
			}
		})
	}
}

// Exercise real thread affinity and fork/exec inheritance without changing the
// test runner's thread affinity. The child reports its own kernel CPU mask.
func TestCPUCoveragePinsSubprocessAndPreservesCallerAffinity(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var before unix.CPUSet
	if err := unix.SchedGetaffinity(0, &before); err != nil {
		t.Fatal(err)
	}
	var cpus []string
	for cpu := 0; cpu < cpuAffinityMaskBits && len(cpus) < 2; cpu++ {
		if before.IsSet(cpu) {
			cpus = append(cpus, strconv.Itoa(cpu))
		}
	}
	requested := strings.Join(cpus, ",")
	var observed []string
	_, err := observeCPUCoverage(t.Context(), requested, cpuAffinityOps{
		get: func() (unix.CPUSet, error) {
			var set unix.CPUSet
			err := unix.SchedGetaffinity(0, &set)
			return set, err
		},
		set: func(set unix.CPUSet) error { return unix.SchedSetaffinity(0, &set) },
	}, func(ctx context.Context) (protocol.MigrationCPUProfile, error) {
		output, err := exec.CommandContext(ctx, "/bin/cat", "/proc/self/status").Output()
		if err != nil {
			return protocol.MigrationCPUProfile{}, err
		}
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(line, "Cpus_allowed_list:") {
				observed = append(observed, strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:")))
				return coverageTestProfile(), nil
			}
		}
		return protocol.MigrationCPUProfile{}, errors.New("child affinity not found")
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cpus, observed) {
		t.Fatalf("subprocess masks: %v, want %v", observed, cpus)
	}
	var after unix.CPUSet
	if err := unix.SchedGetaffinity(0, &after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("measurement changed caller affinity")
	}
}

func TestStockRunscCPUCoverage(t *testing.T) {
	path := os.Getenv("SANDBOX0_CPU_PROFILE_RUNSC")
	if path == "" {
		t.Skip("set SANDBOX0_CPU_PROFILE_RUNSC to a native stock runsc executable")
	}
	var allowed unix.CPUSet
	if err := unix.SchedGetaffinity(0, &allowed); err != nil {
		t.Fatal(err)
	}
	var cpus []string
	for cpu := 0; cpu < cpuAffinityMaskBits; cpu++ {
		if allowed.IsSet(cpu) {
			cpus = append(cpus, strconv.Itoa(cpu))
		}
	}
	requested := strings.Join(cpus, ",")
	runner := New(Config{Path: path, Root: t.TempDir(), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true}).(*Command)
	observation, err := runner.CPUCoverage(t.Context(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if err := observation.Covers(requested); err != nil {
		t.Fatal(err)
	}
	t.Logf("stock %s CPU observation covers %d CPUs (%s)", observation.Profile.RunscVersion, len(cpus), requested)
}
