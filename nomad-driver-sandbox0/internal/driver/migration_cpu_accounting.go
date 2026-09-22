package driver

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// These counters are diagnostics, never execution authority. Retaining the
// descriptor prevents a replaced cgroup path from being mistaken for the same
// accounting interval. Missing or reset counters make the sample unavailable.
type migrationCPUAccounting struct {
	file   *os.File
	before migrationCPUCounters
}

type migrationCPUCounters struct {
	usage, user, system, periods, throttledPeriods, throttled uint64
}

func openMigrationCPUAccounting(root, name string) *migrationCPUAccounting {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return nil
	}
	directory, err := os.OpenRoot(root)
	if err != nil {
		return nil
	}
	file, err := directory.Open(filepath.Join(name, "cpu.stat"))
	_ = directory.Close()
	if err != nil {
		return nil
	}
	before, ok := readMigrationCPUCounters(file)
	if !ok {
		_ = file.Close()
		return nil
	}
	return &migrationCPUAccounting{file: file, before: before}
}

func (a *migrationCPUAccounting) close() {
	if a != nil && a.file != nil {
		_ = a.file.Close()
		a.file = nil
	}
}

func (a *migrationCPUAccounting) finish() (migrationCPUCounters, bool) {
	if a == nil || a.file == nil {
		return migrationCPUCounters{}, false
	}
	defer a.close()
	after, ok := readMigrationCPUCounters(a.file)
	if !ok {
		return migrationCPUCounters{}, false
	}
	before := a.before
	if after.usage < before.usage || after.user < before.user || after.system < before.system ||
		after.periods < before.periods || after.throttledPeriods < before.throttledPeriods || after.throttled < before.throttled {
		return migrationCPUCounters{}, false
	}
	return migrationCPUCounters{usage: after.usage - before.usage, user: after.user - before.user,
		system: after.system - before.system, periods: after.periods - before.periods,
		throttledPeriods: after.throttledPeriods - before.throttledPeriods, throttled: after.throttled - before.throttled}, true
}

func readMigrationCPUCounters(file *os.File) (migrationCPUCounters, bool) {
	var counters migrationCPUCounters
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return counters, false
	}
	payload, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(payload) > 4096 {
		return counters, false
	}
	fields := map[string]*uint64{"usage_usec": &counters.usage, "user_usec": &counters.user,
		"system_usec": &counters.system, "nr_periods": &counters.periods,
		"nr_throttled": &counters.throttledPeriods, "throttled_usec": &counters.throttled}
	seen := make(map[string]bool, len(fields))
	for _, line := range strings.Split(string(payload), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		value, known := fields[parts[0]]
		if !known {
			continue
		}
		if len(parts) != 2 || seen[parts[0]] {
			return migrationCPUCounters{}, false
		}
		n, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return migrationCPUCounters{}, false
		}
		*value, seen[parts[0]] = n, true
	}
	return counters, len(seen) == len(fields)
}
