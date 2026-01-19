// Package process provides process management for Procd.
package process

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CgroupVersion indicates which cgroup version is available.
type CgroupVersion int

const (
	CgroupUnknown CgroupVersion = iota
	CgroupV1
	CgroupV2
)

// procfsReader provides methods to read process stats from /proc.
// All methods are designed to be safe in scratch containers where only
// kernel-provided pseudo-filesystems are available.
type procfsReader struct{}

// cgroupReader provides methods to read container stats from cgroup.
// Supports both cgroup v1 and v2, with automatic detection.
type cgroupReader struct {
	version     CgroupVersion
	versionOnce sync.Once
}

var (
	defaultProcReader   = &procfsReader{}
	defaultCgroupReader = &cgroupReader{}
)

// detectCgroupVersion detects whether we're running with cgroup v1 or v2.
// In cgroup v2, /sys/fs/cgroup/cgroup.controllers exists.
// In cgroup v1, /sys/fs/cgroup/memory/ directory exists.
func (c *cgroupReader) detectVersion() CgroupVersion {
	c.versionOnce.Do(func() {
		// Check for cgroup v2 unified hierarchy
		if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
			c.version = CgroupV2
			return
		}
		// Check for cgroup v1 memory controller
		if _, err := os.Stat("/sys/fs/cgroup/memory"); err == nil {
			c.version = CgroupV1
			return
		}
		c.version = CgroupUnknown
	})
	return c.version
}

// ProcessMemoryStats contains detailed memory statistics for a process.
type ProcessMemoryStats struct {
	// RSS is the Resident Set Size - actual physical memory used.
	RSS int64 `json:"rss"`
	// VMS is the Virtual Memory Size - total virtual address space.
	VMS int64 `json:"vms"`
	// Shared is the amount of shared memory.
	Shared int64 `json:"shared"`
	// Data is the size of data segment.
	Data int64 `json:"data"`
}

// ProcessCPUStats contains CPU time statistics for a process.
type ProcessCPUStats struct {
	// UserTime is the CPU time spent in user mode (in clock ticks).
	UserTime uint64 `json:"user_time"`
	// SystemTime is the CPU time spent in kernel mode (in clock ticks).
	SystemTime uint64 `json:"system_time"`
	// ChildrenUserTime is user time of waited-for children.
	ChildrenUserTime uint64 `json:"children_user_time"`
	// ChildrenSystemTime is system time of waited-for children.
	ChildrenSystemTime uint64 `json:"children_system_time"`
	// StartTime is the time the process started (in clock ticks after boot).
	StartTime uint64 `json:"start_time"`
}

// ProcessIOStats contains I/O statistics for a process.
type ProcessIOStats struct {
	// ReadBytes is the number of bytes read from storage.
	ReadBytes int64 `json:"read_bytes"`
	// WriteBytes is the number of bytes written to storage.
	WriteBytes int64 `json:"write_bytes"`
	// ReadSyscalls is the number of read syscalls.
	ReadSyscalls int64 `json:"read_syscalls"`
	// WriteSyscalls is the number of write syscalls.
	WriteSyscalls int64 `json:"write_syscalls"`
}

// ContainerMemoryStats contains container-level memory statistics from cgroup.
type ContainerMemoryStats struct {
	// Usage is the current memory usage (from cgroup).
	Usage int64 `json:"usage"`
	// Limit is the memory limit (from cgroup), 0 if unlimited.
	Limit int64 `json:"limit"`
	// Cache is the page cache memory.
	Cache int64 `json:"cache"`
	// RSS is the anonymous and swap cache memory.
	RSS int64 `json:"rss"`
	// Swap is the swap usage.
	Swap int64 `json:"swap"`
	// WorkingSet is the non-reclaimable memory (usage - inactive_file).
	// This is what Kubernetes uses for OOM decisions.
	WorkingSet int64 `json:"working_set"`
}

// ContainerCPUStats contains container-level CPU statistics from cgroup.
type ContainerCPUStats struct {
	// UsageTotal is the total CPU time consumed (in nanoseconds).
	UsageTotal uint64 `json:"usage_total"`
	// UsageUser is the CPU time consumed in user mode (in nanoseconds).
	UsageUser uint64 `json:"usage_user"`
	// UsageSystem is the CPU time consumed in kernel mode (in nanoseconds).
	UsageSystem uint64 `json:"usage_system"`
	// ThrottledTime is the total time throttled (in nanoseconds).
	ThrottledTime uint64 `json:"throttled_time"`
	// ThrottledPeriods is the number of throttled periods.
	ThrottledPeriods uint64 `json:"throttled_periods"`
}

// readFileInt64 reads a file and parses it as int64.
// Returns 0 and error if file doesn't exist or can't be parsed.
func readFileInt64(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// readFileUint64 reads a file and parses it as uint64.
func readFileUint64(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

// readKeyValueFile reads a file with "key value" format per line.
func readKeyValueFile(path string) (map[string]int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	result := make(map[string]int64)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			if val, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				result[fields[0]] = val
			}
		}
	}
	return result, scanner.Err()
}

// ReadMemoryStats reads memory statistics for a process from /proc/[pid]/statm.
// Format: size resident shared text lib data dt (all in pages)
func (p *procfsReader) ReadMemoryStats(pid int) (*ProcessMemoryStats, error) {
	path := fmt.Sprintf("/proc/%d/statm", pid)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read statm: %w", err)
	}

	fields := strings.Fields(string(data))
	if len(fields) < 7 {
		return nil, fmt.Errorf("invalid statm format: expected 7 fields, got %d", len(fields))
	}

	pageSize := int64(os.Getpagesize())

	parsePages := func(s string) int64 {
		v, _ := strconv.ParseInt(s, 10, 64)
		return v * pageSize
	}

	return &ProcessMemoryStats{
		VMS:    parsePages(fields[0]),
		RSS:    parsePages(fields[1]),
		Shared: parsePages(fields[2]),
		Data:   parsePages(fields[5]),
	}, nil
}

// ReadCPUStats reads CPU statistics for a process from /proc/[pid]/stat.
// See: man proc(5) for field documentation.
func (p *procfsReader) ReadCPUStats(pid int) (*ProcessCPUStats, error) {
	path := fmt.Sprintf("/proc/%d/stat", pid)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read stat: %w", err)
	}

	// The comm field (2nd) can contain spaces and parentheses,
	// so we find the last ')' to locate the end of comm.
	content := string(data)
	lastParen := strings.LastIndex(content, ")")
	if lastParen == -1 {
		return nil, fmt.Errorf("invalid stat format: no closing paren")
	}

	// Fields after comm start at index 2 (0-indexed: state, ppid, pgrp, ...)
	// utime is field 14, stime is field 15 (1-indexed in man page)
	// After splitting: index 0=state, 1=ppid, ..., 11=utime, 12=stime, etc.
	rest := strings.TrimSpace(content[lastParen+1:])
	fields := strings.Fields(rest)

	if len(fields) < 20 {
		return nil, fmt.Errorf("invalid stat format: not enough fields")
	}

	parseUint64 := func(s string) uint64 {
		v, _ := strconv.ParseUint(s, 10, 64)
		return v
	}

	return &ProcessCPUStats{
		UserTime:           parseUint64(fields[11]),
		SystemTime:         parseUint64(fields[12]),
		ChildrenUserTime:   parseUint64(fields[13]),
		ChildrenSystemTime: parseUint64(fields[14]),
		StartTime:          parseUint64(fields[19]),
	}, nil
}

// ReadIOStats reads I/O statistics for a process from /proc/[pid]/io.
// This may fail if the process doesn't have permission to read another's io.
func (p *procfsReader) ReadIOStats(pid int) (*ProcessIOStats, error) {
	path := fmt.Sprintf("/proc/%d/io", pid)
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open io: %w", err)
	}
	defer file.Close()

	stats := &ProcessIOStats{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val, _ := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)

		switch key {
		case "read_bytes":
			stats.ReadBytes = val
		case "write_bytes":
			stats.WriteBytes = val
		case "syscr":
			stats.ReadSyscalls = val
		case "syscw":
			stats.WriteSyscalls = val
		}
	}
	return stats, scanner.Err()
}

// ReadThreadCount reads the number of threads from /proc/[pid]/status.
func (p *procfsReader) ReadThreadCount(pid int) (int, error) {
	path := fmt.Sprintf("/proc/%d/status", pid)
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open status: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Threads:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				count, _ := strconv.Atoi(parts[1])
				return count, nil
			}
		}
	}
	return 0, scanner.Err()
}

// ReadOpenFileCount counts open file descriptors from /proc/[pid]/fd.
func (p *procfsReader) ReadOpenFileCount(pid int) (int, error) {
	path := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, fmt.Errorf("read fd dir: %w", err)
	}
	return len(entries), nil
}

// ReadChildPIDs reads all child process PIDs from /proc/[pid]/task/[tid]/children.
// Falls back to scanning /proc if children file is unavailable.
func (p *procfsReader) ReadChildPIDs(pid int) ([]int, error) {
	// Try the children file first (requires CONFIG_PROC_CHILDREN)
	childrenPath := fmt.Sprintf("/proc/%d/task/%d/children", pid, pid)
	if data, err := os.ReadFile(childrenPath); err == nil {
		var pids []int
		for _, field := range strings.Fields(string(data)) {
			if childPid, err := strconv.Atoi(field); err == nil {
				pids = append(pids, childPid)
			}
		}
		return pids, nil
	}

	// Fallback: scan /proc for processes with our pid as ppid
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	var children []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		childPid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		// Read ppid from stat
		statPath := filepath.Join("/proc", entry.Name(), "stat")
		data, err := os.ReadFile(statPath)
		if err != nil {
			continue
		}

		content := string(data)
		lastParen := strings.LastIndex(content, ")")
		if lastParen == -1 {
			continue
		}

		fields := strings.Fields(content[lastParen+1:])
		if len(fields) < 2 {
			continue
		}

		ppid, _ := strconv.Atoi(fields[1])
		if ppid == pid {
			children = append(children, childPid)
		}
	}

	return children, nil
}

// ReadContainerMemoryStats reads container memory stats from cgroup.
func (c *cgroupReader) ReadContainerMemoryStats() (*ContainerMemoryStats, error) {
	version := c.detectVersion()

	switch version {
	case CgroupV2:
		return c.readMemoryStatsV2()
	case CgroupV1:
		return c.readMemoryStatsV1()
	default:
		return nil, fmt.Errorf("cgroup not available")
	}
}

func (c *cgroupReader) readMemoryStatsV2() (*ContainerMemoryStats, error) {
	stats := &ContainerMemoryStats{}

	// Read current usage
	if usage, err := readFileInt64("/sys/fs/cgroup/memory.current"); err == nil {
		stats.Usage = usage
	}

	// Read limit (may be "max" for unlimited)
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "max" {
			if limit, err := strconv.ParseInt(s, 10, 64); err == nil {
				stats.Limit = limit
			}
		}
	}

	// Read swap
	if swap, err := readFileInt64("/sys/fs/cgroup/memory.swap.current"); err == nil {
		stats.Swap = swap
	}

	// Read detailed stats from memory.stat
	if memStat, err := readKeyValueFile("/sys/fs/cgroup/memory.stat"); err == nil {
		if v, ok := memStat["file"]; ok {
			stats.Cache = v
		}
		if v, ok := memStat["anon"]; ok {
			stats.RSS = v
		}
		// Working set = usage - inactive_file
		inactiveFile := memStat["inactive_file"]
		stats.WorkingSet = stats.Usage - inactiveFile
		if stats.WorkingSet < 0 {
			stats.WorkingSet = 0
		}
	}

	return stats, nil
}

func (c *cgroupReader) readMemoryStatsV1() (*ContainerMemoryStats, error) {
	stats := &ContainerMemoryStats{}
	basePath := "/sys/fs/cgroup/memory"

	// Read current usage
	if usage, err := readFileInt64(filepath.Join(basePath, "memory.usage_in_bytes")); err == nil {
		stats.Usage = usage
	}

	// Read limit
	if limit, err := readFileInt64(filepath.Join(basePath, "memory.limit_in_bytes")); err == nil {
		// Check if it's effectively unlimited (very large value)
		if limit < 1<<62 {
			stats.Limit = limit
		}
	}

	// Read swap
	if swap, err := readFileInt64(filepath.Join(basePath, "memory.memsw.usage_in_bytes")); err == nil {
		// memsw includes memory, so swap = memsw - usage
		stats.Swap = swap - stats.Usage
		if stats.Swap < 0 {
			stats.Swap = 0
		}
	}

	// Read detailed stats from memory.stat
	if memStat, err := readKeyValueFile(filepath.Join(basePath, "memory.stat")); err == nil {
		if v, ok := memStat["cache"]; ok {
			stats.Cache = v
		}
		if v, ok := memStat["rss"]; ok {
			stats.RSS = v
		}
		// Working set = usage - inactive_file
		inactiveFile := memStat["inactive_file"]
		stats.WorkingSet = stats.Usage - inactiveFile
		if stats.WorkingSet < 0 {
			stats.WorkingSet = 0
		}
	}

	return stats, nil
}

// ReadContainerCPUStats reads container CPU stats from cgroup.
func (c *cgroupReader) ReadContainerCPUStats() (*ContainerCPUStats, error) {
	version := c.detectVersion()

	switch version {
	case CgroupV2:
		return c.readCPUStatsV2()
	case CgroupV1:
		return c.readCPUStatsV1()
	default:
		return nil, fmt.Errorf("cgroup not available")
	}
}

func (c *cgroupReader) readCPUStatsV2() (*ContainerCPUStats, error) {
	stats := &ContainerCPUStats{}

	// Read cpu.stat
	if cpuStat, err := readKeyValueFile("/sys/fs/cgroup/cpu.stat"); err == nil {
		if v, ok := cpuStat["usage_usec"]; ok {
			stats.UsageTotal = uint64(v) * 1000 // convert to nanoseconds
		}
		if v, ok := cpuStat["user_usec"]; ok {
			stats.UsageUser = uint64(v) * 1000
		}
		if v, ok := cpuStat["system_usec"]; ok {
			stats.UsageSystem = uint64(v) * 1000
		}
		if v, ok := cpuStat["nr_throttled"]; ok {
			stats.ThrottledPeriods = uint64(v)
		}
		if v, ok := cpuStat["throttled_usec"]; ok {
			stats.ThrottledTime = uint64(v) * 1000
		}
	}

	return stats, nil
}

func (c *cgroupReader) readCPUStatsV1() (*ContainerCPUStats, error) {
	stats := &ContainerCPUStats{}
	basePath := "/sys/fs/cgroup/cpu,cpuacct"

	// Try combined path first, then separate paths
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		basePath = "/sys/fs/cgroup/cpuacct"
	}

	// Read total usage (in nanoseconds)
	if usage, err := readFileUint64(filepath.Join(basePath, "cpuacct.usage")); err == nil {
		stats.UsageTotal = usage
	}

	// Read user/system split
	if data, err := os.ReadFile(filepath.Join(basePath, "cpuacct.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			val, _ := strconv.ParseUint(fields[1], 10, 64)
			// Values are in USER_HZ (typically 100), convert to nanoseconds
			valNs := val * 10000000 // (1e9 / 100)
			switch fields[0] {
			case "user":
				stats.UsageUser = valNs
			case "system":
				stats.UsageSystem = valNs
			}
		}
	}

	// Read throttling stats
	cpuBasePath := "/sys/fs/cgroup/cpu"
	if _, err := os.Stat(filepath.Join(basePath, "cpu.stat")); err == nil {
		cpuBasePath = basePath
	}

	if data, err := os.ReadFile(filepath.Join(cpuBasePath, "cpu.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			val, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "nr_throttled":
				stats.ThrottledPeriods = val
			case "throttled_time":
				stats.ThrottledTime = val
			}
		}
	}

	return stats, nil
}

// cpuTracker tracks CPU usage over time to calculate percentage.
type cpuTracker struct {
	mu               sync.Mutex
	lastCPUTime      uint64    // total CPU time (user + system) in clock ticks
	lastSampleTime   time.Time // when we last sampled
	clockTicksPerSec int64     // typically 100 on Linux
}

func newCPUTracker() *cpuTracker {
	return &cpuTracker{
		clockTicksPerSec: 100, // sysconf(_SC_CLK_TCK), usually 100 on Linux
	}
}

// CalculateCPUPercent calculates CPU percentage since last call.
// Returns -1 if this is the first call (no previous sample).
func (ct *cpuTracker) CalculateCPUPercent(stats *ProcessCPUStats) float64 {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	currentCPUTime := stats.UserTime + stats.SystemTime
	currentTime := time.Now()

	// First sample
	if ct.lastSampleTime.IsZero() {
		ct.lastCPUTime = currentCPUTime
		ct.lastSampleTime = currentTime
		return -1
	}

	// Calculate delta
	timeDelta := currentTime.Sub(ct.lastSampleTime).Seconds()
	if timeDelta <= 0 {
		return -1
	}

	cpuDelta := float64(currentCPUTime - ct.lastCPUTime)
	// Convert clock ticks to seconds
	cpuSeconds := cpuDelta / float64(ct.clockTicksPerSec)

	// Update last values
	ct.lastCPUTime = currentCPUTime
	ct.lastSampleTime = currentTime

	// CPU percent = (cpu time used / wall time) * 100
	return (cpuSeconds / timeDelta) * 100.0
}

// GetContainerResourceUsage returns container-level resource usage from cgroup.
// This can be called without a specific process and returns the container's overall stats.
func GetContainerResourceUsage() (*ContainerMemoryStats, error) {
	return defaultCgroupReader.ReadContainerMemoryStats()
}

// AggregateProcessTreeStats aggregates stats for a process and all its descendants.
func AggregateProcessTreeStats(rootPID int) (*ProcessMemoryStats, *ProcessCPUStats, int, int, error) {
	pids := []int{rootPID}

	// Get all descendant PIDs
	children, err := defaultProcReader.ReadChildPIDs(rootPID)
	if err == nil {
		pids = append(pids, children...)
		// Recursively get grandchildren
		for _, child := range children {
			grandchildren, _ := defaultProcReader.ReadChildPIDs(child)
			pids = append(pids, grandchildren...)
		}
	}

	totalMem := &ProcessMemoryStats{}
	totalCPU := &ProcessCPUStats{}
	totalThreads := 0
	totalOpenFiles := 0

	for _, pid := range pids {
		if mem, err := defaultProcReader.ReadMemoryStats(pid); err == nil {
			totalMem.RSS += mem.RSS
			totalMem.VMS += mem.VMS
			totalMem.Shared += mem.Shared
			totalMem.Data += mem.Data
		}

		if cpu, err := defaultProcReader.ReadCPUStats(pid); err == nil {
			totalCPU.UserTime += cpu.UserTime
			totalCPU.SystemTime += cpu.SystemTime
		}

		if threads, err := defaultProcReader.ReadThreadCount(pid); err == nil {
			totalThreads += threads
		}

		if openFiles, err := defaultProcReader.ReadOpenFileCount(pid); err == nil {
			totalOpenFiles += openFiles
		}
	}

	return totalMem, totalCPU, totalThreads, totalOpenFiles, nil
}
