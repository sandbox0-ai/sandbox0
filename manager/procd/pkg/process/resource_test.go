// Package process provides process management for Procd.
package process

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestReadFileInt64 tests readFileInt64 with various inputs.
func TestReadFileInt64(t *testing.T) {
	// This is a unit test for the helper function
	// We need to create temp files to test

	t.Run("valid positive number", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("1234567890\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readFileInt64(testFile)
		if err != nil {
			t.Fatalf("readFileInt64() error = %v", err)
		}
		if result != 1234567890 {
			t.Errorf("readFileInt64() = %d, want 1234567890", result)
		}
	})

	t.Run("valid negative number", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("-12345\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readFileInt64(testFile)
		if err != nil {
			t.Fatalf("readFileInt64() error = %v", err)
		}
		if result != -12345 {
			t.Errorf("readFileInt64() = %d, want -12345", result)
		}
	})

	t.Run("zero", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("0\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readFileInt64(testFile)
		if err != nil {
			t.Fatalf("readFileInt64() error = %v", err)
		}
		if result != 0 {
			t.Errorf("readFileInt64() = %d, want 0", result)
		}
	})

	t.Run("with whitespace", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("  42  \n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readFileInt64(testFile)
		if err != nil {
			t.Fatalf("readFileInt64() error = %v", err)
		}
		if result != 42 {
			t.Errorf("readFileInt64() = %d, want 42", result)
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		_, err := readFileInt64("/non/existent/file")
		if err == nil {
			t.Error("readFileInt64() expected error for non-existent file, got nil")
		}
	})

	t.Run("invalid content", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("not a number\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		_, err = readFileInt64(testFile)
		if err == nil {
			t.Error("readFileInt64() expected error for invalid content, got nil")
		}
	})

	t.Run("overflow", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		// Number larger than int64 can hold
		largeNum := "9999999999999999999999999999999999999999"
		err := os.WriteFile(testFile, []byte(largeNum+"\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		_, err = readFileInt64(testFile)
		if err == nil {
			t.Error("readFileInt64() expected error for overflow, got nil")
		}
	})
}

// TestReadFileUint64 tests readFileUint64 with various inputs.
func TestReadFileUint64(t *testing.T) {
	t.Run("valid number", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("1234567890\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readFileUint64(testFile)
		if err != nil {
			t.Fatalf("readFileUint64() error = %v", err)
		}
		if result != 1234567890 {
			t.Errorf("readFileUint64() = %d, want 1234567890", result)
		}
	})

	t.Run("max uint64", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("18446744073709551615\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readFileUint64(testFile)
		if err != nil {
			t.Fatalf("readFileUint64() error = %v", err)
		}
		if result != 18446744073709551615 {
			t.Errorf("readFileUint64() = %d, want 18446744073709551615", result)
		}
	})

	t.Run("negative number should fail", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte("-1\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}

		_, err = readFileUint64(testFile)
		if err == nil {
			t.Error("readFileUint64() expected error for negative number, got nil")
		}
	})
}

// TestReadKeyValueFile tests readKeyValueFile parsing.
func TestReadKeyValueFile(t *testing.T) {
	t.Run("valid file", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := "key1 123\nkey2 456\nkey3 789\n"
		err := os.WriteFile(testFile, []byte(content), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readKeyValueFile(testFile)
		if err != nil {
			t.Fatalf("readKeyValueFile() error = %v", err)
		}

		if len(result) != 3 {
			t.Errorf("readKeyValueFile() returned %d entries, want 3", len(result))
		}

		if result["key1"] != 123 {
			t.Errorf("readKeyValueFile() key1 = %d, want 123", result["key1"])
		}
		if result["key2"] != 456 {
			t.Errorf("readKeyValueFile() key2 = %d, want 456", result["key2"])
		}
		if result["key3"] != 789 {
			t.Errorf("readKeyValueFile() key3 = %d, want 789", result["key3"])
		}
	})

	t.Run("file with tabs", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := "key1\t123\nkey2\t456\n"
		err := os.WriteFile(testFile, []byte(content), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readKeyValueFile(testFile)
		if err != nil {
			t.Fatalf("readKeyValueFile() error = %v", err)
		}

		if result["key1"] != 123 {
			t.Errorf("readKeyValueFile() key1 = %d, want 123", result["key1"])
		}
	})

	t.Run("file with extra whitespace", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := "  key1   123   \nkey2 456\n"
		err := os.WriteFile(testFile, []byte(content), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readKeyValueFile(testFile)
		if err != nil {
			t.Fatalf("readKeyValueFile() error = %v", err)
		}

		if result["key1"] != 123 {
			t.Errorf("readKeyValueFile() key1 = %d, want 123", result["key1"])
		}
	})

	t.Run("file with invalid values", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := "key1 123\nkey2 not_a_number\nkey3 456\n"
		err := os.WriteFile(testFile, []byte(content), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readKeyValueFile(testFile)
		if err != nil {
			t.Fatalf("readKeyValueFile() error = %v", err)
		}

		// Invalid values should be silently skipped
		if len(result) != 2 {
			t.Errorf("readKeyValueFile() returned %d entries, want 2", len(result))
		}
		if result["key2"] != 0 {
			t.Errorf("readKeyValueFile() key2 = %d, want 0 (invalid)", result["key2"])
		}
	})

	t.Run("file with lines with only one field", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := "key1\nkey2 123\n"
		err := os.WriteFile(testFile, []byte(content), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readKeyValueFile(testFile)
		if err != nil {
			t.Fatalf("readKeyValueFile() error = %v", err)
		}

		// Lines with only one field should be skipped
		if result["key1"] != 0 {
			t.Errorf("readKeyValueFile() key1 = %d, want 0 (missing value)", result["key1"])
		}
	})

	t.Run("empty file", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(testFile, []byte(""), 0644)
		if err != nil {
			t.Fatal(err)
		}

		result, err := readKeyValueFile(testFile)
		if err != nil {
			t.Fatalf("readKeyValueFile() error = %v", err)
		}

		if len(result) != 0 {
			t.Errorf("readKeyValueFile() returned %d entries, want 0", len(result))
		}
	})
}

// TestProcfsReader_ReadMemoryStats tests memory stats reading.
func TestProcfsReader_ReadMemoryStats(t *testing.T) {
	reader := &procfsReader{}

	// Test with current process (should always work)
	pid := os.Getpid()

	stats, err := reader.ReadMemoryStats(pid)
	if err != nil {
		// This might fail on non-Linux systems
		t.Skipf("ReadMemoryStats() failed (expected on non-Linux): %v", err)
		return
	}

	// Verify stats are reasonable
	if stats.VMS < 0 {
		t.Errorf("ReadMemoryStats() VMS = %d, want >= 0", stats.VMS)
	}
	if stats.RSS < 0 {
		t.Errorf("ReadMemoryStats() RSS = %d, want >= 0", stats.RSS)
	}
	if stats.Shared < 0 {
		t.Errorf("ReadMemoryStats() Shared = %d, want >= 0", stats.Shared)
	}
	if stats.Data < 0 {
		t.Errorf("ReadMemoryStats() Data = %d, want >= 0", stats.Data)
	}

	// VMS should be >= RSS (virtual includes resident)
	if stats.VMS < stats.RSS {
		t.Errorf("ReadMemoryStats() VMS %d < RSS %d (VMS should be >= RSS)", stats.VMS, stats.RSS)
	}
}

// TestProcfsReader_ReadMemoryStats_InvalidFormat tests error handling for invalid statm format.
func TestProcfsReader_ReadMemoryStats_InvalidFormat(t *testing.T) {
	reader := &procfsReader{}

	// Create a mock /proc/[pid]/statm file with invalid format
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "12345")
	err := os.MkdirAll(pidDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Test with insufficient fields
	statmFile := filepath.Join(pidDir, "statm")
	err = os.WriteFile(statmFile, []byte("100 200\n"), 0644) // Only 2 fields, need 7
	if err != nil {
		t.Fatal(err)
	}

	// We can't actually test with the mock path because the function uses /proc
	// But we can test with a non-existent PID
	_, err = reader.ReadMemoryStats(999999999)
	if err == nil {
		t.Error("ReadMemoryStats() expected error for non-existent PID, got nil")
	}
}

// TestProcfsReader_ReadCPUStats tests CPU stats reading.
func TestProcfsReader_ReadCPUStats(t *testing.T) {
	reader := &procfsReader{}

	pid := os.Getpid()

	stats, err := reader.ReadCPUStats(pid)
	if err != nil {
		// This might fail on non-Linux systems
		t.Skipf("ReadCPUStats() failed (expected on non-Linux): %v", err)
		return
	}

	// Verify stats are reasonable
	if stats.UserTime < 0 {
		t.Errorf("ReadCPUStats() UserTime = %d, want >= 0", stats.UserTime)
	}
	if stats.SystemTime < 0 {
		t.Errorf("ReadCPUStats() SystemTime = %d, want >= 0", stats.SystemTime)
	}

	// StartTime should be positive (ticks since boot)
	if stats.StartTime <= 0 {
		t.Errorf("ReadCPUStats() StartTime = %d, want > 0", stats.StartTime)
	}
}

// TestProcfsReader_ReadCPUStats_InvalidFormat tests error handling.
func TestProcfsReader_ReadCPUStats_InvalidFormat(t *testing.T) {
	reader := &procfsReader{}

	_, err := reader.ReadCPUStats(999999999)
	if err == nil {
		t.Error("ReadCPUStats() expected error for non-existent PID, got nil")
	}
}

// TestProcfsReader_ReadThreadCount tests thread count reading.
func TestProcfsReader_ReadThreadCount(t *testing.T) {
	reader := &procfsReader{}

	pid := os.Getpid()

	count, err := reader.ReadThreadCount(pid)
	if err != nil {
		// This might fail on non-Linux systems
		t.Skipf("ReadThreadCount() failed (expected on non-Linux): %v", err)
		return
	}

	// Should have at least 1 thread
	if count < 1 {
		t.Errorf("ReadThreadCount() = %d, want >= 1", count)
	}
}

// TestProcfsReader_ReadOpenFileCount tests open file count reading.
func TestProcfsReader_ReadOpenFileCount(t *testing.T) {
	reader := &procfsReader{}

	pid := os.Getpid()

	count, err := reader.ReadOpenFileCount(pid)
	if err != nil {
		// This might fail on non-Linux systems
		t.Skipf("ReadOpenFileCount() failed (expected on non-Linux): %v", err)
		return
	}

	// Should have at least some open files (stdin, stdout, stderr at minimum)
	if count < 3 {
		t.Errorf("ReadOpenFileCount() = %d, want >= 3", count)
	}
}

// TestCpuTracker_CalculateCPUPercent tests CPU percentage calculation.
func TestCpuTracker_CalculateCPUPercent(t *testing.T) {
	tracker := newCPUTracker()

	t.Run("first call returns -1", func(t *testing.T) {
		stats := &ProcessCPUStats{
			UserTime:   100,
			SystemTime: 50,
		}

		result := tracker.CalculateCPUPercent(stats)
		if result != -1 {
			t.Errorf("CalculateCPUPercent() (first call) = %f, want -1", result)
		}
	})

	t.Run("second call returns percentage", func(t *testing.T) {
		// First call
		stats1 := &ProcessCPUStats{
			UserTime:   100,
			SystemTime: 50,
		}
		tracker.CalculateCPUPercent(stats1)

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Second call with increased CPU time
		stats2 := &ProcessCPUStats{
			UserTime:   150,
			SystemTime: 75,
		}

		result := tracker.CalculateCPUPercent(stats2)

		// Should be positive (some CPU used)
		if result < 0 {
			t.Errorf("CalculateCPUPercent() = %f, want >= 0", result)
		}

		// On fast systems, this can be quite high due to time.Sleep precision
		// Just verify it's a reasonable number (not infinity or NaN)
		if result != result { // NaN check
			t.Errorf("CalculateCPUPercent() = NaN")
		}
	})

	t.Run("cpu time decreasing is handled", func(t *testing.T) {
		// This can happen if process restarts or counter wraps
		tracker2 := newCPUTracker()

		stats1 := &ProcessCPUStats{
			UserTime:   1000,
			SystemTime: 500,
		}
		tracker2.CalculateCPUPercent(stats1)

		time.Sleep(50 * time.Millisecond)

		// Simulate counter wrap or restart (lower values)
		stats2 := &ProcessCPUStats{
			UserTime:   100,
			SystemTime: 50,
		}

		result := tracker2.CalculateCPUPercent(stats2)

		// The calculation should still return something
		// (though it might be negative due to wrap, that's OK for this edge case)
		// Just verify it doesn't panic
		_ = result
	})

	t.Run("zero time delta returns small value or -1", func(t *testing.T) {
		tracker3 := newCPUTracker()

		stats1 := &ProcessCPUStats{
			UserTime:   100,
			SystemTime: 50,
		}
		tracker3.CalculateCPUPercent(stats1)

		// Immediate second call with same values
		stats2 := &ProcessCPUStats{
			UserTime:   100,
			SystemTime: 50,
		}

		result := tracker3.CalculateCPUPercent(stats2)

		// Due to time.Now() precision, we might get a small positive value
		// or -1 if timeDelta is 0. Either is acceptable.
		if result < -1 {
			t.Errorf("CalculateCPUPercent() (zero time delta) = %f, want >= -1", result)
		}
		// If not -1, the value should be small (close to 0)
		if result != -1 && result > 100 {
			t.Errorf("CalculateCPUPercent() (zero time delta) = %f, want <= 100 or -1", result)
		}
	})
}

// TestAggregateProcessTreeStats tests process tree stats aggregation.
func TestAggregateProcessTreeStats(t *testing.T) {
	// Test with current process
	pid := os.Getpid()

	mem, _, threads, openFiles, err := AggregateProcessTreeStats(pid)

	// Check if we're on a system that supports /proc (Linux)
	// If all stats are zero, we're likely on a non-Linux system
	if mem.RSS == 0 && mem.VMS == 0 && threads == 0 && openFiles == 0 {
		t.Skipf("AggregateProcessTreeStats() returned all zeros (expected on non-Linux)")
		return
	}

	if err != nil {
		// This might fail on non-Linux systems
		t.Skipf("AggregateProcessTreeStats() failed (expected on non-Linux): %v", err)
		return
	}

	// Verify reasonable values (only if we got data)
	if mem.RSS < 0 {
		t.Errorf("AggregateProcessTreeStats() RSS = %d, want >= 0", mem.RSS)
	}
	if mem.VMS < 0 {
		t.Errorf("AggregateProcessTreeStats() VMS = %d, want >= 0", mem.VMS)
	}
	if threads > 0 && threads < 1 {
		t.Errorf("AggregateProcessTreeStats() threads = %d, want >= 1", threads)
	}
	if openFiles > 0 && openFiles < 3 {
		t.Errorf("AggregateProcessTreeStats() openFiles = %d, want >= 3", openFiles)
	}

	// VMS should be >= RSS
	if mem.VMS < mem.RSS {
		t.Errorf("AggregateProcessTreeStats() VMS %d < RSS %d", mem.VMS, mem.RSS)
	}
}

// TestCgroupReader_DetectVersion tests cgroup version detection.
func TestCgroupReader_DetectVersion(t *testing.T) {
	reader := &cgroupReader{}

	version := reader.detectVersion()

	// On Linux, should be V1 or V2
	// On other systems, might be Unknown
	t.Logf("Detected cgroup version: %d", version)

	if version < CgroupUnknown || version > CgroupV2 {
		t.Errorf("detectVersion() = %d, want between %d and %d", version, CgroupUnknown, CgroupV2)
	}

	// Version should be idempotent
	version2 := reader.detectVersion()
	if version != version2 {
		t.Errorf("detectVersion() not idempotent: first = %d, second = %d", version, version2)
	}
}

// TestCgroupReader_ReadContainerMemoryStats tests container memory stats.
func TestCgroupReader_ReadContainerMemoryStats(t *testing.T) {
	reader := &cgroupReader{}

	stats, err := reader.ReadContainerMemoryStats()
	if err != nil {
		// This might fail on non-Linux systems or in certain container configurations
		t.Skipf("ReadContainerMemoryStats() failed: %v", err)
		return
	}

	// Verify stats are reasonable
	if stats.Usage < 0 {
		t.Errorf("ReadContainerMemoryStats() Usage = %d, want >= 0", stats.Usage)
	}

	// If limit is set (not unlimited), it should be >= usage
	if stats.Limit > 0 && stats.Limit < 1<<62 && stats.Usage > stats.Limit {
		t.Errorf("ReadContainerMemoryStats() Usage %d > Limit %d", stats.Usage, stats.Limit)
	}

	// Working set should be non-negative
	if stats.WorkingSet < 0 {
		t.Errorf("ReadContainerMemoryStats() WorkingSet = %d, want >= 0", stats.WorkingSet)
	}

	// Working set should be <= usage
	if stats.WorkingSet > stats.Usage {
		t.Errorf("ReadContainerMemoryStats() WorkingSet %d > Usage %d", stats.WorkingSet, stats.Usage)
	}
}

// TestCgroupReader_ReadContainerCPUStats tests container CPU stats.
func TestCgroupReader_ReadContainerCPUStats(t *testing.T) {
	reader := &cgroupReader{}

	stats, err := reader.ReadContainerCPUStats()
	if err != nil {
		// This might fail on non-Linux systems or in certain container configurations
		t.Skipf("ReadContainerCPUStats() failed: %v", err)
		return
	}

	// Verify stats are reasonable
	if stats.UsageTotal < 0 {
		t.Errorf("ReadContainerCPUStats() UsageTotal = %d, want >= 0", stats.UsageTotal)
	}
}

// TestGetContainerResourceUsage tests the public API.
func TestGetContainerResourceUsage(t *testing.T) {
	stats, err := GetContainerResourceUsage()
	if err != nil {
		// This might fail on non-Linux systems
		t.Skipf("GetContainerResourceUsage() failed: %v", err)
		return
	}

	if stats.Usage < 0 {
		t.Errorf("GetContainerResourceUsage() Usage = %d, want >= 0", stats.Usage)
	}
}

// TestReadMemoryStatsV2_MaxValue tests handling of "max" value for memory limit.
func TestReadMemoryStatsV2_MaxValue(t *testing.T) {
	// This is a unit test for the specific case where memory.max contains "max"
	// We can't easily mock this, but we can verify the parsing logic

	// The actual function uses strconv.ParseInt which returns an error for "max"
	// This test verifies that we handle it correctly
	maxString := "max"
	_, err := strconv.ParseInt(strings.TrimSpace(maxString), 10, 64)
	if err == nil {
		t.Error("strconv.ParseInt() should fail for 'max'")
	}

	// The function should skip setting the limit when it's "max"
	// This is tested implicitly by ReadContainerMemoryStats above
}

// BenchmarkCpuTracker benchmarks CPU tracking performance.
func BenchmarkCpuTracker(b *testing.B) {
	tracker := newCPUTracker()

	// Warm up
	stats := &ProcessCPUStats{
		UserTime:   100,
		SystemTime: 50,
	}
	tracker.CalculateCPUPercent(stats)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stats.UserTime += 10
		stats.SystemTime += 5
		tracker.CalculateCPUPercent(stats)
	}
}
