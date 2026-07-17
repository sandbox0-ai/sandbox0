package session

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// readProcessStartTimeTicks returns Linux /proc starttime (field 22). Unlike a
// PID, this value remains stable for one process lifetime and prevents a reused
// PID from inheriting a previous supervised attempt's attribution identity.
func readProcessStartTimeTicks(procRoot string, pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("pid must be positive")
	}
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	closeParen := strings.LastIndexByte(string(data), ')')
	if closeParen < 0 || closeParen+2 >= len(data) {
		return 0, fmt.Errorf("malformed process stat")
	}
	fields := strings.Fields(string(data[closeParen+2:]))
	const startTimeIndex = 19 // Field 22 after removing pid and comm.
	if len(fields) <= startTimeIndex {
		return 0, fmt.Errorf("malformed process stat")
	}
	startTimeTicks, err := strconv.ParseUint(fields[startTimeIndex], 10, 64)
	if err != nil || startTimeTicks == 0 {
		return 0, fmt.Errorf("parse process starttime")
	}
	return startTimeTicks, nil
}
