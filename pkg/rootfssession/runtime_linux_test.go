//go:build linux

package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestXFSMountOptionsKeepGenericFlagsOutOfFilesystemData(t *testing.T) {
	require.NotZero(t, xfsMountFlags&unix.MS_NOATIME)
	require.NotContains(t, strings.Split(xfsMountData, ","), "noatime")
	require.Contains(t, strings.Split(xfsMountData, ","), "nouuid")
}

func TestLinuxRuntimeCrashFenceInspectsNBDPIDAndHolders(t *testing.T) {
	sysRoot := t.TempDir()
	deviceRoot := filepath.Join(sysRoot, "nbd0")
	require.NoError(t, os.MkdirAll(filepath.Join(deviceRoot, "holders"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(deviceRoot, "pid"), []byte("0\n"), 0o600))
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{"/dev/nbd0"}, SysBlockRoot: sysRoot})
	require.NoError(t, err)
	mountRoot := t.TempDir()

	observation, err := runtime.InspectCrashFence("/dev/nbd0", filepath.Join(mountRoot, "xfs"), filepath.Join(mountRoot, "merged"))
	require.NoError(t, err)
	require.Equal(t, 0, observation.NBDPID)
	require.Empty(t, observation.NBDHolders)
	require.True(t, observation.MergedMountAbsent)
	require.True(t, observation.XFSMountAbsent)

	require.NoError(t, os.WriteFile(filepath.Join(deviceRoot, "pid"), []byte("123\n"), 0o600))
	_, err = runtime.InspectCrashFence("/dev/nbd0", filepath.Join(mountRoot, "xfs"), filepath.Join(mountRoot, "merged"))
	require.ErrorContains(t, err, "remains owned")
	require.NoError(t, os.WriteFile(filepath.Join(deviceRoot, "pid"), []byte("0\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(deviceRoot, "holders", "dm-0"), nil, 0o600))
	_, err = runtime.InspectCrashFence("/dev/nbd0", filepath.Join(mountRoot, "xfs"), filepath.Join(mountRoot, "merged"))
	require.ErrorContains(t, err, "holders")
}

func TestLinuxRuntimeCrashFenceAcceptsMissingPIDOnlyForZeroSizedDevice(t *testing.T) {
	tests := []struct {
		name        string
		size        *string
		errorSubstr string
	}{
		{name: "disconnected", size: stringPointer("0\n")},
		{name: "connected size", size: stringPointer("8\n"), errorSubstr: "size remains"},
		{name: "missing size", errorSubstr: "inspect disconnected NBD size"},
		{name: "invalid size", size: stringPointer("not-a-number\n"), errorSubstr: "size"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sysRoot := t.TempDir()
			deviceRoot := filepath.Join(sysRoot, "nbd0")
			require.NoError(t, os.MkdirAll(filepath.Join(deviceRoot, "holders"), 0o700))
			if test.size != nil {
				require.NoError(t, os.WriteFile(filepath.Join(deviceRoot, "size"), []byte(*test.size), 0o600))
			}
			runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{
				DevicePaths: []string{"/dev/nbd0"}, SysBlockRoot: sysRoot,
			})
			require.NoError(t, err)
			mountRoot := t.TempDir()
			observation, err := runtime.InspectCrashFence(
				"/dev/nbd0", filepath.Join(mountRoot, "xfs"), filepath.Join(mountRoot, "merged"),
			)
			if test.errorSubstr != "" {
				require.ErrorContains(t, err, test.errorSubstr)
				return
			}
			require.NoError(t, err)
			require.Zero(t, observation.NBDPID)
			require.Empty(t, observation.NBDHolders)
		})
	}
}

func TestLinuxRuntimeUnattachedCrashFenceRequiresEntireDevicePoolIdle(t *testing.T) {
	sysRoot := t.TempDir()
	for _, device := range []string{"nbd0", "nbd1"} {
		deviceRoot := filepath.Join(sysRoot, device)
		require.NoError(t, os.MkdirAll(filepath.Join(deviceRoot, "holders"), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(deviceRoot, "pid"), []byte("0\n"), 0o600))
	}
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{
		DevicePaths: []string{"/dev/nbd0", "/dev/nbd1"}, SysBlockRoot: sysRoot,
	})
	require.NoError(t, err)
	mountRoot := t.TempDir()

	observation, err := runtime.InspectUnattachedCrashFence(
		filepath.Join(mountRoot, "xfs"), filepath.Join(mountRoot, "merged"),
	)
	require.NoError(t, err)
	require.True(t, observation.NBDPoolAbsent)
	require.True(t, observation.MergedMountAbsent)
	require.True(t, observation.XFSMountAbsent)

	require.NoError(t, os.WriteFile(filepath.Join(sysRoot, "nbd1", "pid"), []byte("42\n"), 0o600))
	_, err = runtime.InspectUnattachedCrashFence(
		filepath.Join(mountRoot, "xfs"), filepath.Join(mountRoot, "merged"),
	)
	require.ErrorContains(t, err, "nbd1")
}

func TestLinuxRuntimeReservationIsAllocationBoundAndIdempotent(t *testing.T) {
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{"/dev/nbd0", "/dev/nbd1"}})
	require.NoError(t, err)

	first, err := runtime.ReserveDevice("allocation-a")
	require.NoError(t, err)
	require.Equal(t, "/dev/nbd0", first)
	second, err := runtime.ReserveDevice("allocation-b")
	require.NoError(t, err)
	require.Equal(t, "/dev/nbd1", second)
	_, err = runtime.ReserveDevice("allocation-c")
	require.ErrorContains(t, err, "no usable NBD device")

	require.NoError(t, runtime.AdoptDeviceReservation(first, "allocation-a"))
	err = runtime.AdoptDeviceReservation(first, "different-allocation")
	require.ErrorContains(t, err, "already reserved")
	runtime.ReleaseDeviceReservation(first, "different-allocation")
	_, err = runtime.ReserveDevice("allocation-c")
	require.ErrorContains(t, err, "no usable NBD device")
	runtime.ReleaseDeviceReservation(first, "allocation-a")
	reused, err := runtime.ReserveDevice("allocation-c")
	require.NoError(t, err)
	require.Equal(t, first, reused)
}

func TestLinuxRuntimeCrashFenceAcceptsMissingKernelEndpoint(t *testing.T) {
	root := t.TempDir()
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{
		DevicePaths:  []string{"/dev/nbd999"},
		SysBlockRoot: filepath.Join(root, "sys", "block"),
	})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sys", "block"), 0o755))

	observation, err := runtime.InspectCrashFence(
		"/dev/nbd999", filepath.Join(root, "xfs"), filepath.Join(root, "merged"),
	)
	require.NoError(t, err)
	require.Zero(t, observation.NBDPID)
	require.Empty(t, observation.NBDHolders)
	require.True(t, observation.MergedMountAbsent)
	require.True(t, observation.XFSMountAbsent)
}

func stringPointer(value string) *string {
	return &value
}
