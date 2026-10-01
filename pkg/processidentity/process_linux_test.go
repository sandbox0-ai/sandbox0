//go:build linux

package processidentity

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessIdentityCannotConfuseDeadProcessesOrPIDReuse(t *testing.T) {
	current, err := Current()
	require.NoError(t, err)
	alive, err := Alive(current)
	require.NoError(t, err)
	require.True(t, alive)
	fields := strings.Split(current, ":")
	fields[3] = "0"
	alive, err = Alive(strings.Join(fields, ":"))
	require.NoError(t, err)
	require.False(t, alive, "a reused PID with a different start tick is not its predecessor")
	fields = strings.Split(current, ":")
	fields[1] = "different-boot"
	alive, err = Alive(strings.Join(fields, ":"))
	require.NoError(t, err)
	require.False(t, alive)
	for _, invalid := range []string{"", "process-v1:a:-1:0", "process-v2:a:1:0", "process-v1:a:x:0"} {
		_, err = Alive(invalid)
		require.Error(t, err)
	}
	child := exec.CommandContext(t.Context(), "sleep", "30")
	require.NoError(t, child.Start())
	owner, err := identity(child.Process.Pid)
	require.NoError(t, err)
	require.NoError(t, child.Process.Kill())
	_ = child.Wait()
	alive, err = Alive(owner)
	require.NoError(t, err)
	require.False(t, alive)
}
