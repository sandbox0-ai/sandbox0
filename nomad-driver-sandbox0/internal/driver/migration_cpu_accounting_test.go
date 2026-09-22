package driver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const accountingBefore = "usage_usec 100\nuser_usec 40\nsystem_usec 60\nnr_periods 10\nnr_throttled 8\nthrottled_usec 900\n"
const accountingAfter = "usage_usec 150\nuser_usec 60\nsystem_usec 90\nnr_periods 12\nnr_throttled 9\nthrottled_usec 1000\nnr_bursts 0\n"

func accountingFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "lease"), 0o700))
	path := filepath.Join(root, "lease", "cpu.stat")
	require.NoError(t, os.WriteFile(path, []byte(accountingBefore), 0o600))
	return root, path
}

func TestMigrationCPUAccountingRetainsTheSameDescriptor(t *testing.T) {
	root, path := accountingFixture(t)
	sample := openMigrationCPUAccounting(root, "lease")
	require.NotNil(t, sample)
	t.Cleanup(sample.close)
	// A new file at the original path cannot impersonate the retained cgroup.
	require.NoError(t, os.Rename(path, path+".old"))
	require.NoError(t, os.WriteFile(path, []byte(strings.ReplaceAll(accountingAfter, "150", "999999")), 0o600))
	require.NoError(t, os.WriteFile(path+".old", []byte(accountingAfter), 0o600))
	delta, valid := sample.finish()
	require.True(t, valid)
	require.Equal(t, migrationCPUCounters{usage: 50, user: 20, system: 30, periods: 2, throttledPeriods: 1, throttled: 100}, delta)
	require.Nil(t, sample.file)
	_, valid = sample.finish()
	require.False(t, valid)
}

func TestMigrationCPUAccountingTreatsMissingAndInvalidCountersAsUnavailable(t *testing.T) {
	cases := map[string]string{
		"missing":   strings.ReplaceAll(accountingAfter, "nr_throttled 9\n", ""),
		"duplicate": accountingAfter + "usage_usec 150\n",
		"negative":  strings.ReplaceAll(accountingAfter, "usage_usec 150", "usage_usec -1"),
		"overflow":  strings.ReplaceAll(accountingAfter, "usage_usec 150", "usage_usec 18446744073709551616"),
		"malformed": strings.ReplaceAll(accountingAfter, "usage_usec 150", "usage_usec 150 extra"),
		"reset":     strings.ReplaceAll(accountingAfter, "usage_usec 150", "usage_usec 99"),
		"oversized": accountingAfter + strings.Repeat("future_counter 0\n", 300),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			root, path := accountingFixture(t)
			sample := openMigrationCPUAccounting(root, "lease")
			require.NotNil(t, sample)
			t.Cleanup(sample.close)
			require.NoError(t, os.WriteFile(path, []byte(payload), 0o600))
			delta, valid := sample.finish()
			require.False(t, valid)
			require.Zero(t, delta)
			require.Nil(t, sample.file)
		})
	}
	root, path := accountingFixture(t)
	require.NoError(t, os.Remove(path))
	sample := openMigrationCPUAccounting(root, "lease")
	require.Nil(t, sample)
	_, valid := sample.finish()
	require.False(t, valid)
	sample.close()
}

func TestMigrationCPUAccountingCannotFollowAnotherCgroupPath(t *testing.T) {
	root, _ := accountingFixture(t)
	for _, name := range []string{"", ".", "..", "../other", "/other", "lease/cpu.stat"} {
		require.Nil(t, openMigrationCPUAccounting(root, name))
	}
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "cpu.stat"), []byte(accountingBefore), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "outside")))
	require.Nil(t, openMigrationCPUAccounting(root, "outside"))
}
