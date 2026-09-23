package carrierpool

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func catalog(count int) []string {
	result := []string{}
	for i := 0; i < count; i++ {
		result = append(result, fmt.Sprintf("warm-%d", i))
	}
	return result
}

func TestPlanRetainsBusyHighOrdinalWithoutItsIdlePrefix(t *testing.T) {
	groups, err := Plan(catalog(128), []string{"warm-127"}, 16, 128, 14000, 56<<30)
	require.NoError(t, err)
	require.Len(t, groups, 9)
	require.Contains(t, groups, "warm-127")
	require.NotContains(t, groups, "warm-126")
	for i := 0; i < 8; i++ {
		require.Contains(t, groups, fmt.Sprintf("warm-%d", i))
	}
}

func TestPlanResourceExhaustionRetainsOnlyBusyAndEnrollmentAnchors(t *testing.T) {
	for _, resources := range [][2]int64{{0, 56 << 30}, {14000, 0}} {
		groups, err := Plan(catalog(128), []string{"warm-70", "warm-100"}, 16, 128, resources[0], resources[1])
		require.NoError(t, err)
		require.Len(t, groups, 10)
		require.Contains(t, groups, "warm-70")
		require.Contains(t, groups, "warm-100")
	}
}

func TestPlanRejectsIncompleteOrForeignInventory(t *testing.T) {
	_, err := Plan(catalog(7), nil, 16, 128, 14000, 56<<30)
	require.Error(t, err)
	_, err = Plan(catalog(128), []string{"foreign"}, 16, 128, 14000, 56<<30)
	require.Error(t, err)
	for _, name := range []string{"warm-01", "warm--1", "warm-514", "privileged-0", "privileged-256", "other-8"} {
		_, _, err = GroupIndex(name)
		require.Error(t, err, name)
	}
}

func TestPlanBoundsGrowingInventory(t *testing.T) {
	busy := catalog(30)
	groups, err := Plan(catalog(128), busy, 16, 128, 14000, 56<<30)
	require.NoError(t, err)
	require.Len(t, groups, 32) // This catalog has only the two privileged anchors.
	groups, err = Plan(catalog(128), catalog(120), 16, 128, 14000, 56<<30)
	require.NoError(t, err)
	require.Len(t, groups, 122)
	_, err = Plan(catalog(128), catalog(120), 16, 100, 14000, 56<<30)
	require.Error(t, err)
}

func TestPlanRefillsBothSecurityClassesWithoutExceedingCombinedCeiling(t *testing.T) {
	all := catalog(242)
	for i := 2; i < 16; i++ {
		all = append(all, fmt.Sprintf("privileged-%d", i))
	}
	busy := []string{"warm-6", "warm-7", "privileged-2", "warm-100"}
	groups, err := Plan(all, busy, 16, 256, 14000, 56<<30)
	require.NoError(t, err)
	require.Len(t, groups, 20)
	counts := map[string]int{}
	for _, g := range groups {
		class, _, err := GroupIndex(g)
		require.NoError(t, err)
		counts[class]++
	}
	require.Equal(t, 13, counts["privileged"])
	require.Equal(t, 7, counts["standard"])
	require.Contains(t, groups, "warm-100")
	groups, err = Plan(all, busy, 16, 18, 14000, 56<<30)
	require.NoError(t, err)
	require.Len(t, groups, 18)
}

func TestPlanPrivilegedOnlyCatalogRetainsBusyAndRefillsSpare(t *testing.T) {
	catalog := []string{"warm-6", "warm-7"}
	for i := 2; i < 32; i++ {
		catalog = append(catalog, fmt.Sprintf("privileged-%d", i))
	}
	groups, err := Plan(catalog, []string{"privileged-20"}, 16, 32, 14000, 56<<30)
	require.NoError(t, err)
	require.Len(t, groups, 17)
	require.Contains(t, groups, "warm-6")
	require.Contains(t, groups, "warm-7")
	require.Contains(t, groups, "privileged-20")
	for _, group := range groups {
		class, _, err := GroupIndex(group)
		require.NoError(t, err)
		require.Equal(t, "privileged", class)
	}
}
