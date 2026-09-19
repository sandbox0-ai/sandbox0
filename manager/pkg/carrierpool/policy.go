// Package carrierpool maintains bounded, resource-neutral warm inventory.
package carrierpool

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GroupIndex decodes the existing stable carrier identities without accepting
// arbitrary Nomad groups. The first eight carriers are enrollment anchors.
func GroupIndex(name string) (security string, index int, err error) {
	prefix := "warm-"
	limit := 514
	if strings.HasPrefix(name, "privileged-") {
		prefix = "privileged-"
		limit = 64
	}
	raw := strings.TrimPrefix(name, prefix)
	v, e := strconv.Atoi(raw)
	if e != nil || strconv.Itoa(v) != raw || v < 0 || v >= limit {
		return "", 0, fmt.Errorf("noncanonical carrier group")
	}
	if prefix == "privileged-" {
		if v < 2 {
			return "", 0, fmt.Errorf("noncanonical privileged group")
		}
		return "privileged", v, nil
	}
	if v == 6 || v == 7 {
		return "privileged", v - 6, nil
	}
	if v >= 8 {
		v -= 2
	}
	return "standard", v, nil
}

func baseline(name string) bool {
	class, index, err := GroupIndex(name)
	return err == nil && ((class == "standard" && index < 6) || (class == "privileged" && index < 2))
}

// Plan preserves every busy group, even a high ordinal, without retaining all
// lower ordinals. Baseline carriers remain available for enrollment/recovery.
// Only spare carriers are bounded by currently unleased CPU and memory.
func Plan(catalog, busy []string, spare, maximum int, freeCPU, freeMemory int64) ([]string, error) {
	return planDemand(catalog, busy, spare, maximum, freeCPU, freeMemory, nil)
}

// planDemand increases compatible spare inventory for waiting or planned work.
// These are inventory hints, never grants of CPU or memory.
func planDemand(catalog, busy []string, spare, maximum int, freeCPU, freeMemory int64, demand map[string]int) ([]string, error) {
	if spare < 0 || spare > 128 || maximum < 8 || maximum > 576 {
		return nil, fmt.Errorf("invalid carrier policy")
	}
	for class, count := range demand {
		if (class != "standard" && class != "privileged") || count < 0 || count > 576 {
			return nil, fmt.Errorf("invalid compatible carrier demand")
		}
	}
	known := map[string]bool{}
	selected := map[string]bool{}
	occupied := map[string]bool{}
	for _, g := range catalog {
		if _, _, err := GroupIndex(g); err != nil {
			return nil, err
		}
		if known[g] {
			return nil, fmt.Errorf("duplicate carrier group")
		}
		known[g] = true
		if baseline(g) {
			selected[g] = true
		}
	}
	if len(selected) != 8 {
		return nil, fmt.Errorf("carrier enrollment anchors missing")
	}
	for _, g := range busy {
		if !known[g] {
			return nil, fmt.Errorf("busy carrier is outside catalog")
		}
		occupied[g] = true
		selected[g] = true
	}
	if len(selected) > maximum {
		return nil, fmt.Errorf("busy carriers exceed configured ceiling")
	}
	// This is a warm-inventory hint, never an admission budget. The real claim
	// still checks the indivisible request against physical and leased resources.
	if freeCPU <= 0 || freeMemory < 64<<20 {
		spare = 0
		demand = nil
	}
	standardSpare := max(max(0, spare-2), demand["standard"])
	privilegedSpare := max(min(2, spare), demand["privileged"])
	target := min(maximum, max(8, len(occupied)+standardSpare+privilegedSpare))
	counts := map[string]int{}
	wanted := map[string]int{}
	for g := range selected {
		class, _, _ := GroupIndex(g)
		counts[class]++
	}
	for g := range occupied {
		class, _, _ := GroupIndex(g)
		wanted[class]++
	}
	wanted["privileged"] += privilegedSpare
	wanted["standard"] += standardSpare
	candidates := append([]string(nil), catalog...)
	sort.Slice(candidates, func(i, j int) bool {
		ci, ii, _ := GroupIndex(candidates[i])
		cj, ij, _ := GroupIndex(candidates[j])
		if ci != cj {
			return ci == "privileged"
		}
		return ii < ij
	})
	for _, g := range candidates {
		if len(selected) >= target {
			break
		}
		class, _, _ := GroupIndex(g)
		if !selected[g] && counts[class] < wanted[class] {
			selected[g] = true
			counts[class]++
		}
	}
	result := make([]string, 0, len(selected))
	for g := range selected {
		result = append(result, g)
	}
	sort.Strings(result)
	return result, nil
}
