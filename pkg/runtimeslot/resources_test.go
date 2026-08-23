package runtimeslot

import "testing"

func TestRuntimeResourceLeaseDerivesCanonicalCgroupValues(t *testing.T) {
	request := RuntimeResourceRequest{
		Version:       RuntimeResourceRequestVersion,
		CPUMillicores: 1_500, MemoryBytes: 2 << 30, PIDsLimit: DefaultRuntimePIDsLimit,
	}
	lease, err := NewRuntimeResourceLease(
		"operation-1", "claim-1", "slot-1", "cluster-1", "node-1", "node-uid-1", "boot-1",
		request, "0-7", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	if lease.CPUPeriodMicros != 100_000 || lease.CPUQuotaMicros != 150_000 ||
		lease.CPUShares != 1_536 || lease.CPUWeight != 59 {
		t.Fatalf("CPU lease = period %d quota %d shares %d weight %d",
			lease.CPUPeriodMicros, lease.CPUQuotaMicros, lease.CPUShares, lease.CPUWeight)
	}
	if lease.LeaseID == "" || lease.CgroupName == "" {
		t.Fatalf("lease identity = %+v", lease)
	}
	first, err := lease.Digest()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := NewRuntimeResourceLease(
		"operation-1", "claim-1", "slot-1", "cluster-1", "node-1", "node-uid-1", "boot-1",
		request, "0-7", "0",
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := replayed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second || lease != replayed {
		t.Fatal("exact resource lease replay changed")
	}

	lease.CPUQuotaMicros++
	if _, err := lease.Digest(); err == nil {
		t.Fatal("noncanonical CPU quota was accepted")
	}
}

func TestValidateCPUSetRejectsNoncanonicalSets(t *testing.T) {
	if count, err := ValidateCPUSet("0-3,5,7-8"); err != nil || count != 7 {
		t.Fatalf("valid CPU set = %d, %v", count, err)
	}
	for _, value := range []string{"", " 0", "00", "1-1", "2-1", "1,1", "2,1", "1-2,2-3", "1,,2"} {
		t.Run(value, func(t *testing.T) {
			if _, err := ValidateCPUSet(value); err == nil {
				t.Fatalf("CPU set %q was accepted", value)
			}
		})
	}
}

func TestRuntimeResourceRequestRejectsUnboundedValues(t *testing.T) {
	valid := RuntimeResourceRequest{
		Version:       RuntimeResourceRequestVersion,
		CPUMillicores: 1_000, MemoryBytes: 1 << 30, PIDsLimit: DefaultRuntimePIDsLimit,
	}
	for name, mutate := range map[string]func(*RuntimeResourceRequest){
		"version": func(value *RuntimeResourceRequest) { value.Version++ },
		"cpu":     func(value *RuntimeResourceRequest) { value.CPUMillicores = MinRuntimeCPUMillicores - 1 },
		"memory":  func(value *RuntimeResourceRequest) { value.MemoryBytes = 0 },
		"pids":    func(value *RuntimeResourceRequest) { value.PIDsLimit = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := candidate.Digest(); err == nil {
				t.Fatal("invalid resource request was accepted")
			}
		})
	}
}

func TestCPUSetContainsRequiresCanonicalSubset(t *testing.T) {
	for _, test := range []struct {
		parent string
		child  string
		want   bool
	}{
		{parent: "0-7", child: "0-7", want: true},
		{parent: "0-3,8-11", child: "1-2,9-10", want: true},
		{parent: "0-3,8-11", child: "2-8", want: false},
		{parent: "0-3", child: "4", want: false},
	} {
		got, err := CPUSetContains(test.parent, test.child)
		if err != nil || got != test.want {
			t.Fatalf("CPUSetContains(%q, %q) = %t, %v; want %t", test.parent, test.child, got, err, test.want)
		}
	}
	if _, err := CPUSetContains("0-3", "01"); err == nil {
		t.Fatal("noncanonical child CPU set was accepted")
	}
}
