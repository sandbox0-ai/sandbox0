package teamquota

import "testing"

func TestAllocationOperationFingerprintCoversNormalizedMutation(t *testing.T) {
	owner := Owner{
		TeamID:    "team-1",
		Kind:      "sandbox",
		ID:        "sandbox-1",
		ClusterID: "cluster-1",
	}
	operation := Operation{ID: "operation-1", Kind: "resize", Generation: 2}
	values := Values{
		KeySandboxRuntimeCount: 1,
		KeySandboxMemoryBytes:  512,
	}
	runtime := RuntimeRef{
		Namespace:  "sandboxes",
		Name:       "sandbox-1",
		UID:        "uid-1",
		Generation: 2,
	}
	baseline, err := allocationOperationFingerprint(
		owner,
		operation,
		values,
		reserveTarget,
		runtime,
	)
	if err != nil {
		t.Fatalf("allocationOperationFingerprint() error = %v", err)
	}
	reordered, err := allocationOperationFingerprint(
		owner,
		operation,
		Values{
			KeySandboxMemoryBytes:  512,
			KeySandboxRuntimeCount: 1,
		},
		reserveTarget,
		runtime,
	)
	if err != nil {
		t.Fatalf("reordered allocationOperationFingerprint() error = %v", err)
	}
	if reordered != baseline {
		t.Fatalf("map order changed fingerprint: %q != %q", reordered, baseline)
	}

	tests := []struct {
		name      string
		owner     Owner
		operation Operation
		values    Values
		mode      reserveMode
		runtime   RuntimeRef
	}{
		{
			name:      "owner",
			owner:     Owner{TeamID: owner.TeamID, Kind: owner.Kind, ID: owner.ID, ClusterID: "cluster-2"},
			operation: operation,
			values:    values,
			mode:      reserveTarget,
			runtime:   runtime,
		},
		{
			name:      "kind",
			owner:     owner,
			operation: Operation{ID: operation.ID, Kind: "claim", Generation: operation.Generation},
			values:    values,
			mode:      reserveTarget,
			runtime:   runtime,
		},
		{
			name:      "generation",
			owner:     owner,
			operation: Operation{ID: operation.ID, Kind: operation.Kind, Generation: 3},
			values:    values,
			mode:      reserveTarget,
			runtime:   runtime,
		},
		{
			name:      "values",
			owner:     owner,
			operation: operation,
			values: Values{
				KeySandboxRuntimeCount: 1,
				KeySandboxMemoryBytes:  1024,
			},
			mode:    reserveTarget,
			runtime: runtime,
		},
		{
			name:      "mode",
			owner:     owner,
			operation: operation,
			values:    values,
			mode:      reserveRelease,
			runtime:   runtime,
		},
		{
			name:      "runtime",
			owner:     owner,
			operation: operation,
			values:    values,
			mode:      reserveTarget,
			runtime: RuntimeRef{
				Namespace:  runtime.Namespace,
				Name:       runtime.Name,
				UID:        "uid-2",
				Generation: runtime.Generation,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fingerprint, err := allocationOperationFingerprint(
				tt.owner,
				tt.operation,
				tt.values,
				tt.mode,
				tt.runtime,
			)
			if err != nil {
				t.Fatalf("allocationOperationFingerprint() error = %v", err)
			}
			if fingerprint == baseline {
				t.Fatalf("%s mutation did not change fingerprint", tt.name)
			}
		})
	}
}

func TestTransferRequestFingerprintIncludesTransitionReserve(t *testing.T) {
	request := normalizeTransferRequest(TransferRequest{
		Source: Owner{
			TeamID:    "team-transfer-fingerprint",
			Kind:      "warm_pool",
			ID:        "template-1",
			ClusterID: "cluster-1",
		},
		Destination: Owner{
			TeamID:    "team-transfer-fingerprint",
			Kind:      "sandbox",
			ID:        "sandbox-1",
			ClusterID: "cluster-1",
		},
		Operation: Operation{
			ID:         "claim-1",
			Kind:       "hot_claim",
			Generation: 1,
		},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
		DestinationTarget: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
		TransitionReserve: Values{
			KeySandboxRuntimeCount: 1,
			KeySandboxMemoryBytes:  512,
		},
	})
	baseline, err := transferRequestFingerprint(request)
	if err != nil {
		t.Fatalf("transferRequestFingerprint() error = %v", err)
	}

	reordered := request
	reordered.TransitionReserve = Values{
		KeySandboxMemoryBytes:  512,
		KeySandboxRuntimeCount: 1,
	}
	reorderedFingerprint, err := transferRequestFingerprint(reordered)
	if err != nil {
		t.Fatalf("transferRequestFingerprint(reordered) error = %v", err)
	}
	if reorderedFingerprint != baseline {
		t.Fatalf(
			"transition reserve map order changed fingerprint: %q != %q",
			reorderedFingerprint,
			baseline,
		)
	}

	changed := request
	changed.TransitionReserve = request.TransitionReserve.Clone()
	changed.TransitionReserve[KeySandboxRuntimeCount] = 2
	changedFingerprint, err := transferRequestFingerprint(changed)
	if err != nil {
		t.Fatalf("transferRequestFingerprint(changed) error = %v", err)
	}
	if changedFingerprint == baseline {
		t.Fatal("transition reserve mutation did not change fingerprint")
	}

	omitted := request
	omitted.TransitionReserve = nil
	omittedFingerprint, err := transferRequestFingerprint(omitted)
	if err != nil {
		t.Fatalf("transferRequestFingerprint(omitted) error = %v", err)
	}
	if omittedFingerprint == baseline {
		t.Fatal("omitting transition reserve did not change fingerprint")
	}
}
