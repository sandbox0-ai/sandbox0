package teamquota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestTransitionReserveLifecycle(t *testing.T) {
	for _, action := range []string{"commit", "abort"} {
		t.Run(action, func(t *testing.T) {
			repo := newTeamQuotaTestRepository(t)
			ctx := context.Background()
			if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
				Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 2},
			)); err != nil {
				t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
			}

			source := Owner{
				TeamID:    "team-transition-" + action,
				Kind:      "warm_pool",
				ID:        "template-1",
				ClusterID: "cluster-1",
			}
			if err := repo.ReconcileTarget(ctx, source, Values{
				KeySandboxRuntimeCount: 1,
			}, RuntimeRef{}); err != nil {
				t.Fatalf("ReconcileTarget(source) error = %v", err)
			}
			request := transitionTransferRequest(source, "sandbox-1", "claim-1")

			prepared, err := repo.PrepareTransfer(ctx, request)
			if err != nil {
				t.Fatalf("PrepareTransfer() error = %v", err)
			}
			if got := prepared.Reserved[KeySandboxRuntimeCount]; got != 1 {
				t.Fatalf("prepared runtime reserve = %d, want 1", got)
			}
			states, err := repo.TransferStates(
				ctx,
				source.TeamID,
				[]string{request.Operation.ID, "missing-operation"},
			)
			if err != nil {
				t.Fatalf("TransferStates(prepared) error = %v", err)
			}
			if states[request.Operation.ID] != "prepared" {
				t.Fatalf("prepared transfer state = %q, want prepared", states[request.Operation.ID])
			}
			if _, ok := states["missing-operation"]; ok {
				t.Fatalf("missing transfer unexpectedly returned state %q", states["missing-operation"])
			}
			statuses, err := repo.ListStatus(ctx, source.TeamID)
			if err != nil {
				t.Fatalf("ListStatus(prepared) error = %v", err)
			}
			if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 1 || got.Reserved != 1 {
				t.Fatalf("prepared runtime status = %+v, want committed 1 reserved 1", got)
			}

			retried, err := repo.PrepareTransfer(ctx, request)
			if err != nil || retried.AllocationID != prepared.AllocationID {
				t.Fatalf("idempotent PrepareTransfer() = (%+v, %v)", retried, err)
			}
			conflicting := request
			conflicting.TransitionReserve = Values{KeySandboxRuntimeCount: 2}
			if _, err := repo.PrepareTransfer(ctx, conflicting); err == nil {
				t.Fatal("PrepareTransfer(changed transition reserve) error = nil")
			} else {
				var conflict *OperationConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf(
						"PrepareTransfer(changed transition reserve) error = %v, want OperationConflictError",
						err,
					)
				}
			}

			ref := Ref(prepared.Owner, prepared.Operation)
			switch action {
			case "commit":
				if err := repo.CommitTransfer(ctx, ref); err != nil {
					t.Fatalf("CommitTransfer() error = %v", err)
				}
			case "abort":
				if err := repo.AbortTransfer(ctx, ref, "runtime handoff failed"); err != nil {
					t.Fatalf("AbortTransfer() error = %v", err)
				}
			default:
				t.Fatalf("unknown lifecycle action %q", action)
			}
			states, err = repo.TransferStates(ctx, source.TeamID, []string{request.Operation.ID})
			if err != nil {
				t.Fatalf("TransferStates(%s) error = %v", action, err)
			}
			wantState := "aborted"
			if action == "commit" {
				wantState = "committed"
			}
			if states[request.Operation.ID] != wantState {
				t.Fatalf(
					"%s transfer state = %q, want %q",
					action,
					states[request.Operation.ID],
					wantState,
				)
			}

			statuses, err = repo.ListStatus(ctx, source.TeamID)
			if err != nil {
				t.Fatalf("ListStatus(%s) error = %v", action, err)
			}
			runtimeStatus := statusForKey(t, statuses, KeySandboxRuntimeCount)
			if runtimeStatus.Reserved != 0 {
				t.Fatalf("%s runtime reserve = %d, want 0", action, runtimeStatus.Reserved)
			}
			if runtimeStatus.Committed != 1 {
				t.Fatalf("%s runtime committed = %d, want 1", action, runtimeStatus.Committed)
			}

			sourceAllocation, err := repo.GetRecoveryAllocation(ctx, source)
			if err != nil {
				t.Fatalf("GetRecoveryAllocation(source) error = %v", err)
			}
			wantSource := int64(1)
			if action == "commit" {
				wantSource = 0
			}
			if sourceAllocation == nil ||
				sourceAllocation.Committed[KeySandboxRuntimeCount] != wantSource {
				t.Fatalf(
					"%s source allocation = %+v, want runtime committed %d",
					action,
					sourceAllocation,
					wantSource,
				)
			}
			if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
				t.Fatalf("ValidateUsageInvariant(%s) error = %v", action, err)
			}
		})
	}
}

func TestCommitTransferObservedSourceAdoptsOversizedSource(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
		Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 2},
	)); err != nil {
		t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
	}

	source := Owner{
		TeamID:    "team-observed-source-adoption",
		Kind:      "warm_pool",
		ID:        "template-1",
		ClusterID: "cluster-1",
	}
	if err := repo.ReconcileTarget(ctx, source, Values{
		KeySandboxRuntimeCount: 1,
	}, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(source) error = %v", err)
	}
	request := transitionTransferRequest(source, "sandbox-1", "claim-1")
	prepared, err := repo.PrepareTransfer(ctx, request)
	if err != nil {
		t.Fatalf("PrepareTransfer() error = %v", err)
	}

	if err := repo.CommitTransferObservedSource(
		ctx,
		Ref(prepared.Owner, prepared.Operation),
		Values{KeySandboxRuntimeCount: 1},
	); err != nil {
		t.Fatalf("CommitTransferObservedSource() error = %v", err)
	}

	sourceAllocation, err := repo.GetRecoveryAllocation(ctx, source)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(source) error = %v", err)
	}
	if sourceAllocation == nil ||
		sourceAllocation.Committed[KeySandboxRuntimeCount] != 1 {
		t.Fatalf(
			"source allocation = %+v, want adopted runtime committed 1",
			sourceAllocation,
		)
	}
	destinationAllocation, err := repo.GetRecoveryAllocation(ctx, request.Destination)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(destination) error = %v", err)
	}
	if destinationAllocation == nil ||
		destinationAllocation.Committed[KeySandboxRuntimeCount] != 1 {
		t.Fatalf(
			"destination allocation = %+v, want runtime committed 1",
			destinationAllocation,
		)
	}
	statuses, err := repo.ListStatus(ctx, source.TeamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 2 || got.Reserved != 0 {
		t.Fatalf("runtime status = %+v, want committed 2 reserved 0", got)
	}
	if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestCommitTransferObservedSourceStaleObservationDoesNotLowerSource(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		name := "sequential"
		if concurrent {
			name = "concurrent"
		}
		t.Run(name, func(t *testing.T) {
			repo := newTeamQuotaTestRepository(t)
			ctx := context.Background()
			if err := repo.UnsafeReplaceDefaultPoliciesForTest(ctx, completeDefaultPolicies(
				Policy{Key: KeySandboxRuntimeCount, Kind: KindCapacity, Limit: 5},
			)); err != nil {
				t.Fatalf("ReplaceDefaultPolicies() error = %v", err)
			}

			source := Owner{
				TeamID:    "team-stale-observation-" + name,
				Kind:      "warm_pool",
				ID:        "template-1",
				ClusterID: "cluster-1",
			}
			if err := repo.ReconcileTarget(ctx, source, Values{
				KeySandboxRuntimeCount: 3,
			}, RuntimeRef{}); err != nil {
				t.Fatalf("ReconcileTarget(source) error = %v", err)
			}

			prepared := make([]*Reservation, 0, 2)
			for i := 1; i <= 2; i++ {
				request := transitionTransferRequest(
					source,
					fmt.Sprintf("sandbox-%d", i),
					fmt.Sprintf("claim-%d", i),
				)
				reservation, err := repo.PrepareTransfer(ctx, request)
				if err != nil {
					t.Fatalf("PrepareTransfer(%d) error = %v", i, err)
				}
				prepared = append(prepared, reservation)
			}

			commit := func(reservation *Reservation) error {
				return repo.CommitTransferObservedSource(
					ctx,
					Ref(reservation.Owner, reservation.Operation),
					Values{KeySandboxRuntimeCount: 0},
				)
			}
			if concurrent {
				start := make(chan struct{})
				errs := make(chan error, len(prepared))
				var wg sync.WaitGroup
				for _, reservation := range prepared {
					wg.Add(1)
					go func(reservation *Reservation) {
						defer wg.Done()
						<-start
						errs <- commit(reservation)
					}(reservation)
				}
				close(start)
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatalf("concurrent CommitTransferObservedSource() error = %v", err)
					}
				}
			} else {
				for _, reservation := range prepared {
					if err := commit(reservation); err != nil {
						t.Fatalf("CommitTransferObservedSource() error = %v", err)
					}
				}
			}

			sourceAllocation, err := repo.GetRecoveryAllocation(ctx, source)
			if err != nil {
				t.Fatalf("GetRecoveryAllocation(source) error = %v", err)
			}
			if sourceAllocation == nil ||
				sourceAllocation.Committed[KeySandboxRuntimeCount] != 1 {
				t.Fatalf(
					"source allocation = %+v, want latest serialized runtime committed 1",
					sourceAllocation,
				)
			}
			for _, reservation := range prepared {
				destinationAllocation, err := repo.GetRecoveryAllocation(ctx, reservation.Owner)
				if err != nil {
					t.Fatalf("GetRecoveryAllocation(%s) error = %v", reservation.Owner.ID, err)
				}
				if destinationAllocation == nil ||
					destinationAllocation.Committed[KeySandboxRuntimeCount] != 1 {
					t.Fatalf(
						"destination allocation %s = %+v, want runtime committed 1",
						reservation.Owner.ID,
						destinationAllocation,
					)
				}
			}
			statuses, err := repo.ListStatus(ctx, source.TeamID)
			if err != nil {
				t.Fatalf("ListStatus() error = %v", err)
			}
			if got := statusForKey(t, statuses, KeySandboxRuntimeCount); got.Committed != 3 || got.Reserved != 0 {
				t.Fatalf("runtime status = %+v, want committed 3 reserved 0", got)
			}
			if err := repo.ValidateUsageInvariant(ctx, source.TeamID); err != nil {
				t.Fatalf("ValidateUsageInvariant() error = %v", err)
			}
		})
	}
}

func transitionTransferRequest(source Owner, destinationID, operationID string) TransferRequest {
	return TransferRequest{
		Source: source,
		Destination: Owner{
			TeamID:    source.TeamID,
			Kind:      "sandbox",
			ID:        destinationID,
			ClusterID: source.ClusterID,
		},
		Operation: Operation{
			ID:         operationID,
			Kind:       "hot_claim",
			Generation: 1,
		},
		SourceDecrease: Values{
			KeySandboxRuntimeCount: 1,
		},
		DestinationTarget: Values{
			KeySandboxRuntimeCount: 1,
		},
		TransitionReserve: Values{
			KeySandboxRuntimeCount: 1,
		},
	}
}
