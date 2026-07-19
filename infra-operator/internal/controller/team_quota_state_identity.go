package controller

import (
	"context"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

// ensureTeamQuotaStateIdentity initializes a region policy owner's status
// identity exactly once. An explicit spec value is accepted only as a recovery
// input for the same retained state plane; ordinary installations get a fresh
// random UUID.
func (r *Sandbox0InfraReconciler) ensureTeamQuotaStateIdentity(
	ctx context.Context,
	infra *infrav1alpha1.Sandbox0Infra,
) (bool, error) {
	if infra == nil ||
		!infraplan.Compile(infra).TeamQuotaPolicyOwner() ||
		infra.Spec.TeamQuota == nil ||
		(infra.Status.TeamQuota != nil && infra.Status.TeamQuota.StateID != "") {
		return false, nil
	}
	if bootstrap := infra.Spec.TeamQuota.StateID; bootstrap != "" && !validTeamQuotaBootstrapStateID(bootstrap) {
		return false, nil
	}

	generatedStateID := uuid.NewString()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &infrav1alpha1.Sandbox0Infra{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(infra), latest); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !latest.DeletionTimestamp.IsZero() ||
			!infraplan.Compile(latest).TeamQuotaPolicyOwner() ||
			latest.Spec.TeamQuota == nil ||
			(latest.Status.TeamQuota != nil && latest.Status.TeamQuota.StateID != "") {
			return nil
		}

		stateID := latest.Spec.TeamQuota.StateID
		if stateID == "" {
			stateID = generatedStateID
		} else if !validTeamQuotaBootstrapStateID(stateID) {
			return nil
		}
		latest.Status.TeamQuota = &infrav1alpha1.TeamQuotaStatus{StateID: stateID}
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return false, err
	}
	// Requeue even if a concurrent reconciler won the update so this attempt
	// never compiles a runtime plan from its stale, identity-free object.
	return true, nil
}

func validTeamQuotaBootstrapStateID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil &&
		parsed.Version() == 4 &&
		parsed.String() == value
}
