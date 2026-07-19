package controller

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestEnsureTeamQuotaStateIdentityGeneratesOnceAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	infra := teamQuotaIdentityOwner("owner")
	reconciler, kubeClient := newTeamQuotaIdentityTestReconciler(t, infra)

	requeue, err := reconciler.ensureTeamQuotaStateIdentity(ctx, infra.DeepCopy())
	if err != nil {
		t.Fatalf("initialize state identity: %v", err)
	}
	if !requeue {
		t.Fatal("initialization must requeue before compiling runtime config")
	}

	stored := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	stateID := stored.Status.TeamQuota.StateID
	parsed, err := uuid.Parse(stateID)
	if err != nil || parsed.Version() != 4 || parsed.String() != stateID {
		t.Fatalf("generated state ID %q is not a canonical UUID v4", stateID)
	}

	restarted := &Sandbox0InfraReconciler{Client: kubeClient, Scheme: reconciler.Scheme}
	requeue, err = restarted.ensureTeamQuotaStateIdentity(ctx, stored.DeepCopy())
	if err != nil {
		t.Fatalf("verify identity after controller restart: %v", err)
	}
	if requeue {
		t.Fatal("existing state identity unexpectedly requested reinitialization")
	}
	afterRestart := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	if afterRestart.Status.TeamQuota.StateID != stateID {
		t.Fatalf("state ID changed across restart: got %q, want %q", afterRestart.Status.TeamQuota.StateID, stateID)
	}
}

func TestEnsureTeamQuotaStateIdentityUsesExplicitRecoveryInputAndRejectsChange(t *testing.T) {
	ctx := context.Background()
	infra := teamQuotaIdentityOwner("recovered-owner")
	const recoveredStateID = "8f7500d8-6e80-48d0-891e-e569ebd76e4e"
	infra.Spec.TeamQuota.StateID = recoveredStateID
	reconciler, kubeClient := newTeamQuotaIdentityTestReconciler(t, infra)

	requeue, err := reconciler.ensureTeamQuotaStateIdentity(ctx, infra.DeepCopy())
	if err != nil {
		t.Fatalf("initialize recovered state identity: %v", err)
	}
	if !requeue {
		t.Fatal("recovery initialization must requeue")
	}
	stored := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	if stored.Status.TeamQuota == nil || stored.Status.TeamQuota.StateID != recoveredStateID {
		t.Fatalf("recovered status = %#v, want state ID %q", stored.Status.TeamQuota, recoveredStateID)
	}

	stored.Spec.TeamQuota.StateID = "4f54208d-4f01-42da-bdbc-88cc5793857b"
	compiled := infraplan.Compile(stored)
	if !containsTeamQuotaIdentityError(
		compiled.Validation.FatalErrors,
		"region Team Quota owner spec.teamQuota.stateId recovery input must match the immutable status.teamQuota.stateId",
	) {
		t.Fatalf("changed recovery input was accepted: %#v", compiled.Validation.FatalErrors)
	}
}

func TestEnsureTeamQuotaStateIdentityDeleteAndRecreateIssuesFreshIdentity(t *testing.T) {
	ctx := context.Background()
	infra := teamQuotaIdentityOwner("recreated-owner")
	reconciler, kubeClient := newTeamQuotaIdentityTestReconciler(t, infra)
	if _, err := reconciler.ensureTeamQuotaStateIdentity(ctx, infra.DeepCopy()); err != nil {
		t.Fatalf("initialize first owner: %v", err)
	}
	first := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	firstStateID := first.Status.TeamQuota.StateID
	if err := kubeClient.Delete(ctx, first); err != nil {
		t.Fatalf("delete first owner: %v", err)
	}

	recreated := teamQuotaIdentityOwner(infra.Name)
	if err := kubeClient.Create(ctx, recreated); err != nil {
		t.Fatalf("create replacement owner: %v", err)
	}
	if _, err := reconciler.ensureTeamQuotaStateIdentity(ctx, recreated.DeepCopy()); err != nil {
		t.Fatalf("initialize replacement owner: %v", err)
	}
	second := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, recreated)
	if second.Status.TeamQuota == nil || second.Status.TeamQuota.StateID == "" {
		t.Fatal("replacement owner did not receive a state identity")
	}
	if second.Status.TeamQuota.StateID == firstStateID {
		t.Fatalf("replacement owner reused deleted identity %q", firstStateID)
	}
}

func TestEnsureTeamQuotaStateIdentityConcurrentInitializationIsCreateOnce(t *testing.T) {
	ctx := context.Background()
	infra := teamQuotaIdentityOwner("concurrent-owner")
	reconciler, kubeClient := newTeamQuotaIdentityTestReconciler(t, infra)

	const contenders = 2
	start := make(chan struct{})
	errs := make(chan error, contenders)
	candidates := make([]*infrav1alpha1.Sandbox0Infra, contenders)
	for index := range contenders {
		candidates[index] = getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	}
	var wg sync.WaitGroup
	wg.Add(contenders)
	for index := range contenders {
		go func(candidate *infrav1alpha1.Sandbox0Infra) {
			defer wg.Done()
			<-start
			_, err := reconciler.ensureTeamQuotaStateIdentity(ctx, candidate)
			errs <- err
		}(candidates[index])
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent initialization: %v", err)
		}
	}

	stored := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	if stored.Status.TeamQuota == nil || !validTeamQuotaBootstrapStateID(stored.Status.TeamQuota.StateID) {
		t.Fatalf("concurrent initialization stored invalid status %#v", stored.Status.TeamQuota)
	}
}

func TestEnsureTeamQuotaStateIdentityNeverInitializesConsumer(t *testing.T) {
	ctx := context.Background()
	infra := teamQuotaIdentityOwner("consumer")
	infra.Spec.Services.RegionalGateway = nil
	infra.Spec.Services.Manager = &infrav1alpha1.ManagerServiceConfig{
		WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
			EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
		},
	}
	infra.Spec.ControlPlane = &infrav1alpha1.ControlPlaneConfig{}
	reconciler, kubeClient := newTeamQuotaIdentityTestReconciler(t, infra)

	requeue, err := reconciler.ensureTeamQuotaStateIdentity(ctx, infra.DeepCopy())
	if err != nil {
		t.Fatalf("check consumer identity: %v", err)
	}
	if requeue {
		t.Fatal("consumer unexpectedly initialized owner status")
	}
	stored := getTeamQuotaIdentityTestInfra(t, ctx, kubeClient, infra)
	if stored.Status.TeamQuota != nil {
		t.Fatalf("consumer status = %#v, want nil", stored.Status.TeamQuota)
	}
}

func teamQuotaIdentityOwner(name string) *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{},
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}
}

func newTeamQuotaIdentityTestReconciler(
	t *testing.T,
	objects ...client.Object,
) (*Sandbox0InfraReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&infrav1alpha1.Sandbox0Infra{}).
		WithObjects(objects...).
		Build()
	return &Sandbox0InfraReconciler{Client: kubeClient, Scheme: scheme}, kubeClient
}

func getTeamQuotaIdentityTestInfra(
	t *testing.T,
	ctx context.Context,
	kubeClient client.Client,
	keySource client.Object,
) *infrav1alpha1.Sandbox0Infra {
	t.Helper()
	infra := &infrav1alpha1.Sandbox0Infra{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(keySource), infra); err != nil {
		t.Fatalf("get Sandbox0Infra: %v", err)
	}
	return infra
}

func containsTeamQuotaIdentityError(errors []string, want string) bool {
	for _, message := range errors {
		if strings.Contains(message, want) {
			return true
		}
	}
	return false
}
