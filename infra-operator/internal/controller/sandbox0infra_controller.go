/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	stderrors "errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/rbac"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/clustergateway"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/fuseplugin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/globalgateway"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/manager"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/regionalgateway"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/scheduler"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storageproxy"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

const (
	finalizerName          = "sandbox0infra.infra.sandbox0.ai/finalizer"
	requeueInterval        = 30 * time.Second
	retryBaseDelay         = 2 * time.Second
	retryMaxDelay          = 2 * time.Minute
	initUserPasswordLength = 24
	templateIDPodLabelKey  = "sandbox0.ai/template-id"
)

// Sandbox0InfraReconciler reconciles a Sandbox0Infra object
type Sandbox0InfraReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=infra.sandbox0.ai,resources=sandbox0infras,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=infra.sandbox0.ai,resources=sandbox0infras/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=infra.sandbox0.ai,resources=sandbox0infras/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets;replicasets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services;secrets;configmaps;persistentvolumeclaims;serviceaccounts;pods;pods/exec;pods/resize;pods/status;nodes;events;namespaces;endpoints,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete;bind
//+kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingwebhookconfigurations;mutatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=sandbox0.ai,resources=sandboxtemplates;sandboxtemplates/status,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop
func (r *Sandbox0InfraReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling Sandbox0Infra", "namespace", req.Namespace, "name", req.Name)

	// Fetch the Sandbox0Infra instance
	infra := &infrav1alpha1.Sandbox0Infra{}
	if err := r.Get(ctx, req.NamespacedName, infra); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Sandbox0Infra resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Sandbox0Infra")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !infra.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, infra)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(infra, finalizerName) {
		controllerutil.AddFinalizer(infra, finalizerName)
		if err := r.Update(ctx, infra); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Initialize status if needed
	if infra.Status.Phase == "" {
		return r.initializeStatus(ctx, infra)
	}

	// Set default values
	r.setDefaults(infra)
	if err := r.validateSpecSemantics(ctx, infra); err != nil {
		logger.Error(err, "Sandbox0Infra validation failed")
		if updateErr := r.updateValidationFailureStatus(ctx, infra, err); updateErr != nil {
			logger.Error(updateErr, "Failed to project validation failure status")
		}
		return ctrl.Result{}, err
	}

	// Main reconciliation logic based on compiled desired state.
	compiledPlan := infraplan.Compile(infra)
	result, err := r.reconcileComponentPlan(ctx, infra, compiledPlan)

	// Update overall status
	if updateErr := r.updateOverallStatus(ctx, infra); updateErr != nil {
		logger.Error(updateErr, "Failed to update overall status")
		if err == nil {
			err = updateErr
		}
	}

	return result, err
}

// setDefaults sets default values for the spec
func (r *Sandbox0InfraReconciler) setDefaults(infra *infrav1alpha1.Sandbox0Infra) {
	if infra.Spec.Database != nil && infra.Spec.Database.Type == "" {
		infra.Spec.Database.Type = infrav1alpha1.DatabaseTypeBuiltin
	}
	if infra.Spec.Storage != nil && infra.Spec.Storage.Type == "" {
		infra.Spec.Storage.Type = infrav1alpha1.StorageTypeBuiltin
	}
	if infra.Spec.Registry != nil && infra.Spec.Registry.Provider == "" {
		infra.Spec.Registry.Provider = infrav1alpha1.RegistryProviderBuiltin
	}
}

// initializeStatus initializes the status for a new resource
func (r *Sandbox0InfraReconciler) initializeStatus(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Initializing status")

	now := metav1.Now()
	infra.Status.Phase = infrav1alpha1.PhaseInstalling
	infra.Status.ObservedVersion = r.getImageTag(ctx)
	infra.Status.LastOperation = &infrav1alpha1.LastOperation{
		Type:      "Install",
		StartedAt: &now,
		Status:    "InProgress",
	}

	if err := r.Status().Update(ctx, infra); err != nil {
		logger.Error(err, "Failed to initialize status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

// reconcileDelete handles cleanup when the resource is being deleted
func (r *Sandbox0InfraReconciler) reconcileDelete(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Handling deletion")

	// Cleanup logic here - resources owned by this CR will be garbage collected
	// but we might need to clean up external resources

	// Remove finalizer
	controllerutil.RemoveFinalizer(infra, finalizerName)
	if err := r.Update(ctx, infra); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileComponentPlan reconciles components based on spec configuration.
func (r *Sandbox0InfraReconciler) reconcileComponentPlan(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) (ctrl.Result, error) {
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	plan := compiledPlan.Components
	logger := log.FromContext(ctx)
	logger.Info("Reconciling components", "controlPlane", plan.HasControlPlane, "dataPlane", plan.HasDataPlane)

	if len(compiledPlan.Validation.FatalErrors) > 0 {
		return ctrl.Result{}, stderrors.New(strings.Join(compiledPlan.Validation.FatalErrors, "; "))
	}
	fresh, err := r.isLatestReconcileTarget(ctx, infra)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !fresh {
		logger.Info("Stopping stale reconcile before component cleanup")
		return ctrl.Result{}, nil
	}

	resources := common.NewResourceManager(r.Client, r.Scheme, r.getImagePullPolicy(ctx), r.getLocalDevConfig(ctx))
	imageRepo := r.getImageRepo(ctx)
	imageTag := r.getImageTag(ctx)
	authReconciler := internalauth.NewReconciler(resources)
	dbReconciler := database.NewReconciler(resources)
	storageReconciler := storage.NewReconciler(resources)
	registryReconciler := registry.NewReconciler(resources)
	globalGatewayReconciler := globalgateway.NewReconciler(resources)
	regionalGatewayReconciler := regionalgateway.NewReconciler(resources)
	schedulerReconciler := scheduler.NewReconciler(resources)
	clusterGatewayReconciler := clustergateway.NewReconciler(resources)
	managerReconciler := manager.NewReconciler(resources)
	storageProxyReconciler := storageproxy.NewReconciler(resources)
	fusePluginReconciler := fuseplugin.NewReconciler(resources)
	netdReconciler := netd.NewReconciler(resources)
	rbacReconciler := rbac.NewReconciler(resources)

	if err := r.cleanupDisabledServiceResources(ctx, infra, compiledPlan.Cleanup, dbReconciler, storageReconciler, registryReconciler); err != nil {
		return ctrl.Result{RequeueAfter: requeueInterval}, err
	}

	steps, err := r.bindWorkflowSteps(infra, compiledPlan, resources, imageRepo, imageTag, authReconciler, dbReconciler, storageReconciler, registryReconciler, globalGatewayReconciler, regionalGatewayReconciler, schedulerReconciler, clusterGatewayReconciler, managerReconciler, storageProxyReconciler, fusePluginReconciler, netdReconciler, rbacReconciler)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.runSteps(ctx, infra, steps)
}

func (r *Sandbox0InfraReconciler) bindWorkflowSteps(
	infra *infrav1alpha1.Sandbox0Infra,
	compiledPlan *infraplan.InfraPlan,
	resources *common.ResourceManager,
	imageRepo, imageTag string,
	authReconciler *internalauth.Reconciler,
	dbReconciler *database.Reconciler,
	storageReconciler *storage.Reconciler,
	registryReconciler *registry.Reconciler,
	globalGatewayReconciler *globalgateway.Reconciler,
	regionalGatewayReconciler *regionalgateway.Reconciler,
	schedulerReconciler *scheduler.Reconciler,
	clusterGatewayReconciler *clustergateway.Reconciler,
	managerReconciler *manager.Reconciler,
	storageProxyReconciler *storageproxy.Reconciler,
	fusePluginReconciler *fuseplugin.Reconciler,
	netdReconciler *netd.Reconciler,
	rbacReconciler *rbac.Reconciler,
) ([]reconcileStep, error) {
	steps := make([]reconcileStep, 0, len(compiledPlan.Workflow.Steps))
	for _, planned := range compiledPlan.Workflow.Steps {
		run, err := r.workflowStepRunner(infra, compiledPlan, resources, imageRepo, imageTag, planned.Name, authReconciler, dbReconciler, storageReconciler, registryReconciler, globalGatewayReconciler, regionalGatewayReconciler, schedulerReconciler, clusterGatewayReconciler, managerReconciler, storageProxyReconciler, fusePluginReconciler, netdReconciler, rbacReconciler)
		if err != nil {
			return nil, err
		}
		steps = append(steps, reconcileStep{
			Name:                 planned.Name,
			Run:                  run,
			ConditionType:        planned.ConditionType,
			SuccessReason:        planned.SuccessReason,
			SuccessMessage:       planned.SuccessMessage,
			ErrorReason:          planned.ErrorReason,
			SkipSuccessCondition: planned.SkipSuccessCondition,
		})
	}
	return steps, nil
}

func (r *Sandbox0InfraReconciler) workflowStepRunner(
	infra *infrav1alpha1.Sandbox0Infra,
	compiledPlan *infraplan.InfraPlan,
	resources *common.ResourceManager,
	imageRepo, imageTag, name string,
	authReconciler *internalauth.Reconciler,
	dbReconciler *database.Reconciler,
	storageReconciler *storage.Reconciler,
	registryReconciler *registry.Reconciler,
	globalGatewayReconciler *globalgateway.Reconciler,
	regionalGatewayReconciler *regionalgateway.Reconciler,
	schedulerReconciler *scheduler.Reconciler,
	clusterGatewayReconciler *clustergateway.Reconciler,
	managerReconciler *manager.Reconciler,
	storageProxyReconciler *storageproxy.Reconciler,
	fusePluginReconciler *fuseplugin.Reconciler,
	netdReconciler *netd.Reconciler,
	rbacReconciler *rbac.Reconciler,
) (func(context.Context) error, error) {
	switch name {
	case "control-plane-public-key":
		return func(ctx context.Context) error {
			publicKeySecret := &corev1.Secret{}
			return r.Get(ctx, types.NamespacedName{
				Name:      infra.Spec.ControlPlane.InternalAuthPublicKeySecret.Name,
				Namespace: infra.Namespace,
			}, publicKeySecret)
		}, nil
	case "internal-auth":
		return func(ctx context.Context) error { return authReconciler.Reconcile(ctx, infra) }, nil
	case "database":
		return func(ctx context.Context) error { return dbReconciler.Reconcile(ctx, infra) }, nil
	case "storage":
		return func(ctx context.Context) error { return storageReconciler.Reconcile(ctx, infra) }, nil
	case "registry":
		return func(ctx context.Context) error { return registryReconciler.Reconcile(ctx, infra) }, nil
	case "global-gateway-enterprise-license":
		return func(ctx context.Context) error {
			licenseFile := ""
			return common.EnsureEnterpriseLicense(ctx, resources, infra, &licenseFile, true, "global-gateway enterprise SSO")
		}, nil
	case "global-gateway":
		return func(ctx context.Context) error {
			return globalGatewayReconciler.Reconcile(ctx, infra, imageRepo, imageTag, compiledPlan)
		}, nil
	case "init-user-secret":
		return func(ctx context.Context) error { return r.ensureInitUserPasswordSecret(ctx, infra) }, nil
	case "regional-gateway-enterprise-license":
		return func(ctx context.Context) error {
			licenseFile := ""
			return common.EnsureEnterpriseLicense(ctx, resources, infra, &licenseFile, true, "enterprise features")
		}, nil
	case "regional-gateway":
		return func(ctx context.Context) error {
			return regionalGatewayReconciler.Reconcile(ctx, infra, imageRepo, imageTag, compiledPlan)
		}, nil
	case "scheduler-enterprise-license":
		return func(ctx context.Context) error {
			licenseFile := ""
			return common.EnsureEnterpriseLicense(ctx, resources, infra, &licenseFile, true, "scheduler")
		}, nil
	case "scheduler-rbac":
		return func(ctx context.Context) error { return rbacReconciler.ReconcileSchedulerRBAC(ctx, infra) }, nil
	case "scheduler":
		return func(ctx context.Context) error {
			return schedulerReconciler.Reconcile(ctx, infra, imageRepo, imageTag, compiledPlan)
		}, nil
	case "cluster-gateway-enterprise-license":
		return func(ctx context.Context) error {
			licenseFile := ""
			return common.EnsureEnterpriseLicense(ctx, resources, infra, &licenseFile, true, "OIDC SSO")
		}, nil
	case "cluster-gateway":
		return func(ctx context.Context) error {
			return clusterGatewayReconciler.Reconcile(ctx, infra, imageRepo, imageTag, compiledPlan)
		}, nil
	case "fuse-device-plugin":
		return func(ctx context.Context) error {
			return fusePluginReconciler.Reconcile(ctx, infra, imageRepo, imageTag)
		}, nil
	case "manager-rbac":
		return func(ctx context.Context) error { return rbacReconciler.ReconcileManagerRBAC(ctx, infra) }, nil
	case "manager":
		return func(ctx context.Context) error {
			return managerReconciler.Reconcile(ctx, infra, imageRepo, imageTag, compiledPlan)
		}, nil
	case "builtin-template-pods":
		return func(ctx context.Context) error { return r.waitBuiltinTemplatePodsReady(ctx, infra, compiledPlan) }, nil
	case "netd-rbac":
		return func(ctx context.Context) error { return rbacReconciler.ReconcileNetdRBAC(ctx, infra) }, nil
	case "netd":
		return func(ctx context.Context) error {
			return netdReconciler.Reconcile(ctx, infra, imageRepo, imageTag, compiledPlan)
		}, nil
	case "storage-proxy-rbac":
		return func(ctx context.Context) error { return rbacReconciler.ReconcileStorageProxyRBAC(ctx, infra) }, nil
	case "storage-proxy":
		return func(ctx context.Context) error {
			return storageProxyReconciler.Reconcile(ctx, infra, imageRepo, imageTag)
		}, nil
	case "init-user":
		return func(ctx context.Context) error { return r.reconcileInitUser(ctx, infra) }, nil
	case "register-cluster":
		return func(ctx context.Context) error { return r.registerCluster(ctx, infra) }, nil
	default:
		return nil, fmt.Errorf("unsupported workflow step %q", name)
	}
}

func (r *Sandbox0InfraReconciler) cleanupDisabledServiceResources(
	ctx context.Context,
	infra *infrav1alpha1.Sandbox0Infra,
	cleanupPlan infraplan.CleanupPlan,
	dbReconciler *database.Reconciler,
	storageReconciler *storage.Reconciler,
	registryReconciler *registry.Reconciler,
) error {
	deleteResource := func(ref infraplan.ResourceRef, obj client.Object) error {
		key := types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}
		if ref.Namespace == "" {
			key = types.NamespacedName{Name: ref.Name}
		}
		if err := r.Get(ctx, key, obj); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if cleanupPlan.CleanupBuiltinDatabase && dbReconciler != nil {
		if err := dbReconciler.CleanupBuiltinResources(ctx, infra); err != nil {
			return err
		}
	}
	if cleanupPlan.CleanupBuiltinStorage && storageReconciler != nil {
		if err := storageReconciler.CleanupBuiltinResources(ctx, infra); err != nil {
			return err
		}
	}
	if cleanupPlan.CleanupBuiltinRegistry && registryReconciler != nil {
		if err := registryReconciler.CleanupBuiltinResources(ctx, infra); err != nil {
			return err
		}
	}
	for _, ref := range cleanupPlan.DeleteNamespaced {
		obj, err := objectForCleanupKind(ref.Kind)
		if err != nil {
			return err
		}
		if err := deleteResource(ref, obj); err != nil {
			return err
		}
	}
	for _, ref := range cleanupPlan.DeleteClusterScoped {
		obj, err := objectForCleanupKind(ref.Kind)
		if err != nil {
			return err
		}
		if err := deleteResource(ref, obj); err != nil {
			return err
		}
	}

	return nil
}

func objectForCleanupKind(kind string) (client.Object, error) {
	switch kind {
	case "Deployment":
		return &appsv1.Deployment{}, nil
	case "StatefulSet":
		return &appsv1.StatefulSet{}, nil
	case "DaemonSet":
		return &appsv1.DaemonSet{}, nil
	case "Service":
		return &corev1.Service{}, nil
	case "ConfigMap":
		return &corev1.ConfigMap{}, nil
	case "Ingress":
		return &networkingv1.Ingress{}, nil
	case "ServiceAccount":
		return &corev1.ServiceAccount{}, nil
	case "ClusterRole":
		return &rbacv1.ClusterRole{}, nil
	case "ClusterRoleBinding":
		return &rbacv1.ClusterRoleBinding{}, nil
	default:
		return nil, fmt.Errorf("unsupported cleanup resource kind %q", kind)
	}
}

func (r *Sandbox0InfraReconciler) isLatestReconcileTarget(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (bool, error) {
	if infra == nil {
		return false, nil
	}
	if r == nil || r.Client == nil || infra.Name == "" || infra.Namespace == "" {
		return true, nil
	}

	latest := &infrav1alpha1.Sandbox0Infra{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(infra), latest); err != nil {
		return false, err
	}
	if latest.Generation != infra.Generation {
		return false, nil
	}
	return true, nil
}

// updateOverallStatus updates the overall status based on conditions
func (r *Sandbox0InfraReconciler) updateOverallStatus(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	compiledPlan := infraplan.Compile(infra)
	r.projectStatusForPlan(infra, compiledPlan)

	retainedResources, err := collectRetainedResources(ctx, r.Client, infra.Namespace, compiledPlan.Status.RetainedResources)
	if err != nil {
		return err
	}
	infra.Status.RetainedResources = retainedResources

	expectedConditions := compiledPlan.Status.ExpectedConditions
	totalCount := len(expectedConditions)
	readyCount := 0
	allReady := totalCount > 0
	for _, conditionType := range expectedConditions {
		condition := meta.FindStatusCondition(infra.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionTrue {
			allReady = false
			continue
		}
		readyCount++
	}

	if totalCount > 0 {
		infra.Status.Progress = fmt.Sprintf("%d/%d", readyCount, totalCount)
	} else {
		infra.Status.Progress = ""
	}

	statusMessage := ""
	if allReady && totalCount > 0 {
		statusMessage = "All services are healthy"
	} else {
		for _, conditionType := range expectedConditions {
			cond := meta.FindStatusCondition(infra.Status.Conditions, conditionType)
			if cond == nil {
				statusMessage = fmt.Sprintf("%s not reported yet", conditionType)
				break
			}
			if cond.Status != metav1.ConditionTrue {
				switch {
				case cond.Message != "":
					statusMessage = cond.Message
				case cond.Reason != "":
					statusMessage = cond.Reason
				default:
					statusMessage = fmt.Sprintf("%s not ready", conditionType)
				}
				break
			}
		}
	}
	if statusMessage != "" {
		infra.Status.LastMessage = statusMessage
	}

	// Update phase
	if allReady && totalCount > 0 {
		infra.Status.Phase = infrav1alpha1.PhaseReady
		if infra.Status.LastOperation != nil && infra.Status.LastOperation.Status == "InProgress" {
			now := metav1.Now()
			infra.Status.LastOperation.CompletedAt = &now
			infra.Status.LastOperation.Status = "Succeeded"
		}
	} else {
		if infra.Status.LastOperation != nil && infra.Status.LastOperation.Status == "InProgress" {
			switch infra.Status.LastOperation.Type {
			case "Upgrade":
				infra.Status.Phase = infrav1alpha1.PhaseUpgrading
			default:
				infra.Status.Phase = infrav1alpha1.PhaseInstalling
			}
		} else {
			// Check if any condition failed
			hasFailed := false
			for _, conditionType := range expectedConditions {
				cond := meta.FindStatusCondition(infra.Status.Conditions, conditionType)
				if cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason != "" {
					hasFailed = true
					break
				}
			}
			if hasFailed {
				infra.Status.Phase = infrav1alpha1.PhaseDegraded
			}
		}
	}

	// Set the Ready condition
	if allReady {
		r.setCondition(ctx, infra, infrav1alpha1.ConditionTypeReady, metav1.ConditionTrue, "AllServicesHealthy", "All services are healthy")
	} else {
		r.setCondition(ctx, infra, infrav1alpha1.ConditionTypeReady, metav1.ConditionFalse, "ServicesNotReady", "Some services are not ready")
	}

	desiredStatus := infra.Status.DeepCopy()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &infrav1alpha1.Sandbox0Infra{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(infra), latest); err != nil {
			return err
		}
		if latest.Generation != infra.Generation {
			logger.Info("Skipping stale status update", "reconciledGeneration", infra.Generation, "latestGeneration", latest.Generation)
			return nil
		}

		if reflect.DeepEqual(&latest.Status, desiredStatus) {
			return nil
		}

		base := latest.DeepCopy()
		latest.Status = *desiredStatus
		if err := r.Status().Patch(ctx, latest, client.MergeFrom(base)); err != nil {
			logger.Error(err, "Failed to patch status")
			return err
		}
		return nil
	})
}

func (r *Sandbox0InfraReconciler) updateValidationFailureStatus(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, validationErr error) error {
	if infra == nil || validationErr == nil {
		return nil
	}

	desired := infra.DeepCopy()
	desired.Status.Phase = infrav1alpha1.PhaseDegraded
	desired.Status.LastMessage = validationErr.Error()
	r.setCondition(ctx, desired, infrav1alpha1.ConditionTypeReady, metav1.ConditionFalse, "SpecValidationFailed", validationErr.Error())

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &infrav1alpha1.Sandbox0Infra{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(infra), latest); err != nil {
			return err
		}
		if latest.Generation != infra.Generation {
			return nil
		}

		base := latest.DeepCopy()
		latest.Status.Phase = desired.Status.Phase
		latest.Status.LastMessage = desired.Status.LastMessage
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               infrav1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: infra.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             "SpecValidationFailed",
			Message:            validationErr.Error(),
		})
		return r.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

func (r *Sandbox0InfraReconciler) projectStatusForPlan(infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) {
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	r.pruneManagedConditions(infra, compiledPlan.Status.ExpectedConditions)
	r.projectEndpointsForPlan(infra, compiledPlan.Status.Endpoints)
	r.projectClusterForPlan(infra, compiledPlan.Status.Cluster)
}

func (r *Sandbox0InfraReconciler) pruneManagedConditions(infra *infrav1alpha1.Sandbox0Infra, expected []string) {
	if infra == nil || len(infra.Status.Conditions) == 0 {
		return
	}

	expectedSet := make(map[string]struct{}, len(expected)+1)
	expectedSet[infrav1alpha1.ConditionTypeReady] = struct{}{}
	for _, conditionType := range expected {
		expectedSet[conditionType] = struct{}{}
	}

	managed := managedConditionTypeSet()
	filtered := make([]metav1.Condition, 0, len(infra.Status.Conditions))
	for _, condition := range infra.Status.Conditions {
		if _, ok := managed[condition.Type]; ok {
			if _, keep := expectedSet[condition.Type]; !keep {
				continue
			}
		}
		filtered = append(filtered, condition)
	}
	infra.Status.Conditions = filtered
}

func (r *Sandbox0InfraReconciler) projectEndpointsForPlan(infra *infrav1alpha1.Sandbox0Infra, endpointsPlan infraplan.EndpointStatusPlan) {
	if infra == nil {
		return
	}

	if endpointsPlan.GlobalGateway == "" &&
		endpointsPlan.RegionalGateway == "" &&
		endpointsPlan.RegionalGatewayInternal == "" &&
		endpointsPlan.ClusterGateway == "" {
		infra.Status.Endpoints = nil
		return
	}

	if infra.Status.Endpoints == nil {
		infra.Status.Endpoints = &infrav1alpha1.EndpointsStatus{}
	}
	infra.Status.Endpoints.GlobalGateway = endpointsPlan.GlobalGateway
	infra.Status.Endpoints.RegionalGateway = endpointsPlan.RegionalGateway
	infra.Status.Endpoints.RegionalGatewayInternal = endpointsPlan.RegionalGatewayInternal
	infra.Status.Endpoints.ClusterGateway = endpointsPlan.ClusterGateway
}

func (r *Sandbox0InfraReconciler) projectClusterForPlan(infra *infrav1alpha1.Sandbox0Infra, clusterPlan infraplan.ClusterStatusPlan) {
	if infra == nil {
		return
	}

	if !clusterPlan.Present {
		infra.Status.Cluster = nil
		return
	}
	if infra.Status.Cluster == nil {
		infra.Status.Cluster = &infrav1alpha1.ClusterStatus{}
	}
	infra.Status.Cluster.ID = clusterPlan.ID
}

func managedConditionTypeSet() map[string]struct{} {
	return map[string]struct{}{
		infrav1alpha1.ConditionTypeReady:                {},
		infrav1alpha1.ConditionTypeInternalAuthReady:    {},
		infrav1alpha1.ConditionTypeDatabaseReady:        {},
		infrav1alpha1.ConditionTypeStorageReady:         {},
		infrav1alpha1.ConditionTypeRegistryReady:        {},
		infrav1alpha1.ConditionTypeGlobalGatewayReady:   {},
		infrav1alpha1.ConditionTypeRegionalGatewayReady: {},
		infrav1alpha1.ConditionTypeSchedulerReady:       {},
		infrav1alpha1.ConditionTypeClusterGatewayReady:  {},
		infrav1alpha1.ConditionTypeManagerReady:         {},
		infrav1alpha1.ConditionTypeStorageProxyReady:    {},
		infrav1alpha1.ConditionTypeNetdReady:            {},
		infrav1alpha1.ConditionTypeFusePluginReady:      {},
		infrav1alpha1.ConditionTypeInitUserReady:        {},
		infrav1alpha1.ConditionTypeClusterRegistered:    {},
		infrav1alpha1.ConditionTypeSecretsGenerated:     {},
	}
}

// setCondition sets or updates a condition
func (r *Sandbox0InfraReconciler) setCondition(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&infra.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: infra.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
}

// validateExternalDatabase validates connection to external database

// reconcileInitUser creates the initial admin user
func (r *Sandbox0InfraReconciler) reconcileInitUser(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	// This would typically call an API to create the user
	// For now, we just log that we would create the user
	logger := log.FromContext(ctx)
	logger.Info("Would create init user", "email", infra.Spec.InitUser.Email)
	return nil
}

func (r *Sandbox0InfraReconciler) ensureInitUserPasswordSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil || infra.Spec.InitUser == nil {
		return nil
	}

	secretRef := common.ResolveSecretKeyRef(infra.Spec.InitUser.PasswordSecret, "admin-password", "password")
	_, err := common.EnsureSecretValue(ctx, r.Client, r.Scheme, infra, secretRef.Name, secretRef.Key, initUserPasswordLength)
	return err
}

// registerCluster registers the cluster with the control plane
func (r *Sandbox0InfraReconciler) registerCluster(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra.Status.Cluster == nil {
		infra.Status.Cluster = &infrav1alpha1.ClusterStatus{}
	}
	if infra.Spec.Cluster != nil {
		infra.Status.Cluster.ID = infra.Spec.Cluster.ID
	}
	infra.Status.Cluster.Registered = false
	infra.Status.Cluster.RegisteredAt = nil

	if infra.Spec.ControlPlane == nil || infra.Spec.ControlPlane.URL == "" {
		return fmt.Errorf("controlPlane.url is required for cluster registration")
	}

	return fmt.Errorf("cluster registration is not implemented yet; refusing to report success without a real control-plane side effect")
}

func (r *Sandbox0InfraReconciler) waitBuiltinTemplatePodsReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) error {
	if infra == nil || len(infra.Spec.BuiltinTemplates) == 0 {
		return nil
	}
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	if !compiledPlan.Components.EnableManager || !compiledPlan.Manager.TemplateStoreEnabled {
		return nil
	}

	for _, builtin := range infra.Spec.BuiltinTemplates {
		templateID, err := naming.CanonicalTemplateID(builtin.TemplateID)
		if err != nil {
			return fmt.Errorf("invalid builtin template_id %q: %w", builtin.TemplateID, err)
		}

		minIdle, _ := template.ApplyDefaultPool(builtin.Pool.MinIdle, builtin.Pool.MaxIdle)
		if minIdle == 0 {
			continue
		}

		namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
		if err != nil {
			return fmt.Errorf("resolve namespace for builtin template %q: %w", templateID, err)
		}

		podList := &corev1.PodList{}
		if err := r.List(ctx, podList,
			client.InNamespace(namespace),
			client.MatchingLabels{templateIDPodLabelKey: templateID},
		); err != nil {
			return fmt.Errorf("list pods for builtin template %q: %w", templateID, err)
		}

		readyPods := int32(0)
		for i := range podList.Items {
			if isReadyPod(&podList.Items[i]) {
				readyPods++
			}
		}
		if readyPods < minIdle {
			return fmt.Errorf("builtin template %q pods not ready: %d/%d ready", templateID, readyPods, minIdle)
		}
	}
	return nil
}

func isReadyPod(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *Sandbox0InfraReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1alpha1.Sandbox0Infra{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&networkingv1.Ingress{}).
		WithOptions(controller.Options{
			// Keep retries responsive without hammering the API.
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](retryBaseDelay, retryMaxDelay),
		}).
		Complete(r)
}
