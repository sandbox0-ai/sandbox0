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
	"fmt"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/rbac"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/cilium"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/database"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/edgegateway"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/fuseplugin"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalauth"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/internalgateway"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/manager"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/scheduler"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/storage"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/services/storageproxy"
)

const (
	finalizerName   = "sandbox0infra.infra.sandbox0.ai/finalizer"
	requeueInterval = 30 * time.Second
	retryBaseDelay  = 2 * time.Second
	retryMaxDelay   = 2 * time.Minute
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
//+kubebuilder:rbac:groups=core,resources=services;secrets;configmaps;persistentvolumeclaims;serviceaccounts;pods;pods/exec;pods/resize;pods/status;nodes;events;namespaces,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cilium.io,resources=ciliumnetworkpolicies,verbs=get;list;watch;create;update;patch;delete
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
		if errors.IsNotFound(err) {
			logger.Info("Sandbox0Infra resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Sandbox0Infra")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !infra.ObjectMeta.DeletionTimestamp.IsZero() {
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

	// Main reconciliation logic based on configured components
	plan := r.buildComponentPlan(infra)
	result, err := r.reconcileComponentPlan(ctx, infra, plan)

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
	if infra.Spec.Version == "" {
		infra.Spec.Version = "latest"
	}
	if infra.Spec.Database != nil && infra.Spec.Database.Type == "" {
		infra.Spec.Database.Type = infrav1alpha1.DatabaseTypeBuiltin
	}
	if infra.Spec.Storage != nil && infra.Spec.Storage.Type == "" {
		infra.Spec.Storage.Type = infrav1alpha1.StorageTypeBuiltin
	}
}

// initializeStatus initializes the status for a new resource
func (r *Sandbox0InfraReconciler) initializeStatus(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Initializing status")

	now := metav1.Now()
	infra.Status.Phase = infrav1alpha1.PhaseInstalling
	infra.Status.ObservedVersion = infra.Spec.Version
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

type componentPlan struct {
	HasControlPlane           bool
	HasDataPlane              bool
	EnableEdgeGateway         bool
	EnableScheduler           bool
	EnableInternalGateway     bool
	EnableManager             bool
	EnableStorageProxy        bool
	EnableFusePlugin          bool
	EnableInternalAuth        bool
	EnableDatabase            bool
	EnableStorage             bool
	EnableCilium              bool
	EnableInitUser            bool
	EnableClusterRegistration bool
	RequireControlPlaneConfig bool
}

func (r *Sandbox0InfraReconciler) buildComponentPlan(infra *infrav1alpha1.Sandbox0Infra) componentPlan {
	enableEdgeGateway := infrav1alpha1.IsEdgeGatewayEnabled(infra)
	enableScheduler := infrav1alpha1.IsSchedulerEnabled(infra)
	enableInternalGateway := infrav1alpha1.IsInternalGatewayEnabled(infra)
	enableManager := infrav1alpha1.IsManagerEnabled(infra)
	enableStorageProxy := infrav1alpha1.IsStorageProxyEnabled(infra)

	hasControlPlane := enableEdgeGateway || enableScheduler
	hasDataPlane := enableInternalGateway || enableManager || enableStorageProxy

	return componentPlan{
		HasControlPlane:           hasControlPlane,
		HasDataPlane:              hasDataPlane,
		EnableEdgeGateway:         enableEdgeGateway,
		EnableScheduler:           enableScheduler,
		EnableInternalGateway:     enableInternalGateway,
		EnableManager:             enableManager,
		EnableStorageProxy:        enableStorageProxy,
		EnableFusePlugin:          enableStorageProxy,
		EnableInternalAuth:        hasControlPlane || hasDataPlane,
		EnableDatabase:            infrav1alpha1.IsDatabaseEnabled(infra),
		EnableStorage:             infrav1alpha1.IsStorageEnabled(infra),
		EnableCilium:              cilium.IsEnabled(infra),
		EnableInitUser:            hasControlPlane && infra.Spec.InitUser != nil && infra.Spec.InitUser.Enabled,
		EnableClusterRegistration: hasDataPlane && infra.Spec.Cluster != nil,
		RequireControlPlaneConfig: hasDataPlane && infra.Spec.ControlPlane != nil,
	}
}

func (r *Sandbox0InfraReconciler) validateComponentPlan(infra *infrav1alpha1.Sandbox0Infra, plan componentPlan) error {
	if plan.RequireControlPlaneConfig && infra.Spec.ControlPlane != nil &&
		infra.Spec.ControlPlane.InternalAuthPublicKeySecret.Name == "" {
		return fmt.Errorf("controlPlane.internalAuthPublicKeySecret.name is required when controlPlane are enabled")
	}
	if infra.Spec.InitUser != nil && infra.Spec.InitUser.Enabled && !plan.EnableDatabase {
		return fmt.Errorf("initUser can only be enabled when database is enabled")
	}
	if infra.Spec.Cluster != nil && !plan.HasDataPlane {
		return fmt.Errorf("cluster configuration requires at least one data-plane service")
	}
	return nil
}

// reconcileComponentPlan reconciles components based on spec configuration.
func (r *Sandbox0InfraReconciler) reconcileComponentPlan(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, plan componentPlan) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling components", "controlPlane", plan.HasControlPlane, "dataPlane", plan.HasDataPlane)

	if err := r.validateComponentPlan(infra, plan); err != nil {
		return ctrl.Result{}, err
	}

	resources := common.NewResourceManager(r.Client, r.Scheme, r.getImagePullPolicy(ctx), r.getLocalDevConfig(ctx))
	imageRepo := r.getImageRepo(ctx)
	authReconciler := internalauth.NewReconciler(resources)
	dbReconciler := database.NewReconciler(resources)
	storageReconciler := storage.NewReconciler(resources)
	edgeGatewayReconciler := edgegateway.NewReconciler(resources)
	schedulerReconciler := scheduler.NewReconciler(resources)
	internalGatewayReconciler := internalgateway.NewReconciler(resources)
	managerReconciler := manager.NewReconciler(resources)
	storageProxyReconciler := storageproxy.NewReconciler(resources)
	fusePluginReconciler := fuseplugin.NewReconciler(resources)
	ciliumReconciler := cilium.NewReconciler(resources)
	rbacReconciler := rbac.NewReconciler(resources)

	steps := []reconcileStep{}
	if plan.RequireControlPlaneConfig {
		steps = append(steps, reconcileStep{
			Name: "control-plane-config",
			Run: func(ctx context.Context) error {
				if infra.Spec.ControlPlane == nil {
					return fmt.Errorf("controlPlane configuration is required when data-plane services are enabled")
				}
				return nil
			},
			ConditionType:        infrav1alpha1.ConditionTypeInternalAuthReady,
			ErrorReason:          "MissingControlPlane",
			SkipSuccessCondition: true,
			ErrorResult:          &ctrl.Result{},
		})
		steps = append(steps, reconcileStep{
			Name: "control-plane-public-key",
			Run: func(ctx context.Context) error {
				publicKeySecret := &corev1.Secret{}
				return r.Get(ctx, types.NamespacedName{
					Name:      infra.Spec.ControlPlane.InternalAuthPublicKeySecret.Name,
					Namespace: infra.Namespace,
				}, publicKeySecret)
			},
			ConditionType:        infrav1alpha1.ConditionTypeInternalAuthReady,
			ErrorReason:          "PublicKeySecretNotFound",
			SkipSuccessCondition: true,
		})
	}
	if plan.EnableInternalAuth {
		steps = append(steps, reconcileStep{
			Name:           "internal-auth",
			Run:            func(ctx context.Context) error { return authReconciler.Reconcile(ctx, infra) },
			ConditionType:  infrav1alpha1.ConditionTypeInternalAuthReady,
			SuccessReason:  "KeysReady",
			SuccessMessage: "Internal auth keys are ready",
			ErrorReason:    "KeyGenerationFailed",
		})
	}
	if plan.EnableDatabase {
		steps = append(steps, reconcileStep{
			Name:           "database",
			Run:            func(ctx context.Context) error { return dbReconciler.Reconcile(ctx, infra) },
			ConditionType:  infrav1alpha1.ConditionTypeDatabaseReady,
			SuccessReason:  "DatabaseReady",
			SuccessMessage: "Database is ready",
			ErrorReason:    "DatabaseFailed",
		})
	}
	if plan.EnableStorage {
		steps = append(steps, reconcileStep{
			Name:           "storage",
			Run:            func(ctx context.Context) error { return storageReconciler.Reconcile(ctx, infra) },
			ConditionType:  infrav1alpha1.ConditionTypeStorageReady,
			SuccessReason:  "StorageReady",
			SuccessMessage: "Storage is ready",
			ErrorReason:    "StorageFailed",
		})
	}
	if plan.EnableEdgeGateway {
		steps = append(steps, reconcileStep{
			Name:           "edge-gateway",
			Run:            func(ctx context.Context) error { return edgeGatewayReconciler.Reconcile(ctx, infra, imageRepo) },
			ConditionType:  infrav1alpha1.ConditionTypeEdgeGatewayReady,
			SuccessReason:  "EdgeGatewayReady",
			SuccessMessage: "Edge gateway is ready",
			ErrorReason:    "EdgeGatewayFailed",
		})
	}
	if plan.EnableScheduler {
		steps = append(steps,
			reconcileStep{
				Name:                 "scheduler-rbac",
				Run:                  func(ctx context.Context) error { return rbacReconciler.ReconcileSchedulerRBAC(ctx, infra) },
				ConditionType:        infrav1alpha1.ConditionTypeSchedulerReady,
				ErrorReason:          "SchedulerRBACFailed",
				SkipSuccessCondition: true,
			},
			reconcileStep{
				Name:           "scheduler",
				Run:            func(ctx context.Context) error { return schedulerReconciler.Reconcile(ctx, infra, imageRepo) },
				ConditionType:  infrav1alpha1.ConditionTypeSchedulerReady,
				SuccessReason:  "SchedulerReady",
				SuccessMessage: "Scheduler is ready",
				ErrorReason:    "SchedulerFailed",
			},
		)
	}
	if plan.EnableInternalGateway {
		steps = append(steps, reconcileStep{
			Name:           "internal-gateway",
			Run:            func(ctx context.Context) error { return internalGatewayReconciler.Reconcile(ctx, infra, imageRepo) },
			ConditionType:  infrav1alpha1.ConditionTypeInternalGatewayReady,
			SuccessReason:  "InternalGatewayReady",
			SuccessMessage: "Internal gateway is ready",
			ErrorReason:    "InternalGatewayFailed",
		})
	}
	if plan.EnableCilium {
		steps = append(steps, reconcileStep{
			Name:                 "cilium-rbac",
			Run:                  func(ctx context.Context) error { return rbacReconciler.ReconcileCiliumInstallerRBAC(ctx, infra) },
			ConditionType:        infrav1alpha1.ConditionTypeCiliumReady,
			ErrorReason:          "CiliumRBACFailed",
			SkipSuccessCondition: true,
		})
		steps = append(steps, reconcileStep{
			Name:           "cilium",
			Run:            func(ctx context.Context) error { return ciliumReconciler.Reconcile(ctx, infra) },
			ConditionType:  infrav1alpha1.ConditionTypeCiliumReady,
			SuccessReason:  "CiliumReady",
			SuccessMessage: "Cilium is ready",
			ErrorReason:    "CiliumFailed",
		})
	}
	if plan.EnableFusePlugin {
		steps = append(steps, reconcileStep{
			Name:           "fuse-device-plugin",
			Run:            func(ctx context.Context) error { return fusePluginReconciler.Reconcile(ctx, infra, imageRepo) },
			ConditionType:  infrav1alpha1.ConditionTypeFusePluginReady,
			SuccessReason:  "FusePluginReady",
			SuccessMessage: "FUSE device plugin is ready",
			ErrorReason:    "FusePluginFailed",
		})
	}
	if plan.EnableManager {
		steps = append(steps,
			reconcileStep{
				Name:                 "manager-rbac",
				Run:                  func(ctx context.Context) error { return rbacReconciler.ReconcileManagerRBAC(ctx, infra) },
				ConditionType:        infrav1alpha1.ConditionTypeManagerReady,
				ErrorReason:          "ManagerRBACFailed",
				SkipSuccessCondition: true,
			},
			reconcileStep{
				Name:           "manager",
				Run:            func(ctx context.Context) error { return managerReconciler.Reconcile(ctx, infra, imageRepo) },
				ConditionType:  infrav1alpha1.ConditionTypeManagerReady,
				SuccessReason:  "ManagerReady",
				SuccessMessage: "Manager is ready",
				ErrorReason:    "ManagerFailed",
			},
		)
	}
	if plan.EnableStorageProxy {
		steps = append(steps,
			reconcileStep{
				Name:                 "storage-proxy-rbac",
				Run:                  func(ctx context.Context) error { return rbacReconciler.ReconcileStorageProxyRBAC(ctx, infra) },
				ConditionType:        infrav1alpha1.ConditionTypeStorageProxyReady,
				ErrorReason:          "StorageProxyRBACFailed",
				SkipSuccessCondition: true,
			},
			reconcileStep{
				Name:           "storage-proxy",
				Run:            func(ctx context.Context) error { return storageProxyReconciler.Reconcile(ctx, infra, imageRepo) },
				ConditionType:  infrav1alpha1.ConditionTypeStorageProxyReady,
				SuccessReason:  "StorageProxyReady",
				SuccessMessage: "Storage proxy is ready",
				ErrorReason:    "StorageProxyFailed",
			},
		)
	}
	if plan.EnableInitUser {
		steps = append(steps, reconcileStep{
			Name:           "init-user",
			Run:            func(ctx context.Context) error { return r.reconcileInitUser(ctx, infra) },
			ConditionType:  infrav1alpha1.ConditionTypeInitUserReady,
			SuccessReason:  "InitUserReady",
			SuccessMessage: "Initial admin user created",
			ErrorReason:    "InitUserFailed",
		})
	}
	if plan.EnableClusterRegistration {
		steps = append(steps, reconcileStep{
			Name:           "register-cluster",
			Run:            func(ctx context.Context) error { return r.registerCluster(ctx, infra) },
			ConditionType:  infrav1alpha1.ConditionTypeClusterRegistered,
			SuccessReason:  "ClusterRegistered",
			SuccessMessage: "Cluster registration completed",
			ErrorReason:    "ClusterRegistrationFailed",
		})
	}

	return r.runSteps(ctx, infra, steps)
}

// updateOverallStatus updates the overall status based on conditions
func (r *Sandbox0InfraReconciler) updateOverallStatus(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	original := infra.Status.DeepCopy()

	expectedConditions := r.expectedConditionTypes(infra)
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

	// Update status
	if reflect.DeepEqual(original, &infra.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, infra); err != nil {
		logger.Error(err, "Failed to update status")
		return err
	}

	return nil
}

func (r *Sandbox0InfraReconciler) expectedConditionTypes(infra *infrav1alpha1.Sandbox0Infra) []string {
	plan := r.buildComponentPlan(infra)
	conditions := []string{}
	if plan.EnableInternalAuth {
		conditions = append(conditions, infrav1alpha1.ConditionTypeInternalAuthReady)
	}
	if plan.EnableDatabase {
		conditions = append(conditions, infrav1alpha1.ConditionTypeDatabaseReady)
	}
	if plan.EnableStorage {
		conditions = append(conditions, infrav1alpha1.ConditionTypeStorageReady)
	}
	if plan.EnableEdgeGateway {
		conditions = append(conditions, infrav1alpha1.ConditionTypeEdgeGatewayReady)
	}
	if plan.EnableScheduler {
		conditions = append(conditions, infrav1alpha1.ConditionTypeSchedulerReady)
	}
	if plan.EnableInternalGateway {
		conditions = append(conditions, infrav1alpha1.ConditionTypeInternalGatewayReady)
	}
	if plan.EnableManager {
		conditions = append(conditions, infrav1alpha1.ConditionTypeManagerReady)
	}
	if plan.EnableStorageProxy {
		conditions = append(conditions, infrav1alpha1.ConditionTypeStorageProxyReady)
	}
	if plan.EnableFusePlugin {
		conditions = append(conditions, infrav1alpha1.ConditionTypeFusePluginReady)
	}
	if plan.EnableCilium {
		conditions = append(conditions, infrav1alpha1.ConditionTypeCiliumReady)
	}
	if plan.EnableInitUser {
		conditions = append(conditions, infrav1alpha1.ConditionTypeInitUserReady)
	}
	if plan.EnableClusterRegistration {
		conditions = append(conditions, infrav1alpha1.ConditionTypeClusterRegistered)
	}
	return conditions
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

// registerCluster registers the cluster with the control plane
func (r *Sandbox0InfraReconciler) registerCluster(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)

	if infra.Status.Cluster == nil {
		infra.Status.Cluster = &infrav1alpha1.ClusterStatus{}
	}

	if infra.Status.Cluster.Registered {
		return nil
	}

	// TODO: Implement actual registration with control plane
	logger.Info("Would register cluster with control plane",
		"clusterId", infra.Spec.Cluster.ID,
		"controlPlaneUrl", infra.Spec.ControlPlane.URL)

	now := metav1.Now()
	infra.Status.Cluster.ID = infra.Spec.Cluster.ID
	infra.Status.Cluster.Registered = true
	infra.Status.Cluster.RegisteredAt = &now

	return nil
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
