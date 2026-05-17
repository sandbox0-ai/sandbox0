package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	registrysvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/registry"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Sandbox0Infra transition semantics", func() {
	var (
		ctx        context.Context
		reconciler *Sandbox0InfraReconciler
	)

	BeforeEach(func() {
		ctx = context.Background()
		reconciler = &Sandbox0InfraReconciler{
			Client: k8sClient,
			Scheme: scheme.Scheme,
		}
	})

	It("cleans up disabled builtin database runtime while retaining stateful resources in status", func() {
		infra := newTransitionInfra("database-retain", &infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled:                false,
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedStatefulSet("database-retain-postgres", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("database-retain-postgres", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("database-retain-sandbox0-database-credentials", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("database-retain-postgres-data", infra.Namespace))).To(Succeed())

		_, err := reconciler.reconcileComponentPlan(ctx, infra, infraplan.Compile(infra))
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "database-retain-postgres", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "database-retain-postgres", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "database-retain-sandbox0-database-credentials", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "database-retain-postgres-data", Namespace: infra.Namespace}})).To(BeTrue())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(ConsistOf(
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("database"),
				"Kind":      Equal("Secret"),
				"Name":      Equal("database-retain-sandbox0-database-credentials"),
			}),
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("database"),
				"Kind":      Equal("PersistentVolumeClaim"),
				"Name":      Equal("database-retain-postgres-data"),
			}),
		))
	})

	It("deletes builtin database stateful resources when disabled with delete policy", func() {
		infra := newTransitionInfra("database-delete", &infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled:                false,
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyDelete,
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedStatefulSet("database-delete-postgres", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("database-delete-postgres", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("database-delete-sandbox0-database-credentials", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("database-delete-postgres-data", infra.Namespace))).To(Succeed())

		_, err := reconciler.reconcileComponentPlan(ctx, infra, infraplan.Compile(infra))
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "database-delete-postgres", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "database-delete-postgres", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "database-delete-sandbox0-database-credentials", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "database-delete-postgres-data", Namespace: infra.Namespace}})).To(BeFalse())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(BeEmpty())
	})

	It("cleans up builtin database runtime during external transition without deleting external secrets", func() {
		infra := newTransitionInfra("database-external", &infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
					PasswordSecret: infrav1alpha1.SecretKeyRef{
						Name: "external-db-password",
						Key:  "password",
					},
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedStatefulSet("database-external-postgres", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("database-external-postgres", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("database-external-sandbox0-database-credentials", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("database-external-postgres-data", infra.Namespace))).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "external-db-password", Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{"password": "external-password"},
		})).To(Succeed())

		databaseReconciler := database.NewReconciler(common.NewResourceManager(k8sClient, scheme.Scheme, nil, common.LocalDevConfig{}))
		Expect(databaseReconciler.Reconcile(ctx, infra)).To(Succeed())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "database-external-postgres", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "database-external-postgres", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "database-external-sandbox0-database-credentials", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "database-external-postgres-data", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "external-db-password", Namespace: infra.Namespace}})).To(BeTrue())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(ConsistOf(
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("database"),
				"Kind":      Equal("Secret"),
				"Name":      Equal("database-external-sandbox0-database-credentials"),
			}),
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("database"),
				"Kind":      Equal("PersistentVolumeClaim"),
				"Name":      Equal("database-external-postgres-data"),
			}),
		))
	})

	It("cleans up disabled builtin storage runtime while retaining stateful resources in status", func() {
		infra := newTransitionInfra("storage-retain", &infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Enabled:                false,
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedStatefulSet("storage-retain-rustfs", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("storage-retain-rustfs", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("storage-retain-sandbox0-rustfs-credentials", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("storage-retain-rustfs-data", infra.Namespace))).To(Succeed())

		storageReconciler := storage.NewReconciler(common.NewResourceManager(k8sClient, scheme.Scheme, nil, common.LocalDevConfig{}))
		Expect(reconciler.cleanupDisabledServiceResources(ctx, infra, infraplan.Compile(infra).Cleanup, nil, nil, storageReconciler, nil)).To(Succeed())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "storage-retain-rustfs", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "storage-retain-rustfs", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "storage-retain-sandbox0-rustfs-credentials", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "storage-retain-rustfs-data", Namespace: infra.Namespace}})).To(BeTrue())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(ConsistOf(
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("storage"),
				"Kind":      Equal("Secret"),
				"Name":      Equal("storage-retain-sandbox0-rustfs-credentials"),
			}),
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("storage"),
				"Kind":      Equal("PersistentVolumeClaim"),
				"Name":      Equal("storage-retain-rustfs-data"),
			}),
		))
	})

	It("deletes builtin storage stateful resources when disabled with delete policy", func() {
		infra := newTransitionInfra("storage-delete", &infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Enabled:                false,
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyDelete,
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedStatefulSet("storage-delete-rustfs", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("storage-delete-rustfs", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("storage-delete-sandbox0-rustfs-credentials", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("storage-delete-rustfs-data", infra.Namespace))).To(Succeed())

		storageReconciler := storage.NewReconciler(common.NewResourceManager(k8sClient, scheme.Scheme, nil, common.LocalDevConfig{}))
		Expect(reconciler.cleanupDisabledServiceResources(ctx, infra, infraplan.Compile(infra).Cleanup, nil, nil, storageReconciler, nil)).To(Succeed())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "storage-delete-rustfs", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "storage-delete-rustfs", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "storage-delete-sandbox0-rustfs-credentials", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "storage-delete-rustfs-data", Namespace: infra.Namespace}})).To(BeFalse())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(BeEmpty())
	})

	It("validates external storage config and cleans up builtin runtime without deleting external secrets", func() {
		infra := newTransitionInfra("storage-external", &infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeS3,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				S3: &infrav1alpha1.S3StorageConfig{
					Endpoint: "https://s3.example.com",
					Bucket:   "sandbox0",
					Region:   "us-east-1",
					CredentialsSecret: infrav1alpha1.S3CredentialsSecret{
						Name: "external-s3-credentials",
					},
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedStatefulSet("storage-external-rustfs", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("storage-external-rustfs", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("storage-external-sandbox0-rustfs-credentials", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("storage-external-rustfs-data", infra.Namespace))).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "external-s3-credentials", Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{"accessKeyId": "ak", "secretAccessKey": "sk"},
		})).To(Succeed())

		storageReconciler := storage.NewReconciler(common.NewResourceManager(k8sClient, scheme.Scheme, nil, common.LocalDevConfig{}))
		Expect(storage.ValidateExternalStorage(ctx, k8sClient, infra)).To(Succeed())
		Expect(storageReconciler.CleanupBuiltinResources(ctx, infra)).To(Succeed())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "storage-external-rustfs", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "storage-external-rustfs", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "storage-external-sandbox0-rustfs-credentials", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "storage-external-rustfs-data", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "external-s3-credentials", Namespace: infra.Namespace}})).To(BeTrue())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(ConsistOf(
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("storage"),
				"Kind":      Equal("Secret"),
				"Name":      Equal("storage-external-sandbox0-rustfs-credentials"),
			}),
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("storage"),
				"Kind":      Equal("PersistentVolumeClaim"),
				"Name":      Equal("storage-external-rustfs-data"),
			}),
		))
	})

	It("cleans up disabled builtin registry runtime while retaining the builtin registry PVC", func() {
		infra := newTransitionInfra("registry-retain", &infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Enabled:                false,
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedDeployment("registry-retain-registry", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("registry-retain-registry", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry", Namespace: infra.Namespace}})).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("registry-retain-registry-auth", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("registry-retain-registry-pull", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("registry-retain-registry-data", infra.Namespace))).To(Succeed())

		registryReconciler := registrysvc.NewReconciler(common.NewResourceManager(k8sClient, scheme.Scheme, nil, common.LocalDevConfig{}))
		Expect(registryReconciler.Reconcile(ctx, infra)).To(Succeed())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry-auth", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry-pull", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "registry-retain-registry-data", Namespace: infra.Namespace}})).To(BeTrue())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(ConsistOf(
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("registry"),
				"Kind":      Equal("PersistentVolumeClaim"),
				"Name":      Equal("registry-retain-registry-data"),
			}),
		))
	})

	It("cleans up builtin registry runtime during external transition without deleting external secrets", func() {
		infra := newTransitionInfra("registry-external", &infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderHarbor,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				Harbor: &infrav1alpha1.HarborRegistryConfig{
					Registry:   "harbor.example.com",
					PullSecret: infrav1alpha1.DockerConfigSecretRef{Name: "harbor-pull"},
					CredentialsSecret: infrav1alpha1.HarborRegistryCredentialsSecret{
						Name: "harbor-credentials",
					},
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		Expect(createOwnedObject(ctx, infra, newOwnedDeployment("registry-external-registry", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedService("registry-external-registry", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry", Namespace: infra.Namespace}})).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("registry-external-registry-auth", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedOpaqueSecret("registry-external-registry-pull", infra.Namespace))).To(Succeed())
		Expect(createOwnedObject(ctx, infra, newOwnedPVC("registry-external-registry-data", infra.Namespace))).To(Succeed())
		Expect(k8sClient.Create(ctx, dockerConfigSecret("harbor-pull", infra.Namespace))).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "harbor-credentials", Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{"username": "harbor-user", "password": "harbor-password"},
		})).To(Succeed())

		registryReconciler := registrysvc.NewReconciler(common.NewResourceManager(k8sClient, scheme.Scheme, nil, common.LocalDevConfig{}))
		Expect(registryReconciler.Reconcile(ctx, infra)).To(Succeed())
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		Expect(objectExists(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry-auth", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry-pull", Namespace: infra.Namespace}})).To(BeFalse())
		Expect(objectExists(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "registry-external-registry-data", Namespace: infra.Namespace}})).To(BeTrue())
		Expect(objectExists(ctx, dockerConfigSecret("harbor-pull", infra.Namespace))).To(BeTrue())
		Expect(objectExists(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "harbor-credentials", Namespace: infra.Namespace}})).To(BeTrue())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.RetainedResources).To(ConsistOf(
			MatchFields(IgnoreExtras, Fields{
				"Component": Equal("registry"),
				"Kind":      Equal("PersistentVolumeClaim"),
				"Name":      Equal("registry-external-registry-data"),
			}),
		))
	})

	It("persists failed cluster registration status without reporting success", func() {
		infra := newTransitionInfra("cluster-registration", &infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{
				URL: "https://control-plane.example.com",
				InternalAuthPublicKeySecret: infrav1alpha1.SecretKeyRef{
					Name: "control-plane-public-key",
					Key:  "public.key",
				},
			},
			Cluster: &infrav1alpha1.ClusterConfig{
				ID: "cluster-a",
			},
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		})
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())

		result, err := reconciler.runSteps(ctx, infra, []reconcileStep{{
			Name:          "register-cluster",
			Run:           func(ctx context.Context) error { return reconciler.registerCluster(ctx, infra) },
			ConditionType: infrav1alpha1.ConditionTypeClusterRegistered,
			ErrorReason:   "ClusterRegistrationFailed",
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(requeueInterval))
		Expect(reconciler.updateOverallStatus(ctx, infra)).To(Succeed())

		stored := &infrav1alpha1.Sandbox0Infra{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(infra), stored)).To(Succeed())
		Expect(stored.Status.Cluster).NotTo(BeNil())
		Expect(stored.Status.Cluster.ID).To(Equal("cluster-a"))
		Expect(stored.Status.Cluster.Registered).To(BeFalse())
		Expect(stored.Status.Cluster.RegisteredAt).To(BeNil())

		condition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeClusterRegistered)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("ClusterRegistrationFailed"))
		Expect(condition.Message).To(ContainSubstring("not implemented"))
	})
})

func newTransitionInfra(name string, spec *infrav1alpha1.Sandbox0InfraSpec) *infrav1alpha1.Sandbox0Infra {
	namespace := fmt.Sprintf("transition-%d", time.Now().UnixNano())
	Expect(k8sClient.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	})).To(Succeed())

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if spec != nil {
		infra.Spec = *spec
	}
	return infra
}

func createOwnedObject(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, obj client.Object) error {
	if err := controllerutil.SetControllerReference(infra, obj, scheme.Scheme); err != nil {
		return err
	}
	return k8sClient.Create(ctx, obj)
}

func objectExists(ctx context.Context, obj client.Object) bool {
	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	return err == nil
}

func dockerConfigSecret(name, namespace string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: []byte(`{"auths":{"harbor.example.com":{"auth":"Zm9vOmJhcg=="}}}`),
		},
	}
}

func newOwnedDeployment(name, namespace string) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "test",
						Image: "busybox:1.36",
						Command: []string{
							"sh", "-c", "sleep 3600",
						},
					}},
				},
			},
		},
	}
}

func newOwnedStatefulSet(name, namespace string) *appsv1.StatefulSet {
	labels := map[string]string{"app": name}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "test",
						Image: "busybox:1.36",
						Command: []string{
							"sh", "-c", "sleep 3600",
						},
					}},
				},
			},
		},
	}
}

func newOwnedService(name, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt(80),
			}},
		},
	}
}

func newOwnedPVC(name, namespace string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

func newOwnedOpaqueSecret(name, namespace string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{"value": "test"},
	}
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
