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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	config "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

// DatabaseType defines the type of database
// +kubebuilder:validation:Enum=builtin;external
type DatabaseType string

const (
	DatabaseTypeBuiltin  DatabaseType = "builtin"
	DatabaseTypeExternal DatabaseType = "external"
)

// StorageType defines the type of storage backend
// +kubebuilder:validation:Enum=builtin;s3;oss
type StorageType string

const (
	StorageTypeBuiltin StorageType = "builtin"
	StorageTypeS3      StorageType = "s3"
	StorageTypeOSS     StorageType = "oss"
)

// RegistryProvider defines the registry provider type.
// +kubebuilder:validation:Enum=builtin;aws;gcp;azure;aliyun;harbor
type RegistryProvider string

const (
	RegistryProviderBuiltin RegistryProvider = "builtin"
	RegistryProviderAWS     RegistryProvider = "aws"
	RegistryProviderGCP     RegistryProvider = "gcp"
	RegistryProviderAzure   RegistryProvider = "azure"
	RegistryProviderAliyun  RegistryProvider = "aliyun"
	RegistryProviderHarbor  RegistryProvider = "harbor"
)

// Phase represents the current phase of the Sandbox0Infra
// +kubebuilder:validation:Enum=Installing;Ready;Degraded;Failed;Upgrading
type Phase string

const (
	PhaseInstalling Phase = "Installing"
	PhaseReady      Phase = "Ready"
	PhaseDegraded   Phase = "Degraded"
	PhaseFailed     Phase = "Failed"
	PhaseUpgrading  Phase = "Upgrading"
)

// Sandbox0InfraSpec defines the desired state of Sandbox0Infra
type Sandbox0InfraSpec struct {
	// Database configures the main database for sandbox0
	// +optional
	Database *DatabaseConfig `json:"database,omitempty"`

	// JuicefsDatabase configures the JuiceFS metadata database
	// +optional
	JuicefsDatabase *JuicefsDatabaseConfig `json:"juicefsDatabase,omitempty"`

	// Storage configures the storage backend (JuiceFS S3 backend)
	// +optional
	Storage *StorageConfig `json:"storage,omitempty"`

	// Registry configures the container registry
	// +optional
	Registry *RegistryConfig `json:"registry,omitempty"`

	// ControlPlane configures external control plane connection.
	// +optional
	ControlPlane *ControlPlaneConfig `json:"controlPlane,omitempty"`

	// InternalAuth configures internal authentication keys
	// +optional
	InternalAuth *InternalAuthConfig `json:"internalAuth,omitempty"`

	// Services configures individual services
	// +optional
	// +kubebuilder:default={}
	Services *ServicesConfig `json:"services,omitempty"`

	// SandboxNodePlacement configures the shared node placement used by
	// sandbox workloads and node-local sandbox services.
	// +optional
	SandboxNodePlacement *SandboxNodePlacementConfig `json:"sandboxNodePlacement,omitempty"`

	// Region identifies the region for multi-cluster deployments
	// +optional
	Region string `json:"region,omitempty"`

	// PublicExposure configures public URL exposure for sandboxes
	// +optional
	PublicExposure *PublicExposureConfig `json:"publicExposure,omitempty"`

	// Cluster configures cluster identification and capacity
	// +optional
	Cluster *ClusterConfig `json:"cluster,omitempty"`

	// InitUser configures the initial admin user
	// +optional
	InitUser *InitUserConfig `json:"initUser,omitempty"`

	// BuiltinTemplates defines system builtin templates to seed the template store
	// +optional
	// +kubebuilder:default={}
	BuiltinTemplates []BuiltinTemplateConfig `json:"builtinTemplates,omitempty"`
}

// SandboxNodePlacementConfig defines shared scheduling constraints for sandbox
// workloads and node-local sandbox services.
type SandboxNodePlacementConfig struct {
	// NodeSelector constrains sandbox workloads and node-local sandbox services
	// onto a specific node set.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow sandbox workloads and node-local sandbox services to run
	// on tainted sandbox nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// DatabaseConfig defines database configuration
type DatabaseConfig struct {
	// Type specifies the postgres database type: builtin, or external
	// +kubebuilder:default=builtin
	Type DatabaseType `json:"type,omitempty"`

	// Builtin configures the built-in single-node PostgreSQL
	// +optional
	// +kubebuilder:default={}
	Builtin *BuiltinDatabaseConfig `json:"builtin,omitempty"`

	// External configures connection to external database
	// +optional
	External *ExternalDatabaseConfig `json:"external,omitempty"`
}

// BuiltinDatabaseConfig defines built-in database configuration
type BuiltinDatabaseConfig struct {
	// Enabled enables the built-in database
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// Image specifies the postgres image for the builtin database
	// +kubebuilder:default="postgres:16-alpine"
	// +optional
	Image string `json:"image,omitempty"`

	// Port specifies the database port
	// +kubebuilder:default=5432
	// +optional
	Port int32 `json:"port,omitempty"`

	// Username specifies the database username
	// +kubebuilder:default="sandbox0"
	// +optional
	Username string `json:"username,omitempty"`

	// Database specifies the database name
	// +kubebuilder:default="sandbox0"
	// +optional
	Database string `json:"database,omitempty"`

	// SSLMode specifies the SSL mode for builtin DSN
	// +kubebuilder:default="disable"
	// +optional
	SSLMode string `json:"sslMode,omitempty"`

	// Persistence configures database storage
	// +optional
	Persistence *PersistenceConfig `json:"persistence,omitempty"`
}

// PersistenceConfig defines persistence configuration
type PersistenceConfig struct {
	// Enabled enables persistence
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// Size specifies the storage size
	// +kubebuilder:default="20Gi"
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClass specifies the storage class (empty for default)
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
}

// ExternalDatabaseConfig defines external database configuration
type ExternalDatabaseConfig struct {
	// Host specifies the database host
	Host string `json:"host"`

	// Port specifies the database port
	// +kubebuilder:default=5432
	Port int32 `json:"port,omitempty"`

	// Database specifies the database name
	Database string `json:"database"`

	// Username specifies the database username
	Username string `json:"username"`

	// PasswordSecret references the secret containing the password
	PasswordSecret SecretKeyRef `json:"passwordSecret"`

	// SSLMode specifies the SSL mode for connection
	// +kubebuilder:default="require"
	// +optional
	SSLMode string `json:"sslMode,omitempty"`
}

// SecretKeyRef references a key in a secret
type SecretKeyRef struct {
	// Name is the name of the secret
	// +optional
	Name string `json:"name"`

	// Key is the key in the secret
	// +kubebuilder:default="password"
	Key string `json:"key,omitempty"`
}

// JuicefsDatabaseConfig defines JuiceFS metadata database configuration
type JuicefsDatabaseConfig struct {
	// ShareWithMain uses the main database for JuiceFS metadata
	// +kubebuilder:default=true
	ShareWithMain bool `json:"shareWithMain,omitempty"`

	// External configures an independent database for JuiceFS
	// +optional
	External *ExternalDatabaseConfig `json:"external,omitempty"`
}

// StorageConfig defines storage backend configuration
type StorageConfig struct {
	// Type specifies the storage type: builtin, s3, or oss
	// +kubebuilder:default=builtin
	Type StorageType `json:"type,omitempty"`

	// Builtin configures the built-in RustFS storage
	// +optional
	// +kubebuilder:default={}
	Builtin *BuiltinStorageConfig `json:"builtin,omitempty"`

	// S3 configures S3 or S3-compatible storage
	// +optional
	S3 *S3StorageConfig `json:"s3,omitempty"`

	// OSS configures Aliyun OSS storage
	// +optional
	OSS *OSSStorageConfig `json:"oss,omitempty"`
}

// BuiltinStorageConfig defines built-in storage configuration
type BuiltinStorageConfig struct {
	// Enabled enables the built-in storage
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// Image specifies the RustFS image for builtin storage
	// +kubebuilder:default="rustfs/rustfs:1.0.0-alpha.79"
	// +optional
	Image string `json:"image,omitempty"`

	// Port specifies the RustFS API port
	// +kubebuilder:default=9000
	// +optional
	Port int32 `json:"port,omitempty"`

	// ConsolePort specifies the RustFS console port
	// +kubebuilder:default=9001
	// +optional
	ConsolePort int32 `json:"consolePort,omitempty"`

	// Bucket specifies the default bucket name for builtin storage
	// +kubebuilder:default="sandbox0"
	// +optional
	Bucket string `json:"bucket,omitempty"`

	// Region specifies the default region for builtin storage
	// +kubebuilder:default="us-east-1"
	// +optional
	Region string `json:"region,omitempty"`

	// ConsoleEnabled enables the RustFS console
	// +kubebuilder:default=true
	// +optional
	ConsoleEnabled bool `json:"consoleEnabled,omitempty"`

	// Volumes specifies the RustFS data path
	// +kubebuilder:default="/data"
	// +optional
	Volumes string `json:"volumes,omitempty"`

	// ObsLogDirectory specifies the RustFS log directory
	// +kubebuilder:default="/data/logs"
	// +optional
	ObsLogDirectory string `json:"obsLogDirectory,omitempty"`

	// ObsLoggerLevel specifies the RustFS log level
	// +kubebuilder:default="debug"
	// +optional
	ObsLoggerLevel string `json:"obsLoggerLevel,omitempty"`

	// ObsEnvironment specifies the RustFS environment label
	// +kubebuilder:default="develop"
	// +optional
	ObsEnvironment string `json:"obsEnvironment,omitempty"`

	// Persistence configures storage persistence
	// +optional
	Persistence *PersistenceConfig `json:"persistence,omitempty"`

	// Credentials configures access credentials (auto-generated if not specified)
	// +optional
	Credentials *StorageCredentials `json:"credentials,omitempty"`
}

// StorageCredentials defines storage access credentials
type StorageCredentials struct {
	// AccessKey is the access key
	// +optional
	AccessKey string `json:"accessKey,omitempty"`

	// SecretKey is the secret key
	// +optional
	SecretKey string `json:"secretKey,omitempty"`
}

// S3StorageConfig defines S3 storage configuration
type S3StorageConfig struct {
	// Bucket specifies the S3 bucket name
	Bucket string `json:"bucket"`

	// Region specifies the AWS region
	Region string `json:"region"`

	// Endpoint specifies the S3 endpoint (optional for AWS)
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// CredentialsSecret references the secret containing AWS credentials
	CredentialsSecret S3CredentialsSecret `json:"credentialsSecret"`

	// SessionTokenKey is the key for session token in the secret (optional)
	// +optional
	SessionTokenKey string `json:"sessionTokenKey,omitempty"`
}

// S3CredentialsSecret references S3 credentials in a secret
type S3CredentialsSecret struct {
	// Name is the name of the secret
	Name string `json:"name"`

	// AccessKeyKey is the key for access key ID
	// +kubebuilder:default="accessKeyId"
	AccessKeyKey string `json:"accessKeyKey,omitempty"`

	// SecretKeyKey is the key for secret access key
	// +kubebuilder:default="secretAccessKey"
	SecretKeyKey string `json:"secretKeyKey,omitempty"`
}

// OSSStorageConfig defines Aliyun OSS storage configuration
type OSSStorageConfig struct {
	// Bucket specifies the OSS bucket name
	Bucket string `json:"bucket"`

	// Region specifies the Aliyun region
	Region string `json:"region"`

	// Endpoint specifies the OSS endpoint
	Endpoint string `json:"endpoint"`

	// CredentialsSecret references the secret containing Aliyun credentials
	CredentialsSecret OSSCredentialsSecret `json:"credentialsSecret"`
}

// OSSCredentialsSecret references OSS credentials in a secret
type OSSCredentialsSecret struct {
	// Name is the name of the secret
	Name string `json:"name"`

	// AccessKeyKey is the key for access key ID
	// +kubebuilder:default="accessKeyId"
	AccessKeyKey string `json:"accessKeyKey,omitempty"`

	// SecretKeyKey is the key for access key secret
	// +kubebuilder:default="accessKeySecret"
	SecretKeyKey string `json:"secretKeyKey,omitempty"`
}

// RegistryConfig defines container registry configuration.
type RegistryConfig struct {
	// Provider specifies the registry provider: builtin, aws, gcp, azure, aliyun, or harbor.
	// +kubebuilder:default=builtin
	// +kubebuilder:validation:Enum=builtin;aws;gcp;azure;aliyun;harbor
	Provider RegistryProvider `json:"provider,omitempty"`

	// ImagePullSecretName is the secret name to create in template namespaces.
	// +kubebuilder:default="sandbox0-registry-pull"
	// +optional
	ImagePullSecretName string `json:"imagePullSecretName,omitempty"`

	// Builtin configures the built-in registry.
	// +optional
	Builtin *BuiltinRegistryConfig `json:"builtin,omitempty"`

	// AWS configures AWS registry integration.
	// +optional
	AWS *AWSRegistryConfig `json:"aws,omitempty"`

	// GCP configures GCP registry integration.
	// +optional
	GCP *GCPRegistryConfig `json:"gcp,omitempty"`

	// Azure configures Azure registry integration.
	// +optional
	Azure *AzureRegistryConfig `json:"azure,omitempty"`

	// Aliyun configures Aliyun registry integration.
	// +optional
	Aliyun *AliyunRegistryConfig `json:"aliyun,omitempty"`

	// Harbor configures Harbor registry integration.
	// +optional
	Harbor *HarborRegistryConfig `json:"harbor,omitempty"`
}

// BuiltinRegistryConfig defines built-in registry configuration.
type BuiltinRegistryConfig struct {
	// Enabled enables the built-in registry.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// Image specifies the registry image.
	// +kubebuilder:default="registry:2.8.3"
	// +optional
	Image string `json:"image,omitempty"`

	// Port specifies the registry port.
	// +kubebuilder:default=5000
	// +optional
	Port int32 `json:"port,omitempty"`

	// Persistence configures registry persistence.
	// +optional
	Persistence *PersistenceConfig `json:"persistence,omitempty"`

	// Service configures the registry service exposure.
	// +optional
	Service *ServiceNetworkConfig `json:"service,omitempty"`

	// PushEndpoint overrides the external registry endpoint used for image push credentials.
	// Use host[:port] format, without scheme.
	// +optional
	PushEndpoint string `json:"pushEndpoint,omitempty"`

	// Ingress configures ingress settings for external registry access.
	// +optional
	Ingress *IngressConfig `json:"ingress,omitempty"`

	// CredentialsSecret references the secret containing registry credentials.
	// If omitted, the operator will generate a secret named "<infra-name>-registry-credentials".
	// +optional
	CredentialsSecret *RegistryCredentialsSecret `json:"credentialsSecret,omitempty"`
}

// RegistryCredentialsSecret references registry credentials in a secret.
type RegistryCredentialsSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// UsernameKey is the key for username.
	// +kubebuilder:default="username"
	UsernameKey string `json:"usernameKey,omitempty"`

	// PasswordKey is the key for password.
	// +kubebuilder:default="password"
	PasswordKey string `json:"passwordKey,omitempty"`
}

// DockerConfigSecretRef references a dockerconfigjson secret.
type DockerConfigSecretRef struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// Key is the key in the secret.
	// +kubebuilder:default=".dockerconfigjson"
	Key string `json:"key,omitempty"`
}

// AWSRegistryConfig defines AWS registry configuration.
type AWSRegistryConfig struct {
	// Registry specifies the registry hostname.
	// +optional
	Registry string `json:"registry,omitempty"`

	// Region specifies the AWS region.
	Region string `json:"region"`

	// RegistryID specifies the AWS account ID (optional).
	// +optional
	RegistryID string `json:"registryId,omitempty"`

	// PullSecret references the dockerconfigjson secret to use for image pulls.
	PullSecret DockerConfigSecretRef `json:"pullSecret"`

	// CredentialsSecret references AWS credentials for short-lived tokens.
	CredentialsSecret AWSRegistryCredentialsSecret `json:"credentialsSecret"`
}

// AWSRegistryCredentialsSecret references AWS credentials in a secret.
type AWSRegistryCredentialsSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// AccessKeyKey is the key for access key ID.
	// +kubebuilder:default="accessKeyId"
	AccessKeyKey string `json:"accessKeyKey,omitempty"`

	// SecretKeyKey is the key for secret access key.
	// +kubebuilder:default="secretAccessKey"
	SecretKeyKey string `json:"secretKeyKey,omitempty"`

	// SessionTokenKey is the key for session token (optional).
	// +optional
	SessionTokenKey string `json:"sessionTokenKey,omitempty"`
}

// GCPRegistryConfig defines GCP registry configuration.
type GCPRegistryConfig struct {
	// Registry specifies the registry hostname.
	Registry string `json:"registry"`

	// PullSecret references the dockerconfigjson secret to use for image pulls.
	PullSecret DockerConfigSecretRef `json:"pullSecret"`

	// ServiceAccountSecret references the service account JSON key.
	ServiceAccountSecret GCPRegistryServiceAccountSecret `json:"serviceAccountSecret"`
}

// GCPRegistryServiceAccountSecret references a service account key in a secret.
type GCPRegistryServiceAccountSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// Key is the key in the secret.
	// +kubebuilder:default="serviceAccount.json"
	Key string `json:"key,omitempty"`
}

// AzureRegistryConfig defines Azure registry configuration.
type AzureRegistryConfig struct {
	// Registry specifies the registry hostname.
	Registry string `json:"registry"`

	// PullSecret references the dockerconfigjson secret to use for image pulls.
	PullSecret DockerConfigSecretRef `json:"pullSecret"`

	// CredentialsSecret references the client credentials for ACR.
	CredentialsSecret AzureRegistryCredentialsSecret `json:"credentialsSecret"`
}

// AzureRegistryCredentialsSecret references Azure credentials in a secret.
type AzureRegistryCredentialsSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// TenantIDKey is the key for tenant ID.
	// +kubebuilder:default="tenantId"
	TenantIDKey string `json:"tenantIdKey,omitempty"`

	// ClientIDKey is the key for client ID.
	// +kubebuilder:default="clientId"
	ClientIDKey string `json:"clientIdKey,omitempty"`

	// ClientSecretKey is the key for client secret.
	// +kubebuilder:default="clientSecret"
	ClientSecretKey string `json:"clientSecretKey,omitempty"`
}

// AliyunRegistryConfig defines Aliyun registry configuration.
type AliyunRegistryConfig struct {
	// Registry specifies the registry hostname.
	Registry string `json:"registry"`

	// Region specifies the Aliyun region.
	Region string `json:"region"`

	// InstanceID specifies the ACR instance ID.
	InstanceID string `json:"instanceId"`

	// PullSecret references the dockerconfigjson secret to use for image pulls.
	PullSecret DockerConfigSecretRef `json:"pullSecret"`

	// CredentialsSecret references Aliyun credentials for short-lived tokens.
	CredentialsSecret AliyunRegistryCredentialsSecret `json:"credentialsSecret"`
}

// AliyunRegistryCredentialsSecret references Aliyun credentials in a secret.
type AliyunRegistryCredentialsSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// AccessKeyKey is the key for access key ID.
	// +kubebuilder:default="accessKeyId"
	AccessKeyKey string `json:"accessKeyKey,omitempty"`

	// SecretKeyKey is the key for secret access key.
	// +kubebuilder:default="accessKeySecret"
	SecretKeyKey string `json:"secretKeyKey,omitempty"`
}

// HarborRegistryConfig defines Harbor registry configuration.
type HarborRegistryConfig struct {
	// Registry specifies the registry hostname.
	Registry string `json:"registry"`

	// PullSecret references the dockerconfigjson secret to use for image pulls.
	PullSecret DockerConfigSecretRef `json:"pullSecret"`

	// CredentialsSecret references Harbor credentials for push authentication.
	CredentialsSecret HarborRegistryCredentialsSecret `json:"credentialsSecret"`
}

// HarborRegistryCredentialsSecret references Harbor credentials in a secret.
type HarborRegistryCredentialsSecret struct {
	// Name is the name of the secret.
	Name string `json:"name"`

	// UsernameKey is the key for username.
	// +kubebuilder:default="username"
	UsernameKey string `json:"usernameKey,omitempty"`

	// PasswordKey is the key for password.
	// +kubebuilder:default="password"
	PasswordKey string `json:"passwordKey,omitempty"`
}

// ControlPlaneConfig defines external control plane configuration
type ControlPlaneConfig struct {
	// URL is the control plane edge-gateway URL
	URL string `json:"url"`

	// InternalAuthPublicKeySecret references the secret containing control plane's public key
	InternalAuthPublicKeySecret SecretKeyRef `json:"internalAuthPublicKeySecret"`
}

// InternalAuthConfig defines internal authentication configuration
type InternalAuthConfig struct {
	// ControlPlane configures control plane key pair
	// +optional
	ControlPlane *KeyPairConfig `json:"controlPlane,omitempty"`

	// DataPlane configures data plane key pair
	// +optional
	DataPlane *KeyPairConfig `json:"dataPlane,omitempty"`
}

// KeyPairConfig defines key pair configuration
type KeyPairConfig struct {
	// Generate enables automatic key generation
	// +kubebuilder:default=true
	Generate bool `json:"generate,omitempty"`

	// SecretRef references an existing secret containing the key pair
	// +optional
	SecretRef *KeyPairSecretRef `json:"secretRef,omitempty"`
}

// KeyPairSecretRef references a key pair in a secret
type KeyPairSecretRef struct {
	// Name is the name of the secret
	Name string `json:"name"`

	// PrivateKeyKey is the key for private key
	// +kubebuilder:default="private.key"
	PrivateKeyKey string `json:"privateKeyKey,omitempty"`

	// PublicKeyKey is the key for public key
	// +kubebuilder:default="public.key"
	PublicKeyKey string `json:"publicKeyKey,omitempty"`
}

// ServicesConfig defines configuration for all services
type ServicesConfig struct {
	// GlobalDirectory configures the global-directory service (global service layer)
	// +optional
	// +kubebuilder:default={}
	GlobalDirectory *GlobalDirectoryServiceConfig `json:"globalDirectory,omitempty"`

	// EdgeGateway configures the edge-gateway service (control plane)
	// +optional
	// +kubebuilder:default={}
	EdgeGateway *EdgeGatewayServiceConfig `json:"edgeGateway,omitempty"`

	// Scheduler configures the scheduler service (control plane)
	// +optional
	Scheduler *SchedulerServiceConfig `json:"scheduler,omitempty"`

	// InternalGateway configures the internal-gateway service (data plane)
	// +optional
	// +kubebuilder:default={}
	InternalGateway *InternalGatewayServiceConfig `json:"internalGateway,omitempty"`

	// Manager configures the manager service (data plane)
	// +optional
	// +kubebuilder:default={}
	Manager *ManagerServiceConfig `json:"manager,omitempty"`

	// StorageProxy configures the storage-proxy service (data plane)
	// +optional
	// +kubebuilder:default={}
	StorageProxy *StorageProxyServiceConfig `json:"storageProxy,omitempty"`

	// Netd configures the netd service (data plane)
	// +optional
	// +kubebuilder:default={}
	Netd *NetdServiceConfig `json:"netd,omitempty"`
}

// BaseServiceConfig defines common service configuration
type BaseServiceConfig struct {
	// Enabled enables or disables the service
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// Replicas specifies the number of replicas
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// Resources specifies resource requirements
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Service configures the Kubernetes service
	// +optional
	Service *ServiceNetworkConfig `json:"service,omitempty"`

	// Ingress configures ingress settings
	// +optional
	Ingress *IngressConfig `json:"ingress,omitempty"`
}

// GlobalDirectoryServiceConfig defines configuration for global-directory service.
type GlobalDirectoryServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// Config contains global-directory specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.GlobalDirectoryConfig `json:"config,omitempty"`
}

// EdgeGatewayServiceConfig defines configuration for edge-gateway service
type EdgeGatewayServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// Config contains edge-gateway specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.EdgeGatewayConfig `json:"config,omitempty"`
}

// SchedulerServiceConfig defines configuration for scheduler service
type SchedulerServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// Config contains scheduler specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.SchedulerConfig `json:"config,omitempty"`
}

// InternalGatewayServiceConfig defines configuration for internal-gateway service
type InternalGatewayServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// Config contains internal-gateway specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.InternalGatewayConfig `json:"config,omitempty"`
}

// ManagerServiceConfig defines configuration for manager service
type ManagerServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// Config contains manager specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.ManagerConfig `json:"config,omitempty"`
}

// StorageProxyServiceConfig defines configuration for storage-proxy service
type StorageProxyServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// Config contains storage-proxy specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.StorageProxyConfig `json:"config,omitempty"`
}

// NetdServiceConfig defines configuration for netd service
type NetdServiceConfig struct {
	BaseServiceConfig `json:",inline"`
	// RuntimeClassName specifies the Kubernetes runtime class for the netd daemonset.
	// Use a host-compatible runtime such as runc. Do not run netd on gVisor or Kata.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`
	// NodeSelector constrains netd onto a specific node set.
	// Deprecated: use spec.sandboxNodePlacement.nodeSelector instead. This field
	// remains as a backward-compatible alias when the shared placement is unset.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations allow netd to run on tainted sandbox nodes.
	// Deprecated: use spec.sandboxNodePlacement.tolerations instead. This field
	// remains as a backward-compatible alias when the shared placement is unset.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// Config contains netd specific configuration
	// +optional
	// +kubebuilder:default={}
	Config *config.NetdConfig `json:"config,omitempty"`
}

// IsGlobalDirectoryEnabled returns true when global-directory is enabled.
func IsGlobalDirectoryEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.GlobalDirectory == nil {
		return false
	}
	return infra.Spec.Services.GlobalDirectory.Enabled
}

// IsEdgeGatewayEnabled returns true when edge-gateway is enabled.
func IsEdgeGatewayEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.EdgeGateway == nil {
		return false
	}
	return infra.Spec.Services.EdgeGateway.Enabled
}

// IsSchedulerEnabled returns true when scheduler is enabled.
func IsSchedulerEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Scheduler == nil {
		return false
	}
	return infra.Spec.Services.Scheduler.Enabled
}

// IsInternalGatewayEnabled returns true when internal-gateway is enabled.
func IsInternalGatewayEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.InternalGateway == nil {
		return false
	}
	return infra.Spec.Services.InternalGateway.Enabled
}

// IsManagerEnabled returns true when manager is enabled.
func IsManagerEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Manager == nil {
		return false
	}
	return infra.Spec.Services.Manager.Enabled
}

// IsStorageProxyEnabled returns true when storage-proxy is enabled.
func IsStorageProxyEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.StorageProxy == nil {
		return false
	}
	return infra.Spec.Services.StorageProxy.Enabled
}

// IsNetdEnabled returns true when netd is enabled.
func IsNetdEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Netd == nil {
		return false
	}
	return infra.Spec.Services.Netd.Enabled
}

// IsDatabaseEnabled returns true when database should be reconciled.
func IsDatabaseEnabled(infra *Sandbox0Infra) bool {
	if infra == nil {
		return false
	}
	if infra.Spec.Database == nil {
		return false
	}
	switch infra.Spec.Database.Type {
	case DatabaseTypeBuiltin:
		if infra.Spec.Database.Builtin != nil {
			return infra.Spec.Database.Builtin.Enabled
		}
		return true
	case DatabaseTypeExternal:
		return true
	default:
		return true
	}
}

// IsStorageEnabled returns true when storage should be reconciled.
func IsStorageEnabled(infra *Sandbox0Infra) bool {
	if infra == nil {
		return false
	}
	if infra.Spec.Storage == nil {
		return false
	}
	switch infra.Spec.Storage.Type {
	case StorageTypeBuiltin:
		if infra.Spec.Storage.Builtin != nil {
			return infra.Spec.Storage.Builtin.Enabled
		}
		return true
	case StorageTypeS3, StorageTypeOSS:
		return true
	default:
		return true
	}
}

// IsRegistryEnabled returns true when registry should be reconciled.
func IsRegistryEnabled(infra *Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Registry == nil {
		return false
	}
	switch infra.Spec.Registry.Provider {
	case RegistryProviderBuiltin:
		if infra.Spec.Registry.Builtin != nil {
			return infra.Spec.Registry.Builtin.Enabled
		}
		return true
	case RegistryProviderAWS, RegistryProviderGCP, RegistryProviderAzure, RegistryProviderAliyun, RegistryProviderHarbor:
		return true
	default:
		return true
	}
}

// HasControlPlaneServices returns true when any control-plane service is enabled.
func HasControlPlaneServices(infra *Sandbox0Infra) bool {
	return IsEdgeGatewayEnabled(infra) || IsSchedulerEnabled(infra)
}

// HasDataPlaneServices returns true when any data-plane service is enabled.
func HasDataPlaneServices(infra *Sandbox0Infra) bool {
	return IsInternalGatewayEnabled(infra) || IsManagerEnabled(infra) || IsStorageProxyEnabled(infra) || IsNetdEnabled(infra)
}

// HasAnyServiceEnabled returns true when at least one service is enabled.
func HasAnyServiceEnabled(infra *Sandbox0Infra) bool {
	return IsGlobalDirectoryEnabled(infra) || HasControlPlaneServices(infra) || HasDataPlaneServices(infra)
}

// ServiceNetworkConfig defines service network configuration
type ServiceNetworkConfig struct {
	// Type specifies the service type
	// +kubebuilder:default="ClusterIP"
	Type corev1.ServiceType `json:"type,omitempty"`

	// Port specifies the service port
	// +kubebuilder:default=80
	Port int32 `json:"port,omitempty"`
}

// IngressConfig defines ingress configuration
type IngressConfig struct {
	// Enabled enables ingress
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// ClassName specifies the ingress class name
	// +optional
	ClassName string `json:"className,omitempty"`

	// Host specifies the ingress host
	// +optional
	Host string `json:"host,omitempty"`

	// TLSSecret specifies the TLS secret name
	// +optional
	TLSSecret string `json:"tlsSecret,omitempty"`
}

// PublicExposureConfig defines public URL exposure configuration for sandboxes.
// Host format: <exposureLabel>.<regionLabel>.<rootDomain>
type PublicExposureConfig struct {
	// Enabled enables public exposure routing
	// +optional
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// RootDomain is the root domain for public exposure URLs
	// +optional
	// +kubebuilder:default="sandbox0.app"
	RootDomain string `json:"rootDomain,omitempty"`

	// RegionID is the DNS-safe region label used in public URLs.
	// It is not the canonical multi-region tenancy identifier.
	// +optional
	// +kubebuilder:default="aws-us-east-1"
	RegionID string `json:"regionId,omitempty"`
}

// ClusterConfig defines cluster identification and capacity
type ClusterConfig struct {
	// ID is the unique cluster identifier
	ID string `json:"id"`

	// Name is the human-readable cluster name
	// +optional
	Name string `json:"name,omitempty"`

	// Capacity specifies cluster resource capacity
	// +optional
	Capacity *ClusterCapacity `json:"capacity,omitempty"`
}

// ClusterCapacity defines cluster resource capacity
type ClusterCapacity struct {
	// MaxSandboxes is the maximum number of sandboxes
	// +optional
	MaxSandboxes int32 `json:"maxSandboxes,omitempty"`

	// CPU specifies CPU capacity
	// +optional
	CPU *ResourceCapacity `json:"cpu,omitempty"`

	// Memory specifies memory capacity
	// +optional
	Memory *ResourceCapacity `json:"memory,omitempty"`
}

// ResourceCapacity defines resource capacity
type ResourceCapacity struct {
	// Total is the total capacity
	Total string `json:"total,omitempty"`

	// Available is the available capacity
	Available string `json:"available,omitempty"`
}

// InitUserConfig defines initial admin user configuration
type InitUserConfig struct {
	// Email is the admin user's email
	// +optional
	Email string `json:"email"`

	// PasswordSecret references the secret containing the password
	// +optional
	PasswordSecret SecretKeyRef `json:"passwordSecret"`

	// Name is the admin user's display name
	// +optional
	Name string `json:"name,omitempty"`
}

// BuiltinTemplateConfig defines a system builtin template.
type BuiltinTemplateConfig struct {
	TemplateID  string                    `json:"templateId"`
	Image       string                    `json:"image,omitempty"`
	DisplayName string                    `json:"displayName,omitempty"`
	Description string                    `json:"description,omitempty"`
	Pool        BuiltinTemplatePoolConfig `json:"pool,omitempty"`
}

// BuiltinTemplatePoolConfig holds pool defaults for builtin templates.
type BuiltinTemplatePoolConfig struct {
	// +optional
	// +kubebuilder:default=1
	MinIdle int32 `json:"minIdle,omitempty"`
	// +optional
	// +kubebuilder:default=5
	MaxIdle int32 `json:"maxIdle,omitempty"`
}

// Sandbox0InfraStatus defines the observed state of Sandbox0Infra
type Sandbox0InfraStatus struct {
	// Phase represents the current phase of the infrastructure
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Conditions represent the latest available observations of the infrastructure's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedVersion is the last observed version
	// +optional
	ObservedVersion string `json:"observedVersion,omitempty"`

	// Endpoints contains the service endpoints
	// +optional
	Endpoints *EndpointsStatus `json:"endpoints,omitempty"`

	// Cluster contains cluster registration status
	// +optional
	Cluster *ClusterStatus `json:"cluster,omitempty"`

	// InternalAuth contains internal authentication status
	// +optional
	InternalAuth *InternalAuthStatus `json:"internalAuth,omitempty"`

	// LastOperation contains the last operation information
	// +optional
	LastOperation *LastOperation `json:"lastOperation,omitempty"`

	// LastMessage is the most recent status message (success or error)
	// +optional
	LastMessage string `json:"lastMessage,omitempty"`

	// Progress shows readiness progress in "ready/total" format
	// +optional
	Progress string `json:"progress,omitempty"`
}

// EndpointsStatus contains service endpoints
type EndpointsStatus struct {
	// GlobalDirectory is the global-directory URL
	// +optional
	GlobalDirectory string `json:"globalDirectory,omitempty"`

	// EdgeGateway is the external edge-gateway URL
	// +optional
	EdgeGateway string `json:"edgeGateway,omitempty"`

	// EdgeGatewayInternal is the internal edge-gateway URL
	// +optional
	EdgeGatewayInternal string `json:"edgeGatewayInternal,omitempty"`

	// InternalGateway is the internal-gateway URL
	// +optional
	InternalGateway string `json:"internalGateway,omitempty"`
}

// ClusterStatus contains cluster registration status
type ClusterStatus struct {
	// ID is the cluster identifier
	// +optional
	ID string `json:"id,omitempty"`

	// Registered indicates if the cluster is registered
	// +optional
	Registered bool `json:"registered,omitempty"`

	// RegisteredAt is the registration timestamp
	// +optional
	RegisteredAt *metav1.Time `json:"registeredAt,omitempty"`
}

// InternalAuthStatus contains internal authentication status
type InternalAuthStatus struct {
	// ControlPlanePublicKey references the control plane public key
	// +optional
	ControlPlanePublicKey *SecretKeyStatus `json:"controlPlanePublicKey,omitempty"`

	// DataPlanePublicKey references the data plane public key
	// +optional
	DataPlanePublicKey *SecretKeyStatus `json:"dataPlanePublicKey,omitempty"`
}

// SecretKeyStatus references a key in a secret
type SecretKeyStatus struct {
	// SecretName is the name of the secret
	SecretName string `json:"secretName"`

	// SecretKey is the key in the secret
	SecretKey string `json:"secretKey"`
}

// LastOperation contains the last operation information
type LastOperation struct {
	// Type is the operation type
	// +optional
	Type string `json:"type,omitempty"`

	// StartedAt is when the operation started
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the operation completed
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Status is the operation status
	// +optional
	Status string `json:"status,omitempty"`

	// Error contains error message if failed
	// +optional
	Error string `json:"error,omitempty"`
}

// Condition types
const (
	ConditionTypeReady                = "Ready"
	ConditionTypeDatabaseReady        = "DatabaseReady"
	ConditionTypeStorageReady         = "StorageReady"
	ConditionTypeRegistryReady        = "RegistryReady"
	ConditionTypeGlobalDirectoryReady = "GlobalDirectoryReady"
	ConditionTypeEdgeGatewayReady     = "EdgeGatewayReady"
	ConditionTypeInternalGatewayReady = "InternalGatewayReady"
	ConditionTypeManagerReady         = "ManagerReady"
	ConditionTypeStorageProxyReady    = "StorageProxyReady"
	ConditionTypeFusePluginReady      = "FusePluginReady"
	ConditionTypeNetdReady            = "NetdReady"
	ConditionTypeSchedulerReady       = "SchedulerReady"
	ConditionTypeInternalAuthReady    = "InternalAuthReady"
	ConditionTypeCRDsInstalled        = "CRDsInstalled"
	ConditionTypeSecretsGenerated     = "SecretsGenerated"
	ConditionTypeInitUserReady        = "InitUserReady"
	ConditionTypeClusterRegistered    = "ClusterRegistered"
)

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=s0i
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Progress",type=string,JSONPath=`.status.progress`
//+kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.lastMessage`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Sandbox0Infra is the Schema for the sandbox0infras API
type Sandbox0Infra struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Sandbox0InfraSpec   `json:"spec,omitempty"`
	Status Sandbox0InfraStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// Sandbox0InfraList contains a list of Sandbox0Infra
type Sandbox0InfraList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Sandbox0Infra `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Sandbox0Infra{}, &Sandbox0InfraList{})
}
