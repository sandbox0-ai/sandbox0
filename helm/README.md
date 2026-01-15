# Sandbox0 Infrastructure Helm Chart

This is the umbrella Helm chart for deploying all sandbox0 infrastructure services.

## Architecture

This chart includes the following sub-charts:
- **edge-gateway**: Public API gateway for user requests
- **internal-gateway**: Internal service gateway for inter-service communication
- **manager**: Kubernetes operator for sandbox lifecycle management
- **storage-proxy**: JuiceFS storage proxy for persistent volumes
- **netd**: Network daemon for L7 proxy and eBPF-based network policies

## Prerequisites

- Kubernetes 1.35+
- Helm 3.0+
- PostgreSQL database (for metadata and JuiceFS)
- S3-compatible object storage (for JuiceFS data blocks)
- RSA key pair for internal JWT authentication

## Installation

### 1. Generate Internal JWT Key Pair

```bash
# Generate RSA private key
openssl genrsa -out private.key 2048

# Generate RSA public key
openssl rsa -in private.key -pubout -out public.key

# Create Kubernetes secret
kubectl -n sandbox0-system create secret generic sandbox0-internal-jwt \
  --from-file=private.key=private.key \
  --from-file=public.key=public.key
```

### 2. Create values.yaml

Create a `values.yaml` file with your configuration:

```yaml
global:
  # Database configuration (shared by all services)
  database:
    url: "postgres://sandbox0:PASSWORD@postgres-host:5432/sandbox0?sslmode=require"
  
  # JuiceFS configuration (for storage-proxy)
  juicefs:
    meta_url: "postgres://juicefs:PASSWORD@postgres-host:5432/juicefs?sslmode=require"
    s3_bucket: "your-bucket-name"
    s3_region: "us-east-1"
    s3_endpoint: "http://rustfs:9000" # in-cluster RustFS service (if enabled)
    s3_access_key: "YOUR_ACCESS_KEY"
    s3_secret_key: "YOUR_SECRET_KEY"
    s3_session_token: ""
  
  # JWT configuration
  jwt:
    # For edge-gateway user authentication (use a strong random secret)
    secret: "your-strong-random-secret-here"
    
    # For internal service-to-service authentication (k8s secret name)
    internalJwtSecretName: "sandbox0-internal-jwt"
    privateKeyKey: "private.key"
    publicKeyKey: "public.key"
  
  # Optional: create initial admin user
  initUser:
    enabled: true
    email: "admin@yourdomain.com"
    password: "your-secure-password"
    name: "Admin User"

# Service-specific overrides (if needed)
manager:
  replicaCount: 3

storage-proxy:
  replicaCount: 3

edge-gateway:
  replicaCount: 3
```

### 3. Update Chart Dependencies

```bash
cd infra
make helm-update
cd helm
helm dependency update
```

### 3.1 Optional: Use Built-in RustFS as S3

This umbrella chart can deploy [RustFS](https://charts.rustfs.com/) as an in-cluster S3-compatible backend for JuiceFS.

- Enable/disable: set `rustfs.enabled`
- Endpoint for JuiceFS: set `global.juicefs.s3_endpoint` to `http://rustfs:9000`
- Credentials: keep `rustfs.secret.rustfs.*` aligned with `global.juicefs.s3_access_key` / `s3_secret_key`

### 3.2 Optional: Use Built-in PostgreSQL

This umbrella chart can deploy PostgreSQL as an in-cluster database for:

- `global.database.url` (sandbox0 metadata)
- `global.juicefs.meta_url` (JuiceFS metadata)

Enable/disable: set `postgresql.enabled`. Default service hostname is `postgresql` within the release namespace.

### 4. Install the Chart

```bash
helm install sandbox0 . -f values.yaml -n sandbox0-system --create-namespace
```

## Configuration

### Service-Specific Overrides

Each service can override global values if needed. For example:

```yaml
storage-proxy:
  config:
    # Override global S3 bucket for this service only
    s3_bucket: "special-bucket"
    # Other config values use global settings
```

### File-Based Secrets

Only file-based secrets (like RSA keys) are managed via Kubernetes Secrets and mounted as volumes:

- Internal JWT keys: RSA private/public key pair for service-to-service authentication

All other configuration (database URLs, S3 credentials, etc.) is stored in ConfigMaps as YAML configuration.

## Upgrade

```bash
# Update chart dependencies
cd infra
make helm-update
cd helm
helm dependency update

# Upgrade the release
helm upgrade sandbox0 . -f values.yaml -n sandbox0-system
```

## Uninstall

```bash
helm uninstall sandbox0 -n sandbox0-system
```

## Development

### Local Development

For local development, you can override values:

```yaml
global:
  database:
    url: "postgres://sandbox0:sandbox0@localhost:5432/sandbox0?sslmode=disable"
  juicefs:
    meta_url: "postgres://juicefs:juicefs@localhost:5432/juicefs?sslmode=disable"
    s3_endpoint: "http://localhost:9000"  # MinIO
```

### Testing

Validate your configuration before applying:

```bash
# Lint the chart
helm lint .

# Test rendering templates
helm template sandbox0 . -f values.yaml > rendered.yaml

# Dry-run installation
helm install sandbox0 . -f values.yaml -n sandbox0-system --dry-run --debug
```

## Troubleshooting

### Check service status

```bash
kubectl get pods -n sandbox0-system
kubectl logs -n sandbox0-system <pod-name>
```

### Verify configuration

```bash
# Check ConfigMap
kubectl get configmap -n sandbox0-system manager -o yaml

# Check Secret
kubectl get secret -n sandbox0-system sandbox0-internal-jwt -o yaml
```


