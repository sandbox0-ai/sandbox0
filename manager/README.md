# Sandbox0 Manager

Manager is the core control plane component of Sandbox0, responsible for:

- Managing SandboxTemplate CRD
- Maintaining idle pod pools using ReplicaSet
- Handling sandbox claim requests
- Cleaning up expired and excess pods

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Manager Components                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Operator   │  │ PoolManager  │  │   Cleanup    │          │
│  │              │  │              │  │  Controller  │          │
│  │  Reconciles  │  │   Manages    │  │              │          │
│  │  Templates   │  │  ReplicaSet  │  │  Enforces    │          │
│  │              │  │              │  │  maxIdle     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    HTTP API Server                       │   │
│  │  - POST /api/v1/sandboxes/claim                          │   │
│  │  - GET  /api/v1/sandboxes/:id/status                     │   │
│  │  - DELETE /api/v1/sandboxes/:id                          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Features

### 1. SandboxTemplate Management
- Kubernetes Custom Resource Definition (CRD)
- Define sandbox specifications (image, resources, network policies)
- Template inheritance support

### 2. Idle Pool Management
- Uses native Kubernetes ReplicaSet
- Automatic pod replacement when claimed
- Fast sandbox provisioning (hot start)

### 3. Cleanup Controller
- Enforces maxIdle limit
- Removes expired active pods
- Reclaims resources automatically

### 4. HTTP API
- RESTful API for sandbox operations
- Claim sandboxes from idle pool
- Query sandbox status
- Terminate sandboxes

## Configuration

Environment variables:

```bash
# HTTP Server
HTTP_PORT=8080

# Kubernetes
NAMESPACE=default
RESYNC_PERIOD=30s

# Cleanup
CLEANUP_INTERVAL=60s

# Logging
LOG_LEVEL=info

# Metrics
METRICS_PORT=9090
```

## Deployment

### 1. Install CRD

```bash
kubectl apply -f deploy/k8s/crd.yaml
```

### 2. Create RBAC

```bash
kubectl apply -f deploy/k8s/rbac.yaml
```

### 3. Deploy Manager

```bash
kubectl apply -f deploy/k8s/deployment.yaml
```

### 4. Create a SandboxTemplate

```yaml
apiVersion: sandbox0.ai/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-dev
  namespace: default
spec:
  displayName: "Python Development Environment"
  description: "Python 3.11 with common development tools"
  
  mainContainer:
    image: sandbox0/procd:latest
    # Note: Using default capabilities (no NET_ADMIN)
    securityContext:
      capabilities:
        drop:
          - NET_RAW
    resources:
      limits:
        cpu: "2"
        memory: "4Gi"
      requests:
        cpu: "1"
        memory: "2Gi"
  
  resources:
    cpu: "2"
    memory: "4Gi"
  
  pool:
    minIdle: 3
    maxIdle: 10
  
  envdVersion: "0.1.0"
```

## API Examples

### Claim a Sandbox

```bash
curl -X POST http://manager:8080/api/v1/sandboxes/claim \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": "python-dev",
    "team_id": "team-123",
    "user_id": "user-456",
    "sandbox_id": "sb-abc123",
    "config": {
      "ttl": 3600,
      "env_vars": {
        "API_KEY": "secret"
      }
    }
  }'
```

Response:
```json
{
  "sandbox_id": "sb-abc123",
  "template_id": "python-dev",
  "status": "starting",
  "procd_address": "python-dev-pool-xyz.default.svc.cluster.local:8080",
  "pod_name": "python-dev-pool-xyz",
  "namespace": "default"
}
```

### Get Sandbox Status

```bash
curl http://manager:8080/api/v1/sandboxes/sb-abc123/status
```

### Terminate Sandbox

```bash
curl -X DELETE http://manager:8080/api/v1/sandboxes/sb-abc123
```

## Metrics

Prometheus metrics are exposed on `/metrics`:

- `manager_templates_total` - Total number of templates
- `manager_idle_pods_total{template}` - Idle pod count per template
- `manager_active_pods_total{template}` - Active pod count per template
- `manager_sandbox_claims_total{template,status}` - Sandbox claims
- `manager_sandbox_claim_duration_seconds{template,type}` - Claim duration
- `manager_pods_cleaned_total{template,reason}` - Cleaned pods
- `manager_reconcile_total{template,result}` - Reconciliation operations
- `manager_reconcile_duration_seconds{template}` - Reconciliation duration

## Development

### Build

```bash
make build
```

### Run Locally

```bash
export KUBECONFIG=~/.kube/config
./bin/manager
```

### Run Tests

```bash
make test
```

## License

Proprietary - Sandbox0 AI

