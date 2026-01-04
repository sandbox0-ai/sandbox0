# Storage Proxy Deployment Guide

This guide provides detailed instructions for deploying Storage Proxy in a Kubernetes environment.

## Prerequisites

### Infrastructure Requirements

- Kubernetes cluster (1.20+)
- PostgreSQL database for JuiceFS metadata
- S3-compatible object storage (AWS S3, MinIO, etc.)
- Network policies support (if using network isolation)
- Prometheus (optional, for metrics)

### Access Requirements

- `kubectl` access to the cluster
- Permissions to create namespaces, deployments, services, secrets
- AWS credentials (or IRSA setup for EKS)

## Step 1: Prepare Secrets

### Generate JWT Secret

```bash
# Generate a strong JWT secret (32+ bytes)
openssl rand -base64 32
```

### Setup PostgreSQL

Create a dedicated database for JuiceFS metadata:

```sql
CREATE DATABASE juicefs;
CREATE USER juicefs_user WITH ENCRYPTED PASSWORD 'your-password';
GRANT ALL PRIVILEGES ON DATABASE juicefs TO juicefs_user;
```

### Configure AWS Access

**Option A: Static Credentials (Development)**

```bash
# Store in Kubernetes secret
kubectl create secret generic storage-proxy-secrets \
  -n sandbox0-system \
  --from-literal=jwt-secret="$(openssl rand -base64 32)" \
  --from-literal=postgres-url="postgres://juicefs_user:password@postgres:5432/juicefs" \
  --from-literal=aws-access-key-id="AKIAIOSFODNN7EXAMPLE" \
  --from-literal=aws-secret-access-key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

**Option B: IRSA (Production, EKS)**

1. Create IAM role for Storage Proxy:

```bash
# Create trust policy
cat > trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/oidc.eks.REGION.amazonaws.com/id/OIDC_ID"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.eks.REGION.amazonaws.com/id/OIDC_ID:sub": "system:serviceaccount:sandbox0-system:storage-proxy"
        }
      }
    }
  ]
}
EOF

# Create role
aws iam create-role \
  --role-name storage-proxy-role \
  --assume-role-policy-document file://trust-policy.json

# Attach S3 access policy
aws iam put-role-policy \
  --role-name storage-proxy-role \
  --policy-name storage-proxy-s3-access \
  --policy-document file://s3-policy.json
```

2. Update `serviceaccount.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: storage-proxy
  namespace: sandbox0-system
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT_ID:role/storage-proxy-role
```

### S3 Bucket Setup

```bash
# Create S3 bucket
aws s3 mb s3://sandbox0-volumes --region us-east-1

# Enable versioning (recommended)
aws s3api put-bucket-versioning \
  --bucket sandbox0-volumes \
  --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
  --bucket sandbox0-volumes \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }]
  }'

# Block public access
aws s3api put-public-access-block \
  --bucket sandbox0-volumes \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

## Step 2: Configure Deployment

### Update Configuration

Edit `deploy/k8s/secret.yaml`:

```yaml
stringData:
  jwt-secret: "YOUR_GENERATED_JWT_SECRET"
  postgres-url: "postgres://juicefs_user:password@postgres.database.svc:5432/juicefs?sslmode=require"
```

Edit `deploy/k8s/deployment.yaml` for your environment:

```yaml
# Resource requirements
resources:
  requests:
    cpu: "2"      # Adjust based on load
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"

# Cache volume size
volumeClaimTemplates:
- metadata:
    name: cache
  spec:
    resources:
      requests:
        storage: 10Gi  # Adjust based on workload
```

### Storage Class

Ensure you have a fast storage class for cache:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp3
  iops: "3000"
  throughput: "125"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

## Step 3: Deploy to Kubernetes

### Deploy All Components

```bash
# Apply all manifests
kubectl apply -k deploy/k8s/

# Or apply individually in order
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/serviceaccount.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/poddisruptionbudget.yaml
kubectl apply -f deploy/k8s/networkpolicy.yaml
```

### Verify Deployment

```bash
# Check pods are running
kubectl get pods -n sandbox0-system -l app=storage-proxy

# Expected output:
# NAME              READY   STATUS    RESTARTS   AGE
# storage-proxy-0   1/1     Running   0          2m
# storage-proxy-1   1/1     Running   0          2m
# storage-proxy-2   1/1     Running   0          2m

# Check services
kubectl get svc -n sandbox0-system

# Check logs
kubectl logs -n sandbox0-system storage-proxy-0 --tail=50
```

### Health Checks

```bash
# Port forward to local machine
kubectl port-forward -n sandbox0-system svc/storage-proxy 8081:8081

# Check health endpoint
curl http://localhost:8081/health
# Expected: {"status":"healthy","timestamp":1706659200}

# Check readiness
curl http://localhost:8081/ready
# Expected: {"status":"ready","timestamp":1706659200}

# Check metrics
kubectl port-forward -n sandbox0-system svc/storage-proxy-metrics 9090:9090
curl http://localhost:9090/metrics
```

## Step 4: Integration with Procd

### Update Procd Configuration

Add Storage Proxy endpoint to Procd environment:

```yaml
# In Procd deployment
env:
- name: STORAGE_PROXY_URL
  value: "storage-proxy.sandbox0-system.svc.cluster.local:8080"
```

### Network Policy

If using network policies, ensure Procd can reach Storage Proxy:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: procd-egress
  namespace: sandbox0-workloads
spec:
  podSelector:
    matchLabels:
      app: procd
  policyTypes:
  - Egress
  egress:
  # Allow access to Storage Proxy
  - to:
    - namespaceSelector:
        matchLabels:
          name: sandbox0-system
    - podSelector:
        matchLabels:
          app: storage-proxy
    ports:
    - protocol: TCP
      port: 8080
```

## Step 5: Monitoring Setup

### Prometheus ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: storage-proxy
  namespace: sandbox0-system
spec:
  selector:
    matchLabels:
      app: storage-proxy
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
```

### Grafana Dashboard

Import the provided dashboard (create `grafana-dashboard.json`):

- Volume mount/unmount metrics
- Operation latency percentiles
- Cache hit rate
- S3 operation metrics
- Authentication metrics

### Alerts

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: storage-proxy-alerts
  namespace: sandbox0-system
spec:
  groups:
  - name: storage-proxy
    interval: 30s
    rules:
    - alert: StorageProxyDown
      expr: up{job="storage-proxy"} == 0
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "Storage Proxy is down"
    
    - alert: HighOperationLatency
      expr: histogram_quantile(0.99, storage_proxy_operations_duration_seconds) > 1
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "High operation latency (p99 > 1s)"
    
    - alert: LowCacheHitRate
      expr: storage_proxy_cache_hit_rate < 0.5
      for: 15m
      labels:
        severity: warning
      annotations:
        summary: "Low cache hit rate (< 50%)"
```

## Step 6: Scaling and Performance

### Horizontal Scaling

Storage Proxy can be scaled horizontally:

```bash
# Scale to 5 replicas
kubectl scale statefulset storage-proxy -n sandbox0-system --replicas=5
```

Consider scaling based on:
- Number of active volumes
- Request rate (operations per second)
- Cache size requirements

### Vertical Scaling

Adjust resources based on metrics:

```yaml
resources:
  requests:
    cpu: "4"      # Increase for high I/O workloads
    memory: "8Gi" # Increase for more cache
  limits:
    cpu: "8"
    memory: "16Gi"
```

### Cache Optimization

```bash
# Increase cache size for better performance
kubectl edit statefulset -n sandbox0-system storage-proxy

# Update volumeClaimTemplates
storage: 200Gi  # Larger cache = better hit rate
```

## Troubleshooting

### Pod Not Starting

```bash
# Check events
kubectl describe pod -n sandbox0-system storage-proxy-0

# Common issues:
# - Missing secrets
# - PVC not bound
# - Image pull errors
```

### Volume Mount Failures

```bash
# Check PostgreSQL connectivity
kubectl exec -n sandbox0-system storage-proxy-0 -- \
  psql "$DEFAULT_META_URL" -c "SELECT 1"

# Check S3 access
kubectl exec -n sandbox0-system storage-proxy-0 -- \
  aws s3 ls s3://sandbox0-volumes/
```

### Authentication Errors

```bash
# Verify JWT secret matches
kubectl get secret -n sandbox0-system storage-proxy-secrets \
  -o jsonpath='{.data.jwt-secret}' | base64 -d

# Check token claims
echo "$TOKEN" | base64 -d | jq .
```

### Performance Issues

```bash
# Check resource usage
kubectl top pod -n sandbox0-system -l app=storage-proxy

# Check metrics
kubectl port-forward -n sandbox0-system svc/storage-proxy-metrics 9090:9090
curl http://localhost:9090/metrics | grep -E '(latency|cache|s3)'

# Enable debug logging
kubectl set env statefulset/storage-proxy -n sandbox0-system LOG_LEVEL=debug
```

## Maintenance

### Backup and Recovery

JuiceFS data is stored in:
- PostgreSQL (metadata) - backup your database
- S3 (data chunks) - enable versioning and lifecycle policies

### Updates

```bash
# Update image
kubectl set image statefulset/storage-proxy -n sandbox0-system \
  storage-proxy=sandbox0/storage-proxy:v1.1.0

# Rolling update automatically applies
kubectl rollout status statefulset/storage-proxy -n sandbox0-system
```

### Cleanup

```bash
# Delete deployment
kubectl delete -k deploy/k8s/

# Or delete namespace (removes everything)
kubectl delete namespace sandbox0-system
```

## Production Checklist

- [ ] Use IRSA for AWS credentials (no static keys)
- [ ] Enable PostgreSQL SSL/TLS
- [ ] Enable S3 bucket encryption
- [ ] Set up monitoring and alerting
- [ ] Configure backup for PostgreSQL
- [ ] Enable S3 versioning
- [ ] Set resource limits based on load testing
- [ ] Configure PodDisruptionBudget
- [ ] Enable network policies
- [ ] Set up log aggregation (ELK, Loki)
- [ ] Configure audit log retention
- [ ] Test disaster recovery procedures
- [ ] Document runbook for common issues
- [ ] Set up on-call rotation

## Security Hardening

1. **Least Privilege RBAC**: Only grant necessary permissions
2. **Network Policies**: Restrict ingress/egress
3. **Pod Security Policies**: Run as non-root, no privileged mode
4. **Secret Management**: Use Vault or AWS Secrets Manager
5. **Audit Logging**: Enable and monitor audit logs
6. **Image Scanning**: Scan for vulnerabilities
7. **mTLS**: Enable service mesh for encryption in transit

