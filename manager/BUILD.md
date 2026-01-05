# Build Instructions for Manager

## Prerequisites

- Go 1.21 or later
- Docker (for container builds)
- kubectl and access to a Kubernetes cluster
- Make

## Dependency Resolution

The project has a dependency issue that needs to be resolved before building. You may encounter this error:

```
tls: failed to verify certificate: x509: OSStatus -26276
```

### Solution Options

**Option 1: Update Go Proxy Settings**

```bash
export GOPROXY=https://proxy.golang.org,direct
export GOSUMDB=sum.golang.org
go mod download
```

**Option 2: Disable Proxy (if on corporate network)**

```bash
export GOPROXY=direct
go mod download
```

**Option 3: Update System Certificates (macOS)**

```bash
# Update certificates
brew install ca-certificates
# or
open /Applications/Python\ 3.*/Install\ Certificates.command
```

**Option 4: Use Go Private (if using private modules)**

```bash
export GOPRIVATE=github.com/your-org/*
go mod download
```

## Build Steps

### 1. Download Dependencies

```bash
cd /Users/huangzhihao/sandbox0/infra/manager
go mod tidy
go mod download
```

### 2. Build Binary

```bash
make build
# or
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o bin/manager cmd/manager/main.go
```

### 3. Build Docker Image

```bash
make docker-build
# or
docker build -t sandbox0/manager:latest .
```

### 4. Run Locally (for development)

```bash
export KUBECONFIG=~/.kube/config
export LOG_LEVEL=debug
export HTTP_PORT=8080
export METRICS_PORT=9090
export CLEANUP_INTERVAL=60s

./bin/manager
```

## Verification

### Check Build

```bash
./bin/manager --help
# Should show no errors
```

### Check Docker Image

```bash
docker images | grep manager
# Should show: sandbox0/manager latest ...
```

### Deploy to Kubernetes

```bash
# 1. Install CRD
kubectl apply -f deploy/k8s/crd.yaml

# 2. Install RBAC
kubectl apply -f deploy/k8s/rbac.yaml

# 3. Deploy Manager
kubectl apply -f deploy/k8s/deployment.yaml

# 4. Check status
kubectl get pods -n sandbox0-system
kubectl logs -n sandbox0-system -l app=manager -f
```

### Create Test Template

```bash
kubectl apply -f deploy/k8s/example-template.yaml
kubectl get sandboxtemplates
kubectl describe sandboxtemplate python-dev
```

### Test API

```bash
# Port forward
kubectl port-forward -n sandbox0-system svc/manager 8080:8080

# Health check
curl http://localhost:8080/healthz

# Claim sandbox
curl -X POST http://localhost:8080/api/v1/sandboxes/claim \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": "python-dev",
    "sandbox_id": "test-sb-123",
    "team_id": "team-test",
    "user_id": "user-test"
  }'
```

## Troubleshooting

### Issue: Dependencies won't download

**Error**: `tls: failed to verify certificate`

**Fix**:
1. Check network connectivity
2. Update system certificates
3. Try different GOPROXY settings
4. Use `GOINSECURE` for specific hosts (not recommended for production)

### Issue: CRD installation fails

**Error**: `error: unable to recognize "deploy/k8s/crd.yaml"`

**Fix**:
1. Check Kubernetes version (requires 1.16+)
2. Verify kubectl context: `kubectl config current-context`
3. Check cluster access: `kubectl cluster-info`

### Issue: Manager pod crashes

**Error**: `CrashLoopBackOff`

**Fix**:
1. Check logs: `kubectl logs -n sandbox0-system <pod-name>`
2. Verify RBAC permissions
3. Check CRD is installed: `kubectl get crd sandboxtemplates.sandbox0.ai`
4. Verify image exists: `kubectl describe pod -n sandbox0-system <pod-name>`

### Issue: Informer cache sync timeout

**Error**: `failed to wait for caches to sync`

**Fix**:
1. Increase timeout in code
2. Check API server connectivity
3. Verify RBAC permissions for list/watch

## Development Workflow

### Make Changes

```bash
# Edit code
vim pkg/controller/operator.go

# Format
make fmt

# Vet
make vet

# Build
make build

# Test locally
./bin/manager
```

### Run Tests

```bash
make test
```

### Build and Deploy

```bash
# Build image
make docker-build

# Push image
docker tag sandbox0/manager:latest your-registry/manager:latest
docker push your-registry/manager:latest

# Update deployment
kubectl set image deployment/manager manager=your-registry/manager:latest -n sandbox0-system

# Rollback if needed
kubectl rollout undo deployment/manager -n sandbox0-system
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Build and Deploy Manager

on:
  push:
    branches: [main]
    paths:
      - 'infra/manager/**'

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: 1.21
      
      - name: Build
        run: |
          cd infra/manager
          make build
      
      - name: Test
        run: |
          cd infra/manager
          make test
      
      - name: Docker Build
        run: |
          cd infra/manager
          make docker-build
      
      - name: Push
        run: |
          echo "${{ secrets.DOCKER_PASSWORD }}" | docker login -u "${{ secrets.DOCKER_USERNAME }}" --password-stdin
          docker push sandbox0/manager:latest
```

## Next Steps

After successful build:

1. Write unit tests
2. Add integration tests
3. Set up CI/CD pipeline
4. Monitor metrics at `/metrics`
5. Review logs in production

## Support

For issues or questions:
- Check logs: `kubectl logs -n sandbox0-system -l app=manager`
- Check metrics: `kubectl port-forward -n sandbox0-system svc/manager-metrics 9090:9090`
- Review spec: `infra/spec/manager/manager.md`

