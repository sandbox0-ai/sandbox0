# Scheduler Service

The scheduler is a control plane component for sandbox0 that enables multi-cluster template management. It acts as the source of truth for `SandboxTemplate` definitions and distributes them across multiple data-plane clusters based on their capacity and weight.

## Features

### v1 Release

- **Template Management**: Centralized CRUD API for SandboxTemplate definitions
- **Cluster Registry**: Manage multiple data-plane clusters with weight-based distribution
- **Capacity-Aware Allocation**: Automatically distribute minIdle/maxIdle based on cluster capacity
- **Immediate Synchronization**: Changes to templates trigger instant reconciliation
- **Orphan Cleanup**: Automatically remove templates from clusters when deleted from database
- **Observability**: Comprehensive Prometheus metrics and enhanced health checks

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Control Plane                          │
│                                                              │
│  ┌──────────────┐         ┌────────────────┐               │
│  │ Edge Gateway │────────▶│   Scheduler    │               │
│  └──────────────┘         │  (PostgreSQL)  │               │
│        │                  └────────┬───────┘               │
│        │                           │                        │
└────────┼───────────────────────────┼────────────────────────┘
         │                           │
         │                           │ Sync Templates
         │                           │ (via internal-gateway)
         │                           │
         ▼                           ▼
┌────────────────────────────────────────────────────────────┐
│                      Data Plane Clusters                    │
│                                                              │
│  ┌─────────────────────┐     ┌─────────────────────┐       │
│  │ Internal Gateway A  │     │ Internal Gateway B  │       │
│  │        ▼            │     │        ▼            │       │
│  │    Manager A        │     │    Manager B        │       │
│  │  (K8s Operator)     │     │  (K8s Operator)     │       │
│  └─────────────────────┘     └─────────────────────┘       │
└────────────────────────────────────────────────────────────┘
```

## How It Works

### Template Distribution

1. **User creates template** via edge-gateway → scheduler
2. **Scheduler stores** template in PostgreSQL (source of truth)
3. **Reconciler computes allocations** based on:
   - Cluster weights
   - Available capacity (nodes, current pods)
   - Template's global minIdle/maxIdle budget
4. **Scheduler syncs** to each cluster via internal-gateway → manager
5. **Manager creates** K8s SandboxTemplate CRD (as a projection/cache)

### Capacity-Aware Allocation

The scheduler uses a smart allocation algorithm:

```
1. Fetch cluster summaries (nodes, pod counts) in parallel
2. For each template:
   a. Calculate weight-based distribution
   b. Estimate available capacity per cluster
   c. Clamp allocations to not exceed capacity
   d. Log warnings when clamping occurs
3. Sync computed allocations to clusters
```

**Example**: If global minIdle=100 and 2 clusters with weight 1:1:
- Without capacity constraint: 50/50 split
- With capacity (Cluster A has 30 available, B has 80): 30/70 split

### Immediate Synchronization

Templates changes trigger reconciliation immediately (in addition to periodic reconciles):
- `POST /api/v1/templates` → Triggers reconcile
- `PUT /api/v1/templates/:id` → Triggers reconcile  
- `DELETE /api/v1/templates/:id` → Triggers reconcile + orphan cleanup

### Orphan Cleanup

During reconciliation, the scheduler:
1. Gets list of templates from database
2. For each cluster, gets list of templates via internal-gateway
3. Identifies orphans (in cluster but not in database)
4. Deletes orphaned templates from clusters

## API Endpoints

### Template Management

```
GET    /api/v1/templates              List all templates
GET    /api/v1/templates/:id          Get template details
POST   /api/v1/templates              Create new template
PUT    /api/v1/templates/:id          Update template
DELETE /api/v1/templates/:id          Delete template
GET    /api/v1/templates/:id/allocations  Get template allocations
```

### Cluster Management

```
GET    /api/v1/clusters               List all clusters
GET    /api/v1/clusters/:id           Get cluster details
POST   /api/v1/clusters               Register new cluster
PUT    /api/v1/clusters/:id           Update cluster
DELETE /api/v1/clusters/:id           Unregister cluster
```

### Health & Metrics

```
GET    /healthz                       Liveness probe
GET    /readyz                        Readiness probe (includes reconcile status)
GET    /metrics                       Prometheus metrics
```

## Configuration

Example `config.yaml`:

```yaml
http_port: 8080
log_level: info
database_url: "postgres://sandbox0:sandbox0@postgresql:5432/sandbox0?sslmode=disable"
reconcile_interval: 30s
cluster_timeout: 10s
shutdown_timeout: 30s
```

Environment variables:
- `CONFIG_PATH`: Path to config file (default: `/config/config.yaml`)

## Metrics

Key Prometheus metrics exposed:

| Metric | Type | Description |
|--------|------|-------------|
| `scheduler_reconcile_total` | Counter | Total reconciliation attempts (by status) |
| `scheduler_reconcile_duration_seconds` | Histogram | Reconciliation cycle duration |
| `scheduler_template_allocations` | Gauge | Allocated min/max idle per cluster |
| `scheduler_cluster_capacity` | Gauge | Cluster capacity metrics (nodes, pods) |
| `scheduler_template_sync_status` | Gauge | Template sync status per cluster |
| `scheduler_orphans_removed_total` | Counter | Orphaned templates removed |
| `scheduler_capacity_clamps_total` | Counter | Times allocations were clamped by capacity |
| `scheduler_last_reconcile_timestamp_seconds` | Gauge | Last successful reconcile timestamp |

## Database Schema

### scheduler_clusters
```sql
- cluster_id (PK)
- internal_gateway_url
- weight
- enabled
- last_seen_at
- created_at, updated_at
```

### scheduler_templates
```sql
- template_id (PK)
- namespace (PK)
- spec (JSONB - SandboxTemplateSpec)
- created_at, updated_at
```

### scheduler_template_allocations
```sql
- template_id, namespace, cluster_id (composite PK)
- min_idle, max_idle
- last_synced_at
- sync_status (pending/synced/error)
- sync_error
- created_at, updated_at
```

## Deployment

### Standalone (Single Cluster)

In single-cluster mode, the scheduler is **optional**. Edge-gateway can directly route to internal-gateway.

```yaml
scheduler:
  enabled: false  # No scheduler needed
```

### Multi-Cluster Mode

Enable scheduler for multi-cluster deployments:

```yaml
scheduler:
  enabled: true
  config:
    reconcile_interval: "30s"

edge-gateway:
  config:
    scheduler_enabled: true
    scheduler_url: "http://scheduler:8080"

internal-gateway:
  config:
    allowed_callers:
      - "edge-gateway"
      - "scheduler"  # Allow scheduler to call internal-gateway
```

## Operations

### Register a Cluster

```bash
curl -X POST http://scheduler:8080/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_id": "us-west-1",
    "internal_gateway_url": "http://internal-gateway-us-west-1:8443",
    "weight": 100,
    "enabled": true
  }'
```

### Create a Template

```bash
curl -X POST http://edge-gateway/api/v1/templates \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "python-template",
    "namespace": "sandbox0",
    "spec": {
      "image": "sandbox0ai/python:3.11",
      "pool": {
        "minIdle": 10,
        "maxIdle": 50
      }
    }
  }'
```

### Monitor Reconciliation

```bash
# Check health
curl http://scheduler:8080/readyz

# View metrics
curl http://scheduler:8080/metrics | grep scheduler_
```

## Future Enhancements

- **Sandbox Claim Routing**: Route sandbox create/claim requests to appropriate clusters
- **Advanced Capacity Models**: Use CPU/memory metrics for more accurate capacity estimation
- **Multi-Region Support**: Prioritize clusters based on user proximity
- **Auto-Scaling Integration**: Trigger cluster scaling based on capacity pressure
- **Template Versioning**: Support multiple versions of the same template

## Troubleshooting

### Template not syncing to cluster

1. Check scheduler logs for sync errors
2. Verify cluster is registered and enabled
3. Check `scheduler_template_sync_status` metric
4. Verify internal-gateway is reachable from scheduler

### Capacity clamping too aggressive

1. Review `scheduler_capacity_clamps_total` metric
2. Check cluster capacity: `scheduler_cluster_capacity`
3. Adjust cluster weights or add more nodes
4. Consider adjusting the pods-per-node estimate in `computeAllocations`

### Reconcile not running

1. Check readiness endpoint: `/readyz`
2. Look for `last_reconcile_error` in response
3. Check scheduler logs for errors
4. Verify database connectivity
