# netd - Network Data Plane for sandbox0

netd is the node-level network data plane service for sandbox0. It provides:

- **Network Policy Enforcement**: Default deny egress/ingress with policy-based allow rules
- **L7 Proxy**: Transparent HTTP/HTTPS proxy with SNI/Host inspection for domain-based filtering
- **DNS Rebinding Protection**: Prevents DNS rebinding attacks by validating resolved IPs
- **Bandwidth Control**: Per-sandbox rate limiting using tc/HTB
- **Traffic Accounting**: Per-sandbox bytes/packets/connections metrics
- **Audit Logging**: L7 traffic audit logs for security compliance

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Node                                    │
│  ┌─────────────────┐    ┌─────────────────┐                     │
│  │   Sandbox Pod   │    │   Sandbox Pod   │                     │
│  │  ┌───────────┐  │    │  ┌───────────┐  │                     │
│  │  │   procd   │  │    │  │   procd   │  │                     │
│  │  └───────────┘  │    │  └───────────┘  │                     │
│  │       │         │    │       │         │                     │
│  │  ┌────▼────┐    │    │  ┌────▼────┐    │                     │
│  │  │  veth   │    │    │  │  veth   │    │                     │
│  │  └────┬────┘    │    │  └────┬────┘    │                     │
│  └───────│─────────┘    └───────│─────────┘                     │
│          │                      │                                │
│          ▼                      ▼                                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                          netd                             │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │   │
│  │  │   Watcher    │  │  DataPlane   │  │   L7 Proxy   │    │   │
│  │  │  (K8s API)   │  │ (iptables/tc)│  │  (HTTP/TLS)  │    │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### Watcher
- Watches Pods with `sandbox0.ai/pool-type=active` label on this node
- Watches `SandboxNetworkPolicy` and `SandboxBandwidthPolicy` CRDs
- Maintains `podIP -> sandboxID` mapping for identity lookup

### DataPlane
- Applies iptables rules for egress/ingress filtering
- Redirects HTTP (80) and HTTPS (443) to L7 proxy
- Applies tc/HTB rules for bandwidth shaping
- Cleanup rules on pod deletion

### L7 Proxy
- Transparent proxy for HTTP/HTTPS traffic
- Extracts domain from HTTP Host header or TLS SNI
- Checks policy for allow/deny decision
- DNS rebinding protection (blocks internal IP resolution)
- Audit logging for all connections

## CRDs

### SandboxNetworkPolicy
Defines network egress/ingress rules per sandbox:
- Allowed/denied CIDRs
- Allowed/denied domains (with wildcard support)
- Allowed ports
- DNS policy
- Audit settings

### SandboxBandwidthPolicy
Defines bandwidth limits per sandbox:
- Egress rate limit (bps)
- Ingress rate limit (bps)
- Burst size
- Accounting settings

## Configuration

Environment variables:
- `NODE_NAME`: Name of the node (required)
- `LOG_LEVEL`: Logging level (debug, info, warn, error)
- `METRICS_PORT`: Prometheus metrics port (default: 9090)
- `HEALTH_PORT`: Health check port (default: 8080)
- `PROXY_HTTP_PORT`: HTTP proxy port (default: 18080)
- `PROXY_HTTPS_PORT`: HTTPS proxy port (default: 18443)
- `FAIL_CLOSED`: Block traffic when netd is not ready (default: true)
- `STORAGE_PROXY_CIDR`: CIDR for storage-proxy (always allowed)
- `CLUSTER_DNS_CIDR`: CIDR for cluster DNS (allowed for DNS queries)
- `INTERNAL_GATEWAY_CIDR`: CIDR for internal-gateway (allowed ingress to procd)

## Deployment

```bash
# Deploy CRDs first
kubectl apply -f ../manager/deploy/k8s/crd-sandbox-network-policy.yaml
kubectl apply -f ../manager/deploy/k8s/crd-sandbox-bandwidth-policy.yaml

# Deploy netd
kubectl apply -k deploy/k8s/
```

## Security Considerations

1. **Privilege Required**: netd runs as privileged container with NET_ADMIN capability
2. **Fail Closed**: By default, traffic is blocked if netd is not ready or policy is missing
3. **Platform Deny List**: RFC1918, metadata services, and cluster networks are always blocked
4. **DNS Rebinding**: Resolved IPs are validated to prevent internal access via DNS
5. **Audit Logging**: All L7 connections are logged for security compliance

## Metrics

- `netd_sandbox_connections_total`: Connections per sandbox
- `netd_sandbox_bytes_total`: Bytes transferred per sandbox
- `netd_proxy_request_duration_seconds`: Proxy request latency
- `netd_dns_resolution_duration_seconds`: DNS resolution latency
- `netd_active_sandboxes`: Number of active sandboxes on node
- `netd_rule_application_errors_total`: Rule application failures

