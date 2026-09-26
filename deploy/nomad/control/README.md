# Control service host deployment

Run `regional-gateway`, optional `scheduler`, `cluster-gateway`, `manager`, and
`ssh-gateway` as ordinary host services or equivalent Nomad service jobs. The
example systemd unit uses one config and optional environment file per binary:

```sh
sudo install -o root -g root -m 0644 sandbox0-control@.service /etc/systemd/system/
sudo install -o root -g root -m 0644 sandbox0-control.target /etc/systemd/system/
sudo install -d -o root -g sandbox0 -m 0750 /etc/sandbox0 /var/lib/sandbox0
sudo install -o root -g sandbox0 -m 0640 manager.yaml /etc/sandbox0/manager.yaml
sudo install -o root -g root -m 0755 manager /usr/local/bin/manager
sudo systemctl daemon-reload
sudo systemctl enable --now sandbox0-control@manager.service
```

Create a dedicated `sandbox0` system user. Keep database, object-store, signing,
registry, and Nomad credentials in root-owned files or the mode-0600 service
environment file. Expand the manager example for the enabled regional features;
it intentionally contains placeholders and cannot be deployed unchanged.

Run at least two manager instances against the same regional PostgreSQL and S3.
Use the PostgreSQL writer endpoint. Each manager needs the same writer token
key, runtime-class catalog, terminal endpoint catalog, node CA, and credential
encryption key. Node certificates map to exact cluster/node/node-UID/agent-UID
identities in every replica.

## PostgreSQL pool diagnostics

Manager exports `manager_pg_pool_*` metrics on its metrics listener. The
connection gauges and acquisition counters come from one in-memory pgx pool
snapshot; scraping them does not issue SQL. The
`empty_acquires_total` and `empty_acquire_wait_seconds_total` counters measure
successful acquisitions that had to wait for a connection to become available
or be constructed. Compare counter increases over the exact benchmark interval.
The wait total sums time across concurrent acquisitions, so it is not a single
request's latency.

Pool acquisition happens before the existing `manager_pgx_query_duration_seconds`
SQL measurement. Read both to distinguish connection waiting from query
execution and PostgreSQL lock waiting. The `acquired_connections` gauge reaching
`max_connections` shows pool saturation; PostgreSQL blocking relationships then
identify whether those connections are occupied by serial quota or node-capacity
transactions. Increasing the pool limit alone does not remove those locks.

## Sandbox CPU floor

New sandbox resource leases use at least 500 millicores, including claims from
older stored templates and memory overrides. Larger memory-derived CPU values
are preserved. Existing active resource leases keep their recorded limits.
Regional routing, manager admission, metering and node cgroups use the resolved
CPU value; ready carrier count alone does not establish available CPU capacity.
