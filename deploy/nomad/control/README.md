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
