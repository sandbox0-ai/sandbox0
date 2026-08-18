# The Nomad client must explicitly set dev_smoke_enabled = true.
job "sandbox0-gvisor-dev-smoke" {
  datacenters = ["dc1"]
  type        = "batch"

  group "smoke" {
    task "run" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/bin/sh"
        args           = ["-c", "echo gvisor-smoke"]
        wait_for_claim = false
        rootfs_path    = "/var/lib/sandbox0/rootfs/alpine"
      }
    }
  }
}
