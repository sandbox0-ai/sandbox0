job "sandbox0-warm-slots" {
  datacenters = ["dc1"]
  node_pool   = "sandbox0"
  type        = "service"

  # Sandbox0 owns allocatable CPU and memory inside these nodes. Do not place
  # warm carriers on a Nomad client that also accepts general workloads.
  constraint {
    attribute = "${meta.sandbox0_dedicated}"
    value     = "true"
  }

  group "warm" {
    # Eight is the minimum pool width used by the production acceptance gate.
    # Keep replacement capacity and the matching ctld NBD pool at least this
    # wide when changing this value.
    count = 8

    # A warm allocation is consumed exactly once. The driver terminates its
    # task after revoking the sandbox, and only a fresh allocation may replace
    # it because the old network namespace and runtime-slot journal are no
    # longer reusable.
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "bridge"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        wait_for_claim = true
      }

      resources {
        # These values reserve only the carrier/driver overhead. They are not
        # the sandbox limit and must not be copied into OCI. Manager leases
        # sandbox CPU and memory from ctld-reported dedicated-node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }
}
