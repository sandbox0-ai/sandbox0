job "sandbox0-warm-slots" {
  datacenters = ["dc1"]
  type        = "service"

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
        cores  = 1
        memory = 1024
      }
    }
  }
}
