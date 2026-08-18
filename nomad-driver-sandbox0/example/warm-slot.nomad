job "sandbox0-warm-slots" {
  datacenters = ["dc1"]
  type        = "service"

  group "warm" {
    count = 4

    network {
      mode = "bridge"
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        wait_for_claim = true
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
