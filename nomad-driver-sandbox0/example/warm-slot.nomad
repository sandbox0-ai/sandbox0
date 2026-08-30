variable "datacenter" {
  type        = string
  description = "Nomad datacenter that owns this regional Sandbox0 pool"
  default     = "dc1"
}

job "sandbox0-warm-slots" {
  datacenters = [var.datacenter]
  node_pool   = "sandbox0"
  type        = "system"

  constraint {
    attribute = "${meta.sandbox0_dedicated}"
    value     = "true"
  }

  # Enrollment starts every disposable client as unadmitted. Exact Nomad and
  # node-authority identities, route setup, ctld, and capacity heartbeat must
  # be ready before this metadata changes to true.
  constraint {
    attribute = "${meta.sandbox0_admitted}"
    value     = "true"
  }

  # A system job creates exactly eight single-use carriers on every admitted
  # node. Six serve the standard class and two retain guest-confined privileged
  # compatibility without binding CPU or memory limits into the carrier class.
  group "warm-0" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "standard"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-1" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "standard"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-2" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "standard"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-3" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "standard"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-4" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "standard"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-5" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "standard"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-6" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "privileged"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

  group "warm-7" {
    restart {
      attempts = 0
      mode     = "fail"
    }

    network {
      mode = "cni/sandbox0"

      port "procd" {
        to = 49983
      }
    }

    task "slot" {
      driver = "sandbox0-gvisor"

      config {
        command        = "/procd"
        args           = []
        security_class = "privileged"
      }

      resources {
        # These values reserve only carrier and driver overhead. Manager and
        # ctld assign the claimed sandbox CPU and memory from node capacity.
        cpu    = 50
        memory = 64
      }
    }
  }

}
