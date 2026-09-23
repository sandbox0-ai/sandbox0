variable "datacenter" {
  type        = string
  description = "Nomad datacenter that owns this regional Sandbox0 pool"
  default     = "dc1"
}

variable "privileged_slots" {
  type        = number
  description = "Single-use guest-confined privileged carriers per admitted node"
  default     = 256
  validation {
    condition     = var.privileged_slots >= 0 && var.privileged_slots <= 256 && floor(var.privileged_slots) == var.privileged_slots
    error_message = "Privileged slots must be an integer from 0 through 256."
  }
}

variable "warm_shard" {
  type        = number
  description = "Render one bounded carrier job; register each nonempty shard from 0 through 23"
  default     = 0
  validation {
    condition     = var.warm_shard >= 0 && var.warm_shard <= 23 && floor(var.warm_shard) == var.warm_shard
    error_message = "Warm shard must be an integer from 0 through 23."
  }
}

variable "adaptive_carriers" {
  type        = bool
  description = "Manager owns versioned per-node membership beyond the eight enrollment carriers"
  default     = false
}

job "sandbox0-warm-slots" {
  id          = var.warm_shard == 0 ? "sandbox0-warm-slots" : format("sandbox0-warm-slots-shard-%02d", var.warm_shard)
  name        = var.warm_shard == 0 ? "sandbox0-warm-slots" : format("sandbox0-warm-slots-shard-%02d", var.warm_shard)
  datacenters = [var.datacenter]
  node_pool   = "sandbox0"
  type        = "system"

  meta {
    sandbox0_adaptive_carriers = var.adaptive_carriers ? "v1" : "disabled"
  }

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

  # Carrier identities remain stable when counts change. The default uses only
  # privileged carriers. Nomad embeds the entire job in every allocation, so each
  # shard has at most 32 groups. Keep a carrier's shard independent of the
  # requested pool size. Scale only after validating memory, NBD and IP capacity.
  dynamic "group" {
    for_each = {
      for index in range(var.privileged_slots) : (index < 2 ? format("warm-%d", index + 6) : format("privileged-%d", index)) => index
      if floor((index < 2 ? index + 6 : 512 + index) / 32) == var.warm_shard
    }
    labels = [group.key]
    content {
      # Additional carriers require an explicit per-node density profile.
      # Expanding the job leaves existing nodes capped by their metadata.
      dynamic "constraint" {
        for_each = group.value >= 2 ? [1] : []
        content {
          attribute = "${meta.sandbox0_privileged_carriers}"
          operator  = ">="
          value     = format("%d", group.value + 1)
        }
      }

      # A zero UUID places no extra carriers until the regional controller
      # grants membership. Per-node density metadata remains an independent
      # NBD/IP/host ceiling. Changes use Nomad's JobModifyIndex CAS.
      dynamic "constraint" {
        for_each = var.adaptive_carriers && group.value >= 2 ? [1] : []
        content {
          attribute = "${node.unique.id}"
          operator  = "set_contains_any"
          value     = "00000000-0000-0000-0000-000000000000"
        }
      }

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

        # Nomad's external-driver RPC does not forward DisableLogCollection.
        # Carriers never use these FIFOs; guest output is served by procd.
        logs {
          disabled = true
        }

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

}
