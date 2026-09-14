variable "datacenter" {
  type        = string
  description = "Nomad datacenter that owns this regional Sandbox0 pool"
  default     = "dc1"
}

variable "standard_slots" {
  type        = number
  description = "Single-use standard carriers per admitted node; size NBD, IP, and host resources consistently"
  default     = 6
  validation {
    condition     = var.standard_slots >= 0 && var.standard_slots <= 512 && floor(var.standard_slots) == var.standard_slots
    error_message = "Standard slots must be an integer from 0 through 512."
  }
}

variable "privileged_slots" {
  type        = number
  description = "Single-use guest-confined privileged carriers per admitted node"
  default     = 2
  validation {
    condition     = var.privileged_slots >= 0 && var.privileged_slots <= 64 && floor(var.privileged_slots) == var.privileged_slots
    error_message = "Privileged slots must be an integer from 0 through 64."
  }
}

variable "warm_shard" {
  type        = number
  description = "Render one bounded carrier job; register each nonempty shard from 0 through 17"
  default     = 0
  validation {
    condition     = var.warm_shard >= 0 && var.warm_shard <= 17 && floor(var.warm_shard) == var.warm_shard
    error_message = "Warm shard must be an integer from 0 through 17."
  }
}

job "sandbox0-warm-slots" {
  id          = var.warm_shard == 0 ? "sandbox0-warm-slots" : format("sandbox0-warm-slots-shard-%02d", var.warm_shard)
  name        = var.warm_shard == 0 ? "sandbox0-warm-slots" : format("sandbox0-warm-slots-shard-%02d", var.warm_shard)
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

  # Carrier identities remain stable when counts change. The default retains
  # warm-0..5 as standard and warm-6..7 as privileged; added standard carriers
  # start at warm-8. Nomad embeds the entire job in every allocation, so each
  # shard has at most 32 groups. Keep a carrier's shard independent of the
  # requested pool size. Scale only after validating memory, NBD and IP capacity.
  dynamic "group" {
    for_each = merge(
      { for index in range(var.standard_slots) : format("warm-%d", index < 6 ? index : index + 2) => { security_class = "standard", index = index } if floor((index < 6 ? index : index + 2) / 32) == var.warm_shard },
      { for index in range(var.privileged_slots) : (index < 2 ? format("warm-%d", index + 6) : format("privileged-%d", index)) => { security_class = "privileged", index = index } if floor((index < 2 ? index + 6 : 512 + index) / 32) == var.warm_shard }
    )
    labels = [group.key]
    content {
      # Additional carriers require an explicit per-node density profile.
      # Expanding the job therefore leaves unconfigured existing nodes at 6/2.
      dynamic "constraint" {
        for_each = group.value.index >= (group.value.security_class == "standard" ? 6 : 2) ? [1] : []
        content {
          attribute = group.value.security_class == "standard" ? "${meta.sandbox0_standard_carriers}" : "${meta.sandbox0_privileged_carriers}"
          operator  = ">="
          value     = format("%d", group.value.index + 1)
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

        config {
          command        = "/procd"
          args           = []
          security_class = group.value.security_class
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
