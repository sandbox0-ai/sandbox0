package nodebootstrap

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

func renderNomadClientConfig(config Config, nodeName, privateIP string, admitted bool) ([]byte, error) {
	address, err := netip.ParseAddr(strings.TrimSpace(privateIP))
	if err != nil || !address.Is4() || !address.IsPrivate() {
		return nil, errors.New("nomad client private address is invalid")
	}
	if len(nodeName) == 0 || len(nodeName) > 63 || !safeRegionID.MatchString(nodeName) {
		return nil, errors.New("nomad client node name is invalid")
	}
	datacenter := strings.ReplaceAll(config.RegionID, "-", "_")
	servers := make([]string, len(config.NomadServers))
	for index, server := range config.NomadServers {
		servers[index] = strconv.Quote(server)
	}
	payload := fmt.Sprintf(`region = %[1]q
datacenter = %[1]q
name = %[2]q
bind_addr = "0.0.0.0"
disable_update_check = true
leave_on_interrupt = false
leave_on_terminate = false
plugin_dir = "/opt/nomad/plugins"

ports {
  http = 4646
  rpc  = 4647
  serf = 4648
}

advertise {
  http = %[3]q
  rpc  = %[4]q
  serf = %[5]q
}

tls {
  http                   = true
  rpc                    = true
  ca_file                = "/etc/nomad.d/nomad-ca.pem"
  cert_file              = "/etc/nomad.d/nomad.pem"
  key_file               = "/etc/nomad.d/nomad-key.pem"
  verify_server_hostname = true
  verify_https_client     = true
}

acl {
  enabled = true
}

consul {
  auto_advertise   = false
  server_auto_join = false
  client_auto_join = false
}

telemetry {
  disable_hostname           = true
  prometheus_metrics         = true
  publish_allocation_metrics = true
  publish_node_metrics       = true
}

data_dir = "/opt/nomad/data"

client {
  enabled          = true
  node_pool        = "sandbox0"
  servers          = [%[6]s]
  alloc_dir        = "/opt/nomad/alloc"
  alloc_mounts_dir = "/opt/nomad/alloc_mounts"
  cni_path         = "/opt/cni/bin"
  cni_config_dir   = "/opt/cni/config"

  meta {
    sandbox0_dedicated = "true"
    sandbox0_runtime   = "gvisor"
    sandbox0_admitted  = %[7]q
  }

  reserved {
    cpu    = %[8]d
    memory = %[9]d
    disk   = %[10]d
  }
}
`, datacenter, nodeName, address.String()+":4646", address.String()+":4647",
		address.String()+":4648", strings.Join(servers, ", "), strconv.FormatBool(admitted),
		config.ReservedCPUMHz, config.ReservedMemoryMB, config.ReservedDiskMB)
	return []byte(payload), nil
}
