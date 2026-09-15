// Package nomadcni defines the routed carrier network installation contract.
package nomadcni

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const MaxConfigBytes = 2 << 20

// RoutedConfig converts the supported legacy bridge configuration to stock
// CNI ptp while preserving IPAM, masquerading and chained firewall/portmap
// settings. Callers may install it only on a fresh or fully drained node:
// changing a CNI file cannot change an existing allocation's network namespace.
func RoutedConfig(payload []byte) ([]byte, error) {
	if len(payload) == 0 || len(payload) > MaxConfigBytes {
		return nil, errors.New("CNI configuration size is invalid")
	}
	var config map[string]json.RawMessage
	if err := json.Unmarshal(payload, &config); err != nil || config == nil {
		return nil, errors.New("CNI configuration must be a JSON object")
	}
	var plugins []map[string]json.RawMessage
	if err := json.Unmarshal(config["plugins"], &plugins); err != nil || len(plugins) == 0 {
		return nil, errors.New("CNI configuration requires a plugin list")
	}
	dataPlanes, changed := 0, false
	for _, plugin := range plugins {
		var kind string
		if err := json.Unmarshal(plugin["type"], &kind); err != nil {
			return nil, errors.New("CNI plugin requires a type")
		}
		switch kind {
		case "loopback", "firewall", "portmap", "bandwidth", "tuning":
			continue
		case "ptp", "bridge":
			dataPlanes++
		default:
			return nil, fmt.Errorf("unsupported carrier CNI plugin %q", kind)
		}
		var ipam map[string]json.RawMessage
		if err := json.Unmarshal(plugin["ipam"], &ipam); err != nil || len(ipam) == 0 {
			return nil, errors.New("carrier CNI plugin requires IPAM")
		}
		if kind == "ptp" {
			continue
		}
		// Bridge-specific VLAN, hairpin or spoofing options cannot silently be
		// discarded; these topologies need an explicit operator migration.
		for key, value := range plugin {
			switch key {
			case "type", "bridge", "isGateway", "isDefaultGateway", "forceAddress",
				"ipam", "ipMasq", "ipMasqBackend", "mtu", "dns", "capabilities", "args":
			case "hairpinMode", "promiscMode", "portIsolation", "macspoofchk", "enabledad", "disableContainerInterface":
				if !bytes.Equal(bytes.TrimSpace(value), []byte("false")) {
					return nil, fmt.Errorf("unsupported legacy bridge option %q", key)
				}
			default:
				return nil, fmt.Errorf("unsupported legacy bridge option %q", key)
			}
		}
		plugin["type"] = json.RawMessage(`"ptp"`)
		for _, key := range []string{"bridge", "isGateway", "isDefaultGateway", "forceAddress", "hairpinMode",
			"promiscMode", "portIsolation", "macspoofchk", "enabledad", "disableContainerInterface"} {
			delete(plugin, key)
		}
		changed = true
	}
	if dataPlanes != 1 {
		return nil, errors.New("carrier CNI configuration requires exactly one ptp data plane")
	}
	if !changed {
		return bytes.Clone(payload), nil
	}
	var err error
	config["plugins"], err = json.Marshal(plugins)
	if err != nil {
		return nil, err
	}
	result, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		return nil, err
	}
	if len(result)+1 > MaxConfigBytes {
		return nil, errors.New("routed CNI configuration exceeds its size bound")
	}
	return append(result, '\n'), nil
}
