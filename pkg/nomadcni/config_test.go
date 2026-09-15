package nomadcni

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoutedConfigPreservesRegionalAddressAndFirewallContracts(t *testing.T) {
	before := []byte(`{"cniVersion":"1.0.0","name":"sandbox0","plugins":[
		{"type":"loopback"},
		{"type":"bridge","bridge":"s0nomad","isGateway":true,"forceAddress":true,"hairpinMode":false,"ipMasq":true,"mtu":1500,
		 "ipam":{"type":"host-local","ranges":[[{"subnet":"172.26.64.0/20"}]],"routes":[{"dst":"0.0.0.0/0"}],"dataDir":"/var/run/cni"}},
		{"type":"firewall","backend":"iptables","iptablesAdminChainName":"S0-NOMAD-ADMIN"},
		{"type":"portmap","capabilities":{"portMappings":true},"snat":true}]}`)
	after, err := RoutedConfig(before)
	require.NoError(t, err)
	var original, migrated map[string]any
	require.NoError(t, json.Unmarshal(before, &original))
	require.NoError(t, json.Unmarshal(after, &migrated))
	plugins := original["plugins"].([]any)
	carrier := plugins[1].(map[string]any)
	carrier["type"] = "ptp"
	for _, key := range []string{"bridge", "isGateway", "forceAddress", "hairpinMode"} {
		delete(carrier, key)
	}
	require.Equal(t, original, migrated)
	again, err := RoutedConfig(after)
	require.NoError(t, err)
	require.Equal(t, after, again)
}

func TestRoutedConfigRejectsAmbiguousOrUnsupportedTopology(t *testing.T) {
	for _, payload := range []string{
		`{}`, `{"plugins":[]}`, `{"plugins":[{"type":"bridge"}]}`,
		`{"plugins":[{"type":"macvlan","ipam":{"type":"host-local"}}]}`,
		`{"plugins":[{"type":"ptp","ipam":{"type":"host-local"}},{"type":"ptp","ipam":{"type":"host-local"}}]}`,
		`{"plugins":[{"type":"bridge","vlan":20,"ipam":{"type":"host-local"}}]}`,
		`{"plugins":[{"type":"bridge","hairpinMode":true,"ipam":{"type":"host-local"}}]}`,
	} {
		t.Run(payload, func(t *testing.T) {
			_, err := RoutedConfig([]byte(payload))
			require.Error(t, err)
		})
	}
}
