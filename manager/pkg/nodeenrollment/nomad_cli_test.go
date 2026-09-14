package nodeenrollment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNomadCLIValidatesInitialAndRenewalMetadata(t *testing.T) {
	for _, test := range []struct {
		name            string
		metadata        string
		alreadyAdmitted bool
		wantError       bool
	}{
		{name: "initial false", metadata: "false"},
		{name: "initial rejects true", metadata: "true", wantError: true},
		{name: "renewal true", metadata: "true", alreadyAdmitted: true},
		{name: "resumed renewal false", metadata: "false", alreadyAdmitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			binary := filepath.Join(directory, "nomad")
			response := fmt.Sprintf(`{"ID":"node-1","Name":"s0-i-1","HTTPAddr":"10.0.0.9:4646","NodePool":"sandbox0","Status":"ready","SchedulingEligibility":"ineligible","Meta":{"sandbox0_admitted":%q}}`, test.metadata)
			script := "#!/bin/sh\nprintf '%s' '" + response + "'\n"
			require.NoError(t, os.WriteFile(binary, []byte(script), 0o700))
			token := filepath.Join(directory, "token")
			require.NoError(t, os.WriteFile(token, []byte("token"), 0o600))
			cli, err := NewNomadCLI(NomadCLIConfig{
				Binary: binary, Address: "https://127.0.0.1:4646", Region: "ali_ue1",
				CACertFile: "/ca", ClientCertFile: "/cert", ClientKeyFile: "/key",
				TokenFile: token, NodePool: "sandbox0",
			})
			require.NoError(t, err)
			err = cli.ValidateRegisteredNode(context.Background(), "node-1", "s0-i-1",
				"10.0.0.9", test.alreadyAdmitted)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNomadNodeDetailAddressRequiresExactHostAndValidPort(t *testing.T) {
	for _, address := range []string{"", "10.0.0.9", "10.0.0.9:0", "10.0.0.9:65536", "10.0.0.9:http", "10.0.0.10:4646", "127.0.0.1:4646", "https://10.0.0.9:4646"} {
		t.Run(address, func(t *testing.T) {
			require.False(t, nomadNodeAddressMatches(address, "10.0.0.9"))
		})
	}
	require.True(t, nomadNodeAddressMatches("10.0.0.9:4646", "10.0.0.9"))
}

func TestNomadCLIAdmitsExactNodeUsingDetailHTTPAddress(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "nomad")
	response := `{"ID":"node-1","Name":"s0-i-1","HTTPAddr":"10.0.0.9:4646","NodePool":"sandbox0","Status":"ready","SchedulingEligibility":"eligible","Meta":{"sandbox0_admitted":"true"}}`
	enabled := filepath.Join(directory, "enabled")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$2\" = eligibility ]; then : >%q; exit 0; fi\nif [ -f %q ]; then printf '%%s' '%s'; else printf '%%s' '%s'; fi\n", enabled, enabled, response, strings.Replace(response, `"eligible"`, `"ineligible"`, 1))
	require.NoError(t, os.WriteFile(binary, []byte(script), 0o700))
	token := filepath.Join(directory, "token")
	require.NoError(t, os.WriteFile(token, []byte("token"), 0o600))
	cli, err := NewNomadCLI(NomadCLIConfig{Binary: binary, Address: "https://127.0.0.1:4646", Region: "ali_ue1", CACertFile: "/ca", ClientCertFile: "/cert", ClientKeyFile: "/key", TokenFile: token, NodePool: "sandbox0"})
	require.NoError(t, err)
	require.Error(t, cli.AdmitRegisteredNode(t.Context(), "node-1", "s0-i-1", "10.0.0.10"))
	require.NoFileExists(t, enabled)
	require.NoError(t, cli.AdmitRegisteredNode(t.Context(), "node-1", "s0-i-1", "10.0.0.9"))
}
