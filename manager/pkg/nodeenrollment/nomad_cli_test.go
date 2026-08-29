package nodeenrollment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
			response := fmt.Sprintf(`{"ID":"node-1","Name":"s0-i-1","Address":"10.0.0.9","NodePool":"sandbox0","Status":"ready","SchedulingEligibility":"ineligible","Meta":{"sandbox0_admitted":%q}}`, test.metadata)
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
