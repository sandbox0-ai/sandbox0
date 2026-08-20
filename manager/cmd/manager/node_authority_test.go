package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
)

type managerNodeAuthorityTestStore struct {
	nodeauthority.Store
}

func TestBuildManagerNodeAuthorityKeepsDisabledPathEmpty(t *testing.T) {
	authority, err := buildManagerNodeAuthority(&config.ManagerConfig{}, nil)
	if err != nil || authority != nil {
		t.Fatalf("disabled authority = %v, %v", authority, err)
	}
}

func TestBuildManagerNodeAuthorityRejectsDisabledTerminalConfig(t *testing.T) {
	cfg := &config.ManagerConfig{}
	cfg.NodeAuthority.Terminal.NomadEndpointsFile = "/etc/sandbox0/nomad.json"
	authority, err := buildManagerNodeAuthority(cfg, nil)
	if err == nil || authority != nil {
		t.Fatalf("disabled configured authority = %v, %v", authority, err)
	}
}

func TestBuildManagerNodeAuthorityValidatesListenerBeforeCredentials(t *testing.T) {
	for name, mutate := range map[string]func(*config.NodeAuthorityConfig){
		"noncanonical host": func(cfg *config.NodeAuthorityConfig) {
			cfg.ListenHost = " 172.16.100.2"
			cfg.Port = 8421
		},
		"invalid port": func(cfg *config.NodeAuthorityConfig) {
			cfg.Port = 70000
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := &config.ManagerConfig{NodeAuthority: config.NodeAuthorityConfig{Enabled: true}}
			mutate(&cfg.NodeAuthority)
			authority, err := buildManagerNodeAuthority(cfg, &managerNodeAuthorityTestStore{})
			if err == nil || authority != nil {
				t.Fatalf("invalid authority = %v, %v", authority, err)
			}
		})
	}
}
