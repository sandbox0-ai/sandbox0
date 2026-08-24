package runtimeslotterminal

import "testing"

func TestNewRequiresExplicitEnablement(t *testing.T) {
	worker, err := New(nil, nil, Config{})
	if err != nil || worker != nil {
		t.Fatalf("disabled worker = %v, %v", worker, err)
	}
	_, err = New(nil, nil, Config{NomadEndpointsFile: "/etc/sandbox0/nomad-endpoints.json"})
	if err == nil {
		t.Fatal("disabled worker accepted a silently ignored endpoint catalog")
	}
	_, err = New(nil, nil, Config{Enabled: true})
	if err == nil {
		t.Fatal("enabled worker accepted missing dependencies")
	}
}
