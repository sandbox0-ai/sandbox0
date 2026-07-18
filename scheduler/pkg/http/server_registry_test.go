package http

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrivateRegistryHostsIncludesInternalAlias(t *testing.T) {
	assert.Equal(t, []string{
		"registry.example.com",
		"registry.sandbox0-system.svc:5000",
	}, privateRegistryHosts(
		"registry.example.com",
		"registry.example.com",
		"registry.sandbox0-system.svc:5000",
	))
}
