package meteringbackend

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOpenConfigPreservesGatewayMeteringConfiguration(t *testing.T) {
	cfg := config.MeteringClickHouseConfig{
		DSN:                 "  clickhouse://example.test  ",
		Database:            "metering",
		EventsTable:         "events",
		WindowsTable:        "windows",
		WatermarksTable:     "watermarks",
		SandboxStateTable:   "sandbox_state",
		StorageStateTable:   "storage_state",
		SkipSchemaMigration: true,
		ConnectTimeout:      metav1.Duration{Duration: 3 * time.Second},
	}

	got := openConfig(cfg)
	require.Equal(t, "clickhouse://example.test", got.DSN)
	require.Equal(t, "metering", got.Schema.Database)
	require.Equal(t, "events", got.Schema.EventsTable)
	require.Equal(t, "windows", got.Schema.WindowsTable)
	require.Equal(t, "watermarks", got.Schema.WatermarksTable)
	require.Equal(t, "sandbox_state", got.Schema.SandboxStateTable)
	require.Equal(t, "storage_state", got.Schema.StorageStateTable)
	require.False(t, got.Migrate)
	require.Equal(t, 3*time.Second, connectTimeout(cfg))
}

func TestConnectTimeoutUsesGatewayDefault(t *testing.T) {
	require.Equal(t, 10*time.Second, connectTimeout(config.MeteringClickHouseConfig{}))
}
