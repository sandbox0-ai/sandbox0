package aliyunclient_test

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoolautoscaler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodepoollifecycle"
	"github.com/stretchr/testify/require"
)

func TestNodePoolClientsInitializeBeforeInstanceCredentialsAreResolved(t *testing.T) {
	for _, key := range []string{"ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ALIBABA_CLOUD_SECURITY_TOKEN", "ALIBABA_CLOUD_ECS_METADATA"} {
		t.Setenv(key, "")
	}
	// No metadata service is available in this test. Authentication must remain
	// lazy and refreshable instead of snapshotting a token during manager startup.
	t.Setenv("ALIBABA_CLOUD_ECS_METADATA_DISABLED", "true")
	_, err := nodepoolautoscaler.NewAliyunESS("us-east-1", "group")
	require.NoError(t, err)
	_, err = nodepoollifecycle.NewAliyunCloud("us-east-1", "group", []string{"route"})
	require.NoError(t, err)
	_, err = nodeenrollment.NewAliyunCloud("us-east-1", "group", []string{"route"})
	require.NoError(t, err)
}
