package sandboxstore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadCheckpointImageGCWaitsForLastForkAndCollectsExactObjectsIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "checkpoint-image-gc")
	var id string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT checkpoint_id FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&id))
	blocked := func() {
		t.Helper()
		binding, err := f.store.AuthorizeNomadCheckpointImageGC(f.ctx, id)
		require.NoError(t, err)
		require.Nil(t, binding)
	}
	blocked()
	var stagingPayload []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT evidence->'staging' FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&stagingPayload))
	var staging protocol.MigrationStagingRequest
	require.NoError(t, json.Unmarshal(stagingPayload, &staging))
	require.Equal(t, 1, staging.CaptureUpload.Version)
	scope, err := staging.CaptureUpload.Scope(staging.Source)
	require.NoError(t, err)
	beforeCapture, err := f.store.AuthorizeNomadCheckpointCaptureUploadGC(f.ctx, id)
	require.NoError(t, err)
	require.Nil(t, beforeCapture)
	child, err := f.store.ForkNomadPausedSandbox(f.ctx, memoryForkRequest(t, f, f.sandboxID, "checkpoint-gc-child", "checkpoint-gc-fork"))
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='deleted',deleted_at=clock_timestamp() WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	blocked()
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='deleted',deleted_at=clock_timestamp() WHERE sandbox_id=$1`, child.ID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, child.ID)
	require.NoError(t, err)
	ids, err := f.store.ListNomadCheckpointImageGC(f.ctx, "", 8)
	require.NoError(t, err)
	require.Equal(t, []string{id}, ids)
	binding, err := f.store.AuthorizeNomadCheckpointImageGC(f.ctx, id)
	require.NoError(t, err)
	require.NotNil(t, binding)
	require.Equal(t, id, binding.OperationID)
	wrong := *binding
	wrong.TeamID = "another-team"
	require.ErrorIs(t, f.store.CompleteNomadCheckpointImageGC(f.ctx, wrong), ErrNomadCheckpointConflict)
	digest, err := binding.Digest()
	require.NoError(t, err)
	objects := objectstore.NewMemoryStore("")
	key := "runtime-checkpoints/v1/" + strings.TrimPrefix(digest, "sha256:") + "/manifest.json"
	require.NoError(t, objects.Put(key, strings.NewReader("test image")))
	collector, err := runtimecheckpoint.NewCollector(objects)
	require.NoError(t, err)
	worker, err := nomadmigration.NewCheckpointImageGC(f.store, collector)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 0, result.Advanced, "the first bounded pass removes the object")
	result, err = worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.NoError(t, f.store.CompleteNomadCheckpointImageGC(context.Background(), *binding))
	captureWorker, err := nomadmigration.NewCheckpointCaptureUploadGC(f.store, collector)
	require.NoError(t, err)
	result, err = captureWorker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.NoError(t, f.store.CompleteNomadCheckpointCaptureUploadGC(f.ctx, scope))
	captureIDs, err := f.store.ListNomadCheckpointCaptureUploadGC(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, captureIDs)
	ids, err = f.store.ListNomadCheckpointImageGC(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints SET image_gc_completed_at=NULL WHERE operation_id=$1`, id)
	require.Error(t, err)
}
