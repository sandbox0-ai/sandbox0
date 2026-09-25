package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type RuntimeNodeCachePrewarmKey struct {
	WindowName, ClusterID, NodeUID, NodeBootID, TemplateDigest string
}

func (k RuntimeNodeCachePrewarmKey) validate() error {
	for _, value := range []string{k.WindowName, k.ClusterID, k.NodeUID, k.NodeBootID, k.TemplateDigest} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return ErrRuntimeSlotInvalid
		}
	}
	return nil
}

// ListElasticRuntimeCarrierNodeUIDs excludes fixed workers from synthetic work.
func (s *PGSandboxStore) ListElasticRuntimeCarrierNodeUIDs(ctx context.Context, cluster string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_uid FROM manager.runtime_node_instances
		WHERE cluster_id=$1 AND pool_kind='elastic' AND state='active'`, cluster)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]bool{}
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		result[uid] = true
	}
	return result, rows.Err()
}

// BeginRuntimeNodeCachePrewarm grants one bounded attempt. A complete record
// is never reacquired; an expired or failed attempt returns its predecessor
// number so its synthetic sandbox can be cleaned before retrying.
func (s *PGSandboxStore) BeginRuntimeNodeCachePrewarm(ctx context.Context, key RuntimeNodeCachePrewarmKey) (attempt, previous int, acquired bool, err error) {
	if err = key.validate(); err != nil {
		return
	}
	err = s.pool.QueryRow(ctx, `INSERT INTO manager.runtime_node_cache_prewarms
		(window_name,cluster_id,node_uid,node_boot_id,template_digest,lease_expires_at)
		VALUES($1,$2,$3,$4,$5,NOW()+INTERVAL '3 minutes')
		ON CONFLICT(window_name,node_uid,node_boot_id,template_digest) DO UPDATE SET
		attempt=manager.runtime_node_cache_prewarms.attempt+1, state='running',
		lease_expires_at=NOW()+INTERVAL '3 minutes', last_error=NULL, updated_at=NOW()
		WHERE (manager.runtime_node_cache_prewarms.state='failed'
			AND manager.runtime_node_cache_prewarms.lease_expires_at<NOW())
			OR (manager.runtime_node_cache_prewarms.state='running'
				AND manager.runtime_node_cache_prewarms.lease_expires_at<NOW())
		RETURNING attempt`, key.WindowName, key.ClusterID, key.NodeUID, key.NodeBootID, key.TemplateDigest).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	return attempt, attempt - 1, true, nil
}

func (s *PGSandboxStore) FinishRuntimeNodeCachePrewarm(ctx context.Context, key RuntimeNodeCachePrewarmKey, attempt int, succeeded bool, reason string) error {
	if err := key.validate(); err != nil {
		return err
	}
	if attempt < 1 {
		return ErrRuntimeSlotInvalid
	}
	state := "failed"
	if succeeded {
		state, reason = "complete", ""
	}
	if len(reason) > 512 {
		reason = reason[:512]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE manager.runtime_node_cache_prewarms SET state=$6,
		completed_at=CASE WHEN $6='complete' THEN NOW() ELSE NULL END,
		last_error=NULLIF($7,''), lease_expires_at=CASE WHEN $6='failed' THEN NOW()+INTERVAL '1 minute' ELSE lease_expires_at END,
		updated_at=NOW() WHERE window_name=$1 AND cluster_id=$2
		AND node_uid=$3 AND node_boot_id=$4 AND template_digest=$5 AND attempt=$8 AND state='running'`,
		key.WindowName, key.ClusterID, key.NodeUID, key.NodeBootID, key.TemplateDigest, state, reason, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("prewarm attempt %d lost authority", attempt)
	}
	return nil
}

// Cache prewarm completion waits for asynchronous deletion rather than
// equating a delete request with an available warm carrier.
func (s *PGSandboxStore) RuntimeNodeCachePrewarmRestored(ctx context.Context, sandboxID string, node RuntimeCarrierNode, compatibility string, readyBefore int) (bool, error) {
	var deleted bool
	err := s.pool.QueryRow(ctx, `SELECT deleted_at IS NOT NULL FROM manager.sandboxes WHERE sandbox_id=$1`, sandboxID).Scan(&deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	var ready int
	err = s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.runtime_slots WHERE cluster_id=$1
		AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 AND compatibility_digest=$5
		AND state='fastpath_ready' AND NOT carrier_retired AND heartbeat_expires_at>NOW()
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases reserved WHERE reserved.slot_id=runtime_slots.slot_id)`,
		node.ClusterID, node.NodeID, node.NodeUID, node.NodeBootID, compatibility).Scan(&ready)
	return deleted && ready >= readyBefore, err
}
