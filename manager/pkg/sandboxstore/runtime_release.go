package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

var releaseCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var releaseSHAPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var ErrRuntimeReleaseConflict = errors.New("runtime release operation conflicts with current authority")

// RuntimeReleaseArtifact is an immutable worker bundle, independent of the
// control-service release and the executable selected for new guest processes.
type RuntimeReleaseArtifact struct {
	SourceCommit string `json:"source_commit"`
	ObjectKey    string `json:"object_key"`
	SHA256       string `json:"sha256"`
	OSSEndpoint  string `json:"oss_endpoint"`
	OSSBucket    string `json:"oss_bucket"`
}

func (a RuntimeReleaseArtifact) Validate() error {
	endpoint, err := url.Parse(a.OSSEndpoint)
	if !releaseCommitPattern.MatchString(a.SourceCommit) ||
		!releaseSHAPattern.MatchString(a.SHA256) ||
		!strings.HasPrefix(a.ObjectKey, "sandbox0-nomad-runtime/") ||
		strings.Contains(a.ObjectKey, "..") || strings.ContainsAny(a.ObjectKey, "\r\n") ||
		err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		strings.TrimSpace(a.OSSBucket) != a.OSSBucket || a.OSSBucket == "" {
		return fmt.Errorf("invalid immutable runtime release artifact")
	}
	return nil
}

// PinRuntimeNodeReleaseArtifact preserves the first artifact returned to an
// enrolled physical node. A bootstrap retry must not install a new generation
// halfway through an existing enrollment transaction.
func (s *PGSandboxStore) PinRuntimeNodeReleaseArtifact(ctx context.Context, clusterID, nodeUID string, candidate RuntimeReleaseArtifact) (RuntimeReleaseArtifact, error) {
	if clusterID == "" || nodeUID == "" || len(clusterID) > 512 || len(nodeUID) > 512 {
		return RuntimeReleaseArtifact{}, fmt.Errorf("runtime release node identity is invalid")
	}
	if err := candidate.Validate(); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	payload, err := json.Marshal(candidate)
	if err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO manager.runtime_node_release_artifacts
		(cluster_id,node_uid,source_commit,bundle_sha256,artifact) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (cluster_id,node_uid) DO NOTHING`, clusterID, nodeUID, candidate.SourceCommit, candidate.SHA256, payload)
	if err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	var saved []byte
	if err := s.pool.QueryRow(ctx, `SELECT artifact FROM manager.runtime_node_release_artifacts
		WHERE cluster_id=$1 AND node_uid=$2`, clusterID, nodeUID).Scan(&saved); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	var artifact RuntimeReleaseArtifact
	if err := json.Unmarshal(saved, &artifact); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	return artifact, artifact.Validate()
}

type RuntimeReleasePolicy struct {
	ClusterID    string `json:"cluster_id"`
	SourceCommit string `json:"source_commit"`
	BundleSHA256 string `json:"bundle_sha256"`
	OperationID  string `json:"operation_id"`
	Revision     int64  `json:"revision"`
}

// RebindIdleRuntimeNodeReleaseRequest is for verified in-place replacement of
// an empty fixed worker. Enrollment retries continue using Pin, never Rebind.
type RebindIdleRuntimeNodeReleaseRequest struct {
	ClusterID, NodeID, NodeUID, NodeBootID string
	ExpectedBundleSHA256, FenceReason      string
	Artifact                               RuntimeReleaseArtifact
}

func (s *PGSandboxStore) RebindIdleRuntimeNodeReleaseArtifact(ctx context.Context, request RebindIdleRuntimeNodeReleaseRequest) (RuntimeReleaseArtifact, error) {
	if request.ClusterID == "" || request.NodeID == "" || request.NodeUID == "" || request.NodeBootID == "" ||
		!releaseSHAPattern.MatchString(request.ExpectedBundleSHA256) || !strings.HasPrefix(request.FenceReason, "audited-runtime-rollout:") || len(request.FenceReason) > 512 {
		return RuntimeReleaseArtifact{}, fmt.Errorf("invalid fenced runtime artifact replacement")
	}
	if err := request.Artifact.Validate(); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockRuntimeRelease(ctx, tx, request.ClusterID, true); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	var selected bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_release_policies
		WHERE cluster_id=$1 AND source_commit=$2 AND bundle_sha256=$3)`, request.ClusterID, request.Artifact.SourceCommit, request.Artifact.SHA256).Scan(&selected); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	if !selected {
		return RuntimeReleaseArtifact{}, ErrRuntimeReleaseConflict
	}
	var reason string
	if err := tx.QueryRow(ctx, `SELECT reason FROM manager.runtime_node_fences
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 FOR UPDATE`, request.ClusterID, request.NodeID, request.NodeUID).Scan(&reason); err != nil {
		return RuntimeReleaseArtifact{}, ErrRuntimeReleaseConflict
	}
	if reason != request.FenceReason {
		return RuntimeReleaseArtifact{}, ErrRuntimeReleaseConflict
	}
	var safe bool
	if err := tx.QueryRow(ctx, `SELECT
		EXISTS(SELECT 1 FROM manager.runtime_node_capacities WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 AND heartbeat_expires_at>NOW())
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_capacities WHERE cluster_id=$1 AND node_uid=$3 AND (node_id<>$2 OR node_boot_id<>$4) AND heartbeat_expires_at>NOW())
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases WHERE cluster_id=$1 AND node_uid=$3 AND lease_state='active')
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_slots WHERE cluster_id=$1 AND node_uid=$3 AND state NOT IN('registered','fastpath_ready','terminal'))`, request.ClusterID, request.NodeID, request.NodeUID, request.NodeBootID).Scan(&safe); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	if !safe {
		return RuntimeReleaseArtifact{}, fmt.Errorf("fixed worker is busy or its exact boot capacity is unavailable")
	}
	var oldSHA string
	var saved []byte
	if err := tx.QueryRow(ctx, `SELECT bundle_sha256,artifact FROM manager.runtime_node_release_artifacts
		WHERE cluster_id=$1 AND node_uid=$2 FOR UPDATE`, request.ClusterID, request.NodeUID).Scan(&oldSHA, &saved); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	var oldArtifact RuntimeReleaseArtifact
	if err := json.Unmarshal(saved, &oldArtifact); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	if oldArtifact == request.Artifact {
		return oldArtifact, tx.Commit(ctx)
	}
	if oldSHA != request.ExpectedBundleSHA256 {
		return RuntimeReleaseArtifact{}, ErrRuntimeReleaseConflict
	}
	payload, err := json.Marshal(request.Artifact)
	if err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.runtime_node_release_artifacts SET source_commit=$3,bundle_sha256=$4,artifact=$5
		WHERE cluster_id=$1 AND node_uid=$2`, request.ClusterID, request.NodeUID, request.Artifact.SourceCommit, request.Artifact.SHA256, payload); err != nil {
		return RuntimeReleaseArtifact{}, err
	}
	return request.Artifact, tx.Commit(ctx)
}

type ActivateRuntimeReleaseRequest struct {
	ClusterID           string
	SourceCommit        string
	BundleSHA256        string
	OperationID         string
	ExpectedRevision    int64
	CompatibilityDigest string
	MinimumReadyNodes   int
	MinimumReadySlots   int
	ProbeOperationID    string
}

// runtimeReleaseAdmissionSQL is used only for new placement. Existing claims,
// writer renewals, heartbeats and cleanup remain bound to their original runtime.
func runtimeReleaseAdmissionSQL(alias string) string {
	return `NOT EXISTS (SELECT 1 FROM manager.runtime_release_policies release
		WHERE release.cluster_id=` + alias + `.cluster_id AND NOT EXISTS (
			SELECT 1 FROM manager.runtime_node_release_artifacts artifact
			WHERE artifact.cluster_id=release.cluster_id AND artifact.node_uid=` + alias + `.node_uid
			AND artifact.source_commit=release.source_commit AND artifact.bundle_sha256=release.bundle_sha256))`
}

func lockRuntimeRelease(ctx context.Context, tx pgx.Tx, clusterID string, shared bool) error {
	function := "pg_advisory_xact_lock"
	if shared {
		function += "_shared"
	}
	_, err := tx.Exec(ctx, `SELECT `+function+`(hashtextextended('sandbox0-runtime-release/' || $1,0))`, clusterID)
	return err
}

// ActivateRuntimeRelease publishes only future admission. The exclusive lock
// waits for predecessor claims, including the first activation with no policy
// row yet. It never changes a sandbox or stops a physical runtime.
func (s *PGSandboxStore) ActivateRuntimeRelease(ctx context.Context, request ActivateRuntimeReleaseRequest) (*RuntimeReleasePolicy, error) {
	if request.ClusterID == "" || len(request.ClusterID) > 512 || request.OperationID == "" || len(request.OperationID) > 256 ||
		request.ExpectedRevision < 0 || request.MinimumReadyNodes < 1 || request.MinimumReadySlots < 1 ||
		request.CompatibilityDigest == "" || !releaseCommitPattern.MatchString(request.SourceCommit) ||
		!releaseSHAPattern.MatchString(request.BundleSHA256) {
		return nil, fmt.Errorf("invalid runtime release activation")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockRuntimeRelease(ctx, tx, request.ClusterID, false); err != nil {
		return nil, err
	}
	result := &RuntimeReleasePolicy{ClusterID: request.ClusterID, SourceCommit: request.SourceCommit, BundleSHA256: request.BundleSHA256, OperationID: request.OperationID}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	requestDigest := hex.EncodeToString(sum[:])
	var savedSource, savedSHA, savedRequest string
	err = tx.QueryRow(ctx, `SELECT source_commit,bundle_sha256,revision,request_digest FROM manager.runtime_release_operations
		WHERE cluster_id=$1 AND operation_id=$2`, request.ClusterID, request.OperationID).Scan(&savedSource, &savedSHA, &result.Revision, &savedRequest)
	if err == nil {
		if savedSource != request.SourceCommit || savedSHA != request.BundleSHA256 || savedRequest != requestDigest {
			return nil, ErrRuntimeReleaseConflict
		}
		// A historical retry acknowledges its original outcome, never rolls back
		// a newer policy committed by another operation.
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	var revision int64
	err = tx.QueryRow(ctx, `SELECT revision FROM manager.runtime_release_policies WHERE cluster_id=$1`, request.ClusterID).Scan(&revision)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if revision != request.ExpectedRevision {
		return nil, ErrRuntimeReleaseConflict
	}
	if request.ProbeOperationID != "" {
		ready, err := runtimeReleaseProbeCompleted(ctx, tx, request.ProbeOperationID, request.ClusterID, request.SourceCommit, request.BundleSHA256, request.CompatibilityDigest)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, fmt.Errorf("candidate command probe is absent, stale, fenced or belongs to another artifact")
		}
	}
	var readyNodes, readySlots int
	err = tx.QueryRow(ctx, `SELECT COUNT(DISTINCT (slot.node_id,slot.node_uid,slot.node_boot_id)),COUNT(*)
		FROM manager.runtime_slots slot JOIN manager.runtime_node_release_artifacts artifact
		ON artifact.cluster_id=slot.cluster_id AND artifact.node_uid=slot.node_uid
		JOIN manager.runtime_node_capacities capacity ON capacity.cluster_id=slot.cluster_id
		AND capacity.node_id=slot.node_id AND capacity.node_uid=slot.node_uid AND capacity.node_boot_id=slot.node_boot_id
		WHERE slot.cluster_id=$1 AND artifact.source_commit=$2 AND artifact.bundle_sha256=$3
		AND slot.compatibility_digest=$4 AND slot.state='fastpath_ready' AND NOT slot.carrier_retired
		AND slot.heartbeat_expires_at>NOW() AND capacity.heartbeat_expires_at>NOW()
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases lease WHERE lease.slot_id=slot.slot_id)
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_fences fence WHERE fence.cluster_id=slot.cluster_id
		AND fence.node_id=slot.node_id AND fence.node_uid=slot.node_uid)`, request.ClusterID, request.SourceCommit, request.BundleSHA256, request.CompatibilityDigest).Scan(&readyNodes, &readySlots)
	if err != nil {
		return nil, err
	}
	if readyNodes < request.MinimumReadyNodes || readySlots < request.MinimumReadySlots {
		return nil, fmt.Errorf("candidate runtime lacks ready capacity: nodes=%d slots=%d", readyNodes, readySlots)
	}
	result.Revision = revision + 1
	_, err = tx.Exec(ctx, `INSERT INTO manager.runtime_release_policies(cluster_id,source_commit,bundle_sha256,operation_id,revision)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT(cluster_id) DO UPDATE SET source_commit=EXCLUDED.source_commit,
		bundle_sha256=EXCLUDED.bundle_sha256,operation_id=EXCLUDED.operation_id,revision=EXCLUDED.revision,updated_at=NOW()`, result.ClusterID, result.SourceCommit, result.BundleSHA256, result.OperationID, result.Revision)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.runtime_release_operations(cluster_id,operation_id,source_commit,bundle_sha256,revision,request_digest)
		VALUES($1,$2,$3,$4,$5,$6)`, result.ClusterID, result.OperationID, result.SourceCommit, result.BundleSHA256, result.Revision, requestDigest)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}

// RetireIdlePredecessorRuntimeNodes starts ordinary physical/cloud cleanup for
// one idle elastic predecessor. Busy predecessors never acquire a drain fence,
// so automatic consolidation cannot migrate their workloads for this release.
func (s *PGSandboxStore) RetireIdlePredecessorRuntimeNodes(ctx context.Context, poolID string) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var clusterID string
	if err := tx.QueryRow(ctx, `SELECT cluster_id FROM manager.runtime_node_pool_states WHERE pool_id=$1`, poolID).Scan(&clusterID); err != nil {
		return 0, err
	}
	if err := lockRuntimeRelease(ctx, tx, clusterID, true); err != nil {
		return 0, err
	}
	var nodeID, nodeUID, instanceID, operationID string
	err = tx.QueryRow(ctx, `SELECT instance.nomad_node_id,instance.node_uid,instance.provider_instance_id,policy.operation_id
		FROM manager.runtime_node_instances instance JOIN manager.runtime_release_policies policy USING(cluster_id)
		WHERE instance.pool_id=$1 AND instance.pool_kind='elastic' AND instance.state='active'
        AND EXISTS(SELECT 1 FROM manager.runtime_node_release_artifacts artifact
        JOIN manager.runtime_release_operations historical ON historical.cluster_id=artifact.cluster_id
        AND historical.source_commit=artifact.source_commit AND historical.bundle_sha256=artifact.bundle_sha256
        WHERE artifact.cluster_id=instance.cluster_id AND artifact.node_uid=instance.node_uid
        AND historical.revision<policy.revision)
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_release_artifacts artifact WHERE artifact.cluster_id=instance.cluster_id
		AND artifact.node_uid=instance.node_uid AND artifact.source_commit=policy.source_commit AND artifact.bundle_sha256=policy.bundle_sha256)
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases lease WHERE lease.cluster_id=instance.cluster_id
		AND lease.node_id=instance.nomad_node_id AND lease.node_uid=instance.node_uid AND lease.lease_state='active')
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_slots slot WHERE slot.cluster_id=instance.cluster_id
		AND slot.node_id=instance.nomad_node_id AND slot.node_uid=instance.node_uid AND slot.state NOT IN('registered','fastpath_ready','terminal'))
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_fences fence WHERE fence.cluster_id=instance.cluster_id
		AND fence.node_id=instance.nomad_node_id AND fence.node_uid=instance.node_uid)
		ORDER BY instance.provider_instance_id FOR UPDATE OF instance SKIP LOCKED LIMIT 1`, poolID).Scan(&nodeID, &nodeUID, &instanceID, &operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, tx.Commit(ctx)
	}
	if err != nil {
		return 0, err
	}
	reason := "audited-runtime-rollout:hot-retire:" + operationID
	if _, err = tx.Exec(ctx, `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason)
		VALUES($1,$2,$3,'draining',$4)`, clusterID, nodeID, nodeUID, reason); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `UPDATE manager.runtime_node_instances SET state='draining',drain_started_at=NOW(),updated_at=NOW()
		WHERE pool_id=$1 AND provider_instance_id=$2`, poolID, instanceID); err != nil {
		return 0, err
	}
	return 1, tx.Commit(ctx)
}

// Only a root-operated, exact, short-lived synthetic claim may probe a staged
// release before normal admission moves. No ordinary placement sees this grant.
type RuntimeReleaseProbe struct {
	OperationID  string `json:"operation_id"`
	ClusterID    string `json:"cluster_id"`
	NodeID       string `json:"node_id"`
	NodeUID      string `json:"node_uid"`
	NodeBootID   string `json:"node_boot_id"`
	SourceCommit string `json:"source_commit"`
	BundleSHA256 string `json:"bundle_sha256"`
}

func (s *PGSandboxStore) AuthorizeRuntimeReleaseProbe(ctx context.Context, probe RuntimeReleaseProbe) error {
	for _, value := range []string{probe.OperationID, probe.ClusterID, probe.NodeID, probe.NodeUID, probe.NodeBootID} {
		if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
			return fmt.Errorf("invalid runtime release probe")
		}
	}
	if !releaseCommitPattern.MatchString(probe.SourceCommit) || !releaseSHAPattern.MatchString(probe.BundleSHA256) {
		return fmt.Errorf("invalid runtime release probe artifact")
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO manager.runtime_release_probes
        (operation_id,cluster_id,node_id,node_uid,node_boot_id,source_commit,bundle_sha256)
        SELECT $1,$2,$3,$4,$5,$6,$7 WHERE EXISTS(SELECT 1 FROM manager.runtime_node_release_artifacts
        WHERE cluster_id=$2 AND node_uid=$4 AND source_commit=$6 AND bundle_sha256=$7)
        ON CONFLICT(operation_id) DO UPDATE SET operation_id=EXCLUDED.operation_id
        WHERE runtime_release_probes.cluster_id=EXCLUDED.cluster_id AND runtime_release_probes.node_id=EXCLUDED.node_id
        AND runtime_release_probes.node_uid=EXCLUDED.node_uid AND runtime_release_probes.node_boot_id=EXCLUDED.node_boot_id
        AND runtime_release_probes.source_commit=EXCLUDED.source_commit AND runtime_release_probes.bundle_sha256=EXCLUDED.bundle_sha256
        AND runtime_release_probes.expires_at>NOW()`, probe.OperationID, probe.ClusterID, probe.NodeID, probe.NodeUID, probe.NodeBootID, probe.SourceCommit, probe.BundleSHA256)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrRuntimeReleaseConflict
	}
	return nil
}

func (s *PGSandboxStore) ValidateRuntimeReleaseProbe(ctx context.Context, operation, nodeID, nodeUID, nodeBootID string) (bool, error) {
	var valid bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_release_probes probe
        JOIN manager.runtime_node_release_artifacts artifact USING(cluster_id,node_uid)
        WHERE probe.operation_id=$1 AND probe.node_id=$2 AND probe.node_uid=$3 AND probe.node_boot_id=$4
        AND probe.expires_at>NOW() AND probe.source_commit=artifact.source_commit AND probe.bundle_sha256=artifact.bundle_sha256)`, operation, nodeID, nodeUID, nodeBootID).Scan(&valid)
	return valid, err
}

func runtimeReleaseClaimAdmissionSQL(alias, operationParameter string) string {
	return `(` + runtimeReleaseAdmissionSQL(alias) + ` OR EXISTS(SELECT 1 FROM manager.runtime_release_probes probe
        JOIN manager.runtime_node_release_artifacts artifact USING(cluster_id,node_uid)
        WHERE probe.operation_id=` + operationParameter + ` AND probe.cluster_id=` + alias + `.cluster_id
        AND probe.node_id=` + alias + `.node_id AND probe.node_uid=` + alias + `.node_uid AND probe.node_boot_id=` + alias + `.node_boot_id
        AND probe.expires_at>NOW() AND probe.source_commit=artifact.source_commit AND probe.bundle_sha256=artifact.bundle_sha256))`
}

func (s *PGSandboxStore) GetRuntimeReleasePolicy(ctx context.Context, clusterID string) (*RuntimeReleasePolicy, error) {
	result := &RuntimeReleasePolicy{ClusterID: clusterID}
	err := s.pool.QueryRow(ctx, `SELECT source_commit,bundle_sha256,operation_id,revision FROM manager.runtime_release_policies WHERE cluster_id=$1`, clusterID).Scan(&result.SourceCommit, &result.BundleSHA256, &result.OperationID, &result.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	return result, err
}

// Completion combines an operator's authenticated command outcome with the
// node-authority command-ready binding; ready carriers alone are insufficient.
func (s *PGSandboxStore) CompleteRuntimeReleaseProbe(ctx context.Context, operation, stdoutSHA string) error {
	if !releaseSHAPattern.MatchString(stdoutSHA) {
		return fmt.Errorf("invalid command evidence")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE manager.runtime_release_probes probe SET command_completed_at=NOW(),
        command_stdout_sha256=$2,compatibility_digest=slot.compatibility_digest,sandbox_id=slot.sandbox_id
        FROM manager.runtime_slots slot WHERE probe.operation_id=$1 AND slot.claim_operation_id=probe.operation_id
        AND slot.cluster_id=probe.cluster_id AND slot.node_id=probe.node_id AND slot.node_uid=probe.node_uid
        AND slot.node_boot_id=probe.node_boot_id AND slot.state='active' AND slot.command_ready_at IS NOT NULL
        AND slot.heartbeat_expires_at>NOW() AND probe.expires_at>NOW()
        AND (probe.command_stdout_sha256 IS NULL OR probe.command_stdout_sha256=$2)`, operation, stdoutSHA)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrRuntimeReleaseConflict
	}
	return nil
}

func (s *PGSandboxStore) RuntimeReleaseProbeCompleted(ctx context.Context, operation, cluster, source, bundle, compatibility string) (bool, error) {
	return runtimeReleaseProbeCompleted(ctx, s.pool, operation, cluster, source, bundle, compatibility)
}

func runtimeReleaseProbeCompleted(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, operation, cluster, source, bundle, compatibility string) (bool, error) {
	var ready bool
	err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_release_probes probe
        JOIN manager.runtime_node_release_artifacts artifact USING(cluster_id,node_uid)
        JOIN manager.runtime_node_capacities capacity ON capacity.cluster_id=probe.cluster_id AND capacity.node_id=probe.node_id
        AND capacity.node_uid=probe.node_uid AND capacity.node_boot_id=probe.node_boot_id
        WHERE probe.operation_id=$1 AND probe.cluster_id=$2 AND probe.source_commit=$3 AND probe.bundle_sha256=$4
        AND probe.compatibility_digest=$5 AND probe.command_completed_at>NOW()-INTERVAL '10 minutes'
        AND probe.command_stdout_sha256 IS NOT NULL AND artifact.source_commit=probe.source_commit
        AND artifact.bundle_sha256=probe.bundle_sha256 AND capacity.heartbeat_expires_at>NOW()
        AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_fences fence WHERE fence.cluster_id=probe.cluster_id
        AND fence.node_id=probe.node_id AND fence.node_uid=probe.node_uid))`, operation, cluster, source, bundle, compatibility).Scan(&ready)
	return ready, err
}

// Inventory reports authenticated, current-boot capacity. Installed bundle
// evidence is populated by enrollment or a verified on-host operator receipt.
type RuntimeReleaseNode struct {
	NodeID              string `json:"node_id"`
	NodeUID             string `json:"node_uid"`
	NodeBootID          string `json:"node_boot_id"`
	SourceCommit        string `json:"source_commit"`
	BundleSHA256        string `json:"bundle_sha256"`
	CompatibilityDigest string `json:"compatibility_digest"`
	ReadySlots          int    `json:"ready_slots"`
	ActiveLeases        int    `json:"active_leases"`
	PendingSlots        int    `json:"pending_slots"`
	Fenced              bool   `json:"fenced"`
	FenceReason         string `json:"fence_reason"`
}

func (s *PGSandboxStore) GetRuntimeReleaseInventory(ctx context.Context, cluster string) ([]RuntimeReleaseNode, error) {
	rows, err := s.pool.Query(ctx, `WITH live AS (
 SELECT DISTINCT ON(node_uid) * FROM manager.runtime_node_capacities WHERE cluster_id=$1 AND heartbeat_expires_at>NOW()
 ORDER BY node_uid,updated_at DESC)
 SELECT live.node_id,live.node_uid,live.node_boot_id,COALESCE(artifact.source_commit,''),COALESCE(artifact.bundle_sha256,''),
 slot.compatibility_digest,COUNT(slot.slot_id)::integer,
 (SELECT COUNT(*)::integer FROM manager.runtime_resource_leases lease WHERE lease.cluster_id=live.cluster_id AND lease.node_uid=live.node_uid AND lease.lease_state='active'),
 (SELECT COUNT(*)::integer FROM manager.runtime_slots pending WHERE pending.cluster_id=live.cluster_id AND pending.node_uid=live.node_uid AND pending.state NOT IN('registered','fastpath_ready','terminal')),
 EXISTS(SELECT 1 FROM manager.runtime_node_fences fence WHERE fence.cluster_id=live.cluster_id AND fence.node_id=live.node_id AND fence.node_uid=live.node_uid),
 COALESCE((SELECT reason FROM manager.runtime_node_fences fence WHERE fence.cluster_id=live.cluster_id AND fence.node_id=live.node_id AND fence.node_uid=live.node_uid),'')
 FROM live LEFT JOIN manager.runtime_node_release_artifacts artifact USING(cluster_id,node_uid)
 JOIN manager.runtime_slots slot ON slot.cluster_id=live.cluster_id AND slot.node_id=live.node_id AND slot.node_uid=live.node_uid AND slot.node_boot_id=live.node_boot_id
 WHERE slot.state='fastpath_ready' AND NOT slot.carrier_retired AND slot.heartbeat_expires_at>NOW()
 AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases lease WHERE lease.slot_id=slot.slot_id)
 GROUP BY live.cluster_id,live.node_id,live.node_uid,live.node_boot_id,artifact.source_commit,artifact.bundle_sha256,slot.compatibility_digest
 ORDER BY live.node_uid,slot.compatibility_digest`, cluster)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []RuntimeReleaseNode{}
	for rows.Next() {
		var node RuntimeReleaseNode
		if err = rows.Scan(&node.NodeID, &node.NodeUID, &node.NodeBootID, &node.SourceCommit, &node.BundleSHA256, &node.CompatibilityDigest, &node.ReadySlots, &node.ActiveLeases, &node.PendingSlots, &node.Fenced, &node.FenceReason); err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	return result, rows.Err()
}
