package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

// Live publication never rewrites the immutable launch binding. Remote commit
// and local installation are separate transactions; both bases retain custody.
type PublishRootFSLiveCommitRequest struct {
	GrantID, NodeUID, OperationID string
	WriterEpoch                   int64
	BindingVersion                int
	BindingDigest                 []byte
	ExpectedCommittedGenerationID string
	BaseGenerationID              string
	CutSequence                   uint64
	Generation                    *RootFSGeneration
}

type InstallRootFSLiveCommitRequest struct {
	GrantID, NodeUID, OperationID string
	WriterEpoch                   int64
	BindingVersion                int
	BindingDigest                 []byte
	GenerationID                  string
	CutSequence                   uint64
}

type RootFSLiveProgress = protocol.LiveCommitProgress

type ObserveRootFSLiveCommitRequest struct {
	GrantID, NodeUID string
	WriterEpoch      int64
	BindingVersion   int
	BindingDigest    []byte
}

func (s *PGSandboxStore) ObserveRootFSLiveCommit(ctx context.Context, req *ObserveRootFSLiveCommitRequest) (*protocol.LiveCommitObservation, error) {
	if req == nil {
		return nil, errdefs.ErrInvalidArgument
	}
	var observation *protocol.LiveCommitObservation
	err := s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		grant, err := lockRootFSLiveWriter(ctx, tx, req.GrantID, req.NodeUID, req.WriterEpoch, req.BindingVersion, req.BindingDigest)
		if err != nil {
			return err
		}
		progress, err := getRootFSLiveProgress(ctx, tx, grant.ID)
		if err != nil {
			return err
		}
		if progress == nil {
			progress = &RootFSLiveProgress{CommittedGenerationID: grant.InitialGenerationID, InstalledGenerationID: grant.InitialGenerationID}
		}
		observation = &protocol.LiveCommitObservation{WriterState: grant.State, Progress: *progress}
		if progress.CommittedSequence > 0 && (grant.State == RootFSWriterGrantStateConsumed || grant.State == RootFSWriterGrantStateRetiring) {
			var payload []byte
			if err := tx.QueryRow(ctx, `SELECT descriptor FROM manager.rootfs_generations WHERE generation_id=$1`, progress.CommittedGenerationID).Scan(&payload); err != nil {
				return err
			}
			observation.CommittedDescriptorDigest = digest.FromBytes(payload).String()
		}
		return observation.Validate(grant.ID, grant.InitialGenerationID)
	})
	return observation, err
}

func (s *PGSandboxStore) PublishRootFSLiveCommit(ctx context.Context, req *PublishRootFSLiveCommitRequest) (*RootFSLiveProgress, error) {
	normalized, requestDigest, err := validateRootFSLiveCommit(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	var result *RootFSLiveProgress
	err = s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		grant, err := lockRootFSLiveWriter(ctx, tx, normalized.GrantID, normalized.NodeUID, normalized.WriterEpoch, normalized.BindingVersion, normalized.BindingDigest)
		if err != nil {
			return err
		}
		if grant.State != RootFSWriterGrantStateConsumed && grant.State != RootFSWriterGrantStateRetiring {
			return ErrRootFSWriterGrantInvalidState
		}
		progress, err := getRootFSLiveProgress(ctx, tx, grant.ID)
		if err != nil {
			return err
		}
		if progress != nil && progress.OperationID == normalized.OperationID {
			if !bytes.Equal(progress.RequestDigest, requestDigest[:]) {
				return ErrRootFSGenerationConflict
			}
			// The last receipt is sufficient after object-upload receipts have been
			// compacted, and does not require historical parent rows to survive.
			result = progress
			return nil
		}
		if grant.State != RootFSWriterGrantStateConsumed {
			return ErrRootFSWriterGrantInvalidState
		}
		if !grant.LeaseExpiresAt.After(grant.databaseNow) {
			return ErrRootFSWriterLeaseExpired
		}
		expected, installed, sequence := grant.InitialGenerationID, grant.InitialGenerationID, uint64(0)
		if progress != nil {
			expected, installed, sequence = progress.CommittedGenerationID, progress.InstalledGenerationID, progress.CommittedSequence
		}
		if normalized.ExpectedCommittedGenerationID != expected || normalized.BaseGenerationID != installed || normalized.CutSequence <= sequence {
			return ErrRootFSHeadConflict
		}
		generation := normalized.Generation
		var head, artifact string
		var epoch int64
		var format int
		err = tx.QueryRow(ctx, `SELECT COALESCE(filesystem.head_generation_id,''),filesystem.writer_epoch,
			COALESCE(filesystem.base_artifact_digest,''),COALESCE(filesystem.format_generation,0)
			FROM manager.rootfs_filesystems filesystem JOIN manager.sandbox_rootfs_bindings binding USING (filesystem_id)
			WHERE filesystem.filesystem_id=$1 AND binding.sandbox_id=$2 FOR UPDATE OF filesystem`, grant.FilesystemID, grant.SandboxID).
			Scan(&head, &epoch, &artifact, &format)
		if err != nil {
			return err
		}
		if head != grant.InitialGenerationID || epoch != grant.WriterEpoch || artifact != generation.BaseArtifactDigest || format != generation.FormatGeneration || generation.FilesystemID != grant.FilesystemID {
			return ErrRootFSWriterGrantConflict
		}
		parent, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+` WHERE generation_id=$1 FOR SHARE`, expected))
		if err != nil {
			return err
		}
		parentDescriptor, err := rootfsblock.DecodeDescriptor(parent.Descriptor)
		if err != nil {
			return err
		}
		descriptor, _ := rootfsblock.DecodeDescriptor(generation.Descriptor)
		if generation.ID == expected || generation.ID == installed || generation.SourceOCIDigest != parent.SourceOCIDigest || generation.BaseBlockRoot != parent.BaseBlockRoot ||
			descriptor.LogicalSizeBytes != parentDescriptor.LogicalSizeBytes || descriptor.Version != parentDescriptor.Version {
			return fmt.Errorf("%w: live checkpoint changed parent lineage or device geometry", ErrRootFSGenerationConflict)
		}
		// Live commits require the new publisher's reservation/PUT journal. The
		// legacy terminal path's no-journal exception cannot authorize WAL release.
		var rootUploaded bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_node_uploads owner
			JOIN manager.rootfs_node_upload_objects pending USING(grant_id,operation_id)
			JOIN manager.rootfs_materialization_objects object USING(object_key)
			WHERE owner.grant_id=$1 AND owner.operation_id=$2 AND owner.state='uploading' AND pending.uploaded
			AND object.object_key=$3 AND object.object_kind=$4 AND object.uploaded_at IS NOT NULL
			AND object.object_size >= $5)`, grant.ID, normalized.OperationID, descriptor.MappingRoot.Object.Key,
			rootfsblock.ObjectKindMappingPage, descriptor.MappingRoot.Object.Offset+descriptor.MappingRoot.Object.StoredLength()).Scan(&rootUploaded)
		if err != nil {
			return err
		}
		if !rootUploaded {
			return fmt.Errorf("%w: live mapping root lacks uploaded custody", ErrRootFSGenerationConflict)
		}
		_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_generations
			(generation_id,filesystem_id,parent_generation_id,source_oci_digest,base_artifact_digest,base_block_root,
			current_block_head,writer_epoch,format_generation,durability_state,locator_version,descriptor,live_commit_sequence,created_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW()) ON CONFLICT DO NOTHING`, generation.ID, generation.FilesystemID,
			generation.ParentGenerationID, generation.SourceOCIDigest, generation.BaseArtifactDigest, generation.BaseBlockRoot,
			generation.CurrentBlockHead, generation.WriterEpoch, generation.FormatGeneration, generation.DurabilityState, generation.LocatorVersion, generation.Descriptor, int64(normalized.CutSequence))
		if err != nil {
			return err
		}
		matches, err := rootFSWriterGenerationMatches(ctx, tx, generation)
		if err != nil {
			return err
		}
		if !matches {
			return fmt.Errorf("%w: live generation %s has different immutable fields", ErrRootFSGenerationConflict, generation.ID)
		}
		if err = publishRootFSNodeUploads(ctx, tx, grant.ID, normalized.OperationID, generation); err != nil {
			return err
		}
		// First publication still needs its explicit initial read base; subsequent
		// publications acquire the current installed base in the shared publisher.
		if err = retainRootFSLiveDependency(ctx, tx, generation.ID, installed); err != nil {
			return err
		}
		if progress == nil {
			_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_writer_live_progress
			(grant_id,committed_generation_id,committed_generation_ref,committed_sequence,installed_generation_id,installed_generation_ref,installed_sequence,operation_id,request_digest)
			VALUES($1,$2,$2,$3,$4,$4,0,$5,$6)`,
				grant.ID, generation.ID, int64(normalized.CutSequence), installed, normalized.OperationID, requestDigest[:])
		} else {
			_, err = tx.Exec(ctx, `UPDATE manager.rootfs_writer_live_progress SET committed_generation_id=$2,committed_generation_ref=$2,
				committed_sequence=$3,operation_id=$4,request_digest=$5,updated_at=clock_timestamp() WHERE grant_id=$1`,
				grant.ID, generation.ID, int64(normalized.CutSequence), normalized.OperationID, requestDigest[:])
		}
		if err != nil {
			return err
		}
		result, err = getRootFSLiveProgress(ctx, tx, grant.ID)
		if err == nil {
			err = pruneRootFSLiveReceipts(ctx, tx, grant.ID, normalized.OperationID)
		}
		return err
	})
	return result, err
}

// Install acknowledges only after the node has fsynced its new WAL and atomic
// local session pointer. A lost acknowledgement keeps the older base pinned.
func (s *PGSandboxStore) InstallRootFSLiveCommit(ctx context.Context, req *InstallRootFSLiveCommitRequest) (*RootFSLiveProgress, error) {
	if req == nil || req.GenerationID == "" || req.OperationID == "" || req.CutSequence == 0 || req.CutSequence > math.MaxInt64 {
		return nil, fmt.Errorf("invalid live installation: %w", errdefs.ErrInvalidArgument)
	}
	sequence, err := protocol.LiveCommitSequence(req.GrantID, req.OperationID)
	if err != nil || sequence != req.CutSequence {
		return nil, fmt.Errorf("live installation does not match its cut: %w", errdefs.ErrInvalidArgument)
	}
	var result *RootFSLiveProgress
	err = s.withRootFSWriterGrantTx(ctx, func(tx pgx.Tx) error {
		grant, err := lockRootFSLiveWriter(ctx, tx, req.GrantID, req.NodeUID, req.WriterEpoch, req.BindingVersion, req.BindingDigest)
		if err != nil {
			return err
		}
		progress, err := getRootFSLiveProgress(ctx, tx, req.GrantID)
		if err != nil {
			return err
		}
		if progress == nil || progress.OperationID != req.OperationID || progress.CommittedGenerationID != req.GenerationID || progress.CommittedSequence != req.CutSequence {
			return ErrRootFSHeadConflict
		}
		if grant.State != RootFSWriterGrantStateConsumed && grant.State != RootFSWriterGrantStateRetiring {
			return ErrRootFSWriterGrantInvalidState
		}
		_, err = tx.Exec(ctx, `UPDATE manager.rootfs_writer_live_progress SET installed_generation_id=committed_generation_id,
			installed_generation_ref=committed_generation_ref,installed_sequence=committed_sequence,updated_at=clock_timestamp() WHERE grant_id=$1`, req.GrantID)
		if err != nil {
			return err
		}
		// One current receipt plus progress is enough for exact retries. Earlier
		// published receipts have no object custody and must not grow forever.
		err = pruneRootFSLiveReceipts(ctx, tx, req.GrantID, req.OperationID)
		if err != nil {
			return err
		}
		result, err = getRootFSLiveProgress(ctx, tx, req.GrantID)
		return err
	})
	return result, err
}

func lockRootFSLiveWriter(ctx context.Context, db rootFSWriterGrantDB, id, node string, epoch int64, version int, binding []byte) (*rootFSWriterGrantRecord, error) {
	if id == "" || node == "" || epoch <= 0 || version != RootFSWriterBindingVersion || len(binding) != sha256.Size {
		return nil, ErrRootFSWriterGrantConflict
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, db, id)
	if err != nil {
		return nil, err
	}
	if grant.NodeUID != node || grant.ConsumerNodeUID != node || grant.WriterEpoch != epoch || grant.BindingVersion != version || !bytes.Equal(grant.BindingDigest, binding) {
		return nil, ErrRootFSWriterGrantConflict
	}
	return grant, nil
}

func getRootFSLiveProgress(ctx context.Context, db rootFSWriterGrantDB, grant string) (*RootFSLiveProgress, error) {
	var p RootFSLiveProgress
	err := db.QueryRow(ctx, `SELECT committed_generation_id,committed_sequence,installed_generation_id,installed_sequence,operation_id,request_digest
		FROM manager.rootfs_writer_live_progress WHERE grant_id=$1`, grant).Scan(&p.CommittedGenerationID, &p.CommittedSequence, &p.InstalledGenerationID, &p.InstalledSequence, &p.OperationID, &p.RequestDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &p, err
}

func validateRootFSLiveCommit(req *PublishRootFSLiveCommitRequest) (*PublishRootFSLiveCommitRequest, [sha256.Size]byte, error) {
	var empty [sha256.Size]byte
	if req == nil || req.NodeUID == "" || req.BaseGenerationID == "" || req.CutSequence == 0 || req.CutSequence > math.MaxInt64 ||
		!strings.HasPrefix(req.OperationID, "live/") || strings.TrimSpace(req.OperationID) != req.OperationID || len(req.OperationID) > 512 {
		return nil, empty, fmt.Errorf("invalid live commit identity")
	}
	sequence, err := protocol.LiveCommitSequence(req.GrantID, req.OperationID)
	if err != nil || sequence != req.CutSequence {
		return nil, empty, fmt.Errorf("live operation does not match its cut sequence")
	}
	normalized, err := validateCompleteRootFSWriterRetireAndPublishGenerationRequest(&CompleteRootFSWriterRetireAndPublishGenerationRequest{
		LifecycleTxnID: req.OperationID, GrantID: req.GrantID, WriterEpoch: req.WriterEpoch, OperationID: req.OperationID, BindingVersion: req.BindingVersion,
		BindingDigest: req.BindingDigest, ProofDigest: req.BindingDigest, ExpectedOldGenerationID: req.ExpectedCommittedGenerationID, Generation: req.Generation,
	})
	if err != nil {
		return nil, empty, err
	}
	if normalized.Generation.DurabilityState != rootfsblock.DurabilityS3 || normalized.Generation.ResetCopiedSessionState ||
		len(normalized.Generation.Descriptor) > RootFSGenerationDescriptorMaxBytes {
		return nil, empty, fmt.Errorf("live commit requires a bounded materialized descriptor")
	}
	clone := *req
	clone.Generation = normalized.Generation
	clone.BindingDigest = normalized.BindingDigest
	payload, err := json.Marshal(clone)
	if err != nil {
		return nil, empty, err
	}
	return &clone, sha256.Sum256(payload), nil
}

func pruneRootFSLiveReceipts(ctx context.Context, db rootFSWriterGrantDB, grantID, operationID string) error {
	_, err := db.Exec(ctx, `DELETE FROM manager.rootfs_node_uploads WHERE grant_id=$1 AND state='published' AND operation_id<>$2
		AND operation_id LIKE 'live/%'`, grantID, operationID)
	return err
}

func retainRootFSLiveDependency(ctx context.Context, db rootFSWriterGrantDB, generation, base string) error {
	_, err := db.Exec(ctx, `INSERT INTO manager.rootfs_generation_live_dependencies(generation_id,base_generation_id)
		SELECT $1,$2 WHERE $1<>$2 AND NOT (SELECT inventory_complete FROM manager.rootfs_generations WHERE generation_id=$1) ON CONFLICT DO NOTHING`, generation, base)
	return err
}

// Called only after the exact physical crash-fence proof, runtime, lifecycle,
// and filesystem locks have been validated. Lease expiry alone cannot promote
// a live cut or remove the previous writer's custody.
func restoreRootFSLiveCrashHead(ctx context.Context, db rootFSWriterGrantDB, grant *rootFSWriterGrantRecord) error {
	var exists bool
	if err := db.QueryRow(ctx, `SELECT to_regclass('manager.rootfs_writer_live_progress') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	p, err := getRootFSLiveProgress(ctx, db, grant.ID)
	if err != nil || p == nil {
		return err
	}
	tag, err := db.Exec(ctx, `UPDATE manager.rootfs_filesystems filesystem SET head_generation_id=$2,updated_at=NOW()
		FROM manager.rootfs_generations generation WHERE filesystem.filesystem_id=$1 AND filesystem.writer_epoch=$3
		AND filesystem.head_generation_id=$4 AND generation.generation_id=$2 AND generation.filesystem_id=$1
		AND generation.writer_epoch=$3 AND generation.durability_state='s3_materialized'
		AND generation.base_artifact_digest=filesystem.base_artifact_digest AND generation.format_generation=filesystem.format_generation`,
		grant.FilesystemID, p.CommittedGenerationID, grant.WriterEpoch, grant.InitialGenerationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrRootFSHeadConflict
	}
	return nil
}
