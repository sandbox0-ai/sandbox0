package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// Only adopt complete content-addressed legacy objects. A range's checksum is
// insufficient to claim the complete pack size or identity. This repair is
// bounded by one mapping page and the existing 64 MiB pack maximum; it neither
// invents a historical billing start nor rewrites an existing ledger window.
var errRootFSLegacyInventoryPending = errors.New("legacy inventory has more full objects to authenticate")

func adoptRootFSLegacyInventoryObjects(ctx context.Context, tx pgx.Tx, source rootfsblock.RangeSource, generationID string, references []byte) error {
	rows, err := tx.Query(ctx, `SELECT DISTINCT reference.key,reference.kind FROM jsonb_to_recordset($1::jsonb) AS reference(key TEXT,kind TEXT)
  WHERE NOT EXISTS(SELECT 1 FROM manager.rootfs_materialization_objects object WHERE object.object_key=reference.key)
  ORDER BY reference.key`, references)
	if err != nil {
		return err
	}
	type missing struct{ key, kind string }
	var objects []missing
	for rows.Next() {
		var object missing
		if err = rows.Scan(&object.key, &object.kind); err != nil {
			rows.Close()
			return err
		}
		objects = append(objects, object)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	contextual, ok := source.(interface {
		GetContext(context.Context, string, int64, int64) (io.ReadCloser, error)
	})
	if len(objects) > 0 && !ok {
		return fmt.Errorf("legacy inventory requires contextual object reads")
	}
	started := time.Now()
	for index, object := range objects {
		if index >= 4 || index > 0 && time.Since(started) >= 2*time.Second {
			return errRootFSLegacyInventoryPending
		}
		body, err := contextual.GetContext(ctx, object.key, 0, rootfsblock.DefaultPackBytes+1)
		if err != nil {
			return err
		}
		payload, err := io.ReadAll(io.LimitReader(body, rootfsblock.DefaultPackBytes+1))
		closeErr := body.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		reference := rootfsblock.ObjectReference{Key: object.key, Kind: object.kind, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
		if err = rootfsblock.ValidateObjectReference(reference); err != nil {
			return err
		}
		if !strings.HasSuffix(object.key, "/sha256/"+strings.TrimPrefix(reference.Checksum, "sha256:")) {
			return fmt.Errorf("legacy object %s does not match its complete content address", object.key)
		}
		_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_materialization_objects(object_key,object_kind,object_size,checksum,uploaded_at) VALUES($1,$2,$3,$4,clock_timestamp()) ON CONFLICT DO NOTHING`, reference.Key, reference.Kind, reference.Size, reference.Checksum)
		if err != nil {
			return err
		}
		var kind, checksum string
		var size int64
		if err = tx.QueryRow(ctx, `SELECT object_kind,object_size,checksum FROM manager.rootfs_materialization_objects WHERE object_key=$1 FOR UPDATE`, object.key).Scan(&kind, &size, &checksum); err != nil {
			return err
		}
		var deleting bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key=$1)`, object.key).Scan(&deleting); err != nil {
			return err
		}
		if deleting || kind != reference.Kind || size != reference.Size || checksum != reference.Checksum {
			return fmt.Errorf("%w: legacy object catalog/deletion conflict", ErrRootFSGenerationConflict)
		}
		_, err = tx.Exec(ctx, `INSERT INTO manager.rootfs_inventory_object_custody(generation_id,object_key) VALUES($1,$2) ON CONFLICT DO NOTHING`, generationID, object.key)
		if err != nil {
			return err
		}

	}
	return nil
}
