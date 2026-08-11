package s0fs

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

const defaultMetadataCacheBytes int64 = 4 << 20

// sqliteMetadataStore is a rebuildable namespace index. Committed state and
// the WAL remain authoritative; this database may be discarded after any
// unclean shutdown and reconstructed without losing mutations.
type sqliteMetadataStore struct {
	db         *sql.DB
	mutationTx *sql.Tx
	path       string
	cacheBytes int64
	codec      *sqliteMetadataCodec
	errMu      sync.Mutex
	err        error
}

// sqliteMetadataCodec protects the disposable index with an ephemeral key.
// The key is intentionally not persisted: committed state and the WAL are the
// recovery boundary, so an index left by a dead process is rebuilt.
type sqliteMetadataCodec struct {
	aead cipher.AEAD
	key  []byte
}

func newSQLiteMetadataCodec(encryption *EncryptionConfig) (*sqliteMetadataCodec, error) {
	if !encryption.enabled() {
		return nil, nil
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("create metadata index key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sqliteMetadataCodec{aead: aead, key: key}, nil
}

func (c *sqliteMetadataCodec) seal(kind string, key, plaintext []byte) ([]byte, error) {
	if c == nil {
		return slices.Clone(plaintext), nil
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	aad := append(append([]byte(kind), 0), key...)
	return c.aead.Seal(nonce, nonce, plaintext, aad), nil
}

func (c *sqliteMetadataCodec) open(kind string, key, ciphertext []byte) ([]byte, error) {
	if c == nil {
		return slices.Clone(ciphertext), nil
	}
	nonceSize := c.aead.NonceSize()
	if len(ciphertext) < nonceSize+c.aead.Overhead() {
		return nil, fmt.Errorf("%w: invalid encrypted metadata value", ErrInvalidInput)
	}
	aad := append(append([]byte(kind), 0), key...)
	return c.aead.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], aad)
}

func (c *sqliteMetadataCodec) overhead() int {
	if c == nil {
		return 0
	}
	return c.aead.NonceSize() + c.aead.Overhead()
}

func (c *sqliteMetadataCodec) directoryNameKey(parent uint64, name string) []byte {
	if c == nil {
		return []byte(name)
	}
	digest := hmac.New(sha256.New, c.key)
	_, _ = digest.Write(metadataInodeKey(parent))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(name))
	return digest.Sum(nil)
}

func (c *sqliteMetadataCodec) encodeDirectoryName(parent uint64, nameKey []byte, name string) ([]byte, error) {
	key := append(metadataInodeKey(parent), nameKey...)
	return c.seal("dirent", key, []byte(name))
}

func (c *sqliteMetadataCodec) decodeDirectoryName(parent uint64, nameKey, payload []byte) (string, error) {
	key := append(metadataInodeKey(parent), nameKey...)
	plaintext, err := c.open("dirent", key, payload)
	return string(plaintext), err
}

func newSQLiteMetadataStore(ctx context.Context, path string, state *SnapshotState, cacheBytes int64) (*sqliteMetadataStore, error) {
	return newSQLiteMetadataStoreWithEncryption(ctx, path, state, cacheBytes, nil)
}

func newSQLiteMetadataStoreWithEncryption(ctx context.Context, path string, state *SnapshotState, cacheBytes int64, encryption *EncryptionConfig) (*sqliteMetadataStore, error) {
	ctx = nonNilContext(ctx)
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: metadata index path is required", ErrInvalidInput)
	}
	if cacheBytes <= 0 {
		cacheBytes = defaultMetadataCacheBytes
	}
	codec, err := newSQLiteMetadataCodec(encryption)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create metadata index directory: %w", err)
	}
	tempPath := path + ".rebuild"
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale metadata rebuild: %w", err)
	}
	db, err := openSQLiteMetadataDB(tempPath, cacheBytes)
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(tempPath)
	}
	tempStore := &sqliteMetadataStore{db: db, path: tempPath, cacheBytes: cacheBytes, codec: codec}
	if err := initializeSQLiteMetadata(ctx, tempStore, state); err != nil {
		cleanup()
		return nil, err
	}
	if err := db.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("close rebuilt metadata index: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("replace metadata index: %w", err)
	}
	db, err = openSQLiteMetadataDB(path, cacheBytes)
	if err != nil {
		return nil, err
	}
	return &sqliteMetadataStore{db: db, path: path, cacheBytes: cacheBytes, codec: codec}, nil
}

func newSQLiteMetadataStoreFromStateV2(
	ctx context.Context,
	path string,
	reader io.Reader,
	volumeID string,
	binding []byte,
	role StateV2Role,
	encryption *EncryptionConfig,
	cacheBytes int64,
) (*sqliteMetadataStore, *stateV2StreamResult, error) {
	ctx = nonNilContext(ctx)
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil, fmt.Errorf("%w: metadata index path is required", ErrInvalidInput)
	}
	if cacheBytes <= 0 {
		cacheBytes = defaultMetadataCacheBytes
	}
	codec, err := newSQLiteMetadataCodec(encryption)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create metadata index directory: %w", err)
	}
	tempPath := path + ".rebuild"
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("remove stale metadata rebuild: %w", err)
	}
	db, err := openSQLiteMetadataDB(tempPath, cacheBytes)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(tempPath)
	}
	if err := createSQLiteMetadataSchema(ctx, db); err != nil {
		cleanup()
		return nil, nil, err
	}
	tempStore := &sqliteMetadataStore{db: db, path: tempPath, cacheBytes: cacheBytes, codec: codec}
	stream, err := streamStateV2Chunks(ctx, reader, volumeID, binding, role, encryption, func(_ *StateV2Header, descriptor *StateV2ChunkDescriptor, chunk *StateV2Chunk) error {
		return applyStateV2ChunkToSQLite(ctx, tempStore, descriptor, chunk)
	})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if err := db.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, nil, fmt.Errorf("close rebuilt metadata index: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return nil, nil, fmt.Errorf("replace metadata index: %w", err)
	}
	db, err = openSQLiteMetadataDB(path, cacheBytes)
	if err != nil {
		return nil, nil, err
	}
	return &sqliteMetadataStore{db: db, path: path, cacheBytes: cacheBytes, codec: codec}, stream, nil
}

func openSQLiteMetadataDB(path string, cacheBytes int64) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open metadata index: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	cacheKiB := cacheBytes >> 10
	if cacheKiB < 256 {
		cacheKiB = 256
	}
	pragmas := []string{
		"PRAGMA journal_mode=OFF",
		"PRAGMA synchronous=OFF",
		"PRAGMA temp_store=FILE",
		"PRAGMA mmap_size=0",
		fmt.Sprintf("PRAGMA cache_size=-%d", cacheKiB),
	}
	for _, statement := range pragmas {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure metadata index: %w", err)
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("protect metadata index: %w", err)
	}
	return db, nil
}

func initializeSQLiteMetadata(ctx context.Context, store *sqliteMetadataStore, state *SnapshotState) error {
	normalizeState(state)
	if err := createSQLiteMetadataSchema(ctx, store.db); err != nil {
		return err
	}
	return populateSQLiteMetadata(ctx, store, state)
}

func createSQLiteMetadataSchema(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE nodes (inode BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID`,
		`CREATE TABLE directories (inode BLOB PRIMARY KEY) WITHOUT ROWID`,
		`CREATE TABLE dirents (parent BLOB NOT NULL, name_key BLOB NOT NULL, name BLOB NOT NULL, inode BLOB NOT NULL, PRIMARY KEY(parent, name_key)) WITHOUT ROWID`,
		`CREATE INDEX dirents_inode ON dirents(inode, name_key, parent)`,
		`CREATE TABLE file_data (inode BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID`,
		`CREATE TABLE cold_files (inode BLOB PRIMARY KEY) WITHOUT ROWID`,
		`CREATE TABLE file_extents (inode BLOB NOT NULL, position INTEGER NOT NULL, segment_id TEXT NOT NULL, offset BLOB NOT NULL, length BLOB NOT NULL, PRIMARY KEY(inode, position)) WITHOUT ROWID`,
		`CREATE TABLE segments (id TEXT PRIMARY KEY, value BLOB NOT NULL, inline INTEGER NOT NULL) WITHOUT ROWID`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize metadata index: %w", err)
		}
	}
	return nil
}

func populateSQLiteMetadata(ctx context.Context, store *sqliteMetadataStore, state *SnapshotState) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata index rebuild: %w", err)
	}
	defer tx.Rollback()
	nodeStmt, err := tx.PrepareContext(ctx, `INSERT INTO nodes(inode, value) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer nodeStmt.Close()
	for inode, node := range state.Nodes {
		payload, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("encode node %d: %w", inode, err)
		}
		payload, err = store.codec.seal("node", metadataInodeKey(inode), payload)
		if err != nil {
			return fmt.Errorf("protect node %d: %w", inode, err)
		}
		if _, err := nodeStmt.ExecContext(ctx, metadataInodeKey(inode), payload); err != nil {
			return fmt.Errorf("index node %d: %w", inode, err)
		}
	}
	directoryStmt, err := tx.PrepareContext(ctx, `INSERT INTO directories(inode) VALUES(?)`)
	if err != nil {
		return err
	}
	defer directoryStmt.Close()
	direntStmt, err := tx.PrepareContext(ctx, `INSERT INTO dirents(parent, name_key, name, inode) VALUES(?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer direntStmt.Close()
	for parent, children := range state.Children {
		parentKey := metadataInodeKey(parent)
		if _, err := directoryStmt.ExecContext(ctx, parentKey); err != nil {
			return fmt.Errorf("index directory %d: %w", parent, err)
		}
		for name, inode := range children {
			nameKey := store.codec.directoryNameKey(parent, name)
			encodedName, err := store.codec.encodeDirectoryName(parent, nameKey, name)
			if err != nil {
				return fmt.Errorf("protect directory entry %d/%s: %w", parent, name, err)
			}
			if _, err := direntStmt.ExecContext(ctx, parentKey, nameKey, encodedName, metadataInodeKey(inode)); err != nil {
				return fmt.Errorf("index directory entry %d/%s: %w", parent, name, err)
			}
		}
	}
	dataStmt, err := tx.PrepareContext(ctx, `INSERT INTO file_data(inode, value) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer dataStmt.Close()
	for inode, payload := range state.Data {
		key := metadataInodeKey(inode)
		protected, err := store.codec.seal("data", key, payload)
		if err != nil {
			return fmt.Errorf("protect inline data %d: %w", inode, err)
		}
		if _, err := dataStmt.ExecContext(ctx, key, protected); err != nil {
			return fmt.Errorf("index inline data %d: %w", inode, err)
		}
	}
	coldStmt, err := tx.PrepareContext(ctx, `INSERT INTO cold_files(inode) VALUES(?)`)
	if err != nil {
		return err
	}
	defer coldStmt.Close()
	extentStmt, err := tx.PrepareContext(ctx, `INSERT INTO file_extents(inode, position, segment_id, offset, length) VALUES(?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer extentStmt.Close()
	for inode, extents := range state.ColdFiles {
		key := metadataInodeKey(inode)
		if _, err := coldStmt.ExecContext(ctx, key); err != nil {
			return fmt.Errorf("index cold file %d: %w", inode, err)
		}
		for position, extent := range extents {
			if _, err := extentStmt.ExecContext(ctx, key, position, extent.SegmentID, metadataInodeKey(extent.Offset), metadataInodeKey(extent.Length)); err != nil {
				return fmt.Errorf("index cold file extent %d/%d: %w", inode, position, err)
			}
		}
	}
	segmentStmt, err := tx.PrepareContext(ctx, `INSERT INTO segments(id, value, inline) VALUES(?, ?, ?)`)
	if err != nil {
		return err
	}
	defer segmentStmt.Close()
	for id, segment := range state.Segments {
		payload, err := json.Marshal(segment)
		if err != nil {
			return fmt.Errorf("encode segment %s: %w", id, err)
		}
		payload, err = store.codec.seal("segment", []byte(id), payload)
		if err != nil {
			return fmt.Errorf("protect segment %s: %w", id, err)
		}
		if _, err := segmentStmt.ExecContext(ctx, id, payload, isInlineSegment(segment)); err != nil {
			return fmt.Errorf("index segment %s: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata index rebuild: %w", err)
	}
	return nil
}

func applyStateV2ChunkToSQLite(ctx context.Context, store *sqliteMetadataStore, descriptor *StateV2ChunkDescriptor, chunk *StateV2Chunk) error {
	if descriptor == nil || chunk == nil {
		return fmt.Errorf("%w: state v2 chunk is required", ErrInvalidInput)
	}
	if store == nil || store.db == nil {
		return fmt.Errorf("%w: metadata index is required", ErrInvalidInput)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch descriptor.Kind {
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES:
		if len(chunk.Directories)+len(chunk.Data)+len(chunk.ColdFiles)+len(chunk.Segments) != 0 || uint64(len(chunk.Nodes)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed node chunk", ErrInvalidInput)
		}
		for _, record := range chunk.Nodes {
			node, err := decodeStateV2Node(record)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(node)
			if err != nil {
				return err
			}
			key := metadataInodeKey(node.Inode)
			payload, err = store.codec.seal("node", key, payload)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO nodes(inode, value) VALUES(?, ?)`, key, payload); err != nil {
				return fmt.Errorf("%w: duplicate inode %d", ErrInvalidInput, node.Inode)
			}
		}
		if err := validateStateV2InodeRange(descriptor, nodeChunkInodeRange(chunk.Nodes)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_DIRECTORIES:
		if len(chunk.Nodes)+len(chunk.Data)+len(chunk.ColdFiles)+len(chunk.Segments) != 0 || uint64(len(chunk.Directories)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed directory chunk", ErrInvalidInput)
		}
		for _, directory := range chunk.Directories {
			if directory == nil || directory.ParentInode == 0 {
				return fmt.Errorf("%w: invalid directory record", ErrInvalidInput)
			}
			key := metadataInodeKey(directory.ParentInode)
			var directoryCount, childCount int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM directories WHERE inode = ?`, key).Scan(&directoryCount); err != nil {
				return err
			}
			if directoryCount != 0 {
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dirents WHERE parent = ?`, key).Scan(&childCount); err != nil {
					return err
				}
				if childCount == 0 || len(directory.Children) == 0 {
					return fmt.Errorf("%w: duplicate empty directory record", ErrInvalidInput)
				}
			} else if _, err := tx.ExecContext(ctx, `INSERT INTO directories(inode) VALUES(?)`, key); err != nil {
				return err
			}
			for _, child := range directory.Children {
				if child == nil || child.Name == "" || child.Inode == 0 {
					return fmt.Errorf("%w: invalid directory child", ErrInvalidInput)
				}
				nameKey := store.codec.directoryNameKey(directory.ParentInode, child.Name)
				encodedName, err := store.codec.encodeDirectoryName(directory.ParentInode, nameKey, child.Name)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO dirents(parent, name_key, name, inode) VALUES(?, ?, ?, ?)`, key, nameKey, encodedName, metadataInodeKey(child.Inode)); err != nil {
					return fmt.Errorf("%w: duplicate directory child %q", ErrInvalidInput, child.Name)
				}
			}
		}
		if err := validateStateV2InodeRange(descriptor, directoryChunkInodeRange(chunk.Directories)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_DATA:
		if len(chunk.Nodes)+len(chunk.Directories)+len(chunk.ColdFiles)+len(chunk.Segments) != 0 || uint64(len(chunk.Data)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed data chunk", ErrInvalidInput)
		}
		for _, record := range chunk.Data {
			if record == nil || record.Inode == 0 {
				return fmt.Errorf("%w: invalid inline data record", ErrInvalidInput)
			}
			key := metadataInodeKey(record.Inode)
			var current []byte
			err := tx.QueryRowContext(ctx, `SELECT value FROM file_data WHERE inode = ?`, key).Scan(&current)
			exists := err == nil
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if exists {
				current, err = store.codec.open("data", key, current)
				if err != nil {
					return err
				}
			}
			if (exists && (len(current) == 0 || len(record.Payload) == 0)) || uint64(len(current)) != record.Offset {
				return fmt.Errorf("%w: duplicate or non-contiguous inline data record", ErrInvalidInput)
			}
			current = append(current, record.Payload...)
			protected, err := store.codec.seal("data", key, current)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO file_data(inode, value) VALUES(?, ?) ON CONFLICT(inode) DO UPDATE SET value=excluded.value`, key, protected); err != nil {
				return err
			}
		}
		if err := validateStateV2InodeRange(descriptor, dataChunkInodeRange(chunk.Data)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_COLD_FILES:
		if len(chunk.Nodes)+len(chunk.Directories)+len(chunk.Data)+len(chunk.Segments) != 0 || uint64(len(chunk.ColdFiles)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed cold-file chunk", ErrInvalidInput)
		}
		for _, record := range chunk.ColdFiles {
			if record == nil || record.Inode == 0 {
				return fmt.Errorf("%w: invalid cold-file record", ErrInvalidInput)
			}
			key := metadataInodeKey(record.Inode)
			var fileCount, extentCount int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cold_files WHERE inode = ?`, key).Scan(&fileCount); err != nil {
				return err
			}
			if fileCount != 0 {
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_extents WHERE inode = ?`, key).Scan(&extentCount); err != nil {
					return err
				}
				if extentCount == 0 || len(record.Extents) == 0 {
					return fmt.Errorf("%w: duplicate empty cold-file record", ErrInvalidInput)
				}
			} else if _, err := tx.ExecContext(ctx, `INSERT INTO cold_files(inode) VALUES(?)`, key); err != nil {
				return err
			}
			for index, extent := range record.Extents {
				if extent == nil {
					return fmt.Errorf("%w: nil cold-file extent", ErrInvalidInput)
				}
				position := extentCount + index
				if _, err := tx.ExecContext(ctx, `INSERT INTO file_extents(inode, position, segment_id, offset, length) VALUES(?, ?, ?, ?, ?)`, key, position, extent.SegmentId, metadataInodeKey(extent.Offset), metadataInodeKey(extent.Length)); err != nil {
					return err
				}
			}
		}
		if err := validateStateV2InodeRange(descriptor, coldFileChunkInodeRange(chunk.ColdFiles)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_SEGMENTS:
		if len(chunk.Nodes)+len(chunk.Directories)+len(chunk.Data)+len(chunk.ColdFiles) != 0 || uint64(len(chunk.Segments)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed segment chunk", ErrInvalidInput)
		}
		for _, record := range chunk.Segments {
			segment, err := decodeStateV2Segment(record)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(segment)
			if err != nil {
				return err
			}
			payload, err = store.codec.seal("segment", []byte(segment.ID), payload)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO segments(id, value, inline) VALUES(?, ?, ?)`, segment.ID, payload, isInlineSegment(segment)); err != nil {
				return fmt.Errorf("%w: duplicate segment %s", ErrInvalidInput, segment.ID)
			}
		}
		if descriptor.FirstInode != 0 || descriptor.LastInode != 0 {
			return fmt.Errorf("%w: segment chunk has an inode range", ErrInvalidInput)
		}
	default:
		return fmt.Errorf("%w: unsupported state v2 chunk kind %d", ErrInvalidInput, descriptor.Kind)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state v2 metadata chunk: %w", err)
	}
	return nil
}

func metadataInodeKey(inode uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, inode)
	return key
}

func metadataInodeFromKey(key []byte) (uint64, error) {
	if len(key) != 8 {
		return 0, fmt.Errorf("%w: invalid metadata inode key", ErrInvalidInput)
	}
	return binary.BigEndian.Uint64(key), nil
}

func (s *sqliteMetadataStore) setErr(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

func (s *sqliteMetadataStore) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *sqliteMetadataStore) ApplyMutation(apply func() error) error {
	if apply == nil {
		return nil
	}
	if s.mutationTx != nil {
		return apply()
	}
	tx, err := s.db.Begin()
	if err != nil {
		s.setErr(err)
		return err
	}
	s.mutationTx = tx
	applyErr := apply()
	if applyErr == nil {
		applyErr = s.Err()
	}
	if applyErr != nil {
		s.mutationTx = nil
		_ = tx.Rollback()
		return applyErr
	}
	commitErr := tx.Commit()
	s.mutationTx = nil
	if commitErr != nil {
		s.setErr(commitErr)
		return commitErr
	}
	return nil
}

func (s *sqliteMetadataStore) exec(query string, args ...any) (sql.Result, error) {
	if s.mutationTx != nil {
		return s.mutationTx.Exec(query, args...)
	}
	return s.db.Exec(query, args...)
}

func (s *sqliteMetadataStore) query(query string, args ...any) (*sql.Rows, error) {
	if s.mutationTx != nil {
		return s.mutationTx.Query(query, args...)
	}
	return s.db.Query(query, args...)
}

func (s *sqliteMetadataStore) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.mutationTx != nil {
		return s.mutationTx.QueryContext(ctx, query, args...)
	}
	return s.db.QueryContext(ctx, query, args...)
}

func (s *sqliteMetadataStore) queryRow(query string, args ...any) *sql.Row {
	if s.mutationTx != nil {
		return s.mutationTx.QueryRow(query, args...)
	}
	return s.db.QueryRow(query, args...)
}

func (s *sqliteMetadataStore) withMutation(apply func() error) error {
	if s.mutationTx != nil {
		return apply()
	}
	return s.ApplyMutation(apply)
}

func (s *sqliteMetadataStore) Node(inode uint64) (*Node, bool) {
	var payload []byte
	key := metadataInodeKey(inode)
	err := s.queryRow(`SELECT value FROM nodes WHERE inode = ?`, key).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		s.setErr(fmt.Errorf("read node %d: %w", inode, err))
		return nil, false
	}
	payload, err = s.codec.open("node", key, payload)
	if err != nil {
		s.setErr(fmt.Errorf("unprotect node %d: %w", inode, err))
		return nil, false
	}
	var node Node
	if err := json.Unmarshal(payload, &node); err != nil {
		s.setErr(fmt.Errorf("decode node %d: %w", inode, err))
		return nil, false
	}
	return &node, true
}

func (s *sqliteMetadataStore) PutNode(inode uint64, node *Node) {
	payload, err := json.Marshal(node)
	if err == nil {
		payload, err = s.codec.seal("node", metadataInodeKey(inode), payload)
	}
	if err == nil {
		_, err = s.exec(`INSERT INTO nodes(inode, value) VALUES(?, ?) ON CONFLICT(inode) DO UPDATE SET value=excluded.value`, metadataInodeKey(inode), payload)
	}
	s.setErr(err)
}

func (s *sqliteMetadataStore) DeleteNode(inode uint64) {
	_, err := s.exec(`DELETE FROM nodes WHERE inode = ?`, metadataInodeKey(inode))
	s.setErr(err)
}

func (s *sqliteMetadataStore) RangeNodes(yield func(uint64, *Node) bool) {
	rows, err := s.query(`SELECT inode, value FROM nodes ORDER BY inode`)
	if err != nil {
		s.setErr(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var key, payload []byte
		if err := rows.Scan(&key, &payload); err != nil {
			s.setErr(err)
			return
		}
		inode, err := metadataInodeFromKey(key)
		if err != nil {
			s.setErr(err)
			return
		}
		payload, err = s.codec.open("node", key, payload)
		if err != nil {
			s.setErr(err)
			return
		}
		var node Node
		if err := json.Unmarshal(payload, &node); err != nil {
			s.setErr(err)
			return
		}
		if !yield(inode, &node) {
			return
		}
	}
	s.setErr(rows.Err())
}

func (s *sqliteMetadataStore) count(query string) int {
	var count int
	if err := s.queryRow(query).Scan(&count); err != nil {
		s.setErr(err)
		return 0
	}
	return count
}

func (s *sqliteMetadataStore) NodeCount() int { return s.count(`SELECT COUNT(*) FROM nodes`) }

func (s *sqliteMetadataStore) Child(parent uint64, name string) (uint64, bool) {
	var key []byte
	err := s.queryRow(`SELECT inode FROM dirents WHERE parent = ? AND name_key = ?`, metadataInodeKey(parent), s.codec.directoryNameKey(parent, name)).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		s.setErr(err)
		return 0, false
	}
	inode, err := metadataInodeFromKey(key)
	s.setErr(err)
	return inode, err == nil
}

func (s *sqliteMetadataStore) PutChild(parent uint64, name string, inode uint64) {
	nameKey := s.codec.directoryNameKey(parent, name)
	payload, err := s.codec.encodeDirectoryName(parent, nameKey, name)
	if err == nil {
		_, err = s.exec(`INSERT INTO dirents(parent, name_key, name, inode) VALUES(?, ?, ?, ?) ON CONFLICT(parent, name_key) DO UPDATE SET name=excluded.name, inode=excluded.inode`, metadataInodeKey(parent), nameKey, payload, metadataInodeKey(inode))
	}
	s.setErr(err)
}

func (s *sqliteMetadataStore) DeleteChild(parent uint64, name string) {
	_, err := s.exec(`DELETE FROM dirents WHERE parent = ? AND name_key = ?`, metadataInodeKey(parent), s.codec.directoryNameKey(parent, name))
	s.setErr(err)
}

func (s *sqliteMetadataStore) EnsureDirectory(inode uint64) {
	_, err := s.exec(`INSERT OR IGNORE INTO directories(inode) VALUES(?)`, metadataInodeKey(inode))
	s.setErr(err)
}

func (s *sqliteMetadataStore) DeleteDirectory(inode uint64) {
	err := s.withMutation(func() error {
		if _, err := s.exec(`DELETE FROM dirents WHERE parent = ?`, metadataInodeKey(inode)); err != nil {
			return err
		}
		_, err := s.exec(`DELETE FROM directories WHERE inode = ?`, metadataInodeKey(inode))
		return err
	})
	s.setErr(err)
}

func (s *sqliteMetadataStore) DirectoryEntries(inode uint64) (map[string]uint64, bool) {
	var exists int
	if err := s.queryRow(`SELECT 1 FROM directories WHERE inode = ?`, metadataInodeKey(inode)).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return nil, false
	} else if err != nil {
		s.setErr(err)
		return nil, false
	}
	rows, err := s.query(`SELECT name_key, name, inode FROM dirents WHERE parent = ? ORDER BY name_key`, metadataInodeKey(inode))
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	defer rows.Close()
	entries := make(map[string]uint64)
	for rows.Next() {
		var nameKey, namePayload, key []byte
		if err := rows.Scan(&nameKey, &namePayload, &key); err != nil {
			s.setErr(err)
			return nil, false
		}
		child, err := metadataInodeFromKey(key)
		if err != nil {
			s.setErr(err)
			return nil, false
		}
		name, err := s.codec.decodeDirectoryName(inode, nameKey, namePayload)
		if err != nil {
			s.setErr(err)
			return nil, false
		}
		entries[name] = child
	}
	s.setErr(rows.Err())
	return entries, s.Err() == nil
}

func (s *sqliteMetadataStore) DirectoryPage(inode, offset uint64, limit uint32) ([]metadataDirEntry, bool, bool) {
	var exists int
	if err := s.queryRow(`SELECT 1 FROM directories WHERE inode = ?`, metadataInodeKey(inode)).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return nil, false, false
	} else if err != nil {
		s.setErr(err)
		return nil, false, false
	}
	queryLimit := int64(limit)
	if queryLimit <= 0 {
		queryLimit = 1<<31 - 1
	}
	rows, err := s.query(`SELECT name_key, name, inode FROM dirents WHERE parent = ? ORDER BY name_key LIMIT ? OFFSET ?`, metadataInodeKey(inode), queryLimit+1, offset)
	if err != nil {
		s.setErr(err)
		return nil, false, true
	}
	defer rows.Close()
	entries := make([]metadataDirEntry, 0, min(int(queryLimit), 1024))
	for rows.Next() {
		var entry metadataDirEntry
		var nameKey, namePayload, key []byte
		if err := rows.Scan(&nameKey, &namePayload, &key); err != nil {
			s.setErr(err)
			return nil, false, true
		}
		entry.Inode, err = metadataInodeFromKey(key)
		if err != nil {
			s.setErr(err)
			return nil, false, true
		}
		entry.Name, err = s.codec.decodeDirectoryName(inode, nameKey, namePayload)
		if err != nil {
			s.setErr(err)
			return nil, false, true
		}
		entries = append(entries, entry)
	}
	s.setErr(rows.Err())
	if int64(len(entries)) > queryLimit {
		return entries[:queryLimit], false, true
	}
	return entries, true, true
}

func (s *sqliteMetadataStore) DirectoryPageWithNodes(inode, offset uint64, limit uint32) ([]metadataDirNodeEntry, bool, bool) {
	page, eof, ok := s.DirectoryPage(inode, offset, limit)
	if !ok || len(page) == 0 {
		return nil, eof, ok
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(page)), ",")
	args := make([]any, 0, len(page))
	for _, entry := range page {
		args = append(args, metadataInodeKey(entry.Inode))
	}
	rows, err := s.query(`SELECT inode, value FROM nodes WHERE inode IN (`+placeholders+`)`, args...)
	if err != nil {
		s.setErr(err)
		return nil, false, ok
	}
	defer rows.Close()
	nodes := make(map[uint64]*Node, len(page))
	for rows.Next() {
		var inodeKey, nodePayload []byte
		if err := rows.Scan(&inodeKey, &nodePayload); err != nil {
			s.setErr(err)
			return nil, false, ok
		}
		child, err := metadataInodeFromKey(inodeKey)
		if err != nil {
			s.setErr(err)
			return nil, false, ok
		}
		nodePayload, err = s.codec.open("node", inodeKey, nodePayload)
		if err != nil {
			s.setErr(err)
			return nil, false, ok
		}
		var node Node
		if err := json.Unmarshal(nodePayload, &node); err != nil {
			s.setErr(err)
			return nil, false, ok
		}
		nodes[child] = &node
	}
	s.setErr(rows.Err())
	entries := make([]metadataDirNodeEntry, 0, len(page))
	for _, entry := range page {
		if node := nodes[entry.Inode]; node != nil {
			entries = append(entries, metadataDirNodeEntry{metadataDirEntry: entry, Node: node})
		}
	}
	return entries, eof, ok
}

func (s *sqliteMetadataStore) RangeDirectories(yield func(uint64, map[string]uint64) bool) {
	rows, err := s.query(`SELECT d.inode, e.name_key, e.name, e.inode FROM directories d LEFT JOIN dirents e ON e.parent = d.inode ORDER BY d.inode, e.name_key`)
	if err != nil {
		s.setErr(err)
		return
	}
	defer rows.Close()
	var current uint64
	var entries map[string]uint64
	haveCurrent := false
	flush := func() bool {
		return !haveCurrent || yield(current, entries)
	}
	for rows.Next() {
		var parentKey []byte
		var nameKey, namePayload, childKey []byte
		if err := rows.Scan(&parentKey, &nameKey, &namePayload, &childKey); err != nil {
			s.setErr(err)
			return
		}
		parent, err := metadataInodeFromKey(parentKey)
		if err != nil {
			s.setErr(err)
			return
		}
		if !haveCurrent || parent != current {
			if !flush() {
				return
			}
			current, entries, haveCurrent = parent, make(map[string]uint64), true
		}
		if nameKey != nil {
			child, err := metadataInodeFromKey(childKey)
			if err != nil {
				s.setErr(err)
				return
			}
			name, err := s.codec.decodeDirectoryName(parent, nameKey, namePayload)
			if err != nil {
				s.setErr(err)
				return
			}
			entries[name] = child
		}
	}
	if rows.Err() == nil {
		_ = flush()
	}
	s.setErr(rows.Err())
}

func (s *sqliteMetadataStore) RangeDirectoryRecords(yield func(parent uint64, name string, inode uint64, first bool) bool) {
	rows, err := s.query(`SELECT d.inode, e.name_key, e.name, e.inode FROM directories d LEFT JOIN dirents e ON e.parent = d.inode ORDER BY d.inode, e.name_key`)
	if err != nil {
		s.setErr(err)
		return
	}
	defer rows.Close()
	var previous uint64
	var havePrevious bool
	for rows.Next() {
		var parentKey []byte
		var nameKey, namePayload, childKey []byte
		if err := rows.Scan(&parentKey, &nameKey, &namePayload, &childKey); err != nil {
			s.setErr(err)
			return
		}
		parent, err := metadataInodeFromKey(parentKey)
		if err != nil {
			s.setErr(err)
			return
		}
		first := !havePrevious || parent != previous
		previous, havePrevious = parent, true
		if nameKey == nil {
			if !yield(parent, "", 0, first) {
				return
			}
			continue
		}
		child, err := metadataInodeFromKey(childKey)
		if err != nil {
			s.setErr(err)
			return
		}
		name, err := s.codec.decodeDirectoryName(parent, nameKey, namePayload)
		if err != nil {
			s.setErr(err)
			return
		}
		if !yield(parent, name, child, first) {
			return
		}
	}
	s.setErr(rows.Err())
}

func (s *sqliteMetadataStore) DirectoryEntryCount() int {
	return s.count(`SELECT COUNT(*) FROM dirents`)
}

func (s *sqliteMetadataStore) Path(target uint64) (string, bool) {
	if target == RootInode {
		return "/", true
	}
	if _, ok := s.Node(target); !ok {
		return "", false
	}
	var components []string
	seen := make(map[uint64]struct{})
	for target != RootInode {
		if _, ok := seen[target]; ok {
			s.setErr(fmt.Errorf("%w: directory cycle at inode %d", ErrInvalidInput, target))
			return "", false
		}
		seen[target] = struct{}{}
		var parentKey, nameKey, namePayload []byte
		err := s.queryRow(`SELECT parent, name_key, name FROM dirents WHERE inode = ? ORDER BY name_key, parent LIMIT 1`, metadataInodeKey(target)).Scan(&parentKey, &nameKey, &namePayload)
		if errors.Is(err, sql.ErrNoRows) {
			return "", false
		}
		if err != nil {
			s.setErr(err)
			return "", false
		}
		parent, err := metadataInodeFromKey(parentKey)
		if err != nil {
			s.setErr(err)
			return "", false
		}
		name, err := s.codec.decodeDirectoryName(parent, nameKey, namePayload)
		if err != nil {
			s.setErr(err)
			return "", false
		}
		components = append(components, name)
		target = parent
	}
	slices.Reverse(components)
	return "/" + strings.Join(components, "/"), true
}

func (s *sqliteMetadataStore) Data(inode uint64) ([]byte, bool) {
	var payload []byte
	key := metadataInodeKey(inode)
	err := s.queryRow(`SELECT value FROM file_data WHERE inode = ?`, key).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	payload, err = s.codec.open("data", key, payload)
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	return payload, true
}

func (s *sqliteMetadataStore) PutData(inode uint64, payload []byte) {
	key := metadataInodeKey(inode)
	protected, err := s.codec.seal("data", key, payload)
	if err == nil {
		_, err = s.exec(`INSERT INTO file_data(inode, value) VALUES(?, ?) ON CONFLICT(inode) DO UPDATE SET value=excluded.value`, key, protected)
	}
	s.setErr(err)
}
func (s *sqliteMetadataStore) DeleteData(inode uint64) {
	_, err := s.exec(`DELETE FROM file_data WHERE inode = ?`, metadataInodeKey(inode))
	s.setErr(err)
}
func (s *sqliteMetadataStore) RangeData(yield func(uint64, []byte) bool) {
	s.rangeInodePayload(`SELECT inode, value FROM file_data ORDER BY inode`, "data", func(inode uint64, payload []byte) bool {
		return yield(inode, slices.Clone(payload))
	})
}

func (s *sqliteMetadataStore) ColdFile(inode uint64) ([]FileExtent, bool) {
	key := metadataInodeKey(inode)
	var exists int
	err := s.queryRow(`SELECT 1 FROM cold_files WHERE inode = ?`, key).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	rows, err := s.query(`SELECT segment_id, offset, length FROM file_extents WHERE inode = ? ORDER BY position`, key)
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	defer rows.Close()
	var extents []FileExtent
	for rows.Next() {
		var segmentID string
		var offsetKey, lengthKey []byte
		if err := rows.Scan(&segmentID, &offsetKey, &lengthKey); err != nil {
			s.setErr(err)
			return nil, false
		}
		offset, err := metadataInodeFromKey(offsetKey)
		if err != nil {
			s.setErr(err)
			return nil, false
		}
		length, err := metadataInodeFromKey(lengthKey)
		if err != nil {
			s.setErr(err)
			return nil, false
		}
		extents = append(extents, FileExtent{SegmentID: segmentID, Offset: offset, Length: length})
	}
	if err := rows.Err(); err != nil {
		s.setErr(err)
		return nil, false
	}
	return extents, true
}

func (s *sqliteMetadataStore) PutColdFile(inode uint64, extents []FileExtent) {
	key := metadataInodeKey(inode)
	err := s.withMutation(func() error {
		if _, err := s.exec(`INSERT INTO cold_files(inode) VALUES(?) ON CONFLICT(inode) DO NOTHING`, key); err != nil {
			return err
		}
		if _, err := s.exec(`DELETE FROM file_extents WHERE inode = ?`, key); err != nil {
			return err
		}
		for position, extent := range extents {
			if _, err := s.exec(`INSERT INTO file_extents(inode, position, segment_id, offset, length) VALUES(?, ?, ?, ?, ?)`, key, position, extent.SegmentID, metadataInodeKey(extent.Offset), metadataInodeKey(extent.Length)); err != nil {
				return err
			}
		}
		return nil
	})
	s.setErr(err)
}
func (s *sqliteMetadataStore) DeleteColdFile(inode uint64) {
	key := metadataInodeKey(inode)
	err := s.withMutation(func() error {
		if _, err := s.exec(`DELETE FROM file_extents WHERE inode = ?`, key); err != nil {
			return err
		}
		_, err := s.exec(`DELETE FROM cold_files WHERE inode = ?`, key)
		return err
	})
	s.setErr(err)
}
func (s *sqliteMetadataStore) RangeColdFiles(yield func(uint64, []FileExtent) bool) {
	rows, err := s.query(`SELECT c.inode, e.segment_id, e.offset, e.length FROM cold_files c LEFT JOIN file_extents e ON e.inode = c.inode ORDER BY c.inode, e.position`)
	if err != nil {
		s.setErr(err)
		return
	}
	defer rows.Close()
	var current uint64
	var currentSet bool
	var extents []FileExtent
	flush := func() bool {
		if !currentSet {
			return true
		}
		return yield(current, extents)
	}
	for rows.Next() {
		var inodeKey []byte
		var segmentID sql.NullString
		var offsetKey, lengthKey []byte
		if err := rows.Scan(&inodeKey, &segmentID, &offsetKey, &lengthKey); err != nil {
			s.setErr(err)
			return
		}
		inode, err := metadataInodeFromKey(inodeKey)
		if err != nil {
			s.setErr(err)
			return
		}
		if currentSet && inode != current {
			if !flush() {
				return
			}
			extents = nil
		}
		current, currentSet = inode, true
		if segmentID.Valid {
			offset, offsetErr := metadataInodeFromKey(offsetKey)
			length, lengthErr := metadataInodeFromKey(lengthKey)
			if offsetErr != nil || lengthErr != nil {
				s.setErr(errors.Join(offsetErr, lengthErr))
				return
			}
			extents = append(extents, FileExtent{SegmentID: segmentID.String, Offset: offset, Length: length})
		}
	}
	if err := rows.Err(); err != nil {
		s.setErr(err)
		return
	}
	flush()
}

func (s *sqliteMetadataStore) rangeInodePayload(query, kind string, yield func(uint64, []byte) bool) {
	rows, err := s.query(query)
	if err != nil {
		s.setErr(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var key, payload []byte
		if err := rows.Scan(&key, &payload); err != nil {
			s.setErr(err)
			return
		}
		inode, err := metadataInodeFromKey(key)
		if err != nil {
			s.setErr(err)
			return
		}
		payload, err = s.codec.open(kind, key, payload)
		if err != nil {
			s.setErr(err)
			return
		}
		if !yield(inode, payload) {
			return
		}
	}
	s.setErr(rows.Err())
}

func (s *sqliteMetadataStore) Segment(id string) (*Segment, bool) {
	var payload []byte
	err := s.queryRow(`SELECT value FROM segments WHERE id = ?`, id).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	payload, err = s.codec.open("segment", []byte(id), payload)
	if err != nil {
		s.setErr(err)
		return nil, false
	}
	var segment Segment
	if err := json.Unmarshal(payload, &segment); err != nil {
		s.setErr(err)
		return nil, false
	}
	return &segment, true
}

func (s *sqliteMetadataStore) PutSegment(id string, segment *Segment) {
	payload, err := json.Marshal(segment)
	if err == nil {
		payload, err = s.codec.seal("segment", []byte(id), payload)
	}
	if err == nil {
		_, err = s.exec(`INSERT INTO segments(id, value, inline) VALUES(?, ?, ?) ON CONFLICT(id) DO UPDATE SET value=excluded.value, inline=excluded.inline`, id, payload, isInlineSegment(segment))
	}
	s.setErr(err)
}
func (s *sqliteMetadataStore) DeleteSegment(id string) {
	_, err := s.exec(`DELETE FROM segments WHERE id = ?`, id)
	s.setErr(err)
}
func (s *sqliteMetadataStore) RangeSegments(yield func(string, *Segment) bool) {
	rows, err := s.query(`SELECT id, value FROM segments ORDER BY id`)
	if err != nil {
		s.setErr(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			s.setErr(err)
			return
		}
		payload, err = s.codec.open("segment", []byte(id), payload)
		if err != nil {
			s.setErr(err)
			return
		}
		var segment Segment
		if err := json.Unmarshal(payload, &segment); err != nil {
			s.setErr(err)
			return
		}
		if !yield(id, &segment) {
			return
		}
	}
	s.setErr(rows.Err())
}

func (s *sqliteMetadataStore) SegmentCount() int { return s.count(`SELECT COUNT(*) FROM segments`) }

func (s *sqliteMetadataStore) NeedsMaterialization() bool {
	var exists int
	err := s.queryRow(`
		SELECT EXISTS(SELECT 1 FROM file_data WHERE length(value) > ?)
		OR EXISTS(
			SELECT 1 FROM file_extents
			JOIN segments ON segments.id = file_extents.segment_id
			WHERE segments.inline = 1
		)`, s.codec.overhead()).Scan(&exists)
	if err != nil {
		s.setErr(err)
		return false
	}
	return exists != 0
}

func (s *sqliteMetadataStore) PruneUnlinked(ctx context.Context, retain map[uint64]struct{}) error {
	ctx = nonNilContext(ctx)
	var after uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := s.queryContext(ctx, `SELECT inode, value FROM nodes WHERE inode > ? ORDER BY inode LIMIT 256`, metadataInodeKey(after))
		if err != nil {
			s.setErr(err)
			return err
		}
		var scanned int
		var remove []uint64
		for rows.Next() {
			var key, payload []byte
			if err := rows.Scan(&key, &payload); err != nil {
				s.setErr(err)
				_ = rows.Close()
				return err
			}
			inode, err := metadataInodeFromKey(key)
			if err != nil {
				s.setErr(err)
				_ = rows.Close()
				return err
			}
			payload, err = s.codec.open("node", key, payload)
			if err != nil {
				s.setErr(err)
				_ = rows.Close()
				return err
			}
			var node Node
			if err := json.Unmarshal(payload, &node); err != nil {
				s.setErr(err)
				_ = rows.Close()
				return err
			}
			after = inode
			scanned++
			if inode != RootInode && node.Nlink == 0 {
				if _, ok := retain[inode]; !ok {
					remove = append(remove, inode)
				}
			}
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			s.setErr(rowsErr)
			return rowsErr
		}
		for _, inode := range remove {
			if err := ctx.Err(); err != nil {
				return err
			}
			s.DeleteDirectory(inode)
			s.DeleteNode(inode)
			s.DeleteData(inode)
			s.DeleteColdFile(inode)
			if s.Err() != nil {
				return s.Err()
			}
		}
		if scanned < 256 {
			return nil
		}
	}
}

func (s *sqliteMetadataStore) validateSegments(ctx context.Context, materializer *Materializer) error {
	rows, err := s.queryContext(ctx, `
		SELECT e.segment_id, e.offset, e.length, segments.value
		FROM file_extents e
		LEFT JOIN segments ON segments.id = e.segment_id
		ORDER BY e.segment_id, e.inode, e.position`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var previousID string
	var previousSegment *Segment
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var segmentID string
		var offsetKey, lengthKey, segmentPayload []byte
		if err := rows.Scan(&segmentID, &offsetKey, &lengthKey, &segmentPayload); err != nil {
			return err
		}
		if segmentID == "" {
			continue
		}
		if segmentID != previousID {
			if len(segmentPayload) == 0 {
				return fmt.Errorf("%w: recovery state is missing segment %s", ErrInvalidInput, segmentID)
			}
			segmentPayload, err = s.codec.open("segment", []byte(segmentID), segmentPayload)
			if err != nil {
				return err
			}
			var segment Segment
			if err := json.Unmarshal(segmentPayload, &segment); err != nil {
				return err
			}
			if segment.ID != segmentID {
				return fmt.Errorf("%w: recovery state is missing segment %s", ErrInvalidInput, segmentID)
			}
			previousID, previousSegment = segmentID, &segment
			if !isInlineSegment(previousSegment) {
				if strings.TrimSpace(previousSegment.Key) == "" {
					return fmt.Errorf("%w: recovery segment %s has no object key", ErrInvalidInput, segmentID)
				}
				_, store, err := materializer.storeForSegment(previousSegment)
				if err != nil {
					return err
				}
				if _, err := store.Head(previousSegment.Key); err != nil {
					return fmt.Errorf("validate recovery segment %s: %w", segmentID, err)
				}
			}
		}
		offset, err := metadataInodeFromKey(offsetKey)
		if err != nil {
			return err
		}
		length, err := metadataInodeFromKey(lengthKey)
		if err != nil {
			return err
		}
		if offset > previousSegment.Length || length > previousSegment.Length-offset {
			return fmt.Errorf("%w: recovery extent exceeds segment %s", ErrInvalidInput, segmentID)
		}
	}
	return rows.Err()
}

func (s *sqliteMetadataStore) EstimatedMemoryBytes() int64 { return s.cacheBytes + 1<<20 }

func (s *sqliteMetadataStore) EstimatedPersistentBytes() int64 {
	info, err := os.Stat(s.path)
	if err != nil {
		s.setErr(err)
		return 0
	}
	return info.Size()
}

func (s *sqliteMetadataStore) Snapshot(nextSeq, nextInode uint64) *SnapshotState {
	state := &SnapshotState{
		NextSeq: nextSeq, NextInode: nextInode,
		Nodes: make(map[uint64]*Node), Children: make(map[uint64]map[string]uint64),
		Data: make(map[uint64][]byte), ColdFiles: make(map[uint64][]FileExtent), Segments: make(map[string]*Segment),
	}
	s.RangeNodes(func(inode uint64, node *Node) bool { state.Nodes[inode] = node; return true })
	s.RangeDirectories(func(inode uint64, entries map[string]uint64) bool { state.Children[inode] = entries; return true })
	s.RangeData(func(inode uint64, payload []byte) bool { state.Data[inode] = payload; return true })
	s.RangeColdFiles(func(inode uint64, extents []FileExtent) bool { state.ColdFiles[inode] = extents; return true })
	s.RangeSegments(func(id string, segment *Segment) bool { state.Segments[id] = segment; return true })
	return state
}

func (s *sqliteMetadataStore) ReferenceSnapshot(nextSeq, nextInode uint64) *SnapshotState {
	return s.Snapshot(nextSeq, nextInode)
}

func (s *sqliteMetadataStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *sqliteMetadataStore) relocate(path string) error {
	path = strings.TrimSpace(path)
	if s == nil || s.db == nil || path == "" || path == s.path {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	s.db = nil
	if err := os.Rename(s.path, path); err != nil {
		return fmt.Errorf("promote metadata index: %w", err)
	}
	db, err := openSQLiteMetadataDB(path, s.cacheBytes)
	if err != nil {
		return err
	}
	s.db = db
	s.path = path
	return nil
}
