package snapshot

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
)

type ListSnapshotCasefoldCollisionsRequest struct {
	VolumeID   string
	SnapshotID string
	TeamID     string
}

type SnapshotCasefoldCollision struct {
	NormalizedPath string   `json:"normalized_path"`
	Paths          []string `json:"paths"`
}

type ListSnapshotCompatibilityIssuesRequest struct {
	VolumeID     string
	SnapshotID   string
	TeamID       string
	Capabilities pathnorm.FilesystemCapabilities
}

func (m *Manager) ListSnapshotCasefoldCollisions(ctx context.Context, req *ListSnapshotCasefoldCollisionsRequest) ([]SnapshotCasefoldCollision, error) {
	if req == nil {
		return nil, fmt.Errorf("list snapshot casefold collisions request is nil")
	}
	issues, err := m.ListSnapshotCompatibilityIssues(ctx, &ListSnapshotCompatibilityIssuesRequest{
		VolumeID:   req.VolumeID,
		SnapshotID: req.SnapshotID,
		TeamID:     req.TeamID,
		Capabilities: pathnorm.FilesystemCapabilities{
			CaseSensitive:                   false,
			UnicodeNormalizationInsensitive: true,
		},
	})
	if err != nil {
		return nil, err
	}

	collisions := make([]SnapshotCasefoldCollision, 0)
	for _, issue := range issues {
		if issue.Code != pathnorm.IssueCodeCasefoldCollision {
			continue
		}
		collisions = append(collisions, SnapshotCasefoldCollision{
			NormalizedPath: issue.NormalizedPath,
			Paths:          issue.Paths,
		})
	}
	return collisions, nil
}

func (m *Manager) ListSnapshotCompatibilityIssues(ctx context.Context, req *ListSnapshotCompatibilityIssuesRequest) ([]pathnorm.CompatibilityIssue, error) {
	if req == nil {
		return nil, fmt.Errorf("list snapshot compatibility issues request is nil")
	}

	_, snap, session, rootInode, rootAttr, err := m.openSnapshotArchiveSession(ctx, req.VolumeID, req.SnapshotID, req.TeamID)
	if err != nil {
		return nil, err
	}
	_ = snap
	if session.close != nil {
		defer session.close()
	}

	collector := snapshotCompatibilityCollector{
		capabilities: req.Capabilities,
		buckets:      make(map[string]map[string]struct{}),
	}
	if err := collector.collect(ctx, session.meta, rootInode, "", rootAttr); err != nil {
		return nil, err
	}
	return collector.build(), nil
}

func (m *Manager) openSnapshotArchiveSession(
	ctx context.Context,
	volumeID, snapshotID, teamID string,
) (*db.SandboxVolume, *db.Snapshot, *snapshotArchiveSession, meta.Ino, *meta.Attr, error) {
	volume, err := m.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if err == db.ErrNotFound {
			return nil, nil, nil, 0, nil, ErrVolumeNotFound
		}
		return nil, nil, nil, 0, nil, fmt.Errorf("get volume: %w", err)
	}
	if volume.TeamID != teamID {
		return nil, nil, nil, 0, nil, ErrVolumeNotFound
	}

	snap, err := m.GetSnapshot(ctx, volumeID, snapshotID, teamID)
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}

	sessionFactory := m.newArchiveSession
	if sessionFactory == nil {
		sessionFactory = m.openArchiveSession
	}
	session, err := sessionFactory(ctx, volume)
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}

	rootInode := meta.Ino(snap.RootInode)
	rootAttr := &meta.Attr{}
	if errno := session.meta.GetAttr(meta.Background(), rootInode, rootAttr); errno != 0 {
		if errno == syscall.ENOENT {
			if session.close != nil {
				session.close()
			}
			return nil, nil, nil, 0, nil, ErrSnapshotNotFound
		}
		if session.close != nil {
			session.close()
		}
		return nil, nil, nil, 0, nil, fmt.Errorf("get snapshot root inode %d: %w", rootInode, syscall.Errno(errno))
	}

	return volume, snap, session, rootInode, rootAttr, nil
}

type snapshotCompatibilityCollector struct {
	capabilities pathnorm.FilesystemCapabilities
	buckets      map[string]map[string]struct{}
	issues       []pathnorm.CompatibilityIssue
}

func (c *snapshotCompatibilityCollector) collect(
	ctx context.Context,
	metaClient snapshotArchiveMeta,
	inode meta.Ino,
	relPath string,
	attr *meta.Attr,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attr == nil {
		return fmt.Errorf("snapshot attr is nil for %q", relPath)
	}

	if relPath != "" {
		logicalPath := "/" + strings.TrimPrefix(relPath, "/")
		c.issues = append(c.issues, pathnorm.ValidatePathCompatibility(logicalPath, c.capabilities)...)
		if !c.capabilities.CaseSensitive || c.capabilities.UnicodeNormalizationInsensitive {
			normalized := pathnorm.CompatibilityPathKey(logicalPath, c.capabilities)
			if normalized != "" {
				paths := c.buckets[normalized]
				if paths == nil {
					paths = make(map[string]struct{})
					c.buckets[normalized] = paths
				}
				paths[logicalPath] = struct{}{}
			}
		}
	}

	if attr.Typ != meta.TypeDirectory {
		return nil
	}

	var entries []*meta.Entry
	if errno := metaClient.Readdir(meta.Background(), inode, 1, &entries); errno != 0 {
		return fmt.Errorf("readdir inode %d: %w", inode, syscall.Errno(errno))
	}
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Name) < string(entries[j].Name)
	})
	for _, entry := range entries {
		name := string(entry.Name)
		if name == "." || name == ".." || name == "" {
			continue
		}
		nextPath := name
		if relPath != "" {
			nextPath = path.Join(relPath, name)
		}
		entryAttr := entry.Attr
		if entryAttr == nil {
			entryAttr = &meta.Attr{}
			if errno := metaClient.GetAttr(meta.Background(), entry.Inode, entryAttr); errno != 0 {
				return fmt.Errorf("getattr inode %d: %w", entry.Inode, syscall.Errno(errno))
			}
		}
		if err := c.collect(ctx, metaClient, entry.Inode, nextPath, entryAttr); err != nil {
			return err
		}
	}

	return nil
}

func (c *snapshotCompatibilityCollector) build() []pathnorm.CompatibilityIssue {
	issues := append([]pathnorm.CompatibilityIssue{}, c.issues...)
	for normalizedPath, pathSet := range c.buckets {
		if len(pathSet) < 2 {
			continue
		}
		paths := make([]string, 0, len(pathSet))
		for p := range pathSet {
			paths = append(paths, p)
		}
		issues = append(issues, pathnorm.BuildCasefoldCollisionIssue(normalizedPath, paths))
	}
	issues = pathnorm.DeduplicateIssues(issues)
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Code != issues[j].Code {
			return issues[i].Code < issues[j].Code
		}
		if issues[i].NormalizedPath != issues[j].NormalizedPath {
			return issues[i].NormalizedPath < issues[j].NormalizedPath
		}
		return issues[i].Path < issues[j].Path
	})
	return issues
}
