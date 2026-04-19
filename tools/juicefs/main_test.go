package main

import (
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

func TestNewVFSConfigIncludesFuseOptions(t *testing.T) {
	metaConf := meta.DefaultConf()
	format := &meta.Format{BlockSize: 4}
	chunkConf := &chunk.Config{BlockSize: 4096}
	cfg := &mountConfig{
		attrCache:     "2s",
		entryCache:    "3s",
		dirEntryCache: "4s",
		subdir:        "/workspace/data",
	}

	vfsConf := newVFSConfig(metaConf, format, chunkConf, cfg)

	if vfsConf.Meta != metaConf {
		t.Fatal("expected meta config to be preserved")
	}
	if vfsConf.Chunk != chunkConf {
		t.Fatal("expected chunk config to be preserved")
	}
	if vfsConf.FuseOpts == nil {
		t.Fatal("expected FuseOpts to be initialized")
	}
	if vfsConf.AttrTimeout != 2*time.Second {
		t.Fatalf("AttrTimeout = %v, want 2s", vfsConf.AttrTimeout)
	}
	if vfsConf.EntryTimeout != 3*time.Second {
		t.Fatalf("EntryTimeout = %v, want 3s", vfsConf.EntryTimeout)
	}
	if vfsConf.DirEntryTimeout != 4*time.Second {
		t.Fatalf("DirEntryTimeout = %v, want 4s", vfsConf.DirEntryTimeout)
	}
	if vfsConf.Subdir != "/workspace/data" {
		t.Fatalf("Subdir = %q, want /workspace/data", vfsConf.Subdir)
	}
	if vfsConf.Format.BlockSize != format.BlockSize {
		t.Fatalf("BlockSize = %d, want %d", vfsConf.Format.BlockSize, format.BlockSize)
	}
}
