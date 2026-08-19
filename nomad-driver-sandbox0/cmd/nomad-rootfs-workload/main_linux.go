// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

// nomad-rootfs-workload is a deterministic filesystem correctness probe for
// the experimental Nomad RootFS data path. It is intentionally dependency-free
// so the same static binary can run inside a minimal gVisor guest.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

const (
	fsxMaximumBytes = 16 << 20
	sparseSizeBytes = int64(100 << 30)
)

type manifest struct {
	Version           int    `json:"version"`
	FSXSHA256         string `json:"fsx_sha256"`
	FSXSize           int64  `json:"fsx_size"`
	MMapSHA256        string `json:"mmap_sha256"`
	SparseSize        int64  `json:"sparse_size"`
	SparseGuestBlocks int64  `json:"sparse_guest_reported_blocks"`
	UserXAttr         string `json:"user_xattr"`
	ACLHex            string `json:"acl_hex,omitempty"`
	ACLSetError       string `json:"acl_set_error,omitempty"`
	ChownSetError     string `json:"arbitrary_chown_error,omitempty"`
	MetadataUID       uint32 `json:"metadata_uid"`
	MetadataGID       uint32 `json:"metadata_gid"`
	MetadataMode      uint32 `json:"metadata_mode"`
	ZeroFlatCount     int    `json:"zero_flat_count"`
	ZeroDeepCount     int    `json:"zero_deep_count"`
	FourKiBCount      int    `json:"four_kib_count"`
	SixtyFourKiBCount int    `json:"sixty_four_kib_count"`
}

type result struct {
	Mode            string           `json:"mode"`
	StartedAt       time.Time        `json:"started_at"`
	CompletedAt     time.Time        `json:"completed_at"`
	DurationMS      int64            `json:"duration_ms"`
	PhaseDurationMS map[string]int64 `json:"phase_duration_ms"`
	Manifest        manifest         `json:"manifest"`
}

func main() {
	mode := flag.String("mode", "mutate", "mutate or verify")
	root := flag.String("root", "/sandbox0-workload", "absolute workload directory")
	flag.Parse()
	if !filepath.IsAbs(*root) || filepath.Clean(*root) == "/" {
		fatalf("root must be a safe absolute path")
	}
	started := time.Now().UTC()
	output := result{Mode: *mode, StartedAt: started, PhaseDurationMS: make(map[string]int64)}
	var err error
	switch *mode {
	case "mutate":
		output.Manifest, err = mutate(*root, output.PhaseDurationMS)
	case "verify":
		output.Manifest, err = verify(*root, output.PhaseDurationMS)
	default:
		err = fmt.Errorf("unsupported mode %q", *mode)
	}
	if err != nil {
		fatalf("%s: %v", *mode, err)
	}
	output.CompletedAt = time.Now().UTC()
	output.DurationMS = output.CompletedAt.Sub(started).Milliseconds()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fatalf("encode result: %v", err)
	}
}

func mutate(root string, timings map[string]int64) (manifest, error) {
	if err := os.RemoveAll(root); err != nil {
		return manifest{}, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return manifest{}, err
	}
	out := manifest{
		Version: 1, UserXAttr: "sandbox0-xattr-v1",
		MetadataUID: uint32(os.Geteuid()), MetadataGID: uint32(os.Getegid()), MetadataMode: 0o641,
		ZeroFlatCount: 20000, ZeroDeepCount: 5000,
		FourKiBCount: 2000, SixtyFourKiBCount: 200,
	}
	if err := timed(timings, "small_files", func() error { return createSmallFiles(root, out) }); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "fsx", func() error {
		var err error
		out.FSXSHA256, out.FSXSize, err = runFSX(filepath.Join(root, "fsx.bin"), 12000)
		return err
	}); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "mmap", func() error {
		var err error
		out.MMapSHA256, err = writeMMap(filepath.Join(root, "mmap.bin"))
		return err
	}); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "sparse", func() error {
		var err error
		out.SparseSize, out.SparseGuestBlocks, err = writeSparse(filepath.Join(root, "sparse-100g.bin"))
		return err
	}); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "links_xattr_acl", func() error {
		return writeLinksAndMetadata(root, &out)
	}); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "inotify", func() error { return exerciseInotify(root) }); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "overlay_mutations", mutateBasePaths); err != nil {
		return manifest{}, err
	}
	payload, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return manifest{}, err
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(payload, '\n'), 0o644); err != nil {
		return manifest{}, err
	}
	if err := syncTree(root); err != nil {
		return manifest{}, err
	}
	return out, nil
}

func verify(root string, timings map[string]int64) (manifest, error) {
	payload, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return manifest{}, err
	}
	var expected manifest
	if err := json.Unmarshal(payload, &expected); err != nil {
		return manifest{}, err
	}
	if expected.Version != 1 {
		return manifest{}, fmt.Errorf("unsupported manifest version %d", expected.Version)
	}
	if err := timed(timings, "small_files", func() error { return verifySmallFiles(root, expected) }); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "fsx", func() error {
		return verifyFileDigest(filepath.Join(root, "fsx.bin"), expected.FSXSize, expected.FSXSHA256)
	}); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "mmap", func() error {
		return verifyFileDigest(filepath.Join(root, "mmap.bin"), 8<<20, expected.MMapSHA256)
	}); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "sparse", func() error { return verifySparse(root, expected) }); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "links_xattr_acl", func() error { return verifyLinksAndMetadata(root, expected) }); err != nil {
		return manifest{}, err
	}
	if err := timed(timings, "overlay_mutations", verifyBasePaths); err != nil {
		return manifest{}, err
	}
	return expected, nil
}

func createSmallFiles(root string, counts manifest) error {
	zeroFlat := filepath.Join(root, "zero-flat")
	zeroDeep := filepath.Join(root, "zero-deep")
	fourKiB := filepath.Join(root, "4k")
	sixtyFourKiB := filepath.Join(root, "64k")
	for _, path := range []string{zeroFlat, zeroDeep, fourKiB, sixtyFourKiB} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
	}
	for index := 0; index < counts.ZeroFlatCount; index++ {
		if err := os.WriteFile(filepath.Join(zeroFlat, fmt.Sprintf("%06d", index)), nil, 0o640); err != nil {
			return err
		}
	}
	for index := 0; index < counts.ZeroDeepCount; index++ {
		dir := filepath.Join(zeroDeep, fmt.Sprintf("%02d", index%100), fmt.Sprintf("%02d", (index/100)%50))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%06d", index)), nil, 0o640); err != nil {
			return err
		}
	}
	fourPayload := bytes.Repeat([]byte{0x4a}, 4096)
	for index := 0; index < counts.FourKiBCount; index++ {
		if err := os.WriteFile(filepath.Join(fourKiB, fmt.Sprintf("%06d", index)), fourPayload, 0o640); err != nil {
			return err
		}
	}
	sixtyFourPayload := bytes.Repeat([]byte{0x64}, 64<<10)
	for index := 0; index < counts.SixtyFourKiBCount; index++ {
		if err := os.WriteFile(filepath.Join(sixtyFourKiB, fmt.Sprintf("%06d", index)), sixtyFourPayload, 0o640); err != nil {
			return err
		}
	}
	if err := os.Rename(filepath.Join(zeroFlat, "000000"), filepath.Join(zeroFlat, "renamed-000000")); err != nil {
		return err
	}
	return os.Remove(filepath.Join(zeroFlat, "000001"))
}

func verifySmallFiles(root string, counts manifest) error {
	entries, err := os.ReadDir(filepath.Join(root, "zero-flat"))
	if err != nil {
		return err
	}
	if len(entries) != counts.ZeroFlatCount-1 {
		return fmt.Errorf("zero-flat count %d, want %d", len(entries), counts.ZeroFlatCount-1)
	}
	if _, err := os.Stat(filepath.Join(root, "zero-flat", "renamed-000000")); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "zero-flat", "000001")); !os.IsNotExist(err) {
		return fmt.Errorf("unlinked small file is visible")
	}
	for path, count := range map[string]int{"4k": counts.FourKiBCount, "64k": counts.SixtyFourKiBCount} {
		entries, err := os.ReadDir(filepath.Join(root, path))
		if err != nil {
			return err
		}
		if len(entries) != count {
			return fmt.Errorf("%s count %d, want %d", path, len(entries), count)
		}
	}
	return nil
}

func runFSX(path string, operations int) (string, int64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	random := rand.New(rand.NewSource(0x5a17))
	model := make([]byte, 0)
	for operation := 0; operation < operations; operation++ {
		switch random.Intn(4) {
		case 0, 1:
			offset := random.Intn(fsxMaximumBytes)
			length := 1 + random.Intn(8192)
			if offset+length > fsxMaximumBytes {
				length = fsxMaximumBytes - offset
			}
			if offset+length > len(model) {
				model = append(model, make([]byte, offset+length-len(model))...)
			}
			payload := make([]byte, length)
			if _, err := random.Read(payload); err != nil {
				return "", 0, err
			}
			if _, err := file.WriteAt(payload, int64(offset)); err != nil {
				return "", 0, err
			}
			copy(model[offset:offset+length], payload)
		case 2:
			length := random.Intn(fsxMaximumBytes + 1)
			if err := file.Truncate(int64(length)); err != nil {
				return "", 0, err
			}
			if length < len(model) {
				model = model[:length]
			} else if length > len(model) {
				model = append(model, make([]byte, length-len(model))...)
			}
		case 3:
			if len(model) == 0 {
				continue
			}
			offset := random.Intn(len(model))
			length := min(4096, len(model)-offset)
			actual := make([]byte, length)
			if _, err := file.ReadAt(actual, int64(offset)); err != nil && err != io.EOF {
				return "", 0, err
			}
			if !bytes.Equal(actual, model[offset:offset+length]) {
				return "", 0, fmt.Errorf("fsx mismatch at operation %d offset %d", operation, offset)
			}
		}
		if operation%64 == 0 {
			if err := file.Sync(); err != nil {
				return "", 0, err
			}
		}
	}
	if err := file.Sync(); err != nil {
		return "", 0, err
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	if !bytes.Equal(actual, model) {
		return "", 0, fmt.Errorf("fsx final model mismatch")
	}
	sum := sha256.Sum256(actual)
	return hex.EncodeToString(sum[:]), int64(len(actual)), nil
}

func writeMMap(path string) (string, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if err := file.Truncate(8 << 20); err != nil {
		return "", err
	}
	mapping, err := syscall.Mmap(int(file.Fd()), 0, 8<<20, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return "", err
	}
	mapping[0] = 0x11
	copy(mapping[(4<<20)+123:], bytes.Repeat([]byte{0x7e}, 4096))
	mapping[len(mapping)-1] = 0xee
	if err := file.Sync(); err != nil {
		_ = syscall.Munmap(mapping)
		return "", err
	}
	if err := syscall.Munmap(mapping); err != nil {
		return "", err
	}
	return digestFile(path)
}

func writeSparse(path string) (int64, int64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	if err := file.Truncate(sparseSizeBytes); err != nil {
		return 0, 0, err
	}
	payload := bytes.Repeat([]byte{0xa5}, 4096)
	if _, err := file.WriteAt(payload, 0); err != nil {
		return 0, 0, err
	}
	if _, err := file.WriteAt(payload, sparseSizeBytes-int64(len(payload))); err != nil {
		return 0, 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, 0, err
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &stat); err != nil {
		return 0, 0, err
	}
	if stat.Size != sparseSizeBytes {
		return 0, 0, fmt.Errorf("sparse file size %d, want %d", stat.Size, sparseSizeBytes)
	}
	return stat.Size, stat.Blocks, nil
}

func writeLinksAndMetadata(root string, out *manifest) error {
	path := filepath.Join(root, "metadata-source")
	if err := os.WriteFile(path, []byte("hardlink-and-metadata"), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(path, os.FileMode(out.MetadataMode)); err != nil {
		return err
	}
	if err := os.Chown(path, os.Geteuid(), os.Getegid()); err != nil {
		return err
	}
	if err := os.Chown(path, 1234, 2345); err != nil {
		out.ChownSetError = err.Error()
	} else {
		out.MetadataUID = 1234
		out.MetadataGID = 2345
	}
	if err := os.Link(path, filepath.Join(root, "metadata-hardlink")); err != nil {
		return err
	}
	if err := os.Symlink("metadata-source", filepath.Join(root, "metadata-symlink")); err != nil {
		return err
	}
	if err := syscall.Setxattr(path, "user.sandbox0", []byte(out.UserXAttr), 0); err != nil {
		return fmt.Errorf("set user xattr: %w", err)
	}
	acl := minimalACL(0o7, 0o5, 0o5)
	if err := syscall.Setxattr(path, "system.posix_acl_access", acl, 0); err != nil {
		out.ACLSetError = err.Error()
	} else {
		out.ACLHex = hex.EncodeToString(acl)
	}
	return nil
}

func verifyLinksAndMetadata(root string, expected manifest) error {
	first, err := os.Stat(filepath.Join(root, "metadata-source"))
	if err != nil {
		return err
	}
	second, err := os.Stat(filepath.Join(root, "metadata-hardlink"))
	if err != nil {
		return err
	}
	if !os.SameFile(first, second) {
		return fmt.Errorf("hardlink inode identity was not preserved")
	}
	stat, ok := first.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("metadata stat has unexpected type %T", first.Sys())
	}
	if stat.Uid != expected.MetadataUID || stat.Gid != expected.MetadataGID ||
		uint32(first.Mode().Perm()) != expected.MetadataMode {
		return fmt.Errorf("metadata ownership/mode got %d:%d %04o, want %d:%d %04o",
			stat.Uid, stat.Gid, first.Mode().Perm(), expected.MetadataUID, expected.MetadataGID, expected.MetadataMode)
	}
	linkTarget, err := os.Readlink(filepath.Join(root, "metadata-symlink"))
	if err != nil {
		return err
	}
	if linkTarget != "metadata-source" {
		return fmt.Errorf("symlink target %q, want metadata-source", linkTarget)
	}
	xattr := make([]byte, 4096)
	size, err := syscall.Getxattr(filepath.Join(root, "metadata-source"), "user.sandbox0", xattr)
	if err != nil {
		return err
	}
	if string(xattr[:size]) != expected.UserXAttr {
		return fmt.Errorf("user xattr mismatch")
	}
	if expected.ACLHex != "" {
		acl, err := hex.DecodeString(expected.ACLHex)
		if err != nil {
			return err
		}
		actual := make([]byte, 4096)
		size, err := syscall.Getxattr(filepath.Join(root, "metadata-source"), "system.posix_acl_access", actual)
		if err != nil {
			return err
		}
		if !bytes.Equal(actual[:size], acl) {
			return fmt.Errorf("ACL xattr mismatch")
		}
	}
	return nil
}

func exerciseInotify(root string) error {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	if _, err := syscall.InotifyAddWatch(fd, root, syscall.IN_CREATE|syscall.IN_CLOSE_WRITE); err != nil {
		return err
	}
	path := filepath.Join(root, "inotify-probe")
	if err := os.WriteFile(path, []byte("event"), 0o600); err != nil {
		return err
	}
	buffer := make([]byte, syscall.SizeofInotifyEvent+256)
	read, err := syscall.Read(fd, buffer)
	if err != nil {
		return err
	}
	if read < syscall.SizeofInotifyEvent {
		return fmt.Errorf("short inotify event: %d bytes", read)
	}
	return os.Remove(path)
}

func mutateBasePaths() error {
	if _, err := os.Lstat("/etc/os-release.sandbox0-moved"); os.IsNotExist(err) {
		if err := os.Rename("/etc/os-release", "/etc/os-release.sandbox0-moved"); err != nil {
			return fmt.Errorf("rename lower os-release: %w", err)
		}
	}
	if err := os.Remove("/etc/alpine-release"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("whiteout lower alpine-release: %w", err)
	}
	if err := os.RemoveAll("/etc/profile.d"); err != nil {
		return err
	}
	if err := os.Mkdir("/etc/profile.d", 0o755); err != nil {
		return err
	}
	return os.WriteFile("/etc/profile.d/sandbox0-only", []byte("opaque-directory\n"), 0o644)
}

func verifyBasePaths() error {
	if _, err := os.Lstat("/etc/os-release"); !os.IsNotExist(err) {
		return fmt.Errorf("renamed lower source remains visible")
	}
	if _, err := os.Lstat("/etc/os-release.sandbox0-moved"); err != nil {
		return err
	}
	if _, err := os.Lstat("/etc/alpine-release"); !os.IsNotExist(err) {
		return fmt.Errorf("deleted lower file remains visible")
	}
	entries, err := os.ReadDir("/etc/profile.d")
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name() != "sandbox0-only" {
		return fmt.Errorf("opaque directory leaked lower entries: %v", entryNames(entries))
	}
	return nil
}

func verifySparse(root string, expected manifest) error {
	path := filepath.Join(root, "sparse-100g.bin")
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return err
	}
	if stat.Size != expected.SparseSize {
		return fmt.Errorf("sparse shape changed: size=%d blocks=%d", stat.Size, stat.Blocks)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	first := make([]byte, 4096)
	last := make([]byte, 4096)
	hole := make([]byte, 4096)
	if _, err := file.ReadAt(first, 0); err != nil {
		return err
	}
	if _, err := file.ReadAt(last, expected.SparseSize-4096); err != nil {
		return err
	}
	if _, err := file.ReadAt(hole, 50<<30); err != nil {
		return err
	}
	if !bytes.Equal(first, bytes.Repeat([]byte{0xa5}, 4096)) || !bytes.Equal(first, last) ||
		!bytes.Equal(hole, make([]byte, 4096)) {
		return fmt.Errorf("sparse data or hole mismatch")
	}
	return nil
}

func verifyFileDigest(path string, size int64, digest string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() != size {
		return fmt.Errorf("%s size %d, want %d", path, info.Size(), size)
	}
	actual, err := digestFile(path)
	if err != nil {
		return err
	}
	if actual != digest {
		return fmt.Errorf("%s digest %s, want %s", path, actual, digest)
	}
	return nil
}

func digestFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func minimalACL(user, group, other uint16) []byte {
	const undefinedID = ^uint32(0)
	entries := []struct {
		tag  uint16
		perm uint16
		id   uint32
	}{{0x01, user, undefinedID}, {0x04, group, undefinedID}, {0x10, group, undefinedID}, {0x20, other, undefinedID}}
	payload := make([]byte, 4+len(entries)*8)
	binary.LittleEndian.PutUint32(payload[:4], 0x0002)
	for index, entry := range entries {
		offset := 4 + index*8
		binary.LittleEndian.PutUint16(payload[offset:offset+2], entry.tag)
		binary.LittleEndian.PutUint16(payload[offset+2:offset+4], entry.perm)
		binary.LittleEndian.PutUint32(payload[offset+4:offset+8], entry.id)
	}
	return payload
}

func syncTree(root string) error {
	var paths []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, path := range paths {
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func timed(timings map[string]int64, name string, operation func() error) error {
	started := time.Now()
	err := operation()
	timings[name] = time.Since(started).Milliseconds()
	return err
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
