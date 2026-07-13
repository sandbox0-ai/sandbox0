package rootfs

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const rootFSFileSizeIndexVersion = 1

// rootFSFileChange captures the logical regular-file state change represented
// by one filtered OCI layer tar header.
type rootFSFileChange struct {
	Path      string
	Size      int64
	Regular   bool
	LinkPath  string
	Directory bool
	Delete    bool
	Opaque    bool
}

// rootFSFileSizeIndex tracks positive logical sizes for regular files stored
// in the persisted writable rootfs layer chain, including unbound portal
// content embedded in that chain. Base-image files are excluded.
type rootFSFileSizeIndex map[string]int64

type rootFSFileSizeIndexFile struct {
	Version int                 `json:"version"`
	Files   rootFSFileSizeIndex `json:"files"`
}

func rootFSFileChangeFromTarHeader(header *tar.Header) (rootFSFileChange, bool) {
	if header == nil {
		return rootFSFileChange{}, false
	}
	if target, opaque, ok := rootFSTarWhiteoutTargetPath(header.Name); ok {
		return rootFSFileChange{Path: target, Delete: true, Opaque: opaque}, true
	}

	changePath := cleanRootFSPath(header.Name)
	if changePath == "/" {
		return rootFSFileChange{}, false
	}
	change := rootFSFileChange{Path: changePath}
	switch header.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		change.Regular = true
		change.Size = header.Size
	case tar.TypeLink:
		change.Regular = true
		change.LinkPath = cleanRootFSPath(header.Linkname)
	case tar.TypeDir:
		change.Directory = true
	}
	return change, true
}

func (index rootFSFileSizeIndex) Apply(changes []rootFSFileChange) bool {
	if index == nil {
		return false
	}
	lower := make(rootFSFileSizeIndex, len(index))
	for filePath, size := range index {
		lower[filePath] = size
	}
	// OCI opaque whiteouts remove entries inherited from lower layers. Entries
	// added by the same layer survive regardless of tar-header ordering.
	for _, change := range changes {
		if !change.Delete || !change.Opaque {
			continue
		}
		changePath := cleanRootFSPath(change.Path)
		if changePath == "/" {
			continue
		}
		index.deleteInheritedPath(lower, changePath, !change.Opaque)
	}
	for _, change := range changes {
		changePath := cleanRootFSPath(change.Path)
		if changePath == "/" {
			continue
		}
		if change.Delete {
			if !change.Opaque {
				index.deletePath(changePath, true)
			}
			continue
		}
		if change.Directory {
			delete(index, changePath)
			continue
		}
		index.deletePath(changePath, true)
		if change.Regular {
			size := change.Size
			if change.LinkPath != "" {
				var ok bool
				size, ok = index[cleanRootFSPath(change.LinkPath)]
				if !ok {
					return false
				}
			}
			if size > 0 {
				index[changePath] = size
			}
		}
	}
	return true
}

func (index rootFSFileSizeIndex) deleteInheritedPath(lower rootFSFileSizeIndex, target string, includeTarget bool) {
	target = cleanRootFSPath(target)
	prefix := strings.TrimSuffix(target, "/") + "/"
	for filePath := range lower {
		if (includeTarget && filePath == target) || strings.HasPrefix(filePath, prefix) {
			delete(index, filePath)
		}
	}
}

func (index rootFSFileSizeIndex) deletePath(target string, includeTarget bool) {
	target = cleanRootFSPath(target)
	prefix := strings.TrimSuffix(target, "/") + "/"
	for filePath := range index {
		if (includeTarget && filePath == target) || strings.HasPrefix(filePath, prefix) {
			delete(index, filePath)
		}
	}
}

func (index rootFSFileSizeIndex) deletedBytes(target string, includeTarget bool, filter rootFSPathFilter, counted map[string]struct{}) int64 {
	if len(index) == 0 {
		return 0
	}
	target = cleanRootFSPath(target)
	prefix := strings.TrimSuffix(target, "/") + "/"
	var total int64
	for filePath, size := range index {
		if size <= 0 || (!includeTarget || filePath != target) && !strings.HasPrefix(filePath, prefix) {
			continue
		}
		if filter.Excludes(filePath) {
			continue
		}
		if _, ok := counted[filePath]; ok {
			continue
		}
		counted[filePath] = struct{}{}
		total += size
	}
	return total
}

func (index rootFSFileSizeIndex) replacementDeletedBytes(target string, currentIsDir, currentIsRegular bool, filter rootFSPathFilter, counted map[string]struct{}) int64 {
	target = cleanRootFSPath(target)
	if currentIsDir {
		size := index[target]
		if size <= 0 || filter.Excludes(target) {
			return 0
		}
		if _, ok := counted[target]; ok {
			return 0
		}
		counted[target] = struct{}{}
		return size
	}
	if currentIsRegular {
		return index.deletedBytes(target, false, filter, counted)
	}
	return index.deletedBytes(target, true, filter, counted)
}

func rootFSFileSizeIndexPath(baselineDir string) string {
	return filepath.Join(filepath.Clean(baselineDir), "files.json")
}

func writeRootFSFileSizeIndexTemp(parent string, index rootFSFileSizeIndex) (string, error) {
	file, err := os.CreateTemp(parent, ".rootfs-file-sizes-*.json")
	if err != nil {
		return "", err
	}
	name := file.Name()
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = os.Remove(name)
		}
	}()
	if index == nil {
		index = make(rootFSFileSizeIndex)
	}
	if err := json.NewEncoder(file).Encode(rootFSFileSizeIndexFile{
		Version: rootFSFileSizeIndexVersion,
		Files:   index,
	}); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	removeOnError = false
	return name, nil
}

func loadRootFSFileSizeIndex(indexPath string) (rootFSFileSizeIndex, error) {
	file, err := os.Open(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var stored rootFSFileSizeIndexFile
	if err := json.NewDecoder(file).Decode(&stored); err != nil {
		return nil, err
	}
	if stored.Version != rootFSFileSizeIndexVersion {
		return nil, fmt.Errorf("unsupported rootfs file-size index version %d", stored.Version)
	}
	index := make(rootFSFileSizeIndex, len(stored.Files))
	for filePath, size := range stored.Files {
		if size <= 0 {
			continue
		}
		clean := cleanRootFSPath(filePath)
		if clean == "/" {
			continue
		}
		index[clean] = size
	}
	return index, nil
}
