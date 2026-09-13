//go:build !linux

package rootfsblock

import "os"

func materializedFileSeekEnabled(*os.File) (bool, error) { return false, nil }

func seekMaterializedFile(*os.File, int64, bool) (int64, error) {
	return 0, errMaterializedSeekUnsupported
}

func sameMaterializedFile(before, after os.FileInfo) bool {
	return os.SameFile(before, after) && before.Mode() == after.Mode() &&
		before.Size() == after.Size() && before.ModTime() == after.ModTime()
}
