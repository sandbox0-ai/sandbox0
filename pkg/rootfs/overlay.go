package rootfs

import (
	"fmt"
	"strings"
)

func RewriteOverlayUpperWorkOptions(options []string, upperDir, workDir string) ([]string, error) {
	upperDir = strings.TrimSpace(upperDir)
	workDir = strings.TrimSpace(workDir)
	if upperDir == "" || workDir == "" {
		return nil, fmt.Errorf("upperdir and workdir are required")
	}
	out := make([]string, len(options))
	copy(out, options)
	var upperFound, workFound bool
	for i, option := range out {
		switch {
		case strings.HasPrefix(option, "upperdir="):
			out[i] = "upperdir=" + upperDir
			upperFound = true
		case strings.HasPrefix(option, "workdir="):
			out[i] = "workdir=" + workDir
			workFound = true
		}
	}
	if !upperFound {
		out = append(out, "upperdir="+upperDir)
	}
	if !workFound {
		out = append(out, "workdir="+workDir)
	}
	return out, nil
}
