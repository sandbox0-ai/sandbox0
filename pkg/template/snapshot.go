package template

import "strings"

// BuildSnapshotIDPrefix reserves rootfs snapshots created for asynchronous
// template image builds. Public snapshot APIs must not expose these IDs.
const BuildSnapshotIDPrefix = "template-build-"

// BuildSnapshotID derives the deterministic internal rootfs snapshot ID for a
// template build.
func BuildSnapshotID(buildID string) string {
	return BuildSnapshotIDPrefix + strings.ReplaceAll(strings.TrimSpace(buildID), "-", "")
}

// IsBuildSnapshotID reports whether a rootfs snapshot belongs to an internal
// template image build.
func IsBuildSnapshotID(snapshotID string) bool {
	return strings.HasPrefix(strings.TrimSpace(snapshotID), BuildSnapshotIDPrefix)
}
