package http

import "testing"

func TestVolumeWatchSubscriptionGuardEnforcesGlobalLimitAcrossTeams(t *testing.T) {
	guard := volumeWatchSubscriptionGuard{
		maxGlobal: 3,
	}

	var releases []func()
	for index := 0; index < 3; index++ {
		release, limit := guard.acquire()
		if limit != volumeWatchSubscriptionAllowed {
			t.Fatalf("acquire %d limit = %v, want allowed", index, limit)
		}
		releases = append(releases, release)
	}
	if release, limit := guard.acquire(); limit != volumeWatchSubscriptionServerLimit || release != nil {
		t.Fatalf("global-limit acquire returned release=%t limit=%v, want server limit", release != nil, limit)
	}

	releases[0]()
	releases[0]()
	replacement, limit := guard.acquire()
	if limit != volumeWatchSubscriptionAllowed {
		t.Fatalf("replacement acquire limit = %v, want allowed", limit)
	}
	for _, release := range releases[1:] {
		release()
	}
	replacement()

	if globalCount := guard.count(); globalCount != 0 {
		t.Fatalf("global count after release = %d, want 0", globalCount)
	}
}
