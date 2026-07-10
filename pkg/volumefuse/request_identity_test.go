package volumefuse

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestContextForHeaderPreservesResendIdentity(t *testing.T) {
	header := &fuse.InHeader{Unique: uint64(1)<<63 | 42}
	identity, ok := RequestIdentityFromContext(contextForHeader(header))
	if !ok || identity.Unique != 42 || !identity.Resend {
		t.Fatalf("identity = %+v, ok=%v", identity, ok)
	}
}
