package activeconnections

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestNilRedisQuotaFailsClosed(t *testing.T) {
	var quota *RedisQuota
	lease, err := quota.Acquire(context.Background(), "team-1")
	if lease != nil {
		t.Fatalf("Acquire() lease = %#v, want nil", lease)
	}
	if !teamquota.IsUnavailable(err) {
		t.Fatalf("Acquire() error = %v, want unavailable", err)
	}
}
