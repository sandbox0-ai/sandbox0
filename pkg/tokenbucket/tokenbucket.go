package tokenbucket

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const maxExactLuaInteger int64 = 1<<53 - 1

var (
	ErrClosed            = errors.New("token bucket is closed")
	ErrInvalidKey        = errors.New("token bucket key is required")
	ErrInvalidPolicy     = errors.New("invalid token bucket policy")
	ErrInvalidTokenCost  = errors.New("token cost must be positive")
	ErrCostExceedsBurst  = errors.New("token cost exceeds policy burst")
	ErrAdmissionMissing  = errors.New("team quota admission marker is missing")
	ErrAdmissionDisabled = errors.New("team quota admission is disabled")
	ErrAdmissionCorrupt  = errors.New("team quota admission marker is corrupt")
)

// Policy defines how tokens refill and the maximum accumulated burst.
//
// Tokens are added during each Interval. Revision must change whenever Tokens,
// Interval, or Burst changes for an existing key.
type Policy struct {
	Tokens   int64
	Interval time.Duration
	Burst    int64
	Revision int64
}

// Validate verifies that a policy can be represented exactly by the Redis Lua
// implementation.
func (p Policy) Validate() error {
	switch {
	case p.Tokens <= 0:
		return fmt.Errorf("%w: tokens must be positive", ErrInvalidPolicy)
	case p.Tokens > maxExactLuaInteger:
		return fmt.Errorf("%w: tokens exceed the exact Redis Lua integer range", ErrInvalidPolicy)
	case p.Interval < time.Microsecond:
		return fmt.Errorf("%w: interval must be at least one microsecond", ErrInvalidPolicy)
	case p.Interval%time.Microsecond != 0:
		return fmt.Errorf("%w: interval must use whole microseconds", ErrInvalidPolicy)
	case p.Interval/time.Microsecond > time.Duration(maxExactLuaInteger):
		return fmt.Errorf("%w: interval exceeds the exact Redis Lua integer range", ErrInvalidPolicy)
	case p.Burst <= 0:
		return fmt.Errorf("%w: burst must be positive", ErrInvalidPolicy)
	case p.Burst > maxExactLuaInteger:
		return fmt.Errorf("%w: burst exceeds the exact Redis Lua integer range", ErrInvalidPolicy)
	case p.Revision < 0:
		return fmt.Errorf("%w: revision must be non-negative", ErrInvalidPolicy)
	default:
		return nil
	}
}

// Decision reports the result of an immediate token admission attempt.
type Decision struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

// Bucket atomically consumes tokens for a key.
type Bucket interface {
	TakeN(ctx context.Context, key string, policy Policy, tokens int64) (Decision, error)
	Close() error
}

// GuardedBucket adds Team Quota's durable policy barrier without changing the
// generic token-bucket contract used by platform-internal buckets.
type GuardedBucket interface {
	Bucket
	guard.Reader
	TakeNGuarded(
		ctx context.Context,
		key string,
		admissionKey string,
		policy Policy,
		version guard.Version,
		rateRefillFrom time.Time,
		tokens int64,
	) (Decision, error)
}

func validateTake(key string, policy Policy, tokens int64) error {
	if strings.TrimSpace(key) == "" {
		return ErrInvalidKey
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	if tokens <= 0 {
		return ErrInvalidTokenCost
	}
	if tokens > maxExactLuaInteger {
		return fmt.Errorf("%w: token cost exceeds the exact Redis Lua integer range", ErrInvalidTokenCost)
	}
	if tokens > policy.Burst {
		return ErrCostExceedsBurst
	}
	return nil
}
