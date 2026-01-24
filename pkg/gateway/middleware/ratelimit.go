package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// RateLimiter provides rate limiting functionality
type RateLimiter struct {
	logger          *zap.Logger
	rps             int
	burst           int
	cleanupInterval time.Duration
	localLimiters   sync.Map // map[teamID]*rate.Limiter
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rps, burst int, cleanupInterval time.Duration, logger *zap.Logger) *RateLimiter {
	rl := &RateLimiter{
		logger:          logger,
		rps:             rps,
		burst:           burst,
		cleanupInterval: cleanupInterval,
	}

	// Start cleanup goroutine for local limiters
	go rl.cleanupLoop()

	return rl
}

// RateLimit returns a gin middleware that rate limits requests per team
func (rl *RateLimiter) RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx == nil {
			// No auth context, skip rate limiting (will fail at auth middleware)
			c.Next()
			return
		}

		teamID := authCtx.TeamID
		if teamID == "" {
			c.Next()
			return
		}

		// Get or create local limiter for this team
		limiter := rl.getLimiter(teamID)

		if !limiter.Allow() {
			rl.logger.Warn("Rate limit exceeded",
				zap.String("team_id", teamID),
				zap.String("client_ip", c.ClientIP()),
			)

			c.Header("X-RateLimit-Limit", string(rune(rl.rps)))
			c.Header("X-RateLimit-Remaining", "0")
			c.Header("Retry-After", "1")

			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate limit exceeded",
				"retry_after": 1,
			})
			return
		}

		c.Next()
	}
}

// getLimiter gets or creates a rate limiter for a team
func (rl *RateLimiter) getLimiter(teamID string) *rate.Limiter {
	if v, ok := rl.localLimiters.Load(teamID); ok {
		return v.(*rate.Limiter)
	}

	limiter := rate.NewLimiter(rate.Limit(rl.rps), rl.burst)
	actual, _ := rl.localLimiters.LoadOrStore(teamID, limiter)
	return actual.(*rate.Limiter)
}

// cleanupLoop periodically cleans up unused limiters
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Reset all limiters periodically to prevent memory leaks
		rl.localLimiters.Range(func(key, value any) bool {
			return true
		})
	}
}
