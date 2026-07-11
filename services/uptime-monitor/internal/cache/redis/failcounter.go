package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// FailureCounter tracks consecutive check failures per (monitor, region) in
// Redis rather than in an individual process's memory, so incident
// detection stays correct even when multiple result-processor replicas
// share the load (JetStream doesn't guarantee a given monitor's results
// always land on the same replica).
type FailureCounter struct {
	rdb *goredis.Client
	ttl time.Duration
}

func NewFailureCounter(rdb *goredis.Client) *FailureCounter {
	return &FailureCounter{rdb: rdb, ttl: time.Hour}
}

func failKey(monitorID, region string) string { return "fail:" + monitorID + ":" + region }

// Incr increments the consecutive-failure count and returns the new value.
func (f *FailureCounter) Incr(ctx context.Context, monitorID, region string) (int64, error) {
	key := failKey(monitorID, region)
	pipe := f.rdb.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, f.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

// Reset clears the consecutive-failure count on a success.
func (f *FailureCounter) Reset(ctx context.Context, monitorID, region string) error {
	return f.rdb.Del(ctx, failKey(monitorID, region)).Err()
}
