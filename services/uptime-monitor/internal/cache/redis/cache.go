// Package redis provides two narrowly-scoped caches on top of go-redis:
//
//   - MonitorCache: full monitor config (URL, headers, timeouts, ...) so the
//     scheduler's per-dispatch job enrichment never touches Postgres on the
//     hot path — only a background refresh loop does.
//   - NotificationDedup: a short-TTL "have we already alerted for this
//     incident transition" guard, so a result-processor restart or a replica
//     race can never double-fire an alert.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tailus/uptime-monitor/internal/models"
)

func NewClient(addr, password string, db int) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     50,
		MinIdleConns: 10,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
}

type MonitorCache struct {
	rdb *goredis.Client
	ttl time.Duration
}

func NewMonitorCache(rdb *goredis.Client) *MonitorCache {
	return &MonitorCache{rdb: rdb, ttl: 90 * time.Second}
}

func monitorKey(id string) string { return "monitor:" + id }

func (c *MonitorCache) Set(ctx context.Context, m models.Monitor) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, monitorKey(m.ID.String()), data, c.ttl).Err()
}

// SetMany is used by the periodic full-refresh loop (see cmd/scheduler) via
// a pipeline, so refreshing tens of thousands of monitor configs is a
// handful of round trips instead of one per monitor.
func (c *MonitorCache) SetMany(ctx context.Context, monitors []models.Monitor) error {
	if len(monitors) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	for _, m := range monitors {
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		pipe.Set(ctx, monitorKey(m.ID.String()), data, c.ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *MonitorCache) Get(ctx context.Context, id string) (models.Monitor, bool, error) {
	var m models.Monitor
	data, err := c.rdb.Get(ctx, monitorKey(id)).Bytes()
	if err == goredis.Nil {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, false, err
	}
	return m, true, nil
}

func (c *MonitorCache) Delete(ctx context.Context, id string) error {
	return c.rdb.Del(ctx, monitorKey(id)).Err()
}

// NotificationDedup prevents duplicate alerts for the same incident
// transition when multiple result-processor replicas race, or a replica
// retries after a crash before committing its dedup marker.
type NotificationDedup struct {
	rdb *goredis.Client
	ttl time.Duration
}

func NewNotificationDedup(rdb *goredis.Client) *NotificationDedup {
	return &NotificationDedup{rdb: rdb, ttl: 10 * time.Minute}
}

// ShouldSend atomically claims the right to send a notification for this
// key (monitor+region+incident+transition). Returns true only for the
// caller that wins the race.
func (d *NotificationDedup) ShouldSend(ctx context.Context, key string) (bool, error) {
	ok, err := d.rdb.SetNX(ctx, fmt.Sprintf("notify:dedup:%s", key), 1, d.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}
