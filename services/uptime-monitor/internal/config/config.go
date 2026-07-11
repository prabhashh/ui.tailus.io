// Package config loads process configuration from environment variables.
// Every service (scheduler, prober, resultprocessor, api) shares this loader
// so ops only has one convention to remember: everything is UPTIME_*.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Identity
	Region     string // e.g. "us-east", "eu-west" — required for prober/scheduler
	InstanceID string // stable id used as consistent-hash ring member and NATS durable name suffix

	// Postgres
	PostgresDSN      string
	PostgresMaxConns int32
	PostgresMinConns int32

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// NATS / JetStream
	NATSURL          string
	NATSCredsFile    string // optional, for NATS auth via creds file
	JobStreamName    string
	ResultStreamName string

	// Worker pool
	WorkerPoolSize     int // bounded concurrency for the main lane
	QuarantinePoolSize int // bounded concurrency reserved for flaky/slow monitors
	QueueDepth         int // in-process buffered channel depth before JetStream pull backs off

	// Adaptive timeout
	DefaultTimeout time.Duration
	MinTimeout     time.Duration
	MaxTimeout     time.Duration

	// HTTP transport tuning
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	DNSCacheTTL         time.Duration

	// Observability
	MetricsAddr  string
	OTLPEndpoint string // empty disables tracing export
	ServiceName  string

	// Scheduler
	ShardTotal int // total number of scheduler shards in the consistent hash ring (virtual, not instance count)
}

func Load(serviceName string) (*Config, error) {
	cfg := &Config{
		Region:              getEnv("UPTIME_REGION", "default"),
		InstanceID:          getEnv("UPTIME_INSTANCE_ID", hostnameOrRandom()),
		PostgresDSN:         getEnv("UPTIME_POSTGRES_DSN", "postgres://uptime:uptime@localhost:5432/uptime?sslmode=disable"),
		PostgresMaxConns:    int32(getEnvInt("UPTIME_POSTGRES_MAX_CONNS", 20)),
		PostgresMinConns:    int32(getEnvInt("UPTIME_POSTGRES_MIN_CONNS", 2)),
		RedisAddr:           getEnv("UPTIME_REDIS_ADDR", "localhost:6379"),
		RedisPassword:       getEnv("UPTIME_REDIS_PASSWORD", ""),
		RedisDB:             getEnvInt("UPTIME_REDIS_DB", 0),
		NATSURL:             getEnv("UPTIME_NATS_URL", "nats://localhost:4222"),
		NATSCredsFile:       getEnv("UPTIME_NATS_CREDS_FILE", ""),
		JobStreamName:       getEnv("UPTIME_JOB_STREAM", "CHECKS"),
		ResultStreamName:    getEnv("UPTIME_RESULT_STREAM", "RESULTS"),
		WorkerPoolSize:      getEnvInt("UPTIME_WORKER_POOL_SIZE", 500),
		QuarantinePoolSize:  getEnvInt("UPTIME_QUARANTINE_POOL_SIZE", 50),
		QueueDepth:          getEnvInt("UPTIME_QUEUE_DEPTH", 2000),
		DefaultTimeout:      getEnvDuration("UPTIME_DEFAULT_TIMEOUT", 5*time.Second),
		MinTimeout:          getEnvDuration("UPTIME_MIN_TIMEOUT", 2*time.Second),
		MaxTimeout:          getEnvDuration("UPTIME_MAX_TIMEOUT", 30*time.Second),
		MaxIdleConns:        getEnvInt("UPTIME_MAX_IDLE_CONNS", 4000),
		MaxIdleConnsPerHost: getEnvInt("UPTIME_MAX_IDLE_CONNS_PER_HOST", 4),
		IdleConnTimeout:     getEnvDuration("UPTIME_IDLE_CONN_TIMEOUT", 90*time.Second),
		DNSCacheTTL:         getEnvDuration("UPTIME_DNS_CACHE_TTL", 30*time.Second),
		MetricsAddr:         getEnv("UPTIME_METRICS_ADDR", ":9090"),
		OTLPEndpoint:        getEnv("UPTIME_OTLP_ENDPOINT", ""),
		ServiceName:         serviceName,
		ShardTotal:          getEnvInt("UPTIME_SHARD_TOTAL", 256),
	}
	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func hostnameOrRandom() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return fmt.Sprintf("instance-%d", time.Now().UnixNano())
	}
	return strings.TrimSpace(h)
}
