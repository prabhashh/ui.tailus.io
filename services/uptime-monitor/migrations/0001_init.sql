-- Uptime Monitor schema.
--
-- check_results is the highest-volume table by a wide margin: at 50,000
-- monitors on a 30s interval and one region each, that's ~1,667 rows/sec,
-- ~144M rows/day. It is range-partitioned by day so that (a) queries scoped
-- to a recent time window only scan relevant partitions, (b) old partitions
-- can be dropped in O(1) instead of running a slow DELETE, and (c) VACUUM
-- stays cheap per partition. See migrations/0002_partition_maintenance.sql
-- for automated partition creation/retention.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE monitors (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id             UUID NOT NULL,
    name                 TEXT NOT NULL,
    url                  TEXT NOT NULL,
    check_type           TEXT NOT NULL DEFAULT 'http',
    method               TEXT NOT NULL DEFAULT 'GET',
    headers              JSONB NOT NULL DEFAULT '{}'::jsonb,
    body                 TEXT NOT NULL DEFAULT '',
    expected_status_min  INT NOT NULL DEFAULT 200,
    expected_status_max  INT NOT NULL DEFAULT 299,
    interval_seconds     INT NOT NULL DEFAULT 30,
    base_timeout_ms      INT NOT NULL DEFAULT 5000,
    min_timeout_ms       INT NOT NULL DEFAULT 2000,
    max_timeout_ms       INT NOT NULL DEFAULT 30000,
    regions              TEXT[] NOT NULL DEFAULT ARRAY['default'],
    failure_threshold    INT NOT NULL DEFAULT 3,
    enabled              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Scheduler's hot-path read: "give me every monitor I should be scheduling".
CREATE INDEX idx_monitors_enabled ON monitors (enabled) WHERE enabled;
CREATE INDEX idx_monitors_owner ON monitors (owner_id);

CREATE TABLE check_results (
    id               BIGINT GENERATED ALWAYS AS IDENTITY,
    monitor_id       UUID NOT NULL,
    region           TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    duration_ms      INT NOT NULL,
    success          BOOLEAN NOT NULL,
    status_code      INT,
    error_message    TEXT,
    dns_ms           INT,
    connect_ms       INT,
    tls_ms           INT,
    ttfb_ms          INT,
    timeout_used_ms  INT,
    PRIMARY KEY (id, started_at)
) PARTITION BY RANGE (started_at);

-- Applies to every existing and future partition automatically.
CREATE INDEX idx_check_results_monitor_time ON check_results (monitor_id, started_at DESC);
CREATE INDEX idx_check_results_failures ON check_results (monitor_id, started_at DESC) WHERE NOT success;

-- Catch-all so inserts never fail if the maintenance job falls behind;
-- monitor this partition's size as a signal the job needs attention.
CREATE TABLE check_results_default PARTITION OF check_results DEFAULT;

-- Bootstrap partitions for today and the next two days so a fresh install
-- works immediately; migration 0002 adds the function that keeps this
-- rolling automatically.
CREATE TABLE check_results_p0 PARTITION OF check_results
    FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + INTERVAL '1 day');
CREATE TABLE check_results_p1 PARTITION OF check_results
    FOR VALUES FROM (CURRENT_DATE + INTERVAL '1 day') TO (CURRENT_DATE + INTERVAL '2 days');
CREATE TABLE check_results_p2 PARTITION OF check_results
    FOR VALUES FROM (CURRENT_DATE + INTERVAL '2 days') TO (CURRENT_DATE + INTERVAL '3 days');

-- Adaptive-timeout state, periodically flushed from each prober's in-memory
-- registry. Also read by the scheduler/API for observability and to seed a
-- new prober process after a deploy so it doesn't start cold.
CREATE TABLE latency_stats (
    monitor_id   UUID NOT NULL,
    region       TEXT NOT NULL,
    mean_ms      DOUBLE PRECISION NOT NULL,
    stddev_ms    DOUBLE PRECISION NOT NULL,
    p95_ms       DOUBLE PRECISION NOT NULL,
    samples      BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (monitor_id, region)
);

CREATE TABLE incidents (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    monitor_id   UUID NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    region       TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'open',
    cause        TEXT NOT NULL DEFAULT '',
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ
);

-- Enforces "at most one open incident per monitor/region" at the database
-- level, so a race between two result-processor replicas can't double-open.
CREATE UNIQUE INDEX idx_incidents_one_open ON incidents (monitor_id, region) WHERE status = 'open';
CREATE INDEX idx_incidents_monitor ON incidents (monitor_id, started_at DESC);

CREATE TABLE notification_channels (
    id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id  UUID NOT NULL,
    type      TEXT NOT NULL,
    target    TEXT NOT NULL,
    secret    TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_notification_channels_owner ON notification_channels (owner_id);

-- Scheduler replica heartbeats, used to build the consistent-hash ring of
-- live scheduler instances (see internal/scheduler.PeerSource). A row not
-- refreshed within a few heartbeat intervals is treated as dead and its
-- monitors are picked up by the remaining replicas on the next reconcile.
CREATE TABLE scheduler_instances (
    instance_id     TEXT PRIMARY KEY,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Long-term retention: raw check_results partitions are dropped after a
-- short window (e.g. 7 days, see ops doc), but hourly rollups are kept
-- indefinitely for uptime-percentage history and SLA reporting.
CREATE TABLE check_results_hourly (
    monitor_id       UUID NOT NULL,
    region           TEXT NOT NULL,
    bucket_start     TIMESTAMPTZ NOT NULL,
    checks_total     INT NOT NULL,
    checks_success   INT NOT NULL,
    avg_duration_ms  DOUBLE PRECISION NOT NULL,
    p95_duration_ms  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (monitor_id, region, bucket_start)
);
