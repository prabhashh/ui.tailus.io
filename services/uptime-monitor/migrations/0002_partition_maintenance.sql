-- Automated daily partition creation and retention for check_results.
--
-- Requires pg_cron (available as a managed extension on most Postgres
-- providers, or installable on a self-hosted VPS instance — see
-- docs/OPERATIONS.md). If pg_cron isn't available, call
-- uptime_create_daily_partitions() / uptime_drop_old_partitions() from an
-- external cron job (e.g. a systemd timer running `psql -c`) instead — both
-- are plain SQL functions, no pg_cron-specific syntax inside them.

CREATE OR REPLACE FUNCTION uptime_create_daily_partitions(days_ahead INT DEFAULT 3)
RETURNS void AS $$
DECLARE
    d DATE;
    partition_name TEXT;
    start_ts TIMESTAMPTZ;
    end_ts TIMESTAMPTZ;
BEGIN
    FOR i IN 0..days_ahead LOOP
        d := (CURRENT_DATE + i);
        partition_name := 'check_results_' || to_char(d, 'YYYYMMDD');
        start_ts := d::timestamptz;
        end_ts := (d + INTERVAL '1 day')::timestamptz;

        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF check_results FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_ts, end_ts
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Drops raw check_results partitions older than retention_days. Run AFTER
-- the hourly rollup job has aggregated that day's data into
-- check_results_hourly, or you lose the raw-detail window entirely.
CREATE OR REPLACE FUNCTION uptime_drop_old_partitions(retention_days INT DEFAULT 7)
RETURNS void AS $$
DECLARE
    rec RECORD;
    cutoff DATE := CURRENT_DATE - retention_days;
BEGIN
    FOR rec IN
        SELECT relname FROM pg_class
        WHERE relname LIKE 'check_results_2%'
    LOOP
        IF to_date(substring(rec.relname FROM 'check_results_(\d{8})'), 'YYYYMMDD') < cutoff THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', rec.relname);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Rolls up the previous UTC hour of raw check_results into
-- check_results_hourly. Idempotent (ON CONFLICT DO UPDATE) so it's safe to
-- re-run.
CREATE OR REPLACE FUNCTION uptime_rollup_hourly(target_hour TIMESTAMPTZ DEFAULT date_trunc('hour', now() - INTERVAL '1 hour'))
RETURNS void AS $$
BEGIN
    INSERT INTO check_results_hourly (monitor_id, region, bucket_start, checks_total, checks_success, avg_duration_ms, p95_duration_ms)
    SELECT
        monitor_id,
        region,
        target_hour,
        count(*),
        count(*) FILTER (WHERE success),
        avg(duration_ms),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
    FROM check_results
    WHERE started_at >= target_hour AND started_at < target_hour + INTERVAL '1 hour'
    GROUP BY monitor_id, region
    ON CONFLICT (monitor_id, region, bucket_start) DO UPDATE SET
        checks_total = EXCLUDED.checks_total,
        checks_success = EXCLUDED.checks_success,
        avg_duration_ms = EXCLUDED.avg_duration_ms,
        p95_duration_ms = EXCLUDED.p95_duration_ms;
END;
$$ LANGUAGE plpgsql;

-- Example pg_cron wiring (run once, if pg_cron is installed):
--   SELECT cron.schedule('uptime-create-partitions', '0 0 * * *', 'SELECT uptime_create_daily_partitions()');
--   SELECT cron.schedule('uptime-rollup-hourly',      '5 * * * *', 'SELECT uptime_rollup_hourly()');
--   SELECT cron.schedule('uptime-drop-old-partitions','15 0 * * *','SELECT uptime_drop_old_partitions(7)');
