package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/tailus/uptime-monitor/internal/models"
)

// InsertCheckResultsBatch writes a batch of results using the COPY protocol
// (pgx CopyFrom), which is dramatically cheaper than one INSERT per row at
// this table's volume (~1,667 rows/sec sustained at 50k monitors). The
// result processor accumulates results for ~500ms or up to a few thousand
// rows (whichever comes first) before calling this, trading a small amount
// of durability latency for a large reduction in Postgres write amplification.
func (s *Store) InsertCheckResultsBatch(ctx context.Context, results []models.CheckResult) error {
	if len(results) == 0 {
		return nil
	}
	source := pgx.CopyFromSlice(len(results), func(i int) ([]interface{}, error) {
		r := results[i]
		return []interface{}{
			r.MonitorID, r.Region, r.StartedAt, r.DurationMS, r.Success,
			nullableInt(r.StatusCode), nullableString(r.ErrorMessage),
			nullableInt64(r.DNSMS), nullableInt64(r.ConnectMS), nullableInt64(r.TLSMS), nullableInt64(r.TTFBMS),
			nullableInt64(r.TimeoutUsedMS),
		}, nil
	})

	_, err := s.Pool.CopyFrom(ctx,
		pgx.Identifier{"check_results"},
		[]string{
			"monitor_id", "region", "started_at", "duration_ms", "success",
			"status_code", "error_message", "dns_ms", "connect_ms", "tls_ms", "ttfb_ms", "timeout_used_ms",
		},
		source,
	)
	if err != nil {
		return fmt.Errorf("copy check_results: %w", err)
	}
	return nil
}

func nullableInt(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
func nullableInt64(v int64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
func nullableString(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}

// UpsertLatencyStats persists the periodic flush of each probe's in-memory
// adaptive-timeout registry (see internal/latency.Registry.Snapshots).
func (s *Store) UpsertLatencyStats(ctx context.Context, stats []models.LatencyStats) error {
	if len(stats) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, st := range stats {
		batch.Queue(`
			INSERT INTO latency_stats (monitor_id, region, mean_ms, stddev_ms, p95_ms, samples, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6, now())
			ON CONFLICT (monitor_id, region) DO UPDATE SET
				mean_ms = EXCLUDED.mean_ms,
				stddev_ms = EXCLUDED.stddev_ms,
				p95_ms = EXCLUDED.p95_ms,
				samples = EXCLUDED.samples,
				updated_at = now()`,
			st.MonitorID, st.Region, st.Mean, st.StdDev, st.P95, st.Samples,
		)
	}
	br := s.Pool.SendBatch(ctx, batch)
	defer br.Close()
	for range stats {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upsert latency stats: %w", err)
		}
	}
	return nil
}

// LatencyStatsByMonitor loads persisted stats so a freshly-started prober
// can seed its in-memory registry instead of beginning cold after a deploy.
func (s *Store) LatencyStatsByRegion(ctx context.Context, region string) ([]models.LatencyStats, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT monitor_id, region, mean_ms, stddev_ms, p95_ms, samples, updated_at
		FROM latency_stats WHERE region = $1`, region)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.LatencyStats
	for rows.Next() {
		var st models.LatencyStats
		if err := rows.Scan(&st.MonitorID, &st.Region, &st.Mean, &st.StdDev, &st.P95, &st.Samples, &st.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}
