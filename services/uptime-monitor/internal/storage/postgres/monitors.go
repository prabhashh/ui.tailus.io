package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tailus/uptime-monitor/internal/models"
)

// ActiveMonitors implements scheduler.MonitorSource. Called on every
// scheduler reconcile (default: every 15s), so it must stay cheap even with
// millions of rows — the enabled partial index (see migrations/0001) keeps
// this a fast index-only-ish scan rather than a sequential scan.
func (s *Store) ActiveMonitors(ctx context.Context) ([]models.Monitor, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, owner_id, name, url, check_type, method, headers, body,
		       expected_status_min, expected_status_max, interval_seconds,
		       base_timeout_ms, min_timeout_ms, max_timeout_ms, regions,
		       failure_threshold, enabled, created_at, updated_at
		FROM monitors
		WHERE enabled`)
	if err != nil {
		return nil, fmt.Errorf("query active monitors: %w", err)
	}
	defer rows.Close()

	var out []models.Monitor
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func scanMonitor(row pgx.Row) (models.Monitor, error) {
	var m models.Monitor
	var headersRaw []byte
	if err := row.Scan(
		&m.ID, &m.OwnerID, &m.Name, &m.URL, &m.CheckType, &m.Method, &headersRaw, &m.Body,
		&m.ExpectedStatusMin, &m.ExpectedStatusMax, &m.IntervalSeconds,
		&m.BaseTimeoutMS, &m.MinTimeoutMS, &m.MaxTimeoutMS, &m.Regions,
		&m.FailureThreshold, &m.Enabled, &m.CreatedAt, &m.UpdatedAt,
	); err != nil {
		return m, fmt.Errorf("scan monitor: %w", err)
	}
	if len(headersRaw) > 0 {
		if err := json.Unmarshal(headersRaw, &m.Headers); err != nil {
			return m, fmt.Errorf("unmarshal monitor headers: %w", err)
		}
	}
	return m, nil
}

func (s *Store) GetMonitor(ctx context.Context, id uuid.UUID) (models.Monitor, error) {
	row := s.Pool.QueryRow(ctx, `
		SELECT id, owner_id, name, url, check_type, method, headers, body,
		       expected_status_min, expected_status_max, interval_seconds,
		       base_timeout_ms, min_timeout_ms, max_timeout_ms, regions,
		       failure_threshold, enabled, created_at, updated_at
		FROM monitors WHERE id = $1`, id)
	return scanMonitor(row)
}

func (s *Store) ListMonitorsByOwner(ctx context.Context, ownerID uuid.UUID) ([]models.Monitor, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, owner_id, name, url, check_type, method, headers, body,
		       expected_status_min, expected_status_max, interval_seconds,
		       base_timeout_ms, min_timeout_ms, max_timeout_ms, regions,
		       failure_threshold, enabled, created_at, updated_at
		FROM monitors WHERE owner_id = $1 ORDER BY created_at DESC`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.Monitor
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) CreateMonitor(ctx context.Context, m models.Monitor) (models.Monitor, error) {
	headersRaw, err := json.Marshal(m.Headers)
	if err != nil {
		return m, err
	}
	now := time.Now()
	row := s.Pool.QueryRow(ctx, `
		INSERT INTO monitors (owner_id, name, url, check_type, method, headers, body,
		                       expected_status_min, expected_status_max, interval_seconds,
		                       base_timeout_ms, min_timeout_ms, max_timeout_ms, regions,
		                       failure_threshold, enabled, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$17)
		RETURNING id, created_at, updated_at`,
		m.OwnerID, m.Name, m.URL, m.CheckType, m.Method, headersRaw, m.Body,
		m.ExpectedStatusMin, m.ExpectedStatusMax, m.IntervalSeconds,
		m.BaseTimeoutMS, m.MinTimeoutMS, m.MaxTimeoutMS, m.Regions,
		m.FailureThreshold, m.Enabled, now,
	)
	if err := row.Scan(&m.ID, &m.CreatedAt, &m.UpdatedAt); err != nil {
		return m, fmt.Errorf("insert monitor: %w", err)
	}
	return m, nil
}

func (s *Store) DeleteMonitor(ctx context.Context, id uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM monitors WHERE id = $1`, id)
	return err
}

func (s *Store) SetMonitorEnabled(ctx context.Context, id uuid.UUID, enabled bool) error {
	_, err := s.Pool.Exec(ctx, `UPDATE monitors SET enabled = $2, updated_at = now() WHERE id = $1`, id, enabled)
	return err
}
