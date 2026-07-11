package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tailus/uptime-monitor/internal/models"
)

// OpenIncident creates a new open incident, relying on the partial unique
// index (monitor_id, region) WHERE status='open' to make this safe under
// concurrent result-processor replicas: a duplicate open attempt is a no-op
// (ON CONFLICT DO NOTHING), and the caller can re-fetch to get the winner's
// incident row.
func (s *Store) OpenIncident(ctx context.Context, monitorID uuid.UUID, region, cause string) (models.Incident, bool, error) {
	var inc models.Incident
	row := s.Pool.QueryRow(ctx, `
		INSERT INTO incidents (monitor_id, region, status, cause, started_at)
		VALUES ($1, $2, 'open', $3, now())
		ON CONFLICT (monitor_id, region) WHERE status = 'open' DO NOTHING
		RETURNING id, monitor_id, region, status, cause, started_at, resolved_at`,
		monitorID, region, cause,
	)
	err := row.Scan(&inc.ID, &inc.MonitorID, &inc.Region, &inc.Status, &inc.Cause, &inc.StartedAt, &inc.ResolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Someone else already opened it; fetch the existing one.
		existing, ferr := s.GetOpenIncident(ctx, monitorID, region)
		return existing, false, ferr
	}
	if err != nil {
		return inc, false, fmt.Errorf("open incident: %w", err)
	}
	return inc, true, nil
}

func (s *Store) GetOpenIncident(ctx context.Context, monitorID uuid.UUID, region string) (models.Incident, error) {
	var inc models.Incident
	row := s.Pool.QueryRow(ctx, `
		SELECT id, monitor_id, region, status, cause, started_at, resolved_at
		FROM incidents WHERE monitor_id = $1 AND region = $2 AND status = 'open'`,
		monitorID, region,
	)
	err := row.Scan(&inc.ID, &inc.MonitorID, &inc.Region, &inc.Status, &inc.Cause, &inc.StartedAt, &inc.ResolvedAt)
	return inc, err
}

func (s *Store) ResolveIncident(ctx context.Context, monitorID uuid.UUID, region string) (models.Incident, bool, error) {
	var inc models.Incident
	row := s.Pool.QueryRow(ctx, `
		UPDATE incidents SET status = 'resolved', resolved_at = now()
		WHERE monitor_id = $1 AND region = $2 AND status = 'open'
		RETURNING id, monitor_id, region, status, cause, started_at, resolved_at`,
		monitorID, region,
	)
	err := row.Scan(&inc.ID, &inc.MonitorID, &inc.Region, &inc.Status, &inc.Cause, &inc.StartedAt, &inc.ResolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return inc, false, nil // nothing was open, nothing to resolve
	}
	if err != nil {
		return inc, false, fmt.Errorf("resolve incident: %w", err)
	}
	return inc, true, nil
}

func (s *Store) NotificationChannelsByOwner(ctx context.Context, ownerID uuid.UUID) ([]models.NotificationChannel, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, owner_id, type, target, secret FROM notification_channels WHERE owner_id = $1`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.NotificationChannel
	for rows.Next() {
		var c models.NotificationChannel
		if err := rows.Scan(&c.ID, &c.OwnerID, &c.Type, &c.Target, &c.Secret); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// MonitorOwner is a narrow lookup used by the result processor to route a
// resolved/opened incident to the right owner's notification channels
// without loading the full monitor record.
func (s *Store) MonitorOwner(ctx context.Context, monitorID uuid.UUID) (uuid.UUID, error) {
	var ownerID uuid.UUID
	err := s.Pool.QueryRow(ctx, `SELECT owner_id FROM monitors WHERE id = $1`, monitorID).Scan(&ownerID)
	return ownerID, err
}
