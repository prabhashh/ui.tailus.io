package postgres

import (
	"context"
	"fmt"
	"time"
)

// staleAfter is how long a scheduler instance can go without heartbeating
// before it's considered dead and its monitors are reassigned. Set to
// several multiples of the expected heartbeat interval (default 5s, see
// cmd/scheduler) to tolerate a missed beat or two without spuriously
// evicting a healthy replica.
const staleAfter = 20 * time.Second

// Heartbeat upserts this instance's liveness row. Call on an interval
// (default 5s) from every scheduler replica.
func (s *Store) Heartbeat(ctx context.Context, instanceID string) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO scheduler_instances (instance_id, last_heartbeat)
		VALUES ($1, now())
		ON CONFLICT (instance_id) DO UPDATE SET last_heartbeat = now()`,
		instanceID,
	)
	return err
}

// ActivePeers implements scheduler.PeerSource.
func (s *Store) ActivePeers(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT instance_id FROM scheduler_instances
		WHERE last_heartbeat > now() - $1::interval
		ORDER BY instance_id`,
		fmt.Sprintf("%d seconds", int(staleAfter.Seconds())),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
