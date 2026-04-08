package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Store) AcquireOrRenewDiscordShard(ctx context.Context, shardID int, ownerID string, ttl time.Duration) (bool, error) {
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return false, fmt.Errorf("owner_id required")
	}
	if ttl <= 0 {
		ttl = 45 * time.Second
	}

	var gotOwner string
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s.discord_shard_leases (shard_id, owner_id, lease_expires_at, updated_at)
		VALUES ($1, $2, NOW() + $3::interval, NOW())
		ON CONFLICT (shard_id)
		DO UPDATE
			SET owner_id = EXCLUDED.owner_id,
			    lease_expires_at = EXCLUDED.lease_expires_at,
			    updated_at = NOW()
		WHERE %s.discord_shard_leases.owner_id = EXCLUDED.owner_id
		   OR %s.discord_shard_leases.lease_expires_at < NOW()
		RETURNING owner_id
	`, s.schema, s.schema, s.schema), shardID, ownerID, intervalString(ttl)).Scan(&gotOwner)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return gotOwner == ownerID, nil
}

func (s *Store) ReleaseDiscordShard(ctx context.Context, shardID int, ownerID string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		DELETE FROM %s.discord_shard_leases
		WHERE shard_id = $1 AND owner_id = $2
	`, s.schema), shardID, strings.TrimSpace(ownerID))
	return err
}

func intervalString(d time.Duration) string {
	if d <= 0 {
		return "45 seconds"
	}
	seconds := int(d.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%d seconds", seconds)
}
