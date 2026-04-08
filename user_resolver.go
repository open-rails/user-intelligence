package userintelligence

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

const defaultProfilesSchema = "profiles"

type ContextUserResolver struct {
	ContextKey any
	Pool       *pgxpool.Pool
	Schema     string
}

func (r ContextUserResolver) UserFromContext(ctx context.Context) (*uuid.UUID, error) {
	if r.ContextKey == nil {
		return nil, nil
	}
	v := ctx.Value(r.ContextKey)
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case uuid.UUID:
		return &t, nil
	case *uuid.UUID:
		return t, nil
	case string:
		id, err := uuid.Parse(t)
		if err != nil {
			return nil, fmt.Errorf("invalid user_id string in context: %w", err)
		}
		return &id, nil
	default:
		return nil, nil
	}
}

func (r ContextUserResolver) ResolveBySourceIdentity(ctx context.Context, channel ingest.SourceChannel, sourceIdentity string) (*uuid.UUID, error) {
	if r.Pool == nil {
		return nil, nil
	}
	identity := strings.TrimSpace(sourceIdentity)
	if identity == "" {
		return nil, nil
	}

	schema := strings.TrimSpace(r.Schema)
	if schema == "" {
		schema = defaultProfilesSchema
	}

	var query string
	var arg any
	switch channel {
	case ingest.SourceChannelTwilioMail:
		query = fmt.Sprintf(`SELECT id FROM %s.users WHERE lower(email::text) = lower($1) LIMIT 1`, schema)
		arg = identity
	case ingest.SourceChannelTwilioSMS:
		query = fmt.Sprintf(`SELECT id FROM %s.users WHERE phone_number = $1 LIMIT 1`, schema)
		arg = identity
	case ingest.SourceChannelDiscord:
		query = fmt.Sprintf(`SELECT user_id FROM %s.user_providers WHERE provider_slug = 'discord' AND subject = $1 LIMIT 1`, schema)
		arg = identity
	default:
		return nil, nil
	}

	var userID uuid.UUID
	if err := r.Pool.QueryRow(ctx, query, arg).Scan(&userID); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return nil, nil
		}
		return nil, err
	}
	return &userID, nil
}
