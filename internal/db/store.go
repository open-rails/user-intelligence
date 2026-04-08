package db

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

var schemaNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func ValidateSchemaName(schema string) error {
	if !schemaNameRe.MatchString(schema) {
		return fmt.Errorf("invalid schema name %q", schema)
	}
	return nil
}

type Store struct {
	pool   *pgxpool.Pool
	schema string
}

func NewStore(pool *pgxpool.Pool, schema string) (*Store, error) {
	if pool == nil {
		return nil, errors.New("pool is nil")
	}
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = "user_intelligence"
	}
	if err := ValidateSchemaName(schema); err != nil {
		return nil, err
	}
	return &Store{pool: pool, schema: schema}, nil
}

func (s *Store) Receive(ctx context.Context, msg ingest.InboundMessage) (ingest.IngestResult, error) {
	if strings.TrimSpace(string(msg.SourceChannel)) == "" {
		return ingest.IngestResult{}, errors.New("source_channel required")
	}
	if strings.TrimSpace(msg.ConversationKey) == "" {
		return ingest.IngestResult{}, errors.New("conversation_key required")
	}
	if strings.TrimSpace(msg.BodyText) == "" {
		return ingest.IngestResult{}, errors.New("body_text required")
	}

	if msg.ProviderMessageID != nil && strings.TrimSpace(*msg.ProviderMessageID) != "" {
		existing, err := s.findExistingByProviderMessageID(ctx, msg.SourceChannel, *msg.ProviderMessageID)
		if err == nil {
			existing.Deduped = true
			return existing, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return ingest.IngestResult{}, err
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ingest.IngestResult{}, err
	}
	defer tx.Rollback(ctx)

	conversationID, err := s.getOrCreateConversation(ctx, tx, msg.SourceChannel, msg.ConversationKey)
	if err != nil {
		return ingest.IngestResult{}, err
	}

	messageID, createdAt, err := s.insertMessage(ctx, tx, conversationID, msg)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && msg.ProviderMessageID != nil {
			existing, findErr := s.findExistingByProviderMessageID(ctx, msg.SourceChannel, *msg.ProviderMessageID)
			if findErr == nil {
				existing.Deduped = true
				return existing, nil
			}
		}
		return ingest.IngestResult{}, err
	}

	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s.conversations SET last_message_at = $2 WHERE id = $1`, s.schema),
		conversationID, createdAt,
	); err != nil {
		return ingest.IngestResult{}, err
	}

	if msg.UserID != nil {
		if err := s.linkParticipant(ctx, tx, conversationID, *msg.UserID); err != nil {
			return ingest.IngestResult{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ingest.IngestResult{}, err
	}

	return ingest.IngestResult{
		ConversationID: conversationID,
		MessageID:      messageID,
		CreatedAt:      createdAt,
	}, nil
}

func (s *Store) getOrCreateConversation(ctx context.Context, tx pgx.Tx, channel ingest.SourceChannel, key string) (uuid.UUID, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT id FROM %s.conversations WHERE source_channel = $1 AND conversation_key = $2`, s.schema),
		string(channel), key,
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, err
	}

	err = tx.QueryRow(ctx,
		fmt.Sprintf(`
			INSERT INTO %s.conversations (source_channel, conversation_key, last_message_at)
			VALUES ($1, $2, NOW())
			RETURNING id
		`, s.schema),
		string(channel), key,
	).Scan(&id)
	if err == nil {
		return id, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		fetchErr := tx.QueryRow(ctx,
			fmt.Sprintf(`SELECT id FROM %s.conversations WHERE source_channel = $1 AND conversation_key = $2`, s.schema),
			string(channel), key,
		).Scan(&id)
		return id, fetchErr
	}
	return uuid.Nil, err
}

func (s *Store) insertMessage(ctx context.Context, tx pgx.Tx, conversationID uuid.UUID, msg ingest.InboundMessage) (uuid.UUID, time.Time, error) {
	var id uuid.UUID
	var createdAt time.Time

	sourceCtx := []byte("{}")
	if len(msg.SourceContext) > 0 {
		sourceCtx = msg.SourceContext
	}

	err := tx.QueryRow(ctx,
		fmt.Sprintf(`
			INSERT INTO %s.messages (
				conversation_id,
				source_channel,
				direction,
				source_identity,
				source_context,
				security_class,
				body_text,
				provider_message_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''))
			RETURNING id, created_at::text
		`, s.schema),
		conversationID,
		string(msg.SourceChannel),
		string(ingest.DirectionInbound),
		msg.SourceIdentity,
		sourceCtx,
		string(msg.SecurityClass),
		msg.BodyText,
		valueOrEmpty(msg.ProviderMessageID),
	).Scan(&id, &createdAt)
	if err != nil {
		return uuid.Nil, time.Time{}, err
	}
	return id, createdAt, nil
}

func (s *Store) linkParticipant(ctx context.Context, tx pgx.Tx, conversationID, userID uuid.UUID) error {
	_, err := tx.Exec(ctx,
		fmt.Sprintf(`
			INSERT INTO %s.conversation_participants (conversation_id, user_id)
			VALUES ($1, $2)
			ON CONFLICT (conversation_id, user_id) DO NOTHING
		`, s.schema),
		conversationID, userID,
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return nil
	}
	return err
}

func (s *Store) findExistingByProviderMessageID(ctx context.Context, channel ingest.SourceChannel, providerMessageID string) (ingest.IngestResult, error) {
	var out ingest.IngestResult
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf(`
			SELECT id, conversation_id, created_at
			FROM %s.messages
			WHERE source_channel = $1 AND provider_message_id = $2
			LIMIT 1
		`, s.schema),
		string(channel), providerMessageID,
	).Scan(&out.MessageID, &out.ConversationID, &out.CreatedAt)
	return out, err
}

func valueOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}
