package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

type DiscordReplyTarget struct {
	ConversationID uuid.UUID
	GuildID        string
	ChannelID      string
	ThreadID       string
	MessageID      string
	AuthorID       string
	MentionTarget  string
	IsDM           bool
}

func (s *Store) RecordOutbound(ctx context.Context, msg ingest.OutboundMessage) (ingest.IngestResult, error) {
	if msg.ConversationID == uuid.Nil {
		return ingest.IngestResult{}, errors.New("conversation_id required")
	}
	if strings.TrimSpace(string(msg.SourceChannel)) == "" {
		return ingest.IngestResult{}, errors.New("source_channel required")
	}
	if strings.TrimSpace(msg.BodyText) == "" {
		return ingest.IngestResult{}, errors.New("body_text required")
	}
	if strings.TrimSpace(msg.SourceIdentity) == "" {
		return ingest.IngestResult{}, errors.New("source_identity required")
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

	var exists bool
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT EXISTS(SELECT 1 FROM %s.conversations WHERE id = $1)
	`, s.schema), msg.ConversationID).Scan(&exists); err != nil {
		return ingest.IngestResult{}, err
	}
	if !exists {
		return ingest.IngestResult{}, errors.New("conversation not found")
	}

	sourceCtx := []byte("{}")
	if len(msg.SourceContext) > 0 {
		sourceCtx = msg.SourceContext
	}

	var id uuid.UUID
	var createdAt time.Time
	err = tx.QueryRow(ctx, fmt.Sprintf(`
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
		msg.ConversationID,
		string(msg.SourceChannel),
		string(ingest.DirectionOutbound),
		msg.SourceIdentity,
		sourceCtx,
		string(msg.SecurityClass),
		msg.BodyText,
		valueOrEmpty(msg.ProviderMessageID),
	).Scan(&id, &createdAt)
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
		msg.ConversationID, createdAt,
	); err != nil {
		return ingest.IngestResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return ingest.IngestResult{}, err
	}

	return ingest.IngestResult{
		ConversationID: msg.ConversationID,
		MessageID:      id,
		CreatedAt:      createdAt,
	}, nil
}

func (s *Store) LatestDiscordReplyTarget(ctx context.Context, conversationID uuid.UUID) (DiscordReplyTarget, error) {
	var sourceIdentity string
	var sourceContext []byte
	var providerMessageID *string

	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT source_identity, source_context, provider_message_id
		FROM %s.messages
		WHERE conversation_id = $1
		  AND source_channel = $2
		  AND direction = $3
		ORDER BY created_at DESC
		LIMIT 1
	`, s.schema), conversationID, string(ingest.SourceChannelDiscord), string(ingest.DirectionInbound)).Scan(&sourceIdentity, &sourceContext, &providerMessageID)
	if err != nil {
		return DiscordReplyTarget{}, err
	}

	var parsed struct {
		GuildID       string `json:"guild_id"`
		ChannelID     string `json:"channel_id"`
		ThreadID      string `json:"thread_id"`
		MessageID     string `json:"message_id"`
		AuthorID      string `json:"author_id"`
		MentionTarget string `json:"mention_target"`
		IsDM          bool   `json:"is_dm"`
	}
	if len(sourceContext) > 0 {
		_ = json.Unmarshal(sourceContext, &parsed)
	}

	authorID := strings.TrimSpace(parsed.AuthorID)
	if authorID == "" {
		authorID = strings.TrimSpace(sourceIdentity)
	}

	messageID := strings.TrimSpace(parsed.MessageID)
	if messageID == "" && providerMessageID != nil {
		messageID = strings.TrimSpace(*providerMessageID)
	}

	channelID := strings.TrimSpace(parsed.ChannelID)
	if channelID == "" {
		return DiscordReplyTarget{}, errors.New("missing discord channel_id in source_context")
	}

	return DiscordReplyTarget{
		ConversationID: conversationID,
		GuildID:        strings.TrimSpace(parsed.GuildID),
		ChannelID:      channelID,
		ThreadID:       strings.TrimSpace(parsed.ThreadID),
		MessageID:      messageID,
		AuthorID:       authorID,
		MentionTarget:  strings.TrimSpace(parsed.MentionTarget),
		IsDM:           parsed.IsDM || strings.TrimSpace(parsed.GuildID) == "",
	}, nil
}
