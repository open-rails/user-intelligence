package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

func (s *Store) ListMessagesByConversation(ctx context.Context, conversationID uuid.UUID, limit int) ([]ingest.StoredMessage, error) {
	limit = clamp(limit, 50, 500)

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, conversation_id, source_channel, direction, source_identity, security_class, body_text, provider_message_id, created_at
		FROM %s.messages
		WHERE conversation_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT $2
	`, s.schema), conversationID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]ingest.StoredMessage, 0, limit)
	for rows.Next() {
		var m ingest.StoredMessage
		var channel string
		var direction string
		var securityClass string
		var providerMessageID *string
		if err := rows.Scan(
			&m.ID,
			&m.ConversationID,
			&channel,
			&direction,
			&m.SourceIdentity,
			&securityClass,
			&m.BodyText,
			&providerMessageID,
			&m.CreatedAt,
		); err != nil {
			return nil, err
		}
		m.SourceChannel = ingest.SourceChannel(channel)
		m.Direction = ingest.Direction(direction)
		m.SecurityClass = ingest.SecurityClass(securityClass)
		m.ProviderMessageID = providerMessageID
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Query is newest first for LIMIT efficiency; reverse for chronological use.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func clamp(v, fallback, max int) int {
	if v <= 0 {
		return fallback
	}
	if v > max {
		return max
	}
	return v
}
