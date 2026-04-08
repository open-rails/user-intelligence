package db

import (
	"context"
	"encoding/binary"
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

type AgentRunStatus string

const (
	AgentRunStatusRunning   AgentRunStatus = "running"
	AgentRunStatusCompleted AgentRunStatus = "completed"
	AgentRunStatusFailed    AgentRunStatus = "failed"
	AgentRunStatusSkipped   AgentRunStatus = "skipped"
)

type AgentRunStartResult struct {
	RunID          uuid.UUID
	AlreadyStarted bool
	Status         AgentRunStatus
}

type AgentIssueUpsertResult struct {
	ID          uuid.UUID
	Created     bool
	ReportCount int
}

type InboundMessageRoute struct {
	SourceIdentity string
	SourceContext  json.RawMessage
}

func (s *Store) WithConversationLock(ctx context.Context, conversationID uuid.UUID, fn func(context.Context) error) error {
	if conversationID == uuid.Nil {
		return errors.New("conversation_id required")
	}
	if fn == nil {
		return errors.New("fn is nil")
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	lockKey := advisoryLockKey(conversationID)
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey)

	return fn(ctx)
}

func (s *Store) StartAgentRun(ctx context.Context, conversationID, inboundMessageID uuid.UUID, sourceChannel ingest.SourceChannel) (AgentRunStartResult, error) {
	if conversationID == uuid.Nil {
		return AgentRunStartResult{}, errors.New("conversation_id required")
	}
	if inboundMessageID == uuid.Nil {
		return AgentRunStartResult{}, errors.New("inbound_message_id required")
	}
	if strings.TrimSpace(string(sourceChannel)) == "" {
		return AgentRunStartResult{}, errors.New("source_channel required")
	}

	var runID uuid.UUID
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s.agent_runs (conversation_id, inbound_message_id, source_channel, status)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (inbound_message_id) DO NOTHING
		RETURNING id
	`, s.schema), conversationID, inboundMessageID, string(sourceChannel), string(AgentRunStatusRunning)).Scan(&runID)
	if err == nil {
		return AgentRunStartResult{RunID: runID, AlreadyStarted: false, Status: AgentRunStatusRunning}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AgentRunStartResult{}, err
	}

	var existingStatus string
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT id, status
		FROM %s.agent_runs
		WHERE inbound_message_id = $1
		LIMIT 1
	`, s.schema), inboundMessageID).Scan(&runID, &existingStatus); err != nil {
		return AgentRunStartResult{}, err
	}
	return AgentRunStartResult{
		RunID:          runID,
		AlreadyStarted: true,
		Status:         AgentRunStatus(existingStatus),
	}, nil
}

func (s *Store) CompleteAgentRun(
	ctx context.Context,
	runID uuid.UUID,
	status AgentRunStatus,
	responseID string,
	previousResponseID string,
	outputText string,
	trace json.RawMessage,
	errorText string,
) error {
	if runID == uuid.Nil {
		return errors.New("run_id required")
	}
	if strings.TrimSpace(string(status)) == "" {
		return errors.New("status required")
	}
	if len(trace) == 0 {
		trace = []byte("[]")
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		UPDATE %s.agent_runs
		SET status = $2,
		    response_id = NULLIF($3, ''),
		    previous_response_id = NULLIF($4, ''),
		    output_text = NULLIF($5, ''),
		    trace = $6,
		    error_text = NULLIF($7, ''),
		    updated_at = NOW()
		WHERE id = $1
	`, s.schema), runID, string(status), strings.TrimSpace(responseID), strings.TrimSpace(previousResponseID), outputText, trace, strings.TrimSpace(errorText))
	return err
}

func (s *Store) UpsertUserMemory(ctx context.Context, userID uuid.UUID, memoryBlob string) (time.Time, error) {
	if userID == uuid.Nil {
		return time.Time{}, errors.New("user_id required")
	}
	memoryBlob = strings.TrimSpace(memoryBlob)
	if memoryBlob == "" {
		return time.Time{}, errors.New("memory_blob required")
	}

	var updatedAt time.Time
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s.user_memory (user_id, memory_blob, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (user_id)
		DO UPDATE SET memory_blob = EXCLUDED.memory_blob, updated_at = NOW()
		RETURNING updated_at
	`, s.schema), userID, memoryBlob).Scan(&updatedAt)
	return updatedAt, err
}

func (s *Store) UpsertAgentIssue(
	ctx context.Context,
	conversationID uuid.UUID,
	sourceChannel ingest.SourceChannel,
	issueType string,
	title string,
	body string,
	dedupeHash string,
) (AgentIssueUpsertResult, error) {
	if conversationID == uuid.Nil {
		return AgentIssueUpsertResult{}, errors.New("conversation_id required")
	}
	if strings.TrimSpace(string(sourceChannel)) == "" {
		return AgentIssueUpsertResult{}, errors.New("source_channel required")
	}
	issueType = strings.TrimSpace(issueType)
	title = strings.TrimSpace(title)
	body = strings.TrimSpace(body)
	dedupeHash = strings.TrimSpace(dedupeHash)
	if issueType == "" || title == "" || body == "" || dedupeHash == "" {
		return AgentIssueUpsertResult{}, errors.New("issue_type, title, body, dedupe_hash are required")
	}

	var out AgentIssueUpsertResult
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s.agent_issues (
			conversation_id, source_channel, issue_type, title, body, dedupe_hash, status
		) VALUES ($1, $2, $3, $4, $5, $6, 'open')
		ON CONFLICT (dedupe_hash) WHERE status = 'open'
		DO UPDATE SET
			report_count = %s.agent_issues.report_count + 1,
			last_seen_at = NOW(),
			title = EXCLUDED.title,
			body = EXCLUDED.body,
			conversation_id = EXCLUDED.conversation_id
		RETURNING id, (xmax = 0) AS created, report_count
	`, s.schema, s.schema), conversationID, string(sourceChannel), issueType, title, body, dedupeHash).Scan(&out.ID, &out.Created, &out.ReportCount)
	if err == nil {
		return out, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P10" {
		// Older postgres variants can fail partial-index conflict inference.
		var existingID uuid.UUID
		var reportCount int
		findErr := s.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT id, report_count
			FROM %s.agent_issues
			WHERE dedupe_hash = $1 AND status = 'open'
			LIMIT 1
		`, s.schema), dedupeHash).Scan(&existingID, &reportCount)
		if findErr == nil {
			_, updateErr := s.pool.Exec(ctx, fmt.Sprintf(`
				UPDATE %s.agent_issues
				SET report_count = report_count + 1,
				    last_seen_at = NOW(),
				    title = $2,
				    body = $3,
				    conversation_id = $4
				WHERE id = $1
			`, s.schema), existingID, title, body, conversationID)
			if updateErr != nil {
				return AgentIssueUpsertResult{}, updateErr
			}
			return AgentIssueUpsertResult{ID: existingID, Created: false, ReportCount: reportCount + 1}, nil
		}
	}
	return AgentIssueUpsertResult{}, err
}

func (s *Store) LatestInboundByConversationAndChannel(ctx context.Context, conversationID uuid.UUID, channel ingest.SourceChannel) (InboundMessageRoute, error) {
	if conversationID == uuid.Nil {
		return InboundMessageRoute{}, errors.New("conversation_id required")
	}

	var out InboundMessageRoute
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT source_identity, source_context
		FROM %s.messages
		WHERE conversation_id = $1
		  AND source_channel = $2
		  AND direction = $3
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, s.schema), conversationID, string(channel), string(ingest.DirectionInbound)).Scan(&out.SourceIdentity, &out.SourceContext)
	return out, err
}

func advisoryLockKey(id uuid.UUID) int64 {
	b := id
	u := binary.BigEndian.Uint64(b[:8])
	return int64(u)
}
