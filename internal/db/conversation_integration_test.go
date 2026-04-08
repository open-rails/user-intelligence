package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

func TestStoreReceive_ConversationContinuationSameKey(t *testing.T) {
	ctx, pool, store := setupTestStore(t)
	defer pool.Close()

	providerA := "cont-a-" + uuid.NewString()
	first, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelTwilioSMS,
		ConversationKey:   "sms:+15550000001:+15550000002",
		BodyText:          "first",
		SourceIdentity:    "+15550000001",
		SecurityClass:     ingest.SecurityClassInsecure,
		ProviderMessageID: &providerA,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Millisecond)

	providerB := "cont-b-" + uuid.NewString()
	second, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelTwilioSMS,
		ConversationKey:   "sms:+15550000001:+15550000002",
		BodyText:          "second",
		SourceIdentity:    "+15550000001",
		SecurityClass:     ingest.SecurityClassInsecure,
		ProviderMessageID: &providerB,
	})
	if err != nil {
		t.Fatal(err)
	}

	if first.ConversationID != second.ConversationID {
		t.Fatalf("expected continuation in same conversation, first=%s second=%s", first.ConversationID, second.ConversationID)
	}
}

func TestStoreReceive_SameConversationKeyDifferentChannels(t *testing.T) {
	ctx, pool, store := setupTestStore(t)
	defer pool.Close()

	sameKey := "shared-key"
	webProviderID := "web-" + uuid.NewString()
	webRes, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelWeb,
		ConversationKey:   sameKey,
		BodyText:          "web message",
		SourceIdentity:    "web-user",
		SecurityClass:     ingest.SecurityClassSecure,
		ProviderMessageID: &webProviderID,
	})
	if err != nil {
		t.Fatal(err)
	}

	smsProviderID := "sms-" + uuid.NewString()
	smsRes, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelTwilioSMS,
		ConversationKey:   sameKey,
		BodyText:          "sms message",
		SourceIdentity:    "+15551112222",
		SecurityClass:     ingest.SecurityClassInsecure,
		ProviderMessageID: &smsProviderID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if webRes.ConversationID == smsRes.ConversationID {
		t.Fatalf("expected separate conversations across source channels")
	}
}

func TestStoreReceive_MultiUserParticipantLinkingSameConversation(t *testing.T) {
	ctx, pool, store := setupTestStore(t)
	defer pool.Close()

	userA := uuid.New()
	userB := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO profiles.users (id, email, phone_number) VALUES ($1, $2, $3)`, userA, "a@example.com", "+15550001111"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO profiles.users (id, email, phone_number) VALUES ($1, $2, $3)`, userB, "b@example.com", "+15550002222"); err != nil {
		t.Fatal(err)
	}

	key := "discord:guild:g1:channel:c1"
	providerA := "multi-a-" + uuid.NewString()
	res, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelDiscord,
		ConversationKey:   key,
		BodyText:          "hello from a",
		SourceIdentity:    "discord-a",
		SecurityClass:     ingest.SecurityClassSecure,
		UserID:            &userA,
		ProviderMessageID: &providerA,
	})
	if err != nil {
		t.Fatal(err)
	}

	providerB := "multi-b-" + uuid.NewString()
	if _, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelDiscord,
		ConversationKey:   key,
		BodyText:          "hello from b",
		SourceIdentity:    "discord-b",
		SecurityClass:     ingest.SecurityClassSecure,
		UserID:            &userB,
		ProviderMessageID: &providerB,
	}); err != nil {
		t.Fatal(err)
	}

	var participantCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM user_intelligence.conversation_participants
		WHERE conversation_id = $1
	`, res.ConversationID).Scan(&participantCount); err != nil {
		t.Fatal(err)
	}
	if participantCount != 2 {
		t.Fatalf("expected 2 conversation participants, got=%d", participantCount)
	}
}

func TestMessagesRequireConversationID(t *testing.T) {
	ctx, pool, _ := setupTestStore(t)
	defer pool.Close()

	_, err := pool.Exec(ctx, `
		INSERT INTO user_intelligence.messages (
			conversation_id, source_channel, direction, source_identity, security_class, body_text
		) VALUES (NULL, 'web', 'inbound', 'anon', 'secure', 'hello')
	`)
	if err == nil {
		t.Fatalf("expected NOT NULL/FK failure for missing conversation_id")
	}
}

func TestStoreReadHelpers_ListMessages(t *testing.T) {
	ctx, pool, store := setupTestStore(t)
	defer pool.Close()

	userID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO profiles.users (id, email, phone_number) VALUES ($1, $2, $3)`, userID, "read@example.com", "+15559990000"); err != nil {
		t.Fatal(err)
	}

	webA := "web-a-" + uuid.NewString()
	firstA, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelWeb,
		ConversationKey:   "web:read-a",
		BodyText:          "a1",
		SourceIdentity:    "read-user",
		SecurityClass:     ingest.SecurityClassSecure,
		UserID:            &userID,
		ProviderMessageID: &webA,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	webB := "web-b-" + uuid.NewString()
	if _, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelWeb,
		ConversationKey:   "web:read-a",
		BodyText:          "a2",
		SourceIdentity:    "read-user",
		SecurityClass:     ingest.SecurityClassSecure,
		ProviderMessageID: &webB,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	webD := "web-d-" + uuid.NewString()
	if _, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelWeb,
		ConversationKey:   "web:read-a",
		BodyText:          "a3",
		SourceIdentity:    "read-user",
		SecurityClass:     ingest.SecurityClassSecure,
		ProviderMessageID: &webD,
	}); err != nil {
		t.Fatal(err)
	}

	lastTwo, err := store.ListMessagesByConversation(ctx, firstA.ConversationID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lastTwo) != 2 {
		t.Fatalf("expected two latest messages, got=%d", len(lastTwo))
	}
	if lastTwo[0].BodyText != "a2" || lastTwo[1].BodyText != "a3" {
		t.Fatalf("expected chronological tail [a2, a3], got [%s, %s]", lastTwo[0].BodyText, lastTwo[1].BodyText)
	}
}

func TestStoreRecordOutbound_SameConversation(t *testing.T) {
	ctx, pool, store := setupTestStore(t)
	defer pool.Close()

	inID := "in-" + uuid.NewString()
	inbound, err := store.Receive(ctx, ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelDiscord,
		ConversationKey:   "discord:dm:outbound-user",
		BodyText:          "ping",
		SourceIdentity:    "outbound-user",
		SecurityClass:     ingest.SecurityClassSecure,
		ProviderMessageID: &inID,
	})
	if err != nil {
		t.Fatal(err)
	}

	outID := "out-" + uuid.NewString()
	outbound, err := store.RecordOutbound(ctx, ingest.OutboundMessage{
		ConversationID:    inbound.ConversationID,
		SourceChannel:     ingest.SourceChannelDiscord,
		SourceIdentity:    "doujins-bot",
		SecurityClass:     ingest.SecurityClassSecure,
		BodyText:          "pong",
		ProviderMessageID: &outID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outbound.ConversationID != inbound.ConversationID {
		t.Fatalf("expected outbound conversation_id=%s got=%s", inbound.ConversationID, outbound.ConversationID)
	}

	var direction string
	if err := pool.QueryRow(ctx, `
		SELECT direction
		FROM user_intelligence.messages
		WHERE id = $1
	`, outbound.MessageID).Scan(&direction); err != nil {
		t.Fatal(err)
	}
	if direction != "outbound" {
		t.Fatalf("expected outbound direction, got=%s", direction)
	}
}

func setupTestStore(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	setupStoreSchema(t, ctx, pool)

	store, err := NewStore(pool, "user_intelligence")
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return ctx, pool, store
}
