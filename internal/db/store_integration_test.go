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

func TestStoreReceive_IdempotentByProviderMessageID(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	setupStoreSchema(t, ctx, pool)

	store, err := NewStore(pool, "user_intelligence")
	if err != nil {
		t.Fatal(err)
	}

	providerID := "sms-123"
	msg := ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelTwilioSMS,
		ConversationKey:   "sms:+1:+2",
		BodyText:          "ping",
		SourceIdentity:    "+1",
		SecurityClass:     ingest.SecurityClassInsecure,
		ProviderMessageID: &providerID,
	}

	first, err := store.Receive(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Receive(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Deduped {
		t.Fatalf("expected deduped=true")
	}
	if first.MessageID != second.MessageID {
		t.Fatalf("expected same message id on duplicate")
	}
}

func TestStoreReceive_ParticipantLinking(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	setupStoreSchema(t, ctx, pool)

	userID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO profiles.users (id, email, phone_number) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`, userID, "p-link@example.com", "+15550008888"); err != nil {
		t.Fatal(err)
	}

	store, err := NewStore(pool, "user_intelligence")
	if err != nil {
		t.Fatal(err)
	}

	providerID := "sms-participant-" + uuid.NewString()
	msg := ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelTwilioSMS,
		ConversationKey:   "sms:+15550008888:+15550009999",
		BodyText:          "hello",
		SourceIdentity:    "+15550008888",
		SecurityClass:     ingest.SecurityClassInsecure,
		UserID:            &userID,
		ProviderMessageID: &providerID,
	}

	res, err := store.Receive(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM user_intelligence.conversation_participants
		WHERE conversation_id = $1 AND user_id = $2
	`, res.ConversationID, userID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected participant link row, got=%d", count)
	}
}

func TestStoreReceive_AnonymousNoParticipant(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	setupStoreSchema(t, ctx, pool)

	store, err := NewStore(pool, "user_intelligence")
	if err != nil {
		t.Fatal(err)
	}

	providerID := "web-anon-" + uuid.NewString()
	msg := ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelWeb,
		ConversationKey:   "web:anon-" + uuid.NewString(),
		BodyText:          "anon",
		SourceIdentity:    "anon",
		SecurityClass:     ingest.SecurityClassSecure,
		ProviderMessageID: &providerID,
	}

	res, err := store.Receive(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM user_intelligence.conversation_participants
		WHERE conversation_id = $1
	`, res.ConversationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no participant rows, got=%d", count)
	}
}

func setupStoreSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS user_intelligence`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS profiles`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS profiles.users (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			email text,
			phone_number text
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS profiles.user_providers (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id uuid NOT NULL REFERENCES profiles.users(id) ON DELETE CASCADE,
			provider_slug text,
			subject text NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_intelligence.conversations (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			source_channel text NOT NULL,
			conversation_key text NOT NULL,
			last_message_at timestamptz NOT NULL DEFAULT NOW(),
			created_at timestamptz NOT NULL DEFAULT NOW(),
			UNIQUE (source_channel, conversation_key)
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_intelligence.conversation_participants (
			conversation_id uuid NOT NULL REFERENCES user_intelligence.conversations(id) ON DELETE CASCADE,
			user_id uuid NOT NULL REFERENCES profiles.users(id) ON DELETE CASCADE,
			created_at timestamptz NOT NULL DEFAULT NOW(),
			PRIMARY KEY (conversation_id, user_id)
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_intelligence.messages (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			conversation_id uuid NOT NULL REFERENCES user_intelligence.conversations(id) ON DELETE CASCADE,
			source_channel text NOT NULL,
			direction text NOT NULL,
			source_identity text NOT NULL,
			source_context jsonb NOT NULL DEFAULT '{}'::jsonb,
			security_class text NOT NULL,
			body_text text NOT NULL,
			provider_message_id text NULL,
			created_at timestamptz NOT NULL DEFAULT NOW()
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS uq_ui_messages_channel_provider_message
		ON user_intelligence.messages (source_channel, provider_message_id)
		WHERE provider_message_id IS NOT NULL
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_intelligence.discord_shard_leases (
			shard_id integer PRIMARY KEY,
			owner_id text NOT NULL,
			lease_expires_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT NOW()
		)`); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReceive_DiscordDedupeByMessageID(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	setupStoreSchema(t, ctx, pool)

	store, err := NewStore(pool, "user_intelligence")
	if err != nil {
		t.Fatal(err)
	}

	messageID := "discord-msg-" + uuid.NewString()
	msg := ingest.InboundMessage{
		SourceChannel:     ingest.SourceChannelDiscord,
		ConversationKey:   "discord:dm:user-1",
		BodyText:          "hello",
		SourceIdentity:    "user-1",
		SecurityClass:     ingest.SecurityClassSecure,
		ProviderMessageID: &messageID,
	}
	first, err := store.Receive(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Receive(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Deduped || first.MessageID != second.MessageID {
		t.Fatalf("expected discord dedupe with same message id")
	}
}

func TestDiscordShardLeaseOwnership(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	setupStoreSchema(t, ctx, pool)

	store, err := NewStore(pool, "user_intelligence")
	if err != nil {
		t.Fatal(err)
	}

	ok, err := store.AcquireOrRenewDiscordShard(ctx, 0, "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected node-a to acquire shard")
	}

	ok, err = store.AcquireOrRenewDiscordShard(ctx, 0, "node-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected node-b to fail while lease active")
	}
}
