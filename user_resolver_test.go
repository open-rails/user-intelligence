package userintelligence

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

func TestContextUserResolver_UserFromContext(t *testing.T) {
	id := uuid.New()
	resolver := ContextUserResolver{ContextKey: "user_id"}

	ctx := context.WithValue(context.Background(), "user_id", id.String())
	got, err := resolver.UserFromContext(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != id {
		t.Fatalf("expected user id %s", id)
	}
}

func TestContextUserResolver_ResolveBySourceIdentity_DB(t *testing.T) {
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

	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS profiles`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS profiles.users (
			id uuid PRIMARY KEY,
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

	userID := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO profiles.users (id, email, phone_number)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, phone_number = EXCLUDED.phone_number
	`, userID, "lookup@example.com", "+15550007777"); err != nil {
		t.Fatal(err)
	}

	resolver := ContextUserResolver{
		ContextKey: "user_id",
		Pool:       pool,
		Schema:     "profiles",
	}

	gotEmail, err := resolver.ResolveBySourceIdentity(ctx, ingest.SourceChannelTwilioMail, "LOOKUP@example.com")
	if err != nil {
		t.Fatalf("email lookup failed: %v", err)
	}
	if gotEmail == nil || *gotEmail != userID {
		t.Fatalf("expected email lookup user id %s", userID)
	}

	gotSMS, err := resolver.ResolveBySourceIdentity(ctx, ingest.SourceChannelTwilioSMS, "+15550007777")
	if err != nil {
		t.Fatalf("sms lookup failed: %v", err)
	}
	if gotSMS == nil || *gotSMS != userID {
		t.Fatalf("expected sms lookup user id %s", userID)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO profiles.user_providers (user_id, provider_slug, subject)
		VALUES ($1, 'discord', $2)
	`, userID, "123456789"); err != nil {
		t.Fatal(err)
	}

	gotDiscord, err := resolver.ResolveBySourceIdentity(ctx, ingest.SourceChannelDiscord, "123456789")
	if err != nil {
		t.Fatalf("discord lookup failed: %v", err)
	}
	if gotDiscord == nil || *gotDiscord != userID {
		t.Fatalf("expected discord lookup user id %s", userID)
	}
}
