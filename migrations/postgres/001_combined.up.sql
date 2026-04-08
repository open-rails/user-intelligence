CREATE SCHEMA IF NOT EXISTS user_intelligence;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS user_intelligence.conversations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_channel text NOT NULL,
    conversation_key text NOT NULL,
    last_message_at timestamptz NOT NULL DEFAULT NOW(),
    created_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (source_channel, conversation_key)
);

CREATE TABLE IF NOT EXISTS user_intelligence.conversation_participants (
    conversation_id uuid NOT NULL REFERENCES user_intelligence.conversations(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES profiles.users(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (conversation_id, user_id)
);

CREATE TABLE IF NOT EXISTS user_intelligence.messages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id uuid NOT NULL REFERENCES user_intelligence.conversations(id) ON DELETE CASCADE,
    source_channel text NOT NULL,
    direction text NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    source_identity text NOT NULL,
    source_context jsonb NOT NULL DEFAULT '{}'::jsonb,
    security_class text NOT NULL CHECK (security_class IN ('secure', 'insecure')),
    body_text text NOT NULL,
    provider_message_id text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ui_conversations_last_message
    ON user_intelligence.conversations (source_channel, last_message_at DESC);

CREATE INDEX IF NOT EXISTS idx_ui_participants_user
    ON user_intelligence.conversation_participants (user_id, conversation_id);

CREATE INDEX IF NOT EXISTS idx_ui_messages_conversation_created
    ON user_intelligence.messages (conversation_id, created_at);

CREATE INDEX IF NOT EXISTS idx_ui_messages_provider_message_id
    ON user_intelligence.messages (provider_message_id)
    WHERE provider_message_id IS NOT NULL;

-- Idempotency: provider message ids are unique per source channel when present.
CREATE UNIQUE INDEX IF NOT EXISTS uq_ui_messages_channel_provider_message
    ON user_intelligence.messages (source_channel, provider_message_id)
    WHERE provider_message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS user_intelligence.discord_shard_leases (
    shard_id integer PRIMARY KEY,
    owner_id text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ui_discord_shard_leases_expires
    ON user_intelligence.discord_shard_leases (lease_expires_at);

CREATE TABLE IF NOT EXISTS user_intelligence.user_memory (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES profiles.users(id) ON DELETE CASCADE,
    memory_blob text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (user_id)
);

CREATE TABLE IF NOT EXISTS user_intelligence.agent_issues (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id uuid NOT NULL REFERENCES user_intelligence.conversations(id) ON DELETE CASCADE,
    source_channel text NOT NULL,
    issue_type text NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    dedupe_hash text NOT NULL,
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    report_count integer NOT NULL DEFAULT 1,
    first_seen_at timestamptz NOT NULL DEFAULT NOW(),
    last_seen_at timestamptz NOT NULL DEFAULT NOW(),
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ui_agent_issues_open_hash
    ON user_intelligence.agent_issues (dedupe_hash)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_ui_agent_issues_conversation
    ON user_intelligence.agent_issues (conversation_id, created_at DESC);

CREATE TABLE IF NOT EXISTS user_intelligence.agent_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id uuid NOT NULL REFERENCES user_intelligence.conversations(id) ON DELETE CASCADE,
    inbound_message_id uuid NOT NULL REFERENCES user_intelligence.messages(id) ON DELETE CASCADE,
    source_channel text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'skipped')),
    response_id text NULL,
    previous_response_id text NULL,
    output_text text NULL,
    trace jsonb NOT NULL DEFAULT '[]'::jsonb,
    error_text text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (inbound_message_id)
);

CREATE INDEX IF NOT EXISTS idx_ui_agent_runs_conversation
    ON user_intelligence.agent_runs (conversation_id, created_at DESC);
