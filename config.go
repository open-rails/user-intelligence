package userintelligence

import "time"

type OpenAIConfig struct {
	APIKey string
	Model  string
}

type TwilioConfig struct {
	AccountSID  string
	AuthToken   string
	SMSFrom     string
	EmailFrom   string
	EmailAPIKey string
}

// DiscordConfig is the host-supplied surface for the embedded Discord bot.
// All fields except BotToken are optional and fall back to sensible defaults
// inside the runtime; the host need only set what it wants to override.
type DiscordConfig struct {
	// BotToken is the Discord bot token (required to start the runtime).
	BotToken string
	// MentionTarget is the mention/trigger word the bot responds to in guild
	// channels (DMs always match). Defaults to "doujins" when empty.
	MentionTarget string
	// AllowedGuildIDs restricts the bot to these guild IDs. Empty = all guilds.
	AllowedGuildIDs []string
	// ShardOwnerID identifies this process for Discord shard-lease ownership.
	// Defaults to "<hostname>:<pid>" when empty.
	ShardOwnerID string
	// ShardLeaseTTL and ShardRenewEvery tune the Postgres-lease leader election
	// that keeps exactly one instance connected to the Discord gateway.
	// Default to 45s and 15s respectively when zero.
	ShardLeaseTTL   time.Duration
	ShardRenewEvery time.Duration
	// MaxUserMessagesPerMin and MaxGuildMessagesPerMin cap inbound message
	// handling per user and per guild. Zero = unlimited.
	MaxUserMessagesPerMin  int
	MaxGuildMessagesPerMin int
}
