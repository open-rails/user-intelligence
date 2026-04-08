package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"

	"github.com/open-rails/user-intelligence/internal/db"
	"github.com/open-rails/user-intelligence/internal/ingest"
)

const (
	defaultShardLeaseTTL   = 45 * time.Second
	defaultShardRenewEvery = 15 * time.Second
	defaultSendMaxAttempts = 3
)

type RuntimeConfig struct {
	BotToken               string
	MentionTarget          string
	AllowedGuildIDs        []string
	ShardOwnerID           string
	ShardLeaseTTL          time.Duration
	ShardRenewEvery        time.Duration
	MaxUserMessagesPerMin  int
	MaxGuildMessagesPerMin int
	OnInboundPersisted     func(context.Context, ingest.InboundMessage, ingest.IngestResult)
}

type Runtime struct {
	store        *db.Store
	userResolver ingest.UserResolver
	logger       *slog.Logger

	cfg           RuntimeConfig
	allowedGuilds map[string]struct{}
	userLimiter   *slidingCounter
	guildLimiter  *slidingCounter

	mu            sync.Mutex
	running       bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	sendQueue     chan sendJob
	shardSessions map[int]*discordgo.Session
	shardCount    int
	botUserID     string
}

type sendJob struct {
	conversationID uuid.UUID
	body           string
}

func NewRuntime(store *db.Store, userResolver ingest.UserResolver, logger *slog.Logger, cfg RuntimeConfig) (*Runtime, error) {
	if store == nil {
		return nil, errors.New("store is required")
	}
	if userResolver == nil {
		return nil, errors.New("user resolver is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	cfg.BotToken = strings.TrimSpace(cfg.BotToken)
	if cfg.BotToken == "" {
		return nil, errors.New("discord bot token is required")
	}
	if cfg.ShardLeaseTTL <= 0 {
		cfg.ShardLeaseTTL = defaultShardLeaseTTL
	}
	if cfg.ShardRenewEvery <= 0 {
		cfg.ShardRenewEvery = defaultShardRenewEvery
	}
	if cfg.ShardOwnerID == "" {
		host, _ := os.Hostname()
		if strings.TrimSpace(host) == "" {
			host = "unknown-host"
		}
		cfg.ShardOwnerID = fmt.Sprintf("%s:%d", host, os.Getpid())
	}
	if strings.TrimSpace(cfg.MentionTarget) == "" {
		cfg.MentionTarget = defaultMentionTarget
	}

	allowed := make(map[string]struct{}, len(cfg.AllowedGuildIDs))
	for _, g := range cfg.AllowedGuildIDs {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		allowed[g] = struct{}{}
	}

	return &Runtime{
		store:         store,
		userResolver:  userResolver,
		logger:        logger,
		cfg:           cfg,
		allowedGuilds: allowed,
		userLimiter:   newSlidingCounter(cfg.MaxUserMessagesPerMin, time.Minute),
		guildLimiter:  newSlidingCounter(cfg.MaxGuildMessagesPerMin, time.Minute),
		sendQueue:     make(chan sendJob, 256),
		shardSessions: map[int]*discordgo.Session{},
	}, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return nil
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.running = true
	r.mu.Unlock()

	baseSession, err := discordgo.New(normalizeToken(r.cfg.BotToken))
	if err != nil {
		r.Stop(context.Background())
		return fmt.Errorf("discord session init: %w", err)
	}
	gb, err := baseSession.GatewayBot()
	if err != nil {
		r.Stop(context.Background())
		return fmt.Errorf("discord get gateway bot: %w", err)
	}
	if gb.Shards <= 0 {
		gb.Shards = 1
	}
	r.shardCount = gb.Shards

	me, err := baseSession.User("@me")
	if err != nil {
		r.Stop(context.Background())
		return fmt.Errorf("discord get current bot user: %w", err)
	}
	r.botUserID = strings.TrimSpace(me.ID)
	r.logger.Info("discord runtime configured",
		slog.Int("shards", r.shardCount),
		slog.Int("session_start_total", gb.SessionStartLimit.Total),
		slog.Int("session_start_remaining", gb.SessionStartLimit.Remaining),
		slog.Int("session_start_max_concurrency", gb.SessionStartLimit.MaxConcurrency),
	)

	r.wg.Add(2)
	go r.shardReconcilerLoop()
	go r.sendLoop()

	if err := r.waitForShardZero(r.ctx, 30*time.Second); err != nil {
		r.Stop(context.Background())
		return err
	}
	return nil
}

func (r *Runtime) Stop(_ context.Context) error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	cancel := r.cancel
	r.running = false
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	r.wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	for shardID, sess := range r.shardSessions {
		_ = sess.Close()
		_ = r.store.ReleaseDiscordShard(context.Background(), shardID, r.cfg.ShardOwnerID)
	}
	r.shardSessions = map[int]*discordgo.Session{}
	return nil
}

func (r *Runtime) EnqueueReply(ctx context.Context, conversationID uuid.UUID, body string) error {
	body = strings.TrimSpace(body)
	if conversationID == uuid.Nil {
		return errors.New("conversation_id required")
	}
	if body == "" {
		return errors.New("body required")
	}

	job := sendJob{conversationID: conversationID, body: body}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.sendQueue <- job:
		return nil
	}
}

func (r *Runtime) waitForShardZero(ctx context.Context, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		r.mu.Lock()
		_, ok := r.shardSessions[0]
		r.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("discord shard 0 was not acquired before startup timeout")
		case <-ticker.C:
		}
	}
}

func (r *Runtime) shardReconcilerLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.cfg.ShardRenewEvery)
	defer ticker.Stop()

	r.reconcileShards()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.reconcileShards()
		}
	}
}

func (r *Runtime) reconcileShards() {
	for shardID := 0; shardID < r.shardCount; shardID++ {
		owned, err := r.store.AcquireOrRenewDiscordShard(r.ctx, shardID, r.cfg.ShardOwnerID, r.cfg.ShardLeaseTTL)
		if err != nil {
			r.logger.Warn("discord shard lease failure", slog.Int("shard_id", shardID), slog.String("error", err.Error()))
			continue
		}

		r.mu.Lock()
		sess, running := r.shardSessions[shardID]
		r.mu.Unlock()

		if owned && !running {
			if err := r.startShardSession(shardID); err != nil {
				r.logger.Warn("discord shard start failed", slog.Int("shard_id", shardID), slog.String("error", err.Error()))
			}
			continue
		}
		if !owned && running {
			_ = sess.Close()
			_ = r.store.ReleaseDiscordShard(r.ctx, shardID, r.cfg.ShardOwnerID)
			r.mu.Lock()
			delete(r.shardSessions, shardID)
			r.mu.Unlock()
		}
	}

	r.mu.Lock()
	_, shardZeroOwned := r.shardSessions[0]
	r.mu.Unlock()
	if !shardZeroOwned {
		r.logger.Warn("discord shard 0 not currently owned")
	}
}

func (r *Runtime) startShardSession(shardID int) error {
	sess, err := discordgo.New(normalizeToken(r.cfg.BotToken))
	if err != nil {
		return err
	}
	sess.ShardID = shardID
	sess.ShardCount = r.shardCount
	sess.Identify.Intents = discordgo.IntentGuildMessages | discordgo.IntentDirectMessages | discordgo.IntentMessageContent
	sess.ShouldReconnectOnError = true
	sess.ShouldRetryOnRateLimit = true
	sess.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		r.handleMessageCreate(s, m)
	})

	if err := sess.Open(); err != nil {
		return err
	}

	r.mu.Lock()
	r.shardSessions[shardID] = sess
	r.mu.Unlock()
	r.logger.Info("discord shard connected", slog.Int("shard_id", shardID), slog.Int("shard_count", r.shardCount))
	return nil
}

func (r *Runtime) handleMessageCreate(_ *discordgo.Session, m *discordgo.MessageCreate) {
	msg, accepted := NormalizeMessageCreate(m, InboundFilterConfig{
		BotUserID:       r.botUserID,
		MentionTarget:   r.cfg.MentionTarget,
		AllowedGuildIDs: r.allowedGuilds,
	})
	if !accepted {
		return
	}

	now := time.Now().UTC()
	if !r.userLimiter.Allow(msg.SourceIdentity, now) {
		r.logger.Warn("discord inbound dropped by per-user limit", slog.String("author_id", msg.SourceIdentity))
		return
	}

	guildID := ""
	if len(msg.SourceContext) > 0 {
		var v struct {
			GuildID string `json:"guild_id"`
		}
		_ = json.Unmarshal(msg.SourceContext, &v)
		guildID = strings.TrimSpace(v.GuildID)
	}
	if guildID != "" && !r.guildLimiter.Allow(guildID, now) {
		r.logger.Warn("discord inbound dropped by per-guild limit", slog.String("guild_id", guildID))
		return
	}

	userID, err := r.userResolver.ResolveBySourceIdentity(r.ctx, ingest.SourceChannelDiscord, msg.SourceIdentity)
	if err != nil {
		r.logger.Warn("discord user resolution failed", slog.String("author_id", msg.SourceIdentity), slog.String("error", err.Error()))
	}
	msg.UserID = userID

	res, err := r.store.Receive(r.ctx, msg)
	if err != nil {
		r.logger.Warn("discord ingest persist failed", slog.String("author_id", msg.SourceIdentity), slog.String("error", err.Error()))
		return
	}
	r.logger.Info("discord message ingested",
		slog.String("conversation_id", res.ConversationID.String()),
		slog.String("message_id", res.MessageID.String()),
		slog.Bool("deduped", res.Deduped),
	)
	if r.cfg.OnInboundPersisted != nil {
		r.cfg.OnInboundPersisted(r.ctx, msg, res)
	}
}

func (r *Runtime) sendLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.sendQueue:
			r.processSendJob(job)
		}
	}
}

func (r *Runtime) processSendJob(job sendJob) {
	target, err := r.store.LatestDiscordReplyTarget(r.ctx, job.conversationID)
	if err != nil {
		r.logger.Warn("discord send dead-letter: missing reply target",
			slog.String("conversation_id", job.conversationID.String()),
			slog.String("error", err.Error()),
		)
		return
	}

	var sent *discordgo.Message
	for attempt := 1; attempt <= defaultSendMaxAttempts; attempt++ {
		sent, err = r.sendToTarget(target, job.body)
		if err == nil {
			break
		}
		backoff := time.Duration(attempt*attempt) * time.Second
		r.logger.Warn("discord send failed, retrying",
			slog.String("conversation_id", job.conversationID.String()),
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()),
			slog.Duration("backoff", backoff),
		)
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
	if err != nil {
		r.logger.Error("discord send dead-letter",
			slog.String("conversation_id", job.conversationID.String()),
			slog.String("error", err.Error()),
		)
		return
	}

	providerMessageID := strings.TrimSpace(sent.ID)
	sourceContext := map[string]any{
		"guild_id":       target.GuildID,
		"channel_id":     target.ChannelID,
		"thread_id":      target.ThreadID,
		"message_id":     providerMessageID,
		"author_id":      r.botUserID,
		"mention_target": target.MentionTarget,
		"is_dm":          target.IsDM,
	}
	rawContext, _ := json.Marshal(sourceContext)
	_, err = r.store.RecordOutbound(r.ctx, ingest.OutboundMessage{
		ConversationID:    target.ConversationID,
		SourceChannel:     ingest.SourceChannelDiscord,
		SourceIdentity:    "discord_bot:" + r.botUserID,
		SourceContext:     rawContext,
		SecurityClass:     ingest.SecurityClassSecure,
		BodyText:          job.body,
		ProviderMessageID: &providerMessageID,
	})
	if err != nil {
		r.logger.Warn("discord outbound persist failed",
			slog.String("conversation_id", target.ConversationID.String()),
			slog.String("error", err.Error()),
		)
	}
}

func (r *Runtime) sendToTarget(target db.DiscordReplyTarget, body string) (*discordgo.Message, error) {
	sess := r.pickSession()
	if sess == nil {
		return nil, errors.New("no active discord shard session")
	}

	if target.IsDM {
		channelID := strings.TrimSpace(target.ChannelID)
		if channelID == "" && strings.TrimSpace(target.AuthorID) != "" {
			ch, err := sess.UserChannelCreate(target.AuthorID)
			if err != nil {
				return nil, err
			}
			channelID = ch.ID
		}
		msg, err := sess.ChannelMessageSend(channelID, body)
		if err == nil {
			return msg, nil
		}
		if strings.TrimSpace(target.AuthorID) == "" {
			return nil, err
		}
		ch, dmErr := sess.UserChannelCreate(target.AuthorID)
		if dmErr != nil {
			return nil, fmt.Errorf("send dm failed: %w; recreate dm failed: %v", err, dmErr)
		}
		return sess.ChannelMessageSend(ch.ID, body)
	}

	sendChannelID, ref := buildSendPlan(target)
	if ref != nil {
		return sess.ChannelMessageSendReply(sendChannelID, body, ref)
	}
	return sess.ChannelMessageSend(sendChannelID, body)
}

func buildSendPlan(target db.DiscordReplyTarget) (string, *discordgo.MessageReference) {
	sendChannelID := strings.TrimSpace(target.ChannelID)
	if strings.TrimSpace(target.ThreadID) != "" {
		sendChannelID = target.ThreadID
	}
	if strings.TrimSpace(target.MessageID) == "" {
		return sendChannelID, nil
	}
	return sendChannelID, &discordgo.MessageReference{
		MessageID: target.MessageID,
		ChannelID: sendChannelID,
		GuildID:   target.GuildID,
	}
}

func (r *Runtime) pickSession() *discordgo.Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.shardSessions[0]; ok && s != nil {
		return s
	}
	for _, s := range r.shardSessions {
		if s != nil {
			return s
		}
	}
	return nil
}

func normalizeToken(token string) string {
	token = strings.TrimSpace(token)
	if strings.HasPrefix(strings.ToLower(token), "bot ") {
		return token
	}
	return "Bot " + token
}
