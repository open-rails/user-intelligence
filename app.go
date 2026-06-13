package userintelligence

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/open-rails/user-intelligence/internal/agent"
	"github.com/open-rails/user-intelligence/internal/db"
	"github.com/open-rails/user-intelligence/internal/httpapi"
	"github.com/open-rails/user-intelligence/internal/ingest"
	discordprovider "github.com/open-rails/user-intelligence/internal/providers/discord"
	"github.com/open-rails/user-intelligence/internal/providers/twilio"
)

const defaultSchema = "user_intelligence"
const hardInboundBodyCapBytes int64 = 128 * 1024

type App struct {
	store          *db.Store
	handlers       *httpapi.Handlers
	agent          *agent.Service
	logger         *slog.Logger
	userResolver   ingest.UserResolver
	discordRuntime *discordprovider.Runtime
}

func New(pool *pgxpool.Pool, openAI OpenAIConfig, twilioCfg TwilioConfig) (*App, error) {
	if pool == nil {
		return nil, errors.New("pool is nil")
	}

	store, err := db.NewStore(pool, defaultSchema)
	if err != nil {
		return nil, err
	}

	logger := slog.Default()
	userResolver := ContextUserResolver{
		ContextKey: "user_id",
		Pool:       pool,
		Schema:     defaultProfilesSchema,
	}

	twilioSender := twilio.Sender{
		AccountSID:  twilioCfg.AccountSID,
		AuthToken:   twilioCfg.AuthToken,
		SMSFrom:     twilioCfg.SMSFrom,
		EmailFrom:   twilioCfg.EmailFrom,
		EmailAPIKey: twilioCfg.EmailAPIKey,
	}

	app := &App{
		store:        store,
		logger:       logger,
		userResolver: userResolver,
	}

	var agentClient agent.ResponsesClient
	if strings.TrimSpace(openAI.APIKey) != "" {
		client, err := agent.NewOpenAIResponsesClient(openAI.APIKey)
		if err != nil {
			return nil, err
		}
		agentClient = client
	}
	agentSvc, err := agent.NewService(
		store,
		agentClient,
		twilioSender,
		func(ctx context.Context, conversationID uuid.UUID, body string) error {
			if app.discordRuntime == nil {
				return errors.New("discord runtime is not started")
			}
			return app.discordRuntime.EnqueueReply(ctx, conversationID, body)
		},
		logger,
		agent.Config{Model: strings.TrimSpace(openAI.Model)},
	)
	if err != nil {
		return nil, err
	}
	app.agent = agentSvc

	h, err := httpapi.New(httpapi.Dependencies{
		Receiver:        store,
		SMSNormalizer:   twilio.SMSNormalizer{},
		EmailNormalizer: twilio.EmailNormalizer{},
		TwilioVerifier: twilio.SignatureVerifier{
			AuthToken: twilioCfg.AuthToken,
		},
		Agent:        agentSvc,
		UserResolver: userResolver,
		Logger:       logger,
		MaxBodyBytes: hardInboundBodyCapBytes,
	})
	if err != nil {
		return nil, err
	}
	app.handlers = h
	return app, nil
}

type Route struct {
	Method  string
	Path    string
	Handler http.Handler
}

func (a *App) Routes(prefix string) []Route {
	base := normalizePrefix(prefix)
	return []Route{
		{Method: http.MethodPost, Path: base, Handler: a.handlers.Web()},
		{Method: http.MethodPost, Path: base + "/sms", Handler: a.handlers.TwilioSMS()},
		{Method: http.MethodPost, Path: base + "/email", Handler: a.handlers.TwilioEmail()},
	}
}

func (a *App) StartDiscord(ctx context.Context, cfg DiscordConfig) error {
	if a.discordRuntime != nil {
		return nil
	}
	runtime, err := discordprovider.NewRuntime(a.store, a.userResolver, a.logger, discordprovider.RuntimeConfig{
		BotToken:               cfg.BotToken,
		MentionTarget:          cfg.MentionTarget,
		AllowedGuildIDs:        cfg.AllowedGuildIDs,
		ShardOwnerID:           cfg.ShardOwnerID,
		ShardLeaseTTL:          cfg.ShardLeaseTTL,
		ShardRenewEvery:        cfg.ShardRenewEvery,
		MaxUserMessagesPerMin:  cfg.MaxUserMessagesPerMin,
		MaxGuildMessagesPerMin: cfg.MaxGuildMessagesPerMin,
		OnInboundPersisted: func(ctx context.Context, _ ingest.InboundMessage, res ingest.IngestResult) {
			if a.agent == nil || !a.agent.Enabled() {
				return
			}
			a.agent.ProcessAsync(ctx, agent.ProcessRequest{
				ConversationID: res.ConversationID,
				MessageID:      res.MessageID,
				SourceChannel:  ingest.SourceChannelDiscord,
			})
		},
	})
	if err != nil {
		return err
	}
	if err := runtime.Start(ctx); err != nil {
		return err
	}
	a.discordRuntime = runtime
	return nil
}

func (a *App) StopDiscord(ctx context.Context) error {
	if a.discordRuntime == nil {
		return nil
	}
	err := a.discordRuntime.Stop(ctx)
	a.discordRuntime = nil
	return err
}

func normalizePrefix(prefix string) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		return "/messages"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return "/messages"
	}
	return p
}
