package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/open-rails/user-intelligence/internal/agent"
	"github.com/open-rails/user-intelligence/internal/ingest"
	"github.com/open-rails/user-intelligence/internal/verify"
)

type AgentProcessor interface {
	Enabled() bool
	ProcessSync(ctx context.Context, req agent.ProcessRequest, emit func(agent.Event)) (agent.ProcessResult, error)
	ProcessAsync(ctx context.Context, req agent.ProcessRequest)
}

type Handlers struct {
	receiver        ingest.Receiver
	smsNormalizer   ingest.Normalizer
	emailNormalizer ingest.Normalizer
	twilioVerifier  verify.Verifier
	agent           AgentProcessor
	userResolver    ingest.UserResolver
	logger          *slog.Logger
	maxBodyBytes    int64
}

type Dependencies struct {
	Receiver        ingest.Receiver
	SMSNormalizer   ingest.Normalizer
	EmailNormalizer ingest.Normalizer
	TwilioVerifier  verify.Verifier
	Agent           AgentProcessor
	UserResolver    ingest.UserResolver
	Logger          *slog.Logger
	MaxBodyBytes    int64
}

func New(deps Dependencies) (*Handlers, error) {
	if deps.Receiver == nil {
		return nil, errors.New("receiver is required")
	}
	if deps.SMSNormalizer == nil {
		return nil, errors.New("sms normalizer is required")
	}
	if deps.EmailNormalizer == nil {
		return nil, errors.New("email normalizer is required")
	}
	if deps.TwilioVerifier == nil {
		return nil, errors.New("twilio verifier is required")
	}
	if deps.UserResolver == nil {
		return nil, errors.New("user resolver is required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.MaxBodyBytes <= 0 {
		deps.MaxBodyBytes = 128 * 1024
	}

	return &Handlers{
		receiver:        deps.Receiver,
		smsNormalizer:   deps.SMSNormalizer,
		emailNormalizer: deps.EmailNormalizer,
		twilioVerifier:  deps.TwilioVerifier,
		agent:           deps.Agent,
		userResolver:    deps.UserResolver,
		logger:          deps.Logger,
		maxBodyBytes:    deps.MaxBodyBytes,
	}, nil
}

func (h *Handlers) Web() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID, traceID := requestTracing(r)
		if r.Method != http.MethodPost {
			h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

		msg, err := h.normalizeWeb(r)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		userID, err := h.userResolver.UserFromContext(r.Context())
		if err != nil {
			h.writeError(w, http.StatusUnauthorized, "invalid_user_context")
			return
		}
		msg.UserID = userID

		res, err := h.receiver.Receive(r.Context(), msg)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "persist_failed")
			return
		}

		h.logger.Info("user-intelligence web ingested",
			slog.String("source_channel", string(msg.SourceChannel)),
			slog.String("conversation_id", res.ConversationID.String()),
			slog.String("message_id", res.MessageID.String()),
			slog.Bool("deduped", res.Deduped),
			slog.String("request_id", requestID),
			slog.String("trace_id", traceID),
			slog.Duration("latency", time.Since(start)),
		)

		acceptsSSE := strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream")
		if acceptsSSE {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			if h.agent != nil && h.agent.Enabled() {
				processReq := agent.ProcessRequest{
					ConversationID: res.ConversationID,
					MessageID:      res.MessageID,
					SourceChannel:  msg.SourceChannel,
				}
				agentResult, agentErr := h.agent.ProcessSync(r.Context(), processReq, func(evt agent.Event) {
					h.writeSSEEvent(w, evt.Type, evt.Data)
				})
				if agentErr != nil {
					h.writeSSEEvent(w, "response.failed", map[string]any{
						"conversation_id": res.ConversationID,
						"message_id":      res.MessageID,
						"error":           agentErr.Error(),
					})
				}
				h.writeSSEEvent(w, "done", map[string]any{
					"status":          "recorded",
					"conversation_id": res.ConversationID,
					"message_id":      res.MessageID,
					"agent":           agentStatus(agentErr, agentResult.Enabled),
				})
			} else {
				h.writeSSEEvent(w, "done", map[string]any{
					"status":          "recorded",
					"conversation_id": res.ConversationID,
					"message_id":      res.MessageID,
					"agent":           "disabled",
				})
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}

		agentState := "disabled"
		agentOutput := ""
		if h.agent != nil && h.agent.Enabled() {
			agentResult, agentErr := h.agent.ProcessSync(r.Context(), agent.ProcessRequest{
				ConversationID: res.ConversationID,
				MessageID:      res.MessageID,
				SourceChannel:  msg.SourceChannel,
			}, nil)
			agentState = agentStatus(agentErr, agentResult.Enabled)
			agentOutput = strings.TrimSpace(agentResult.OutputText)
		}

		h.writeJSON(w, http.StatusOK, map[string]any{
			"status":          "recorded",
			"conversation_id": res.ConversationID,
			"message_id":      res.MessageID,
			"deduped":         res.Deduped,
			"agent":           agentState,
			"agent_output":    agentOutput,
		})
	})
}

func (h *Handlers) TwilioSMS() http.Handler {
	return h.twilioInboundHandler(ingest.SourceChannelTwilioSMS, h.smsNormalizer)
}

func (h *Handlers) TwilioEmail() http.Handler {
	return h.twilioInboundHandler(ingest.SourceChannelTwilioMail, h.emailNormalizer)
}

func (h *Handlers) twilioInboundHandler(channel ingest.SourceChannel, normalizer ingest.Normalizer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID, traceID := requestTracing(r)
		if r.Method != http.MethodPost {
			h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

		if err := h.twilioVerifier.Verify(r); err != nil {
			h.writeError(w, http.StatusUnauthorized, "invalid_signature")
			return
		}

		msg, err := normalizer.Normalize(r.Context(), r)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		userID, err := h.userResolver.ResolveBySourceIdentity(r.Context(), channel, msg.SourceIdentity)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "user_lookup_failed")
			return
		}
		msg.UserID = userID

		res, err := h.receiver.Receive(r.Context(), msg)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "persist_failed")
			return
		}

		h.logger.Info("user-intelligence twilio ingested",
			slog.String("source_channel", string(channel)),
			slog.String("conversation_id", res.ConversationID.String()),
			slog.String("message_id", res.MessageID.String()),
			slog.Bool("deduped", res.Deduped),
			slog.String("request_id", requestID),
			slog.String("trace_id", traceID),
			slog.Duration("latency", time.Since(start)),
		)

		if h.agent != nil && h.agent.Enabled() {
			h.agent.ProcessAsync(r.Context(), agent.ProcessRequest{
				ConversationID: res.ConversationID,
				MessageID:      res.MessageID,
				SourceChannel:  msg.SourceChannel,
			})
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

func (h *Handlers) normalizeWeb(r *http.Request) (ingest.InboundMessage, error) {
	type webRequest struct {
		Text            string          `json:"text"`
		BodyText        string          `json:"body_text"`
		ConversationKey string          `json:"conversation_key"`
		SourceIdentity  string          `json:"source_identity"`
		SourceContext   json.RawMessage `json:"source_context"`
		ProviderMessage string          `json:"provider_message_id"`
	}

	var req webRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return ingest.InboundMessage{}, fmt.Errorf("invalid json")
	}

	body := strings.TrimSpace(req.BodyText)
	if body == "" {
		body = strings.TrimSpace(req.Text)
	}
	if body == "" {
		return ingest.InboundMessage{}, errors.New("body_text is required")
	}

	sourceIdentity := strings.TrimSpace(req.SourceIdentity)
	if sourceIdentity == "" {
		sourceIdentity = defaultWebSourceIdentity(r)
	}
	if sourceIdentity == "" {
		sourceIdentity = "web"
	}

	conversationKey := strings.TrimSpace(req.ConversationKey)
	if conversationKey == "" {
		conversationKey = "web:" + sourceIdentity
	}

	sourceContext := req.SourceContext
	if len(sourceContext) == 0 {
		sourceContext, _ = json.Marshal(map[string]any{"remote_addr": r.RemoteAddr})
	}

	msg := ingest.InboundMessage{
		SourceChannel:   ingest.SourceChannelWeb,
		ConversationKey: conversationKey,
		BodyText:        body,
		SourceIdentity:  sourceIdentity,
		SourceContext:   sourceContext,
		SecurityClass:   ingest.ClassifySecurity(ingest.SourceChannelWeb),
	}

	providerID := strings.TrimSpace(req.ProviderMessage)
	if providerID != "" {
		msg.ProviderMessageID = &providerID
	}

	return msg, nil
}

func (h *Handlers) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handlers) writeError(w http.ResponseWriter, status int, code string) {
	h.writeJSON(w, status, map[string]any{"error": code})
}

func requestTracing(r *http.Request) (requestID string, traceID string) {
	requestID = strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID = strings.TrimSpace(r.Header.Get("X-Request-Id"))
	}
	traceID = strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	if traceID == "" {
		traceID = strings.TrimSpace(r.Header.Get("X-Trace-Id"))
	}
	if traceID == "" {
		traceID = strings.TrimSpace(r.Header.Get("Traceparent"))
	}
	return requestID, traceID
}

func (h *Handlers) writeSSEEvent(w http.ResponseWriter, event string, payload map[string]any) {
	if strings.TrimSpace(event) == "" {
		event = "message"
	}
	data, err := json.Marshal(payload)
	if err != nil {
		data = []byte(`{"error":"marshal_failed"}`)
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func defaultWebSourceIdentity(r *http.Request) string {
	if r == nil {
		return ""
	}

	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			candidate := strings.TrimSpace(parts[0])
			if candidate != "" {
				return candidate
			}
		}
	}

	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}

	remote := strings.TrimSpace(r.RemoteAddr)
	if remote == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remote)
	if err == nil {
		return strings.TrimSpace(host)
	}
	return remote
}

func agentStatus(err error, enabled bool) string {
	if !enabled {
		return "disabled"
	}
	if err != nil {
		return "failed"
	}
	return "completed"
}
