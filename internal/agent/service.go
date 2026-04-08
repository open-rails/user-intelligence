package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/open-rails/user-intelligence/internal/db"
	"github.com/open-rails/user-intelligence/internal/ingest"
)

const (
	defaultModel          = "gpt-5.4-mini"
	defaultMaxSteps       = 6
	defaultMaxOutputToken = 600
	defaultTimeout        = 45 * time.Second
	defaultContextLimit   = 20
	defaultMaxReplyChars  = 2000
)

type Store interface {
	WithConversationLock(ctx context.Context, conversationID uuid.UUID, fn func(context.Context) error) error
	StartAgentRun(ctx context.Context, conversationID, inboundMessageID uuid.UUID, sourceChannel ingest.SourceChannel) (db.AgentRunStartResult, error)
	CompleteAgentRun(ctx context.Context, runID uuid.UUID, status db.AgentRunStatus, responseID, previousResponseID, outputText string, trace json.RawMessage, errorText string) error
	ListMessagesByConversation(ctx context.Context, conversationID uuid.UUID, limit int) ([]ingest.StoredMessage, error)
	LatestInboundByConversationAndChannel(ctx context.Context, conversationID uuid.UUID, channel ingest.SourceChannel) (db.InboundMessageRoute, error)
	RecordOutbound(ctx context.Context, msg ingest.OutboundMessage) (ingest.IngestResult, error)
	UpsertUserMemory(ctx context.Context, userID uuid.UUID, memoryBlob string) (time.Time, error)
	UpsertAgentIssue(ctx context.Context, conversationID uuid.UUID, sourceChannel ingest.SourceChannel, issueType, title, body, dedupeHash string) (db.AgentIssueUpsertResult, error)
}

type TwilioSender interface {
	SendSMS(ctx context.Context, to string, body string) (string, error)
	SendEmail(ctx context.Context, to string, subject string, body string) error
}

type Config struct {
	Model              string
	MaxSteps           int
	MaxOutputTokens    int
	MaxContextMessages int
	Timeout            time.Duration
}

type Event struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data,omitempty"`
}

type ProcessRequest struct {
	ConversationID uuid.UUID
	MessageID      uuid.UUID
	SourceChannel  ingest.SourceChannel
}

type ProcessResult struct {
	Enabled    bool
	Deduped    bool
	ResponseID string
	OutputText string
	ToolCalls  int
	Completed  bool
}

type Service struct {
	store          Store
	client         ResponsesClient
	twilioSender   TwilioSender
	discordEnqueue func(context.Context, uuid.UUID, string) error
	logger         *slog.Logger
	cfg            Config
	maxReplyChars  int
	maxContextMsgs int
}

func NewService(
	store Store,
	client ResponsesClient,
	twilioSender TwilioSender,
	discordEnqueue func(context.Context, uuid.UUID, string) error,
	logger *slog.Logger,
	cfg Config,
) (*Service, error) {
	if store == nil {
		return nil, errors.New("store is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = defaultModel
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = defaultMaxSteps
	}
	if cfg.MaxOutputTokens <= 0 {
		cfg.MaxOutputTokens = defaultMaxOutputToken
	}
	if cfg.MaxContextMessages <= 0 {
		cfg.MaxContextMessages = defaultContextLimit
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}

	return &Service{
		store:          store,
		client:         client,
		twilioSender:   twilioSender,
		discordEnqueue: discordEnqueue,
		logger:         logger,
		cfg:            cfg,
		maxReplyChars:  defaultMaxReplyChars,
		maxContextMsgs: cfg.MaxContextMessages,
	}, nil
}

func (s *Service) Enabled() bool {
	return s != nil && s.client != nil
}

func (s *Service) ProcessAsync(parent context.Context, req ProcessRequest) {
	if !s.Enabled() {
		return
	}
	go func() {
		ctx := parent
		if ctx == nil {
			ctx = context.Background()
		}
		if s.cfg.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.cfg.Timeout)
			defer cancel()
		}
		_, err := s.ProcessSync(ctx, req, nil)
		if err != nil {
			s.logger.Warn("agent async processing failed",
				slog.String("conversation_id", req.ConversationID.String()),
				slog.String("message_id", req.MessageID.String()),
				slog.String("source_channel", string(req.SourceChannel)),
				slog.String("error", err.Error()),
			)
		}
	}()
}

func (s *Service) ProcessSync(ctx context.Context, req ProcessRequest, emit func(Event)) (ProcessResult, error) {
	if !s.Enabled() {
		return ProcessResult{Enabled: false}, nil
	}
	if req.ConversationID == uuid.Nil {
		return ProcessResult{}, errors.New("conversation_id required")
	}
	if req.MessageID == uuid.Nil {
		return ProcessResult{}, errors.New("message_id required")
	}
	if strings.TrimSpace(string(req.SourceChannel)) == "" {
		return ProcessResult{}, errors.New("source_channel required")
	}

	startedAt := time.Now()
	if s.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.Timeout)
		defer cancel()
	}

	var result ProcessResult
	replySent := false
	err := s.store.WithConversationLock(ctx, req.ConversationID, func(lockCtx context.Context) error {
		started, err := s.store.StartAgentRun(lockCtx, req.ConversationID, req.MessageID, req.SourceChannel)
		if err != nil {
			return err
		}
		if started.AlreadyStarted {
			result = ProcessResult{
				Enabled: true,
				Deduped: true,
			}
			return nil
		}

		runID := started.RunID
		trace := make([]map[string]any, 0, s.cfg.MaxSteps)
		completeRun := func(status db.AgentRunStatus, responseID string, previousResponseID string, outputText string, errText string) {
			traceJSON, _ := json.Marshal(trace)
			if completeErr := s.store.CompleteAgentRun(lockCtx, runID, status, responseID, previousResponseID, outputText, traceJSON, errText); completeErr != nil {
				s.logger.Warn("agent run completion write failed",
					slog.String("run_id", runID.String()),
					slog.String("error", completeErr.Error()),
				)
			}
		}

		messages, err := s.store.ListMessagesByConversation(lockCtx, req.ConversationID, s.maxContextMsgs)
		if err != nil {
			completeRun(db.AgentRunStatusFailed, "", "", "", err.Error())
			return err
		}
		if !s.eligibleInbound(messages, req) {
			trace = append(trace, map[string]any{
				"type":   "eligibility_skip",
				"reason": "system_internal_or_allowlist",
			})
			result.Enabled = true
			completeRun(db.AgentRunStatusSkipped, "", "", "", "")
			return nil
		}

		if simplePingPong(lockCtx, s, req, messages, emit, &result, &trace, &replySent) {
			completeRun(db.AgentRunStatusCompleted, "local:ping-pong", "", result.OutputText, "")
			return nil
		}

		previousResponseID := ""
		input := buildInitialInput(messages, req.SourceChannel)
		tools := toolDefinitions()

		for step := 0; step < s.cfg.MaxSteps; step++ {
			resp, callErr := s.client.Create(lockCtx, ResponsesRequest{
				Model:              s.cfg.Model,
				Input:              input,
				Tools:              tools,
				PreviousResponseID: previousResponseID,
				MaxOutputTokens:    s.cfg.MaxOutputTokens,
				Metadata: map[string]string{
					"conversation_id": req.ConversationID.String(),
					"inbound_message": req.MessageID.String(),
					"source_channel":  string(req.SourceChannel),
				},
			})
			if callErr != nil {
				emitEvent(emit, Event{Type: "response.failed", Data: map[string]any{"error": callErr.Error()}})
				trace = append(trace, map[string]any{
					"step":                 step + 1,
					"previous_response_id": previousResponseID,
					"error":                callErr.Error(),
				})
				completeRun(db.AgentRunStatusFailed, "", previousResponseID, result.OutputText, callErr.Error())
				return callErr
			}

			stepTrace := map[string]any{
				"step":                 step + 1,
				"response_id":          resp.ID,
				"previous_response_id": previousResponseID,
				"output_items":         outputItemTrace(resp.Output),
			}

			text := extractAssistantText(resp)
			if strings.TrimSpace(text) != "" {
				result.OutputText = strings.TrimSpace(text)
				emitEvent(emit, Event{
					Type: "response.output_text.delta",
					Data: map[string]any{
						"response_id": resp.ID,
						"delta":       result.OutputText,
					},
				})
			}

			functionCalls := extractFunctionCalls(resp.Output)
			if len(functionCalls) == 0 {
				if strings.TrimSpace(result.OutputText) != "" && !replySent {
					sendOut, sendErr := s.toolSend(lockCtx, req, sendArgs{BodyText: result.OutputText})
					if sendErr != nil {
						if isProviderUnavailable(sendErr) {
							trace = append(trace, map[string]any{
								"type":   "auto_send_skipped",
								"reason": "provider_unavailable",
								"error":  sendErr.Error(),
							})
						} else {
							trace = append(trace, map[string]any{
								"type":  "auto_send_failed",
								"error": sendErr.Error(),
							})
							completeRun(db.AgentRunStatusFailed, resp.ID, previousResponseID, result.OutputText, sendErr.Error())
							return sendErr
						}
					} else if toolSendSucceeded(sendOut) {
						replySent = true
					}
				}

				trace = append(trace, stepTrace)
				result.Enabled = true
				result.Completed = true
				result.ResponseID = resp.ID
				emitEvent(emit, Event{
					Type: "response.completed",
					Data: map[string]any{
						"response_id": resp.ID,
						"output_text": result.OutputText,
					},
				})
				completeRun(db.AgentRunStatusCompleted, resp.ID, previousResponseID, result.OutputText, "")
				return nil
			}

			callOut := make([]map[string]any, 0, len(functionCalls))
			traceCalls := make([]map[string]any, 0, len(functionCalls))
			for _, fc := range functionCalls {
				output, toolErr := s.executeTool(lockCtx, req, fc)
				if toolErr != nil {
					emitEvent(emit, Event{
						Type: "response.failed",
						Data: map[string]any{
							"response_id": resp.ID,
							"error":       toolErr.Error(),
							"function":    fc.Name,
							"call_id":     fc.CallID,
						},
					})
					traceCalls = append(traceCalls, map[string]any{
						"call_id":   fc.CallID,
						"name":      fc.Name,
						"arguments": fc.Arguments,
						"status":    "error",
						"error":     toolErr.Error(),
					})
					stepTrace["function_calls"] = traceCalls
					trace = append(trace, stepTrace)
					completeRun(db.AgentRunStatusFailed, resp.ID, previousResponseID, result.OutputText, toolErr.Error())
					return toolErr
				}
				if fc.Name == "send" && toolSendSucceeded(output) {
					replySent = true
				}
				result.ToolCalls++
				emitEvent(emit, Event{
					Type: "response.function_call.completed",
					Data: map[string]any{
						"response_id": resp.ID,
						"call_id":     fc.CallID,
						"name":        fc.Name,
					},
				})
				traceCalls = append(traceCalls, map[string]any{
					"call_id":   fc.CallID,
					"name":      fc.Name,
					"arguments": fc.Arguments,
					"status":    "ok",
					"output":    output,
				})
				callOut = append(callOut, map[string]any{
					"type":    "function_call_output",
					"call_id": fc.CallID,
					"output":  mustJSON(output),
				})
			}
			stepTrace["function_calls"] = traceCalls
			trace = append(trace, stepTrace)

			if len(callOut) == 0 {
				loopErr := errors.New("no function_call_output generated")
				completeRun(db.AgentRunStatusFailed, resp.ID, previousResponseID, result.OutputText, loopErr.Error())
				return loopErr
			}

			previousResponseID = resp.ID
			input = callOut
		}

		loopErr := fmt.Errorf("max steps reached (%d)", s.cfg.MaxSteps)
		emitEvent(emit, Event{Type: "response.failed", Data: map[string]any{"error": loopErr.Error()}})
		completeRun(db.AgentRunStatusFailed, "", previousResponseID, result.OutputText, loopErr.Error())
		return loopErr
	})
	if err != nil {
		s.logger.Warn("reply processing failed",
			slog.String("conversation_id", req.ConversationID.String()),
			slog.String("message_id", req.MessageID.String()),
			slog.String("source_channel", string(req.SourceChannel)),
			slog.Duration("latency", time.Since(startedAt)),
			slog.String("error", err.Error()),
		)
		return result, err
	}
	if !result.Enabled {
		result.Enabled = true
	}
	s.logger.Info("reply processed",
		slog.String("conversation_id", req.ConversationID.String()),
		slog.String("message_id", req.MessageID.String()),
		slog.String("source_channel", string(req.SourceChannel)),
		slog.Bool("completed", result.Completed),
		slog.Bool("deduped", result.Deduped),
		slog.Bool("reply_sent", replySent),
		slog.Duration("latency", time.Since(startedAt)),
	)
	return result, nil
}

type functionCall struct {
	CallID    string
	Name      string
	Arguments string
}

type sendArgs struct {
	BodyText string `json:"body_text"`
	Subject  string `json:"subject,omitempty"`
}

type updateMemoryArgs struct {
	UserID     string `json:"user_id"`
	MemoryBlob string `json:"memory_blob"`
}

type createIssueArgs struct {
	IssueType  string `json:"issue_type"`
	Title      string `json:"title"`
	Body       string `json:"body"`
	DedupeHash string `json:"dedupe_hash,omitempty"`
}

func (s *Service) executeTool(ctx context.Context, req ProcessRequest, call functionCall) (map[string]any, error) {
	name := strings.TrimSpace(call.Name)
	switch name {
	case "send":
		var args sendArgs
		if err := unmarshalStrict(call.Arguments, &args); err != nil {
			return nil, fmt.Errorf("invalid send args: %w", err)
		}
		out, err := s.toolSend(ctx, req, args)
		if err != nil && isProviderUnavailable(err) {
			return map[string]any{
				"status": "skipped",
				"reason": "provider_unavailable",
			}, nil
		}
		return out, err
	case "update_memory":
		var args updateMemoryArgs
		if err := unmarshalStrict(call.Arguments, &args); err != nil {
			return nil, fmt.Errorf("invalid update_memory args: %w", err)
		}
		return s.toolUpdateMemory(ctx, args)
	case "create_issue":
		var args createIssueArgs
		if err := unmarshalStrict(call.Arguments, &args); err != nil {
			return nil, fmt.Errorf("invalid create_issue args: %w", err)
		}
		return s.toolCreateIssue(ctx, req, args)
	default:
		return nil, fmt.Errorf("unknown function: %s", name)
	}
}

func (s *Service) toolSend(ctx context.Context, req ProcessRequest, args sendArgs) (map[string]any, error) {
	body := strings.TrimSpace(args.BodyText)
	if body == "" {
		return nil, errors.New("body_text is required")
	}
	body, err := s.guardOutboundBody(body)
	if err != nil {
		return nil, err
	}

	channel := req.SourceChannel
	switch channel {
	case ingest.SourceChannelTwilioSMS:
		route, err := s.store.LatestInboundByConversationAndChannel(ctx, req.ConversationID, ingest.SourceChannelTwilioSMS)
		if err != nil {
			return nil, err
		}
		if s.twilioSender == nil {
			return nil, errors.New("twilio sender is unavailable")
		}
		sid, err := s.twilioSender.SendSMS(ctx, strings.TrimSpace(route.SourceIdentity), body)
		if err != nil {
			return nil, err
		}
		if _, err := s.store.RecordOutbound(ctx, ingest.OutboundMessage{
			ConversationID:    req.ConversationID,
			SourceChannel:     ingest.SourceChannelTwilioSMS,
			SourceIdentity:    "assistant",
			SourceContext:     []byte(`{"tool":"send"}`),
			SecurityClass:     ingest.ClassifySecurity(ingest.SourceChannelTwilioSMS),
			BodyText:          body,
			ProviderMessageID: &sid,
		}); err != nil {
			return nil, err
		}
		return map[string]any{"status": "sent", "source_channel": string(channel), "provider_message_id": sid}, nil
	case ingest.SourceChannelTwilioMail:
		route, err := s.store.LatestInboundByConversationAndChannel(ctx, req.ConversationID, ingest.SourceChannelTwilioMail)
		if err != nil {
			return nil, err
		}
		if s.twilioSender == nil {
			return nil, errors.New("twilio sender is unavailable")
		}
		subject := strings.TrimSpace(args.Subject)
		if subject == "" {
			subject = "Re: message"
		}
		if err := s.twilioSender.SendEmail(ctx, strings.TrimSpace(route.SourceIdentity), subject, body); err != nil {
			return nil, err
		}
		if _, err := s.store.RecordOutbound(ctx, ingest.OutboundMessage{
			ConversationID: req.ConversationID,
			SourceChannel:  ingest.SourceChannelTwilioMail,
			SourceIdentity: "assistant",
			SourceContext:  []byte(`{"tool":"send"}`),
			SecurityClass:  ingest.ClassifySecurity(ingest.SourceChannelTwilioMail),
			BodyText:       body,
		}); err != nil {
			return nil, err
		}
		return map[string]any{"status": "sent", "source_channel": string(channel)}, nil
	case ingest.SourceChannelDiscord:
		if s.discordEnqueue == nil {
			return nil, errors.New("discord runtime is unavailable")
		}
		if err := s.discordEnqueue(ctx, req.ConversationID, body); err != nil {
			return nil, err
		}
		return map[string]any{"status": "queued", "source_channel": string(channel)}, nil
	case ingest.SourceChannelWeb:
		if _, err := s.store.RecordOutbound(ctx, ingest.OutboundMessage{
			ConversationID: req.ConversationID,
			SourceChannel:  ingest.SourceChannelWeb,
			SourceIdentity: "assistant",
			SourceContext:  []byte(`{"tool":"send"}`),
			SecurityClass:  ingest.ClassifySecurity(ingest.SourceChannelWeb),
			BodyText:       body,
		}); err != nil {
			return nil, err
		}
		return map[string]any{"status": "stored", "source_channel": string(channel)}, nil
	default:
		return nil, fmt.Errorf("unsupported source_channel: %s", channel)
	}
}

func (s *Service) toolUpdateMemory(ctx context.Context, args updateMemoryArgs) (map[string]any, error) {
	userID, err := uuid.Parse(strings.TrimSpace(args.UserID))
	if err != nil {
		return nil, fmt.Errorf("invalid user_id: %w", err)
	}
	updatedAt, err := s.store.UpsertUserMemory(ctx, userID, args.MemoryBlob)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":     "updated",
		"user_id":    userID.String(),
		"updated_at": updatedAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func (s *Service) toolCreateIssue(ctx context.Context, req ProcessRequest, args createIssueArgs) (map[string]any, error) {
	issueType := strings.TrimSpace(args.IssueType)
	title := strings.TrimSpace(args.Title)
	body := strings.TrimSpace(args.Body)
	if issueType == "" || title == "" || body == "" {
		return nil, errors.New("issue_type, title, and body are required")
	}

	dedupeHash := strings.TrimSpace(args.DedupeHash)
	if dedupeHash == "" {
		dedupeHash = normalizeIssueHash(issueType, title, body)
	}

	out, err := s.store.UpsertAgentIssue(ctx, req.ConversationID, req.SourceChannel, issueType, title, body, dedupeHash)
	if err != nil {
		return nil, err
	}
	status := "updated"
	if out.Created {
		status = "created"
	}
	return map[string]any{
		"status":       status,
		"issue_id":     out.ID.String(),
		"report_count": out.ReportCount,
		"dedupe_hash":  dedupeHash,
	}, nil
}

func buildInitialInput(messages []ingest.StoredMessage, sourceChannel ingest.SourceChannel) []map[string]any {
	var b strings.Builder
	b.WriteString("Channel: ")
	b.WriteString(string(sourceChannel))
	b.WriteString("\nConversation (oldest -> newest):\n")
	for _, m := range messages {
		b.WriteString("- ")
		b.WriteString(string(m.Direction))
		b.WriteString(": ")
		b.WriteString(strings.TrimSpace(m.BodyText))
		b.WriteString("\n")
	}

	return []map[string]any{
		{
			"role": "system",
			"content": []map[string]any{
				{
					"type": "input_text",
					"text": "You are the user-intelligence agent. Use tools when needed. Prefer calling send to reply on non-web channels. Use create_issue for actionable bug reports and update_memory for stable user facts.",
				},
			},
		},
		{
			"role": "user",
			"content": []map[string]any{
				{
					"type": "input_text",
					"text": b.String(),
				},
			},
		},
	}
}

func toolDefinitions() []map[string]any {
	return []map[string]any{
		{
			"type":        "function",
			"name":        "send",
			"description": "Send a reply back in the same source_channel and conversation.",
			"strict":      true,
			"parameters": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"body_text": map[string]any{"type": "string"},
					"subject":   map[string]any{"type": "string"},
				},
				"required": []string{"body_text"},
			},
		},
		{
			"type":        "function",
			"name":        "update_memory",
			"description": "Update the user memory blob for a known user.",
			"strict":      true,
			"parameters": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"user_id":     map[string]any{"type": "string"},
					"memory_blob": map[string]any{"type": "string"},
				},
				"required": []string{"user_id", "memory_blob"},
			},
		},
		{
			"type":        "function",
			"name":        "create_issue",
			"description": "Create or update a deduped issue for human triage.",
			"strict":      true,
			"parameters": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"issue_type":  map[string]any{"type": "string"},
					"title":       map[string]any{"type": "string"},
					"body":        map[string]any{"type": "string"},
					"dedupe_hash": map[string]any{"type": "string"},
				},
				"required": []string{"issue_type", "title", "body"},
			},
		},
	}
}

func extractAssistantText(resp ResponsesResponse) string {
	if strings.TrimSpace(resp.OutputText) != "" {
		return strings.TrimSpace(resp.OutputText)
	}
	var b strings.Builder
	for _, item := range resp.Output {
		if item.Type != "message" {
			continue
		}
		for _, c := range item.Content {
			if strings.EqualFold(strings.TrimSpace(c.Type), "output_text") || strings.EqualFold(strings.TrimSpace(c.Type), "text") {
				txt := strings.TrimSpace(c.Text)
				if txt == "" {
					continue
				}
				if b.Len() > 0 {
					b.WriteString("\n")
				}
				b.WriteString(txt)
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func extractFunctionCalls(items []ResponsesOutput) []functionCall {
	out := make([]functionCall, 0)
	for _, item := range items {
		if strings.TrimSpace(item.Type) != "function_call" {
			continue
		}
		out = append(out, functionCall{
			CallID:    strings.TrimSpace(item.CallID),
			Name:      strings.TrimSpace(item.Name),
			Arguments: item.Arguments,
		})
	}
	return out
}

func outputItemTrace(items []ResponsesOutput) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"id":      strings.TrimSpace(item.ID),
			"type":    strings.TrimSpace(item.Type),
			"call_id": strings.TrimSpace(item.CallID),
			"name":    strings.TrimSpace(item.Name),
		})
	}
	return out
}

func unmarshalStrict(raw string, out any) error {
	dec := json.NewDecoder(strings.NewReader(strings.TrimSpace(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("invalid trailing content")
	}
	return nil
}

func normalizeIssueHash(issueType, title, body string) string {
	payload := strings.ToLower(strings.TrimSpace(issueType)) + "\n" +
		strings.ToLower(strings.TrimSpace(title)) + "\n" +
		strings.ToLower(strings.TrimSpace(body))
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"status":"error","error":"marshal_failed"}`
	}
	return string(b)
}

func emitEvent(emit func(Event), event Event) {
	if emit != nil {
		emit(event)
	}
}

func simplePingPong(
	ctx context.Context,
	s *Service,
	req ProcessRequest,
	messages []ingest.StoredMessage,
	emit func(Event),
	result *ProcessResult,
	trace *[]map[string]any,
	replySent *bool,
) bool {
	if len(messages) == 0 {
		return false
	}
	last := messages[len(messages)-1]
	if last.ID != req.MessageID || last.Direction != ingest.DirectionInbound {
		return false
	}
	if strings.ToLower(strings.TrimSpace(last.BodyText)) != "ping" {
		return false
	}
	_, err := s.toolSend(ctx, req, sendArgs{BodyText: "pong"})
	if err != nil {
		*trace = append(*trace, map[string]any{
			"type":  "local_ping_pong",
			"error": err.Error(),
		})
		return false
	}
	*trace = append(*trace, map[string]any{
		"type": "local_ping_pong",
		"tool": "send",
	})
	if replySent != nil {
		*replySent = true
	}
	result.Enabled = true
	result.Completed = true
	result.OutputText = "pong"
	result.ResponseID = "local:ping-pong"
	emitEvent(emit, Event{
		Type: "response.output_text.delta",
		Data: map[string]any{
			"response_id": result.ResponseID,
			"delta":       "pong",
		},
	})
	emitEvent(emit, Event{
		Type: "response.completed",
		Data: map[string]any{
			"response_id": result.ResponseID,
			"output_text": "pong",
		},
	})
	return true
}

func (s *Service) guardOutboundBody(body string) (string, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return "", errors.New("empty outbound body")
	}
	if s.maxReplyChars > 0 && len(body) > s.maxReplyChars {
		body = body[:s.maxReplyChars]
	}
	return body, nil
}

func (s *Service) eligibleInbound(messages []ingest.StoredMessage, req ProcessRequest) bool {
	if len(messages) == 0 {
		return false
	}
	last := messages[len(messages)-1]
	if last.ID != req.MessageID || last.Direction != ingest.DirectionInbound {
		return false
	}
	body := strings.ToLower(strings.TrimSpace(last.BodyText))
	src := strings.ToLower(strings.TrimSpace(last.SourceIdentity))
	if body == "" {
		return false
	}
	if strings.HasPrefix(body, "[system]") || strings.HasPrefix(body, "[internal]") {
		return false
	}
	if strings.HasPrefix(src, "system:") || strings.HasPrefix(src, "internal:") {
		return false
	}
	return true
}

func toolSendSucceeded(output map[string]any) bool {
	if len(output) == 0 {
		return false
	}
	status, _ := output["status"].(string)
	status = strings.ToLower(strings.TrimSpace(status))
	return status == "sent" || status == "queued" || status == "stored"
}

func isProviderUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "unavailable") || strings.Contains(msg, "unsupported source_channel")
}
