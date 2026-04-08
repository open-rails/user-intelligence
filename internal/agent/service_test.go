package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/open-rails/user-intelligence/internal/db"
	"github.com/open-rails/user-intelligence/internal/ingest"
)

type fakeClient struct {
	responses []ResponsesResponse
	requests  []ResponsesRequest
}

func (f *fakeClient) Create(_ context.Context, req ResponsesRequest) (ResponsesResponse, error) {
	f.requests = append(f.requests, req)
	if len(f.responses) == 0 {
		return ResponsesResponse{}, errors.New("no fake response configured")
	}
	resp := f.responses[0]
	f.responses = f.responses[1:]
	return resp, nil
}

type fakeTwilio struct {
	smsCalls   int
	emailCalls int
	lastTo     string
	lastBody   string
	lastSubj   string
}

func (f *fakeTwilio) SendSMS(_ context.Context, to string, body string) (string, error) {
	f.smsCalls++
	f.lastTo = to
	f.lastBody = body
	return "SM-TEST", nil
}

func (f *fakeTwilio) SendEmail(_ context.Context, to string, subject string, body string) error {
	f.emailCalls++
	f.lastTo = to
	f.lastSubj = subject
	f.lastBody = body
	return nil
}

type fakeStore struct {
	messagesByConversation map[uuid.UUID][]ingest.StoredMessage
	inboundRoutes          map[string]db.InboundMessageRoute
	memory                 map[uuid.UUID]string
	issues                 map[string]int
	runsByInbound          map[uuid.UUID]db.AgentRunStartResult
	outbound               []ingest.OutboundMessage
	lastRunStatus          db.AgentRunStatus
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		messagesByConversation: map[uuid.UUID][]ingest.StoredMessage{},
		inboundRoutes:          map[string]db.InboundMessageRoute{},
		memory:                 map[uuid.UUID]string{},
		issues:                 map[string]int{},
		runsByInbound:          map[uuid.UUID]db.AgentRunStartResult{},
		outbound:               []ingest.OutboundMessage{},
	}
}

func (s *fakeStore) WithConversationLock(ctx context.Context, _ uuid.UUID, fn func(context.Context) error) error {
	return fn(ctx)
}

func (s *fakeStore) StartAgentRun(_ context.Context, _, inboundMessageID uuid.UUID, _ ingest.SourceChannel) (db.AgentRunStartResult, error) {
	if out, ok := s.runsByInbound[inboundMessageID]; ok {
		out.AlreadyStarted = true
		return out, nil
	}
	out := db.AgentRunStartResult{RunID: uuid.New(), Status: db.AgentRunStatusRunning}
	s.runsByInbound[inboundMessageID] = out
	return out, nil
}

func (s *fakeStore) CompleteAgentRun(_ context.Context, _ uuid.UUID, status db.AgentRunStatus, _, _, _ string, _ json.RawMessage, _ string) error {
	s.lastRunStatus = status
	return nil
}

func (s *fakeStore) ListMessagesByConversation(_ context.Context, conversationID uuid.UUID, _ int) ([]ingest.StoredMessage, error) {
	return s.messagesByConversation[conversationID], nil
}

func (s *fakeStore) LatestInboundByConversationAndChannel(_ context.Context, conversationID uuid.UUID, channel ingest.SourceChannel) (db.InboundMessageRoute, error) {
	key := conversationID.String() + "|" + string(channel)
	route, ok := s.inboundRoutes[key]
	if !ok {
		return db.InboundMessageRoute{}, errors.New("route not found")
	}
	return route, nil
}

func (s *fakeStore) RecordOutbound(_ context.Context, msg ingest.OutboundMessage) (ingest.IngestResult, error) {
	s.outbound = append(s.outbound, msg)
	return ingest.IngestResult{ConversationID: msg.ConversationID, MessageID: uuid.New(), CreatedAt: time.Now().UTC()}, nil
}

func (s *fakeStore) UpsertUserMemory(_ context.Context, userID uuid.UUID, memoryBlob string) (time.Time, error) {
	s.memory[userID] = memoryBlob
	return time.Now().UTC(), nil
}

func (s *fakeStore) UpsertAgentIssue(_ context.Context, _ uuid.UUID, _ ingest.SourceChannel, _, _, _, dedupeHash string) (db.AgentIssueUpsertResult, error) {
	if count, ok := s.issues[dedupeHash]; ok {
		s.issues[dedupeHash] = count + 1
		return db.AgentIssueUpsertResult{ID: uuid.New(), Created: false, ReportCount: count + 1}, nil
	}
	s.issues[dedupeHash] = 1
	return db.AgentIssueUpsertResult{ID: uuid.New(), Created: true, ReportCount: 1}, nil
}

func TestProcessSync_FunctionCallChainAndPreviousResponseID(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	userID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "help", SourceChannel: ingest.SourceChannelWeb},
	}

	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c1", Name: "update_memory", Arguments: `{"user_id":"` + userID.String() + `","memory_blob":"likes cats"}`},
				},
			},
			{
				ID: "resp-2",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c2", Name: "create_issue", Arguments: `{"issue_type":"bug","title":"Search fails","body":"Search endpoint returns 500"}`},
				},
			},
			{
				ID: "resp-3",
				Output: []ResponsesOutput{
					{Type: "message", Content: []ResponsesContent{{Type: "output_text", Text: "Thanks, logged and tracked."}}},
				},
			},
		},
	}
	svc, err := NewService(store, client, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 5})
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Completed || result.ToolCalls != 2 {
		t.Fatalf("expected completed with 2 tool calls, got completed=%v tool_calls=%d", result.Completed, result.ToolCalls)
	}
	if strings.TrimSpace(result.OutputText) == "" {
		t.Fatalf("expected non-empty output text")
	}
	if client.requests[1].PreviousResponseID != "resp-1" {
		t.Fatalf("expected previous_response_id chaining on 2nd call")
	}
	if client.requests[2].PreviousResponseID != "resp-2" {
		t.Fatalf("expected previous_response_id chaining on 3rd call")
	}
	if got := store.memory[userID]; got != "likes cats" {
		t.Fatalf("expected memory update, got=%q", got)
	}
	if len(store.issues) != 1 {
		t.Fatalf("expected issue upsert")
	}
}

func TestProcessSync_InvalidArgumentsFailClosed(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "hello", SourceChannel: ingest.SourceChannelWeb},
	}
	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c1", Name: "send", Arguments: `{"bad":"field"}`},
				},
			},
		},
	}

	svc, err := NewService(store, client, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil)
	if err == nil {
		t.Fatalf("expected invalid args failure")
	}
	if store.lastRunStatus != db.AgentRunStatusFailed {
		t.Fatalf("expected failed run status, got=%s", store.lastRunStatus)
	}
}

func TestProcessSync_LoopCapTermination(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "hello", SourceChannel: ingest.SourceChannelWeb},
	}
	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c1", Name: "send", Arguments: `{"body_text":"one"}`},
				},
			},
			{
				ID: "resp-2",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c2", Name: "send", Arguments: `{"body_text":"two"}`},
				},
			},
		},
	}
	svc, err := NewService(store, client, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "max steps") {
		t.Fatalf("expected loop-cap error, got=%v", err)
	}
}

func TestProcessSync_UnknownFunctionFails(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "hello", SourceChannel: ingest.SourceChannelWeb},
	}
	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c1", Name: "not_a_tool", Arguments: `{}`},
				},
			},
		},
	}
	svc, err := NewService(store, client, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "unknown function") {
		t.Fatalf("expected unknown function failure, got=%v", err)
	}
}

func TestProcessSync_IssueDedupAcrossMessages(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	msgA := uuid.New()
	msgB := uuid.New()

	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-a-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "a1", Name: "create_issue", Arguments: `{"issue_type":"bug","title":"A","body":"B","dedupe_hash":"same-hash"}`},
				},
			},
			{ID: "resp-a-2", Output: []ResponsesOutput{{Type: "message", Content: []ResponsesContent{{Type: "output_text", Text: "ok"}}}}},
			{
				ID: "resp-b-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "b1", Name: "create_issue", Arguments: `{"issue_type":"bug","title":"A","body":"B","dedupe_hash":"same-hash"}`},
				},
			},
			{ID: "resp-b-2", Output: []ResponsesOutput{{Type: "message", Content: []ResponsesContent{{Type: "output_text", Text: "ok"}}}}},
		},
	}
	svc, err := NewService(store, client, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}

	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: msgA, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "report 1", SourceChannel: ingest.SourceChannelWeb},
	}
	if _, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      msgA,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil); err != nil {
		t.Fatal(err)
	}

	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: msgB, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "report 2", SourceChannel: ingest.SourceChannelWeb},
	}
	if _, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      msgB,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil); err != nil {
		t.Fatal(err)
	}

	if store.issues["same-hash"] != 2 {
		t.Fatalf("expected deduped issue count=2, got=%d", store.issues["same-hash"])
	}
}

func TestProcessSync_PingPongLocalPath(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "PING", SourceChannel: ingest.SourceChannelWeb},
	}
	client := &fakeClient{}
	svc, err := NewService(store, client, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputText != "pong" {
		t.Fatalf("expected pong, got=%q", result.OutputText)
	}
	if len(client.requests) != 0 {
		t.Fatalf("expected no Responses API calls for local ping/pong")
	}
	if len(store.outbound) != 1 || store.outbound[0].BodyText != "pong" {
		t.Fatalf("expected one outbound pong record")
	}
}

func TestProcessSync_SendSMSPath(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "hello", SourceChannel: ingest.SourceChannelTwilioSMS},
	}
	store.inboundRoutes[conversationID.String()+"|"+string(ingest.SourceChannelTwilioSMS)] = db.InboundMessageRoute{
		SourceIdentity: "+15550000001",
	}
	twilioSender := &fakeTwilio{}
	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c1", Name: "send", Arguments: `{"body_text":"hello back"}`},
				},
			},
			{
				ID: "resp-2",
				Output: []ResponsesOutput{
					{Type: "message", Content: []ResponsesContent{{Type: "output_text", Text: "sent"}}},
				},
			},
		},
	}
	svc, err := NewService(store, client, twilioSender, nil, nil, Config{Model: "gpt-5.4", MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelTwilioSMS,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if twilioSender.smsCalls != 1 || twilioSender.lastTo != "+15550000001" {
		t.Fatalf("expected sms send call to route identity")
	}
	if len(store.outbound) != 1 || store.outbound[0].SourceChannel != ingest.SourceChannelTwilioSMS {
		t.Fatalf("expected outbound persisted for twilio sms")
	}
}

func TestServiceEnabledRequiresClient(t *testing.T) {
	store := newFakeStore()
	svc, err := NewService(store, nil, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4"})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Enabled() {
		t.Fatalf("expected service disabled when client is nil")
	}
}

func TestProcessSync_PingPongTwilioEmailPath(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "ping", SourceChannel: ingest.SourceChannelTwilioMail, SourceIdentity: "user@example.com"},
	}
	store.inboundRoutes[conversationID.String()+"|"+string(ingest.SourceChannelTwilioMail)] = db.InboundMessageRoute{
		SourceIdentity: "user@example.com",
	}
	twilioSender := &fakeTwilio{}
	svc, err := NewService(store, &fakeClient{}, twilioSender, nil, nil, Config{Model: "gpt-5.4"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelTwilioMail,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputText != "pong" {
		t.Fatalf("expected pong, got=%q", result.OutputText)
	}
	if twilioSender.emailCalls != 1 || twilioSender.lastTo != "user@example.com" {
		t.Fatalf("expected email send call")
	}
	if len(store.outbound) != 1 || store.outbound[0].ConversationID != conversationID {
		t.Fatalf("expected single outbound persisted in same conversation")
	}
}

func TestProcessSync_PingPongDiscordPath(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "ping", SourceChannel: ingest.SourceChannelDiscord, SourceIdentity: "discord-user"},
	}
	var enqueuedConversation uuid.UUID
	var enqueuedBody string
	svc, err := NewService(store, &fakeClient{}, &fakeTwilio{}, func(_ context.Context, cid uuid.UUID, body string) error {
		enqueuedConversation = cid
		enqueuedBody = body
		return nil
	}, nil, Config{Model: "gpt-5.4"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelDiscord,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputText != "pong" {
		t.Fatalf("expected pong response")
	}
	if enqueuedConversation != conversationID || enqueuedBody != "pong" {
		t.Fatalf("expected discord enqueue on same conversation")
	}
}

func TestProcessSync_SingleReplyIdempotency(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "ping", SourceChannel: ingest.SourceChannelTwilioSMS, SourceIdentity: "+15550000001"},
	}
	store.inboundRoutes[conversationID.String()+"|"+string(ingest.SourceChannelTwilioSMS)] = db.InboundMessageRoute{
		SourceIdentity: "+15550000001",
	}
	twilioSender := &fakeTwilio{}
	svc, err := NewService(store, &fakeClient{}, twilioSender, nil, nil, Config{Model: "gpt-5.4"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelTwilioSMS,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelTwilioSMS,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if twilioSender.smsCalls != 1 {
		t.Fatalf("expected exactly one outbound send due idempotency, got=%d", twilioSender.smsCalls)
	}
}

func TestProcessSync_EligibilityFilterSkipsSystemMessages(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "[system] sync", SourceChannel: ingest.SourceChannelWeb, SourceIdentity: "system:worker"},
	}
	svc, err := NewService(store, &fakeClient{}, &fakeTwilio{}, nil, nil, Config{Model: "gpt-5.4"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelWeb,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed {
		t.Fatalf("expected system message skip")
	}
}

func TestProcessSync_SendToolProviderUnavailableNoOp(t *testing.T) {
	store := newFakeStore()
	conversationID := uuid.New()
	messageID := uuid.New()
	store.messagesByConversation[conversationID] = []ingest.StoredMessage{
		{ID: messageID, ConversationID: conversationID, Direction: ingest.DirectionInbound, BodyText: "hello", SourceChannel: ingest.SourceChannelTwilioSMS, SourceIdentity: "+15550000001"},
	}
	store.inboundRoutes[conversationID.String()+"|"+string(ingest.SourceChannelTwilioSMS)] = db.InboundMessageRoute{
		SourceIdentity: "+15550000001",
	}
	client := &fakeClient{
		responses: []ResponsesResponse{
			{
				ID: "resp-1",
				Output: []ResponsesOutput{
					{Type: "function_call", CallID: "c1", Name: "send", Arguments: `{"body_text":"hello back"}`},
				},
			},
			{
				ID: "resp-2",
				Output: []ResponsesOutput{
					{Type: "message", Content: []ResponsesContent{{Type: "output_text", Text: "ok"}}},
				},
			},
		},
	}
	svc, err := NewService(store, client, nil, nil, nil, Config{Model: "gpt-5.4"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.ProcessSync(context.Background(), ProcessRequest{
		ConversationID: conversationID,
		MessageID:      messageID,
		SourceChannel:  ingest.SourceChannelTwilioSMS,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Completed {
		t.Fatalf("expected completion with provider-unavailable no-op")
	}
}
