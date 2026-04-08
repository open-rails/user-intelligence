package httpapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	agentpkg "github.com/open-rails/user-intelligence/internal/agent"
	"github.com/open-rails/user-intelligence/internal/ingest"
	"github.com/open-rails/user-intelligence/internal/verify"
)

type fakeReceiver struct {
	last ingest.InboundMessage
}

func (f *fakeReceiver) Receive(_ context.Context, msg ingest.InboundMessage) (ingest.IngestResult, error) {
	f.last = msg
	return ingest.IngestResult{ConversationID: uuid.New(), MessageID: uuid.New()}, nil
}

type fakeResolver struct{}

func (fakeResolver) UserFromContext(_ context.Context) (*uuid.UUID, error) {
	id := uuid.New()
	return &id, nil
}

func (fakeResolver) ResolveBySourceIdentity(_ context.Context, _ ingest.SourceChannel, _ string) (*uuid.UUID, error) {
	return nil, nil
}

type anonymousResolver struct{}

func (anonymousResolver) UserFromContext(_ context.Context) (*uuid.UUID, error) {
	return nil, nil
}

func (anonymousResolver) ResolveBySourceIdentity(_ context.Context, _ ingest.SourceChannel, _ string) (*uuid.UUID, error) {
	return nil, nil
}

type fakeSMSNorm struct{}

func (fakeSMSNorm) Normalize(_ context.Context, _ *http.Request) (ingest.InboundMessage, error) {
	return ingest.InboundMessage{SourceChannel: ingest.SourceChannelTwilioSMS, ConversationKey: "sms:a:b", BodyText: "ping", SourceIdentity: "+1", SecurityClass: ingest.SecurityClassInsecure}, nil
}

type fakeEmailNorm struct{}

func (fakeEmailNorm) Normalize(_ context.Context, _ *http.Request) (ingest.InboundMessage, error) {
	return ingest.InboundMessage{SourceChannel: ingest.SourceChannelTwilioMail, ConversationKey: "email:a:b", BodyText: "ping", SourceIdentity: "a@example.com", SecurityClass: ingest.SecurityClassInsecure}, nil
}

type failVerifier struct{}

func (failVerifier) Verify(_ *http.Request) error { return errors.New("bad sig") }

type passVerifier struct{}

func (passVerifier) Verify(_ *http.Request) error { return nil }

type fakeAgent struct {
	enabled      bool
	syncCalls    int
	asyncCalls   int
	syncErr      error
	syncResult   agentpkg.ProcessResult
	syncEvents   []agentpkg.Event
	lastSyncReq  agentpkg.ProcessRequest
	lastAsyncReq agentpkg.ProcessRequest
}

func (f *fakeAgent) Enabled() bool { return f.enabled }

func (f *fakeAgent) ProcessSync(_ context.Context, req agentpkg.ProcessRequest, emit func(agentpkg.Event)) (agentpkg.ProcessResult, error) {
	f.syncCalls++
	f.lastSyncReq = req
	for _, evt := range f.syncEvents {
		if emit != nil {
			emit(evt)
		}
	}
	return f.syncResult, f.syncErr
}

func (f *fakeAgent) ProcessAsync(_ context.Context, req agentpkg.ProcessRequest) {
	f.asyncCalls++
	f.lastAsyncReq = req
}

func TestWebHandlerRecordsMessage(t *testing.T) {
	receiver := &fakeReceiver{}
	h, err := New(Dependencies{
		Receiver:        receiver,
		SMSNormalizer:   fakeSMSNorm{},
		EmailNormalizer: fakeEmailNorm{},
		TwilioVerifier:  passVerifier{},
		UserResolver:    fakeResolver{},
		MaxBodyBytes:    1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/messages", bytes.NewBufferString(`{"text":"hello","conversation_key":"web:test"}`))
	rr := httptest.NewRecorder()

	h.Web().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if receiver.last.SecurityClass != ingest.SecurityClassSecure {
		t.Fatalf("expected secure class, got %s", receiver.last.SecurityClass)
	}
}

func TestWebHandlerAllowsAnonymous(t *testing.T) {
	receiver := &fakeReceiver{}
	h, err := New(Dependencies{
		Receiver:        receiver,
		SMSNormalizer:   fakeSMSNorm{},
		EmailNormalizer: fakeEmailNorm{},
		TwilioVerifier:  passVerifier{},
		UserResolver:    anonymousResolver{},
		MaxBodyBytes:    1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/messages", bytes.NewBufferString(`{"text":"hello","conversation_key":"web:test-anon"}`))
	rr := httptest.NewRecorder()
	h.Web().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if receiver.last.UserID != nil {
		t.Fatalf("expected anonymous user, got=%s", *receiver.last.UserID)
	}
}

func TestWebHandler_DefaultConversationKeyDeterministic(t *testing.T) {
	receiver := &fakeReceiver{}
	h, err := New(Dependencies{
		Receiver:        receiver,
		SMSNormalizer:   fakeSMSNorm{},
		EmailNormalizer: fakeEmailNorm{},
		TwilioVerifier:  passVerifier{},
		UserResolver:    anonymousResolver{},
		MaxBodyBytes:    1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/messages", bytes.NewBufferString(`{"text":"hello"}`))
	req.RemoteAddr = "203.0.113.10:54123"
	rr := httptest.NewRecorder()
	h.Web().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if receiver.last.SourceIdentity != "203.0.113.10" {
		t.Fatalf("expected stable source identity without port, got=%s", receiver.last.SourceIdentity)
	}
	if receiver.last.ConversationKey != "web:203.0.113.10" {
		t.Fatalf("expected deterministic web conversation key, got=%s", receiver.last.ConversationKey)
	}
}

func TestTwilioSMSRejectsBadSignature(t *testing.T) {
	receiver := &fakeReceiver{}
	h, err := New(Dependencies{
		Receiver:        receiver,
		SMSNormalizer:   fakeSMSNorm{},
		EmailNormalizer: fakeEmailNorm{},
		TwilioVerifier:  failVerifier{},
		UserResolver:    fakeResolver{},
		MaxBodyBytes:    1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/messages/sms", nil)
	rr := httptest.NewRecorder()
	h.TwilioSMS().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 got=%d", rr.Code)
	}
}

func TestWebHandlerSSEStreamsAgentEvents(t *testing.T) {
	receiver := &fakeReceiver{}
	agent := &fakeAgent{
		enabled: true,
		syncResult: agentpkg.ProcessResult{
			Enabled:    true,
			Completed:  true,
			OutputText: "pong",
		},
		syncEvents: []agentpkg.Event{
			{Type: "response.output_text.delta", Data: map[string]any{"delta": "pong"}},
			{Type: "response.completed", Data: map[string]any{"output_text": "pong"}},
		},
	}
	h, err := New(Dependencies{
		Receiver:        receiver,
		SMSNormalizer:   fakeSMSNorm{},
		EmailNormalizer: fakeEmailNorm{},
		TwilioVerifier:  passVerifier{},
		Agent:           agent,
		UserResolver:    fakeResolver{},
		MaxBodyBytes:    1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/messages", bytes.NewBufferString(`{"text":"ping","conversation_key":"web:test-sse"}`))
	req.Header.Set("Accept", "text/event-stream")
	rr := httptest.NewRecorder()
	h.Web().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "event: response.output_text.delta") {
		t.Fatalf("expected output_text delta SSE event, got=%s", body)
	}
	if !strings.Contains(body, "event: response.completed") {
		t.Fatalf("expected completed SSE event, got=%s", body)
	}
	if !strings.Contains(body, "event: done") {
		t.Fatalf("expected done SSE event, got=%s", body)
	}
	if agent.syncCalls != 1 {
		t.Fatalf("expected one agent sync call, got=%d", agent.syncCalls)
	}
}

func TestTwilioSMSHandlerTriggersAsyncAgent(t *testing.T) {
	receiver := &fakeReceiver{}
	agent := &fakeAgent{enabled: true}
	h, err := New(Dependencies{
		Receiver:        receiver,
		SMSNormalizer:   fakeSMSNorm{},
		EmailNormalizer: fakeEmailNorm{},
		TwilioVerifier:  passVerifier{},
		Agent:           agent,
		UserResolver:    fakeResolver{},
		MaxBodyBytes:    1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/messages/sms", nil)
	rr := httptest.NewRecorder()
	h.TwilioSMS().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 got=%d", rr.Code)
	}
	if agent.asyncCalls != 1 {
		t.Fatalf("expected one async agent call, got=%d", agent.asyncCalls)
	}
}

func TestVerifierInterfaceFromInternalVerifyPackage(t *testing.T) {
	var _ verify.Verifier = passVerifier{}
}
