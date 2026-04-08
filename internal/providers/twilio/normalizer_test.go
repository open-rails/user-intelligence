package twilio

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

func TestSMSNormalizer(t *testing.T) {
	form := url.Values{}
	form.Set("From", "+15550001111")
	form.Set("To", "+15550002222")
	form.Set("Body", "hello")
	form.Set("MessageSid", "SM123")

	req := httptest.NewRequest("POST", "/messages/sms", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	msg, err := SMSNormalizer{}.Normalize(context.Background(), req)
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if msg.SourceChannel != ingest.SourceChannelTwilioSMS {
		t.Fatalf("unexpected channel: %s", msg.SourceChannel)
	}
	if msg.ConversationKey == "" || msg.SourceIdentity == "" {
		t.Fatalf("expected conversation key and source identity")
	}
	if msg.ProviderMessageID == nil || *msg.ProviderMessageID != "SM123" {
		t.Fatalf("expected provider message id")
	}
}

func TestEmailNormalizer_AttachmentMetadata(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("from", "user@example.com")
	_ = writer.WriteField("to", "support@example.com")
	_ = writer.WriteField("subject", "help")
	_ = writer.WriteField("text", "hello there")
	_ = writer.WriteField("MessageSid", "EM123")

	part, err := writer.CreateFormFile("attachment1", "report.txt")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	_, _ = part.Write([]byte("abc"))
	_ = writer.Close()

	req := httptest.NewRequest("POST", "/messages/email", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	msg, err := EmailNormalizer{}.Normalize(context.Background(), req)
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if msg.SourceChannel != ingest.SourceChannelTwilioMail {
		t.Fatalf("unexpected channel: %s", msg.SourceChannel)
	}
	if !strings.Contains(string(msg.SourceContext), "attachments") {
		t.Fatalf("expected attachment metadata in source_context")
	}
}
