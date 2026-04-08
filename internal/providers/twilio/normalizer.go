package twilio

import (
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

type SMSNormalizer struct{}

func (SMSNormalizer) Normalize(_ context.Context, r *http.Request) (ingest.InboundMessage, error) {
	if err := r.ParseForm(); err != nil {
		return ingest.InboundMessage{}, fmt.Errorf("parse sms form: %w", err)
	}

	from := normalizePhone(r.FormValue("From"))
	to := normalizePhone(r.FormValue("To"))
	body := strings.TrimSpace(r.FormValue("Body"))
	providerID := strings.TrimSpace(r.FormValue("MessageSid"))

	if from == "" || to == "" {
		return ingest.InboundMessage{}, fmt.Errorf("sms requires From and To")
	}
	if body == "" {
		return ingest.InboundMessage{}, fmt.Errorf("sms body is empty")
	}

	ctxJSON, _ := json.Marshal(map[string]any{
		"from": from,
		"to":   to,
	})

	msg := ingest.InboundMessage{
		SourceChannel:   ingest.SourceChannelTwilioSMS,
		ConversationKey: "sms:" + from + ":" + to,
		BodyText:        body,
		SourceIdentity:  from,
		SourceContext:   ctxJSON,
		SecurityClass:   ingest.ClassifySecurity(ingest.SourceChannelTwilioSMS),
	}
	if providerID != "" {
		msg.ProviderMessageID = &providerID
	}
	return msg, nil
}

type EmailNormalizer struct{}

func (EmailNormalizer) Normalize(_ context.Context, r *http.Request) (ingest.InboundMessage, error) {
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		_ = r.ParseForm()
	}

	from := normalizeEmail(r.FormValue("from"))
	to := normalizeEmail(r.FormValue("to"))
	if from == "" {
		from = normalizeEmail(r.FormValue("From"))
	}
	if to == "" {
		to = normalizeEmail(r.FormValue("To"))
	}

	body := strings.TrimSpace(firstNonEmpty(
		r.FormValue("text"),
		r.FormValue("TextBody"),
		r.FormValue("stripped-text"),
	))
	if body == "" {
		body = strings.TrimSpace(r.FormValue("subject"))
	}

	providerID := strings.TrimSpace(firstNonEmpty(
		r.FormValue("MessageSid"),
		r.FormValue("Message-Id"),
		r.FormValue("message_id"),
	))
	inReplyTo := strings.TrimSpace(firstNonEmpty(r.FormValue("In-Reply-To"), r.FormValue("in_reply_to")))
	references := strings.TrimSpace(r.FormValue("References"))
	attachments := extractAttachments(r.MultipartForm)

	if from == "" || to == "" {
		return ingest.InboundMessage{}, fmt.Errorf("email requires from and to")
	}
	if body == "" {
		return ingest.InboundMessage{}, fmt.Errorf("email body is empty")
	}

	threadKey := firstNonEmpty(inReplyTo, references)
	if threadKey == "" {
		threadKey = from + ":" + to
	}

	ctxJSON, _ := json.Marshal(map[string]any{
		"from":        from,
		"to":          to,
		"subject":     strings.TrimSpace(r.FormValue("subject")),
		"in_reply_to": inReplyTo,
		"references":  references,
		"message_id":  providerID,
		"attachments": attachments,
	})

	msg := ingest.InboundMessage{
		SourceChannel:   ingest.SourceChannelTwilioMail,
		ConversationKey: "email:" + threadKey,
		BodyText:        body,
		SourceIdentity:  from,
		SourceContext:   ctxJSON,
		SecurityClass:   ingest.ClassifySecurity(ingest.SourceChannelTwilioMail),
	}
	if providerID != "" {
		msg.ProviderMessageID = &providerID
	}
	return msg, nil
}

func normalizePhone(s string) string {
	return strings.TrimSpace(s)
}

func normalizeEmail(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func extractAttachments(form *multipart.Form) []map[string]any {
	if form == nil || len(form.File) == 0 {
		return nil
	}
	attachments := make([]map[string]any, 0, len(form.File))
	for field, files := range form.File {
		for _, fh := range files {
			if fh == nil {
				continue
			}
			attachments = append(attachments, map[string]any{
				"field":        field,
				"filename":     strings.TrimSpace(fh.Filename),
				"size_bytes":   fh.Size,
				"content_type": strings.TrimSpace(fh.Header.Get("Content-Type")),
			})
		}
	}
	return attachments
}
